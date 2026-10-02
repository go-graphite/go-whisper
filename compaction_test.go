package whisper_test

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

type compactionFixture struct {
	path          string
	main, sidecar []byte
	from, until   int
	want          []float64
}

func newCompactionFixture(tb testing.TB) compactionFixture {
	return newCompactionFixtureWithLateStart(tb, 0)
}

func newCompactionFixtureWithLateStart(tb testing.TB, lateStart int) compactionFixture {
	tb.Helper()
	const count = 32768
	retention := whisper.NewRetention(1, 131071)
	path := filepath.Join(tb.TempDir(), "compaction.wsp")
	w, err := whisper.CreateWithOptions(path, whisper.Retentions{&retention}, whisper.Last, 0, &whisper.Options{
		Compressed: true, PointsPerBlock: 1024, OutOfOrder: true,
	})
	if err != nil {
		tb.Fatal(err)
	}
	defer w.Close()

	base := int(time.Now().Unix()) - 65536
	f := compactionFixture{path: path, from: base - 1, until: base + count - 1, want: make([]float64, count)}
	var onTime, late []*whisper.TimeSeriesPoint
	for i := range f.want {
		value := float64((i*7919)%65521) / 7
		f.want[i] = value
		p := &whisper.TimeSeriesPoint{Time: base + i, Value: value}
		if i >= lateStart && i%97 == 1 {
			late = append(late, p)
		} else {
			onTime = append(onTime, p)
		}
	}
	// A later raw-sample correction must survive reads and compaction.
	late = append(late, &whisper.TimeSeriesPoint{Time: base + lateStart + 2, Value: -1})
	f.want[lateStart+2] = -1
	if err := w.UpdateMany(onTime); err != nil {
		tb.Fatal(err)
	}
	if err := w.UpdateMany(late); err != nil {
		tb.Fatal(err)
	}
	if w.OutOfOrderPoints != uint32(len(late)) {
		tb.Fatalf("diverted %d points, want %d", w.OutOfOrderPoints, len(late))
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	f.main, err = os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	f.sidecar, err = os.ReadFile(whisper.OutOfOrderSidecarPath(path))
	if err != nil {
		tb.Fatal(err)
	}
	return f
}

func (f compactionFixture) restore(tb testing.TB) *whisper.Whisper {
	tb.Helper()
	for path, data := range map[string][]byte{f.path: f.main, whisper.OutOfOrderSidecarPath(f.path): f.sidecar} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			tb.Fatal(err)
		}
	}
	w, err := whisper.OpenWithOptions(f.path, &whisper.Options{OutOfOrder: true})
	if err != nil {
		tb.Fatal(err)
	}
	return w
}

func (f compactionFixture) check(tb testing.TB, w *whisper.Whisper) {
	tb.Helper()
	ts, err := w.Fetch(f.from, f.until)
	if err != nil {
		tb.Fatal(err)
	}
	if ts.Step() != 1 || len(ts.Values()) != len(f.want) {
		tb.Fatalf("step=%d points=%d, want step=1 points=%d", ts.Step(), len(ts.Values()), len(f.want))
	}
	for i, got := range ts.Values() {
		if math.Float64bits(got) != math.Float64bits(f.want[i]) {
			tb.Fatalf("point %d = %v, want %v", i, got, f.want[i])
		}
	}
}

func (f compactionFixture) checkReopened(tb testing.TB, w *whisper.Whisper) {
	tb.Helper()
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	if _, err := os.Stat(whisper.OutOfOrderSidecarPath(f.path)); !os.IsNotExist(err) {
		tb.Fatalf("sidecar remains after compaction: %v", err)
	}
	w, err := whisper.OpenWithOptions(f.path, &whisper.Options{})
	if err != nil {
		tb.Fatal(err)
	}
	defer w.Close()
	f.check(tb, w)
}

func TestWhisperCompactionPreservesValues(t *testing.T) {
	f := newCompactionFixture(t)
	w := f.restore(t)
	f.check(t, w)
	if err := w.MergeOutOfOrder(); err != nil {
		w.Close()
		t.Fatal(err)
	}
	f.checkReopened(t, w)
}

func TestWhisperCompactionTruncatedSidecar(t *testing.T) {
	f := newCompactionFixture(t)
	w := f.restore(t)
	defer w.Close()
	if err := os.Truncate(whisper.OutOfOrderSidecarPath(f.path), int64(len(f.sidecar)-1)); err != nil {
		t.Fatal(err)
	}
	if err := w.MergeOutOfOrder(); err == nil {
		t.Fatal("compaction accepted a truncated sidecar")
	}
	main, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(main, f.main) {
		t.Fatal("failed compaction changed the main file")
	}
}

func BenchmarkWhisperCompaction(b *testing.B) {
	benchmarkWhisperCompaction(b, 0)
}

func BenchmarkWhisperCompactionRecent(b *testing.B) {
	benchmarkWhisperCompaction(b, 32000)
}

func benchmarkWhisperCompaction(b *testing.B, lateStart int) {
	b.StopTimer()
	f := newCompactionFixtureWithLateStart(b, lateStart)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := f.restore(b)
		b.StartTimer()
		err := w.MergeOutOfOrder()
		b.StopTimer()
		if err != nil {
			w.Close()
			b.Fatal(err)
		}
		// Validate every result, excluding setup and verification from timing.
		f.checkReopened(b, w)
	}
}
