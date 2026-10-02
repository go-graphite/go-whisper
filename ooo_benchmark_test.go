package whisper_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

// Use the retention from daily state publishers, with ten days of minute data.
// Corrections land in already encoded blocks, outside the active write buffer.
func historicalBenchmarkFixture(tb testing.TB, stride int) (*whisper.Whisper, string, int) {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "historical.wsp")
	w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1m:14d,30m:2y"), whisper.Average, 0,
		&whisper.Options{Compressed: true, OutOfOrder: true, PointsPerBlock: 1024})
	if err != nil {
		tb.Fatal(err)
	}
	base := int(time.Now().Truncate(30 * time.Minute).Add(-10 * 24 * time.Hour).Unix())
	points := make([]*whisper.TimeSeriesPoint, 10*24*60)
	for i := range points {
		points[i] = &whisper.TimeSeriesPoint{Time: base + i*60, Value: 1}
	}
	if err := w.UpdateMany(points); err != nil {
		tb.Fatal(err)
	}
	if stride > 0 {
		var late []*whisper.TimeSeriesPoint
		for i := 2; i < len(points)-120; i += stride {
			late = append(late, &whisper.TimeSeriesPoint{Time: base + i*60, Value: 3})
		}
		if err := w.UpdateMany(late); err != nil {
			tb.Fatal(err)
		}
		if w.OutOfOrderPoints == 0 {
			tb.Fatal("fixture did not exercise encoded out-of-order writes")
		}
	}
	return w, path, base
}

func checkHistoricalBenchmarkCorrection(tb testing.TB, w *whisper.Whisper, base int) {
	tb.Helper()
	ts, err := w.Fetch(base+60, base+120)
	if err != nil {
		tb.Fatal(err)
	}
	const want = 3.0
	if len(ts.Values()) != 1 || ts.Values()[0] != want {
		tb.Fatalf("corrected sample = %v, want %v", ts.Values(), want)
	}
}

func BenchmarkHistoricalFetch(b *testing.B) {
	for _, tc := range []struct {
		name   string
		stride int
		coarse bool
	}{
		{"NoSidecar", 0, false},
		{"SparseSidecar", 97, false},
		{"DenseSidecar", 1, false},
		{"CoarseSidecar", 97, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			w, _, base := historicalBenchmarkFixture(b, tc.stride)
			defer w.Close()
			if tc.stride > 0 {
				checkHistoricalBenchmarkCorrection(b, w, base)
			}
			from, until := base-60, base+10*86400-60
			if tc.coarse {
				from = base - 5*86400
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := w.Fetch(from, until); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkHistoricalCompactionRollups(b *testing.B) {
	b.StopTimer()
	w, path, base := historicalBenchmarkFixture(b, 97)
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	main, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	sidecar, err := os.ReadFile(whisper.OutOfOrderSidecarPath(path))
	if err != nil {
		b.Fatal(err)
	}
	f := compactionFixture{path: path, main: main, sidecar: sidecar}
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
		checkHistoricalBenchmarkCorrection(b, w, base)
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
