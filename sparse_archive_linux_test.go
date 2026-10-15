package whisper

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSparseArchiveReaderAgainstReference(t *testing.T) {
	const count = 100003
	path := filepath.Join(t.TempDir(), "sparse.wsp")
	w, err := CreateWithOptions(path, Retentions{&Retention{secondsPerPoint: 1, numberOfPoints: count}}, Sum, 0, &Options{Sparse: true})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	a := w.archives[0]
	if newArchiveExtentReader(w.file, a.End(), a.Size()) == nil {
		t.Skip("filesystem does not report sparse allocation")
	}
	const base = 1700000000
	// Include records on both sides of each block boundary and at the circular
	// wrap. Values include NaN payloads, signed zero and infinities.
	positions := []int{0, 1, count - 1, count - 2, 20000, 40000, 80000}
	for _, block := range []int{4096, 8192, 65536, 524288, 1048576} {
		slot := (block - a.offset) / PointSize
		positions = append(positions, slot-1, slot, slot+1)
	}
	bits := []uint64{0, 1 << 63, 0x7ff800000000002a, 0x7ff0000000000000, 0xfff0000000000000, 0x3ff0000000000000}
	for i, slot := range positions {
		b := make([]byte, PointSize)
		packInt(b, base+slot, 0)
		packFloat64(b, math.Float64frombits(bits[i%len(bits)]), IntSize)
		if _, err := w.file.WriteAt(b, a.Offset()+int64(slot*PointSize)); err != nil {
			t.Fatal(err)
		}
	}
	rng := rand.New(rand.NewSource(87423))
	windows := [][2]int{{-1, math.MaxInt32}, {base, base + count - 1}, {base + count - 100, base + count + 50000}, {base - count/2, base + count/2}, {base + 1, base}, {base + count + 20, base + count + 20000}}
	for i := 0; i < 250; i++ {
		start := base - count + rng.Intn(count*3)
		windows = append(windows, [2]int{start, start + rng.Intn(count*2)})
	}
	for _, window := range windows {
		if _, err := w.file.Seek(113, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		got, err := readArchivePointsInRange(w, 0, window[0], window[1])
		if position, seekErr := w.file.Seek(0, io.SeekCurrent); seekErr != nil || position != 113 {
			t.Fatalf("file position changed: %d, %v", position, seekErr)
		}
		want, werr := referenceArchivePointsInRange(w, 0, window[0], window[1])
		if (err == nil) != (werr == nil) {
			t.Fatalf("window %v: err %v, reference %v", window, err, werr)
		}
		assertArchivePointBits(t, got, want)
	}
	// A short file must produce the original read error, rather than an empty
	// sparse tail. The constructor disables the fast path for incomplete files.
	if err := w.file.Truncate(a.End() - PointSize); err != nil {
		t.Fatal(err)
	}
	_, err = readArchivePointsInRange(w, 0, -1, math.MaxInt32)
	if err == nil {
		t.Fatal("truncation silently accepted")
	}
}

// This is opt-in: production snapshots stay outside the source tree. It reads
// a locked copy only and compares all archives and random circular windows.
func TestSparseArchiveCapturedFile(t *testing.T) {
	path := os.Getenv("WHISPER_SPARSE_FIXTURE")
	if path == "" {
		t.Skip("no captured fixture supplied")
	}
	readOnly := os.O_RDONLY
	w, err := OpenWithOptions(path, &Options{OpenFileFlag: &readOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	rng := rand.New(rand.NewSource(45123))
	for index, a := range w.archives {
		extents := newArchiveExtentReader(w.file, a.End(), a.Size())
		if a.Size() >= 256<<10 && extents == nil {
			t.Fatalf("archive %d sparse fast path not selected", index)
		}
		all, err := referenceArchivePointsInRange(w, index, -1, math.MaxInt32)
		if err != nil {
			t.Fatal(err)
		}
		windows := [][2]int{{-1, math.MaxInt32}}
		if len(all) > 0 {
			base := all[0].interval
			for n := 0; n < 100; n++ {
				start := base - a.MaxRetention() + rng.Intn(a.MaxRetention()*2)
				windows = append(windows, [2]int{start, start + rng.Intn(a.MaxRetention()*2)})
			}
		}
		for _, window := range windows {
			got, err := readArchivePointsInRange(w, index, window[0], window[1])
			want, werr := referenceArchivePointsInRange(w, index, window[0], window[1])
			if err != nil || werr != nil {
				t.Fatalf("archive %d window %v: %v/%v", index, window, err, werr)
			}
			assertArchivePointBits(t, got, want)
		}
		t.Logf("archive %d: %d bytes, %d points; all windows match", index, a.Size(), len(all))
	}
}

func BenchmarkSparseArchiveCapturedFile(b *testing.B) {
	path := os.Getenv("WHISPER_SPARSE_FIXTURE")
	if path == "" {
		b.Skip("no captured fixture supplied")
	}
	readOnly := os.O_RDONLY
	w, err := OpenWithOptions(path, &Options{OpenFileFlag: &readOnly})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	for _, test := range []struct {
		name string
		read func(*Whisper, int, int, int) ([]dataPoint, error)
	}{{"reference", referenceArchivePointsInRange}, {"sparse", readArchivePointsInRange}} {
		b.Run(test.name, func(b *testing.B) {
			snapshot := func() (rchar, syscr int64) {
				data, err := os.ReadFile("/proc/self/io")
				if err != nil {
					b.Fatal(err)
				}
				for _, line := range strings.Split(string(data), "\n") {
					var key string
					var v int64
					if _, err := fmt.Sscan(line, &key, &v); err != nil {
						continue
					}
					switch key {
					case "rchar:":
						rchar = v
					case "syscr:":
						syscr = v
					}
				}
				return
			}
			c0, s0 := snapshot()
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				for index := range w.archives {
					if _, err := test.read(w, index, -1, math.MaxInt32); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			c1, s1 := snapshot()
			b.ReportMetric(float64(c1-c0)/float64(b.N), "read-bytes/op")
			b.ReportMetric(float64(s1-s0)/float64(b.N), "read-calls/op")
		})
	}
}

func TestSparseOutOfOrderCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compressed.wsp")
	w, err := CreateWithOptions(path, Retentions{&Retention{secondsPerPoint: 1, numberOfPoints: 100003}}, Sum, 0, &Options{Compressed: true, OutOfOrder: true, IgnoreNowOnWrite: true, PointsPerBlock: 1200})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if w != nil {
			w.Close()
		}
	}()
	base := int(time.Now().Unix()) - 3600
	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: base, Value: 1}, {Time: base + 2, Value: 2}, {Time: base + 4, Value: 3}}); err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 7}, {Time: base + 2, Value: 99}}); err != nil {
		t.Fatal(err)
	}
	before, err := w.Fetch(base-1, base+5)
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, before, []float64{1, 7, 99, math.NaN(), 3, math.NaN()})
	sidecar, err := w.openOOO(false)
	if err != nil {
		t.Fatal(err)
	}
	if newArchiveExtentReader(sidecar.file, sidecar.archives[0].End(), sidecar.archives[0].Size()) == nil {
		t.Skip("filesystem does not report sparse allocation")
	}
	merged, err := w.MergeOutOfOrderWithOptions(OutOfOrderMergeOptions{MinPoints: 1})
	if err != nil || !merged {
		t.Fatalf("compaction: %v, %v", merged, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = OpenWithOptions(path, &Options{OutOfOrder: true})
	if err != nil {
		t.Fatal(err)
	}
	after, err := w.Fetch(base-1, base+5)
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, after, before.Values())
	if _, err := os.Stat(OutOfOrderSidecarPath(path)); !os.IsNotExist(err) {
		t.Fatalf("sidecar remains: %v", err)
	}
}

func TestArchiveExtentEligibility(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "sparse"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	const size = 1 << 20
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if newArchiveExtentReader(f, size, 128<<10) != nil {
		t.Fatal("small window uses extent queries")
	}
	if newArchiveExtentReader(f, size+1, size) != nil {
		t.Fatal("incomplete archive uses extent queries")
	}
	if newArchiveExtentReader(newMemFile("memory"), size, size) != nil {
		t.Fatal("memory file uses operating-system seeks")
	}
	if _, err := f.WriteAt(make([]byte, size), 0); err != nil {
		t.Fatal(err)
	}
	if newArchiveExtentReader(f, size, size) != nil {
		t.Fatal("dense file uses extent queries")
	}
}
