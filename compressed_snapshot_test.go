package whisper

import (
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"
)

// The encoder can retain several logical rings in well-compressed blocks.
// Importing ordered history keeps those aliases available to the read path.
func compressedSnapshotFixture(tb testing.TB, slots, rings int) *Whisper {
	tb.Helper()
	name := filepath.Join(tb.TempDir(), "snapshot.wsp")
	w, err := CreateWithOptions(name, Retentions{{secondsPerPoint: 1, numberOfPoints: slots}}, Last, 0,
		&Options{Compressed: true, InMemory: true, IgnoreNowOnWrite: true})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = w.Close(); releaseMemFile(name) })
	points := make([]*TimeSeriesPoint, slots*rings)
	for i := range points {
		points[i] = &TimeSeriesPoint{Time: 1700000000 + i, Value: float64(i % 17)}
	}
	for i := 0; i < rings; i++ {
		if _, err := w.archiveUpdateManyCompressed(w.archives[0], points[i*slots:(i+1)*slots]); err != nil {
			tb.Fatal(err)
		}
	}
	if err := w.WriteHeaderCompressed(); err != nil {
		tb.Fatal(err)
	}
	stored, err := w.storedPoints(w.archives[0], 1, maxInt)
	if err != nil {
		tb.Fatal(err)
	}
	if rings > 1 && len(stored) <= slots {
		tb.Fatalf("fixture lost circular aliases: %d stored points, %d slots", len(stored), slots)
	}
	return w
}

func TestCompressedSnapshotSlots(t *testing.T) {
	for _, slots := range []int{64, 1024} {
		for _, rings := range []int{1, 3} {
			t.Run(fmt.Sprintf("slots=%d/rings=%d", slots, rings), func(t *testing.T) {
				w := compressedSnapshotFixture(t, slots, rings)
				archive := w.archives[0]
				stored, err := w.storedPoints(archive, 1, maxInt)
				if err != nil {
					t.Fatal(err)
				}
				// A classic ring is an independent oracle for slot ownership.
				path := filepath.Join(t.TempDir(), "classic.wsp")
				classic, err := CreateWithOptions(path, Retentions{{secondsPerPoint: 1, numberOfPoints: slots}}, Last, 0, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer classic.Close()
				input := make([]TimeSeriesPoint, len(stored))
				for i, p := range stored {
					input[i] = TimeSeriesPoint{Time: p.interval, Value: p.value}
				}
				if err := classic.ReplaceArchivePoints(0, input); err != nil {
					t.Fatal(err)
				}
				physical, err := classic.ArchivePoints(0)
				if err != nil {
					t.Fatal(err)
				}
				for _, bounds := range [][2]int{{1, maxInt}, {1700000000, 1700000000 + slots - 1}, {1700000000 + slots/2, 1700000000 + slots*rings - 2}} {
					got, err := w.fetchCompressed(int64(bounds[0]), int64(bounds[1]), archive)
					if err != nil {
						t.Fatal(err)
					}
					var want []dataPoint
					for _, p := range physical {
						if bounds[0] <= p.Time && p.Time <= bounds[1] {
							want = append(want, dataPoint{p.Time, p.Value})
						}
					}
					if !equalPointBits(got, want) {
						t.Fatalf("range %v differs from classic ring: got %d points, want %d", bounds, len(got), len(want))
					}
				}
			})
		}
	}
}

type snapshotReadCounter struct {
	file
	reads map[int64]int
}

func (f *snapshotReadCounter) ReadAt(p []byte, offset int64) (int, error) {
	f.reads[offset]++
	return f.file.ReadAt(p, offset)
}

func TestCompressedSnapshotReadsBlocksOnce(t *testing.T) {
	w := compressedSnapshotFixture(t, 1024, 3)
	original := w.file
	counter := &snapshotReadCounter{file: original, reads: make(map[int64]int)}
	w.file = counter
	defer func() { w.file = original }()
	points, err := w.fetchCompressed(1, int64(maxInt), w.archives[0])
	if err != nil || len(points) != 1024 {
		t.Fatalf("snapshot: %d points, err=%v", len(points), err)
	}
	if len(counter.reads) == 0 {
		t.Fatal("fixture did not read compressed blocks")
	}
	for offset, count := range counter.reads {
		if count != 1 {
			t.Fatalf("block at %d read %d times", offset, count)
		}
	}
}

func TestCompressedSnapshotBufferedRollups(t *testing.T) {
	now := correctnessClock(t)
	for _, method := range []AggregationMethod{Average, Sum, Last, Max, Min, First} {
		for _, xff := range []float32{0, .5, 1} {
			t.Run(fmt.Sprintf("%s/xff=%g", method, xff), func(t *testing.T) {
				*now = 1700006400
				schema := "1s:1m,10s:10m,60s:1h"
				classic := correctnessFile(t, false, schema, xff)
				w := correctnessFile(t, true, schema, xff)
				for _, handle := range []*Whisper{classic, w} {
					if err := handle.UpdateConfig(MustParseRetentionDefs(schema), method, xff, handle.opts); err != nil {
						t.Fatal(err)
					}
				}
				for round := 0; round < 12; round++ {
					*now += 16
					var input []*TimeSeriesPoint
					for i := 0; i < 16; i++ {
						if i%7 != 0 {
							input = append(input, &TimeSeriesPoint{Time: *now - 15 + i, Value: float64(i + round)})
						}
					}
					correctnessUpdate(t, classic, input)
					correctnessUpdate(t, w, input)
					for _, age := range []int{60, 600, 3600} {
						correctnessCompare(t, classic, w, *now-age, *now)
					}
					for i, archive := range w.archives {
						full, err := w.fetchCompressed(1, int64(maxInt), archive)
						if err != nil {
							t.Fatal(err)
						}
						// This range contains the same data but still takes the
						// path which checks external newer slot owners.
						ranged, err := w.fetchCompressed(1, int64(maxInt-1), archive)
						if err != nil {
							t.Fatal(err)
						}
						if !equalPointBits(full, ranged) {
							t.Fatalf("round %d archive %d: full snapshot differs from range read", round, i)
						}
					}
				}
			})
		}
	}
}

func BenchmarkCompressedSnapshot(b *testing.B) {
	for _, rings := range []int{1, 3} {
		b.Run(fmt.Sprintf("rings=%d", rings), func(b *testing.B) {
			w := compressedSnapshotFixture(b, 21600, rings)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				points, err := w.fetchCompressed(1, int64(maxInt), w.archives[0])
				if err != nil {
					b.Fatal(err)
				}
				if len(points) != 21600 || math.IsNaN(points[len(points)-1].value) {
					b.Fatal("snapshot lost retained points")
				}
			}
		})
	}
}

// Restore the same wrapped image outside the timer: a replay itself removes
// aliases, so repeating writes to its output would benchmark a different case.
func BenchmarkCompressedSnapshotReplay(b *testing.B) {
	const slots = 21600
	const now = 1700000000 + 3*slots - 1
	previousNow := Now
	Now = func() time.Time { return time.Unix(now, 0) }
	b.Cleanup(func() { Now = previousNow })
	w := compressedSnapshotFixture(b, slots, 3)
	name := w.file.Name()
	image := append([]byte(nil), w.file.(*memFile).data...)
	input := []*TimeSeriesPoint{{Time: now - 50, Value: 42}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
		mf := newMemFile(name)
		mf.data = append(mf.data[:0], image...)
		var err error
		w, err = OpenWithOptions(name, &Options{InMemory: true})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := w.updateCompressedOverlappingBatch(input); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	series, err := w.Fetch(now-51, now-50)
	if err != nil || len(series.Values()) != 1 || series.Values()[0] != 42 {
		b.Fatalf("replay value differs: series=%v err=%v", series, err)
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
}
