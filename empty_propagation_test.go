package whisper

import (
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"
)

// Exercise the typed block reads used by continuation storage as well as bytes.
type emptyWindowBlockReader struct{ file }

func (f emptyWindowBlockReader) ReadStorageBlock(block ContinuationBlock) ([]TimeSeriesPoint, error) {
	data := make([]byte, block.Size)
	if _, err := f.ReadAt(data, int64(block.Offset)); err != nil {
		return nil, err
	}
	return DecodeContinuationBlock(block, data)
}

func TestCompressedEmptyWindowPropagation(t *testing.T) {
	for _, fixture := range []struct {
		name    string
		archive int
		typed   bool
	}{{"direct", 0, false}, {"recursive", 1, false}, {"continuation", 1, true}} {
		t.Run(fixture.name, func(t *testing.T) {
			for _, method := range []AggregationMethod{Max, Average, Min, First, Last, Sum} {
				for _, xff := range []float32{0, .5, 1} {
					for _, stale := range []bool{true, false} {
						t.Run(fmt.Sprintf("%s/xff=%g/stale=%t", method, xff, stale), func(t *testing.T) {
							const start = 1700006400 // aligned to every archive
							now := start + 2
							w, err := CreateWithOptions(filepath.Join(t.TempDir(), "metric.wsp"),
								MustParseRetentionDefs("1s:10s,10s:100s,100s:1000s,1000s:10000s"), method, xff,
								&Options{Compressed: true, PointsPerBlock: 64, Now: func() time.Time { return time.Unix(int64(now), 0) }})
							if err != nil {
								t.Fatal(err)
							}
							defer w.Close()
							if fixture.typed {
								w.file = emptyWindowBlockReader{w.file}
							}
							higher, lower := w.archives[fixture.archive], w.archives[fixture.archive+1]
							step := higher.secondsPerPoint
							// Model an imported/replayed partial window: its beginning is
							// encoded, with the rest arriving in the circular buffer.
							if _, err := higher.appendToBlockAndRotate([]dataPoint{{start + step, 7}}); err != nil {
								t.Fatal(err)
							}
							// A skipped propagation must preserve an existing lower value.
							if _, err := lower.appendToBlockAndRotate([]dataPoint{{start, 99}}); err != nil {
								t.Fatal(err)
							}
							update := func(timestamp int, points ...*TimeSeriesPoint) {
								t.Helper()
								now = timestamp
								if err := w.UpdateManyForArchive(points, higher.MaxRetention()); err != nil {
									t.Fatal(err)
								}
							}
							update(start+2*step, &TimeSeriesPoint{start + 2*step, 8})
							newer := []*TimeSeriesPoint{{start + 12*step, 18}}
							if stale {
								// Reuse both slots before the old unit is flushed. The
								// stored window then filters to empty, despite a nonempty flush.
								newer = append(newer, &TimeSeriesPoint{start + 11*step, 17})
							}
							update(start+12*step, newer...)
							if fixture.archive == 0 {
								update(start+20*step, &TimeSeriesPoint{start + 20*step, 20})
							} else {
								// Flush a finer archive into this one, triggering the
								// empty-window check in a recursive propagation call.
								for n := 20; n <= 22; n++ {
									now = start + n*step
									if err := w.UpdateManyForArchive([]*TimeSeriesPoint{{now, 20}}, w.archives[0].MaxRetention()); err != nil {
										t.Fatal(err)
									}
								}
							}

							want := 99.0
							if !stale && xff == 0 {
								want = 7 // the one surviving slot still propagates at XFF zero
							}
							assertLower := func() {
								t.Helper()
								points, err := w.ArchivePoints(fixture.archive + 1)
								if err != nil {
									t.Fatal(err)
								}
								if len(points) != 1 || points[0].Time != start || math.Float64bits(points[0].Value) != math.Float64bits(want) {
									t.Fatalf("lower archive = %v, want one point {%d %g}", points, start, want)
								}
							}
							assertLower()
							path, opts := w.file.Name(), w.opts
							if err := w.Close(); err != nil {
								t.Fatal(err)
							}
							w, err = OpenWithOptions(path, opts)
							if err != nil {
								t.Fatal(err)
							}
							if fixture.typed {
								w.file = emptyWindowBlockReader{w.file}
							}
							defer w.Close()
							assertLower()
						})
					}
				}
			}
		})
	}
}

func TestClassicEmptyWindowPropagation(t *testing.T) {
	const start = 1700006400
	for _, method := range []AggregationMethod{Max, Average, Min, First, Last, Sum} {
		t.Run(method.String(), func(t *testing.T) {
			w, err := CreateWithOptions(filepath.Join(t.TempDir(), "metric.wsp"),
				MustParseRetentionDefs("1s:10s,10s:100s"), method, 0,
				&Options{Now: func() time.Time { return time.Unix(start+12, 0) }})
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if err := w.ReplaceArchivePoints(1, []TimeSeriesPoint{{start, 99}}); err != nil {
				t.Fatal(err)
			}
			// Both samples occupy the same circular slot. The old window is
			// empty by the time propagation reads it; the new one has a value.
			if err := w.UpdateManyForArchive([]*TimeSeriesPoint{{start + 2, 8}, {start + 12, 18}}, 10); err != nil {
				t.Fatal(err)
			}
			points, err := w.ArchivePoints(1)
			if err != nil {
				t.Fatal(err)
			}
			if len(points) != 2 || points[0] != (TimeSeriesPoint{start, 99}) || points[1] != (TimeSeriesPoint{start + 10, 18}) {
				t.Fatalf("lower archive = %v, want preserved old value and propagated new value", points)
			}
		})
	}
}
