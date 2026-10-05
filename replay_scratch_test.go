package whisper

import (
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// Keep the original replay as an independent oracle for the optimized scratch
// representation. It uses the public archive import/export path.
func (whisper *Whisper) referenceOverlappingBatch(points []*TimeSeriesPoint) error {
	if err := whisper.MergeOutOfOrder(); err != nil {
		return err
	}
	name := auxiliaryPath(whisper.file.Name(), ".batch")
	replay, err := CreateWithOptions(name, NewRetentionsNoPointer(whisper.Retentions()), whisper.aggregationMethod, whisper.xFilesFactor, &Options{InMemory: true})
	if err != nil {
		return fmt.Errorf("create overlapping batch scratch: %w", err)
	}
	defer func() {
		_ = replay.Close()
		releaseMemFile(name)
	}()
	for i, archive := range whisper.archives {
		stored, err := whisper.fetchCompressed(1, int64(maxInt), archive)
		if err != nil {
			return err
		}
		values := make([]TimeSeriesPoint, len(stored))
		for j, point := range stored {
			values[j] = TimeSeriesPoint{Time: point.interval, Value: point.value}
		}
		if err := replay.ReplaceArchivePoints(i, values); err != nil {
			return err
		}
	}
	// Restore the caller order that UpdateMany's reverse/stable sort expects.
	input := append([]*TimeSeriesPoint(nil), points...)
	reversePoints(input)
	if err := replay.UpdateMany(input); err != nil {
		return err
	}
	extras := make([][]extraPoint, len(whisper.archives))
	for i := range extras {
		stored, err := replay.ArchivePoints(i)
		if err != nil {
			return err
		}
		for _, point := range stored {
			extras[i] = append(extras[i], extraPoint{dataPoint: dataPoint{point.Time, point.Value}, replace: true})
		}
	}
	rets := make([]*Retention, len(whisper.archives))
	for i, archive := range whisper.archives {
		ret := archive.Retention
		rets[i] = &ret
	}
	return whisper.rewrite(rets, "batch", func(i int) []extraPoint { return extras[i] })
}

func TestCompressedReplayScratchOracle(t *testing.T) {
	old := Now
	now := 1700006400
	Now = func() time.Time { return time.Unix(int64(now), 0) }
	defer func() { Now = old }()
	for _, method := range []AggregationMethod{Average, Sum, Last, Max, Min, First} {
		for _, xff := range []float32{0, .5, 1} {
			for _, ooo := range []bool{false, true} {
				t.Run(fmt.Sprintf("%v/xff=%g/ooo=%t", method, xff, ooo), func(t *testing.T) {
					now = 1700006400
					opts := &Options{Compressed: true, OutOfOrder: ooo, PointsPerBlock: 60}
					rets := MustParseRetentionDefs("1s:2m,10s:1h,1m:1d")
					candidate, e := CreateWithOptions(filepath.Join(t.TempDir(), "candidate.wsp"), rets, method, xff, opts)
					if e != nil {
						t.Fatal(e)
					}
					defer candidate.Close()
					oracle, e := CreateWithOptions(filepath.Join(t.TempDir(), "oracle.wsp"), rets, method, xff, opts)
					if e != nil {
						t.Fatal(e)
					}
					defer oracle.Close()
					rng := rand.New(rand.NewSource(29))
					for round := 0; round < 8; round++ {
						input := make([]*TimeSeriesPoint, 0, 150)
						for i := 0; i < 150; i++ {
							timestamp := now - rng.Intn(6000)
							if i%11 == 0 {
								timestamp = now + rng.Intn(300)
							}
							value := float64(rng.Intn(100) - 50)
							switch i % 17 {
							case 0:
								value = math.Float64frombits(0x7ff8000000000042)
							case 1:
								value = math.Copysign(0, -1)
							case 2:
								value = math.Inf(1)
							case 3:
								value = math.Inf(-1)
							}
							input = append(input, &TimeSeriesPoint{timestamp, value})
							if i%13 == 0 {
								input = append(input, &TimeSeriesPoint{timestamp, 42})
							}
						}
						// This helper receives UpdateMany's newest-first, stable ordering.
						reversePoints(input)
						sort.Stable(timeSeriesPointsNewestFirst{input})
						if e = candidate.updateCompressedOverlappingBatch(input); e != nil {
							t.Fatal(e)
						}
						if e = oracle.referenceOverlappingBatch(input); e != nil {
							t.Fatal(e)
						}
						for i := range candidate.archives {
							got, e := candidate.fetchCompressed(1, int64(maxInt), candidate.archives[i])
							if e != nil {
								t.Fatal(e)
							}
							want, e := oracle.fetchCompressed(1, int64(maxInt), oracle.archives[i])
							if e != nil {
								t.Fatal(e)
							}
							if !equalPointBits(got, want) {
								t.Fatalf("round %d archive %d differs from original replay", round, i)
							}
						}
						// Subsequent incremental writes also see the same ring state.
						more := []*TimeSeriesPoint{{Time: now + 301, Value: 17}, {Time: now + 302, Value: 23}}
						if e = candidate.UpdateMany(append([]*TimeSeriesPoint(nil), more...)); e != nil {
							t.Fatal(e)
						}
						if e = oracle.UpdateMany(append([]*TimeSeriesPoint(nil), more...)); e != nil {
							t.Fatal(e)
						}
						now += 600
					}
				})
			}
		}
	}
}
