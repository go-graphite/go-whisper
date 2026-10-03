package whisper

import (
	"fmt"
	"math"
	"testing"
)

func TestOutOfOrderVisibleBeforeAndAfterCompaction(t *testing.T) {
	now := correctnessClock(t)
	for _, method := range []AggregationMethod{Average, Sum, Last, Max, Min, First} {
		for _, xff := range []float32{0, 0.5, 1} {
			for _, holes := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/xff=%g/holes=%t", method, xff, holes), func(t *testing.T) {
					classic := correctnessFile(t, false, "1s:5m,10s:1h,60s:6h", xff)
					ooo := correctnessFile(t, true, "1s:5m,10s:1h,60s:6h", xff)
					ooo.opts.OutOfOrder = true
					// Persist the chosen policy for reopened handles and sidecars.
					for _, w := range []*Whisper{classic, ooo} {
						if err := w.UpdateConfig(MustParseRetentionDefs("1s:5m,10s:1h,60s:6h"), method, xff, w.opts); err != nil {
							t.Fatal(err)
						}
					}
					var initial, late []*TimeSeriesPoint
					for i := 0; i < 180; i++ {
						point := &TimeSeriesPoint{Time: *now - 180 + i, Value: float64((i*17)%31 - 15)}
						if holes && i%13 == 0 {
							late = append(late, point)
						} else {
							initial = append(initial, point)
						}
					}
					if !holes {
						late = []*TimeSeriesPoint{{Time: *now - 170, Value: 101}, {Time: *now - 169, Value: -203}, {Time: *now - 155, Value: math.NaN()}, {Time: *now - 154, Value: math.Copysign(0, -1)}}
					}
					for _, input := range [][]*TimeSeriesPoint{initial, late, late} {
						correctnessUpdate(t, classic, input)
						correctnessUpdate(t, ooo, input)
						for _, age := range []int{300, 301, 3601} {
							correctnessCompare(t, classic, ooo, *now-age, *now)
						}
					}
					path, opts := ooo.file.Name(), ooo.opts
					if err := ooo.Close(); err != nil {
						t.Fatal(err)
					}
					reopened, err := OpenWithOptions(path, opts)
					if err != nil {
						t.Fatal(err)
					}
					ooo = reopened
					t.Cleanup(func() { _ = reopened.Close() })
					for _, age := range []int{300, 301, 3601} {
						correctnessCompare(t, classic, ooo, *now-age, *now)
					}
					if err := ooo.CheckIntegrity(); err != nil {
						t.Fatal(err)
					}
					if err := ooo.MergeOutOfOrder(); err != nil {
						t.Fatal(err)
					}
					for _, age := range []int{300, 301, 3601} {
						correctnessCompare(t, classic, ooo, *now-age, *now)
					}
					input := []*TimeSeriesPoint{{Time: *now - 1, Value: 17}, {Time: *now, Value: 31}}
					correctnessUpdate(t, classic, input)
					correctnessUpdate(t, ooo, input)
					for _, age := range []int{300, 301, 3601} {
						correctnessCompare(t, classic, ooo, *now-age, *now)
					}
				})
			}
		}
	}
}

func TestOutOfOrderCircularSlotWriteOrder(t *testing.T) {
	now := correctnessClock(t)
	for _, schema := range []string{"1s:1m", "1s:10s,10s:1m,60s:1h"} {
		for _, correctionFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/correction-first=%t", schema, correctionFirst), func(t *testing.T) {
				*now = 1700006400
				classic := correctnessFile(t, false, schema, 0)
				ooo := correctnessFile(t, true, schema, 0)
				ooo.opts.OutOfOrder = true
				initial := []*TimeSeriesPoint{{Time: *now - 50, Value: 7}, {Time: *now - 40, Value: 8}}
				correction := []*TimeSeriesPoint{{Time: *now - 50, Value: 11}}
				future := []*TimeSeriesPoint{{Time: *now + 10, Value: 9}}
				batches := [][]*TimeSeriesPoint{initial, future, correction}
				if correctionFirst {
					batches = [][]*TimeSeriesPoint{initial, correction, future}
				}
				for _, input := range batches {
					correctnessUpdate(t, classic, input)
					correctnessUpdate(t, ooo, input)
					correctnessCompare(t, classic, ooo, *now-60, *now)
					correctnessCompare(t, classic, ooo, *now-59, *now-10)
					correctnessCompare(t, classic, ooo, *now-61, *now)
				}
				*now += 20
				correctnessCompare(t, classic, ooo, *now-60, *now)
				correctnessCompare(t, classic, ooo, *now-59, *now-10)
				correctnessCompare(t, classic, ooo, *now-61, *now)
				if err := ooo.MergeOutOfOrder(); err != nil {
					t.Fatal(err)
				}
				correctnessCompare(t, classic, ooo, *now-60, *now)
				correctnessCompare(t, classic, ooo, *now-59, *now-10)
				correctnessCompare(t, classic, ooo, *now-61, *now)
			})
		}
	}
}
