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

func TestOutOfOrderRollupsSurviveRetentionWrap(t *testing.T) {
	now := correctnessClock(t)
	for _, method := range []AggregationMethod{Average, Sum, Last, Max, Min, First} {
		for _, xff := range []float32{0, 0.5, 1} {
			t.Run(fmt.Sprintf("%s/xff=%g", method, xff), func(t *testing.T) {
				*now = 1700006400
				const schema = "1s:10m,10s:1h,60s:6h"
				classic := correctnessFile(t, false, schema, xff)
				ooo := correctnessFile(t, true, schema, xff)
				ooo.opts.OutOfOrder = true
				for _, w := range []*Whisper{classic, ooo} {
					if err := w.UpdateConfig(MustParseRetentionDefs(schema), method, xff, w.opts); err != nil {
						t.Fatal(err)
					}
				}
				compare := func(stage string) {
					t.Helper()
					t.Run(stage, func(t *testing.T) {
						for _, age := range []int{600, 3600, 21600} {
							correctnessCompare(t, classic, ooo, *now-age, *now)
						}
					})
					if t.Failed() {
						t.FailNow()
					}
				}
				reopen := func() {
					t.Helper()
					path, opts := ooo.file.Name(), ooo.opts
					if err := ooo.Close(); err != nil {
						t.Fatal(err)
					}
					var err error
					ooo, err = OpenWithOptions(path, opts)
					if err != nil {
						t.Fatal(err)
					}
				}
				defer func() { _ = ooo.Close() }()
				var seed []*TimeSeriesPoint
				for i := 0; i < 600; i++ {
					seed = append(seed, &TimeSeriesPoint{Time: *now - 600 + i, Value: float64((i*17)%31 - 15)})
				}
				for _, w := range []*Whisper{classic, ooo} {
					correctnessUpdate(t, w, seed)
				}
				if err := ooo.MergeOutOfOrder(); err != nil {
					t.Fatal(err)
				}
				// Alternate upper and lower halves of each batch pair, covering many
				// fine and coarse ring wraps while corrections await compaction.
				for round := 0; round < 4096; round++ {
					*now = 1700006400 + (round/2+1)*16
					start := *now - 8
					if round%2 == 1 {
						start -= 8
					}
					var batch []*TimeSeriesPoint
					for j := 0; j < 8; j++ {
						batch = append(batch, &TimeSeriesPoint{Time: start + j, Value: float64((start + j) % 101)})
					}
					correctnessUpdate(t, classic, batch)
					correctnessUpdate(t, ooo, batch)

					// Check every update through the first wrap, then sample
					// every 32 batches across the longer retention cycles.
					if round < 128 || round%32 == 0 || round == 4095 {
						compare(fmt.Sprintf("round=%d", round))
					}
					if round == 76 || round == 511 {
						reopen()
						compare(fmt.Sprintf("reopened=%d", round))
					}
					if round == 513 {
						if err := ooo.MergeOutOfOrder(); err != nil {
							t.Fatal(err)
						}
						compare("mid-compaction")
					}
				}
				// Fine samples expire while their aggregates remain queryable.
				*now += 601
				compare("idle-expiry")
				if err := ooo.MergeOutOfOrder(); err != nil {
					t.Fatal(err)
				}
				reopen()
				compare("final-compaction")
				if err := ooo.CheckIntegrity(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestOutOfOrderRangeReadsMatchPhysicalSlots(t *testing.T) {
	now := correctnessClock(t)
	w := correctnessFile(t, false, "1s:20s,5s:1m", 0)
	for index, archive := range w.archives {
		input := []TimeSeriesPoint{}
		for i := 0; i < archive.numberOfPoints*3; i++ {
			if i%7 == 0 {
				continue
			}
			input = append(input, TimeSeriesPoint{Time: *now - 2*archive.MaxRetention() + i*archive.secondsPerPoint, Value: float64(i)})
		}
		if err := w.ReplaceArchivePoints(index, input); err != nil {
			t.Fatal(err)
		}
		physical, err := w.ArchivePoints(index)
		if err != nil {
			t.Fatal(err)
		}
		for from := *now - archive.MaxRetention()*2; from < *now+archive.MaxRetention(); from += 3 {
			for _, width := range []int{-1, 0, 1, 4, 13, 19, 20, 21, 90} {
				until := from + width
				got, err := readArchivePointsInRange(w, index, from, until)
				if err != nil {
					t.Fatal(err)
				}
				var want []dataPoint
				for _, p := range physical {
					if p.Time >= from && p.Time <= until {
						want = append(want, dataPoint{p.Time, p.Value})
					}
				}
				if len(got) != len(want) {
					t.Fatalf("archive=%d range=%d:%d got=%v want=%v", index, from, until, got, want)
				}
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("got=%v want=%v", got, want)
					}
				}
			}
		}
		// A historical snapshot horizon before Unix epoch zero must not overflow
		// the width calculation when the upper bound is maxInt.
		got, err := readArchivePointsInRange(w, index, -100, maxInt)
		if err != nil || len(got) != len(physical) {
			t.Fatalf("historical range: got=%v err=%v", got, err)
		}
	}
}
