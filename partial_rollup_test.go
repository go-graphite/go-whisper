package whisper

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestCompressedPartialRollupContinuation(t *testing.T) {
	for _, method := range []AggregationMethod{Average, Sum, Last, Max, Min, First} {
		for _, xff := range []float32{0, .5, 1} {
			t.Run(fmt.Sprintf("%s/xff=%g", method, xff), func(t *testing.T) {
				now := correctnessClock(t)
				rets := MustParseRetentionDefs("1s:1m,10s:10m,60s:1h")
				classic, err := CreateWithOptions(filepath.Join(t.TempDir(), "classic.wsp"), rets, method, xff, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer classic.Close()
				opts := &Options{Compressed: true, OutOfOrder: true, FLock: true, PointsPerBlock: 20}
				path := filepath.Join(t.TempDir(), "compressed.wsp")
				compressed, err := CreateWithOptions(path, rets, method, xff, opts)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = compressed.Close() }()
				var seed []*TimeSeriesPoint
				for ts := *now - 50; ts < *now-5; ts++ {
					seed = append(seed, &TimeSeriesPoint{Time: ts, Value: float64(ts % 17)})
				}
				correctnessUpdate(t, classic, seed)
				reversePoints(seed)
				sort.Stable(timeSeriesPointsNewestFirst{seed})
				// Replay and compaction can encode the beginning of an unfinished
				// window. Continuing it must not start another full replay cycle.
				if err := compressed.updateCompressedOverlappingBatch(seed); err != nil {
					t.Fatal(err)
				}
				batchPath := auxiliaryPath(path, ".batch")
				if err := os.Mkdir(batchPath, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(batchPath, "blocker"), nil, 0600); err != nil {
					t.Fatal(err)
				}
				*now -= 5
				for round := 0; round < 150; round++ {
					var input []*TimeSeriesPoint
					for ts := *now; ts < *now+10; ts++ {
						value := float64(ts % 17)
						if ts%97 == 0 {
							value = math.Copysign(0, -1)
						}
						input = append(input, &TimeSeriesPoint{Time: ts, Value: value})
					}
					*now += 10
					correctnessUpdate(t, classic, input)
					correctnessUpdate(t, compressed, input)
					if err := compressed.Close(); err != nil {
						t.Fatal(err)
					}
					compressed, err = OpenWithOptions(path, opts)
					if err != nil {
						t.Fatal(err)
					}
					for _, age := range []int{60, 61, 601} {
						correctnessCompare(t, classic, compressed, *now-age, *now)
					}
					if round%37 == 0 {
						if err := compressed.MergeOutOfOrder(); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := compressed.CheckIntegrity(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestBufferedContinuationFailureCanRetry(t *testing.T) {
	now := correctnessClock(t)
	classic := correctnessFile(t, false, "1s:1m,10s:10m,60s:1h", .5)
	compressed := correctnessFile(t, true, "1s:1m,10s:10m,60s:1h", .5)
	compressed.opts.OutOfOrder = true
	var seed []*TimeSeriesPoint
	for ts := *now - 50; ts < *now-5; ts++ {
		seed = append(seed, &TimeSeriesPoint{Time: ts, Value: float64(ts % 17)})
	}
	correctnessUpdate(t, classic, seed)
	reversePoints(seed)
	sort.Stable(timeSeriesPointsNewestFirst{seed})
	if err := compressed.updateCompressedOverlappingBatch(seed); err != nil {
		t.Fatal(err)
	}
	*now -= 5
	for round := 0; round < 2; round++ {
		var input []*TimeSeriesPoint
		for ts := *now; ts < *now+10; ts++ {
			input = append(input, &TimeSeriesPoint{Time: ts, Value: float64(ts % 17)})
		}
		*now += 10
		correctnessUpdate(t, classic, input)
		correctnessUpdate(t, compressed, input)
	}
	blocked := auxiliaryPath(compressed.file.Name(), ".rollup")
	if err := os.Mkdir(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blocked, "blocker"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	input := []*TimeSeriesPoint{{Time: *now, Value: 123}}
	*now += 1
	if err := compressed.UpdateMany(append([]*TimeSeriesPoint(nil), input...)); err == nil {
		t.Fatal("expected blocked rollup rewrite to fail")
	}
	path, opts := compressed.file.Name(), compressed.opts
	if err := compressed.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	compressed, err = OpenWithOptions(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()
	for _, age := range []int{60, 61, 601} {
		correctnessCompare(t, classic, compressed, *now-age, *now)
	}
	if err := os.RemoveAll(blocked); err != nil {
		t.Fatal(err)
	}
	correctnessUpdate(t, classic, input)
	correctnessUpdate(t, compressed, input)
	for _, age := range []int{60, 61, 601} {
		correctnessCompare(t, classic, compressed, *now-age, *now)
	}
	if err := compressed.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}
