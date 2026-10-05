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
	for _, method := range []AggregationMethod{Average, Sum, Last, Max, Min, First} {
		for _, xff := range []float32{0, .5, 1} {
			for _, ooo := range []bool{false, true} {
				t.Run(fmt.Sprintf("%v/xff=%g/ooo=%t", method, xff, ooo), func(t *testing.T) {
					checkReplayScratchOracle(t, method, xff, ooo)
				})
			}
		}
	}
}

// checkReplayScratchOracle compares full archive state before and after further
// incremental writes for one aggregation and retention configuration.
func checkReplayScratchOracle(t *testing.T, method AggregationMethod, xff float32, ooo bool) {
	t.Helper()
	old := Now
	now := 1700006400
	Now = func() time.Time { return time.Unix(int64(now), 0) }
	t.Cleanup(func() { Now = old })
	opts := &Options{Compressed: true, OutOfOrder: ooo, PointsPerBlock: 60}
	rets := MustParseRetentionDefs("1s:2m,10s:1h,1m:1d")
	candidate, err := CreateWithOptions(filepath.Join(t.TempDir(), "candidate.wsp"), rets, method, xff, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Close()
	oracle, err := CreateWithOptions(filepath.Join(t.TempDir(), "oracle.wsp"), rets, method, xff, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer oracle.Close()
	// skipcq: GSC-G404 -- Fixed-seed test data, not security-sensitive randomness.
	rng := rand.New(rand.NewSource(29))
	for round := 0; round < 8; round++ {
		input := replayScratchInput(now, rng)
		if err := candidate.updateCompressedOverlappingBatch(input); err != nil {
			t.Fatal(err)
		}
		if err := oracle.referenceOverlappingBatch(input); err != nil {
			t.Fatal(err)
		}
		compareReplayArchives(t, candidate, oracle)
		// Subsequent incremental writes also see the same ring state.
		more := []*TimeSeriesPoint{{Time: now + 301, Value: 17}, {Time: now + 302, Value: 23}}
		if err := candidate.UpdateMany(append([]*TimeSeriesPoint(nil), more...)); err != nil {
			t.Fatal(err)
		}
		if err := oracle.UpdateMany(append([]*TimeSeriesPoint(nil), more...)); err != nil {
			t.Fatal(err)
		}
		compareReplayArchives(t, candidate, oracle)
		now += 600
	}
}

// replayScratchInput includes late, future, duplicate and special floating-point
// values, ordered as UpdateMany delivers them to overlapping replay.
func replayScratchInput(now int, rng *rand.Rand) []*TimeSeriesPoint {
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
	reversePoints(input)
	sort.Stable(timeSeriesPointsNewestFirst{input})
	return input
}

// compareReplayArchives checks exact timestamp and floating-point bits for every
// stored archive, including retention levels not directly touched by the batch.
func compareReplayArchives(t *testing.T, candidate, oracle *Whisper) {
	t.Helper()
	for i := range candidate.archives {
		got, err := candidate.fetchCompressed(1, int64(maxInt), candidate.archives[i])
		if err != nil {
			t.Fatal(err)
		}
		want, err := oracle.fetchCompressed(1, int64(maxInt), oracle.archives[i])
		if err != nil {
			t.Fatal(err)
		}
		if !equalPointBits(got, want) {
			t.Fatalf("archive %d differs from original replay", i)
		}
	}
}
