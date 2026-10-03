package whisper

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func correctnessClock(t *testing.T) *int {
	t.Helper()
	now := 1700006400
	previous := Now
	Now = func() time.Time { return time.Unix(int64(now), 0) }
	t.Cleanup(func() { Now = previous })
	return &now
}

func correctnessFile(t *testing.T, compressed bool, retentions string, xff float32) *Whisper {
	t.Helper()
	w, err := CreateWithOptions(filepath.Join(t.TempDir(), "metric.wsp"), MustParseRetentionDefs(retentions), Average, xff, &Options{Compressed: compressed, Sparse: true, FLock: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func correctnessUpdate(t *testing.T, w *Whisper, points []*TimeSeriesPoint) {
	t.Helper()
	copyPoints := append([]*TimeSeriesPoint(nil), points...)
	if err := w.UpdateMany(copyPoints); err != nil {
		t.Fatal(err)
	}
}

func correctnessCompare(t *testing.T, classic, compressed *Whisper, from, until int) {
	t.Helper()
	want, err := classic.Fetch(from, until)
	if err != nil {
		t.Fatal(err)
	}
	got, err := compressed.Fetch(from, until)
	if err != nil {
		t.Fatal(err)
	}
	if want.fromTime != got.fromTime || want.untilTime != got.untilTime || want.step != got.step || len(want.values) != len(got.values) {
		t.Fatalf("query %d:%d: classic=%+v compressed=%+v", from, until, want, got)
	}
	for i, value := range want.values {
		if math.IsNaN(value) && math.IsNaN(got.values[i]) {
			continue
		}
		if math.Float64bits(value) != math.Float64bits(got.values[i]) {
			t.Fatalf("timestamp %d: classic=%g compressed=%g", want.fromTime+i*want.step, value, got.values[i])
		}
	}
}

func TestCompressedRetainsFullWindowAcrossBatchSizes(t *testing.T) {
	now := correctnessClock(t)
	for _, batch := range []int{1, 30, 600} {
		for _, noisy := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch=%d/noisy=%t", batch, noisy), func(t *testing.T) {
				*now = 1700006400
				classic := correctnessFile(t, false, "1s:10m", 0.5)
				compressed := correctnessFile(t, true, "1s:10m", 0.5)
				rng := rand.New(rand.NewSource(721))
				for round := 0; round < 3; round++ {
					if round > 0 {
						*now += 600
					}
					var input []*TimeSeriesPoint
					for i := 0; i < 600; i++ {
						value := float64((i*17)%31 - 15)
						if noisy {
							value = math.Float64frombits(rng.Uint64() & 0x7fefffffffffffff)
						}
						input = append(input, &TimeSeriesPoint{Time: *now - 600 + i, Value: value})
					}
					for i := 0; i < len(input); i += batch {
						correctnessUpdate(t, classic, input[i:i+batch])
						correctnessUpdate(t, compressed, input[i:i+batch])
					}
					correctnessCompare(t, classic, compressed, *now-600, *now)
					stored, err := compressed.ArchivePoints(0)
					if err != nil {
						t.Fatal(err)
					}
					live := 0
					for _, point := range stored {
						if point.Time >= *now-600 && point.Time < *now {
							live++
						}
					}
					if live != 600 {
						t.Fatalf("physical archive retains %d of 600 samples", live)
					}
					path, opts := compressed.file.Name(), compressed.opts
					if err := compressed.Close(); err != nil {
						t.Fatal(err)
					}
					reopened, err := OpenWithOptions(path, opts)
					if err != nil {
						t.Fatal(err)
					}
					compressed = reopened
					t.Cleanup(func() { _ = reopened.Close() })
					correctnessCompare(t, classic, compressed, *now-600, *now)
					if err := compressed.CheckIntegrity(); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestCompressedGrowthFailurePreservesAcknowledgedPoints(t *testing.T) {
	now := correctnessClock(t)
	w := correctnessFile(t, true, "1s:10m", 0.5)
	var input []*TimeSeriesPoint
	for i := 0; i < 600; i++ {
		input = append(input, &TimeSeriesPoint{Time: *now - 600 + i, Value: float64((i*17)%31 - 15)})
	}
	correctnessUpdate(t, w, input[:20])
	blocked := auxiliaryPath(w.file.Name(), ".grow")
	if err := os.Mkdir(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blocked, "keep"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany(input[20:]); err == nil {
		t.Fatal("growth failure was acknowledged as a successful write")
	}
	path, opts := w.file.Name(), w.opts
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenWithOptions(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	w = reopened
	t.Cleanup(func() { _ = reopened.Close() })
	got, err := w.Fetch(*now-600, *now-580)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 19; i++ {
		if got.values[i] != input[i+1].Value {
			t.Fatalf("previously acknowledged sample %d lost: %g", i+1, got.values[i])
		}
	}
}

func TestCompressedBufferedRollupsAndFetchGrid(t *testing.T) {
	now := correctnessClock(t)
	for _, xff := range []float32{0, 0.5, 1} {
		t.Run(fmt.Sprintf("xff=%g", xff), func(t *testing.T) {
			classic := correctnessFile(t, false, "1s:5m,10s:1h,60s:6h", xff)
			compressed := correctnessFile(t, true, "1s:5m,10s:1h,60s:6h", xff)
			correctnessCompare(t, classic, compressed, *now, *now)
			for _, count := range []int{4, 5, 6, 10, 10, 10} {
				var input []*TimeSeriesPoint
				for i := 0; i < count; i++ {
					input = append(input, &TimeSeriesPoint{Time: *now - 100 + i, Value: float64(i)})
				}
				correctnessUpdate(t, classic, input)
				correctnessUpdate(t, compressed, input)
				correctnessCompare(t, classic, compressed, *now-301, *now)
				correctnessCompare(t, classic, compressed, *now-3601, *now)
				correctnessCompare(t, classic, compressed, *now, *now)
				*now += 10
			}
		})
	}
}

func TestCompressedOverlappingArchiveSlots(t *testing.T) {
	now := correctnessClock(t)
	classic := correctnessFile(t, false, "1s:1m,10s:10m,60s:1h", 0)
	compressed := correctnessFile(t, true, "1s:1m,10s:10m,60s:1h", 0)
	input := []*TimeSeriesPoint{{Time: *now - 3600, Value: 2}, {Time: *now - 600, Value: 4}, {Time: *now - 60, Value: 6}, {Time: *now - 59, Value: 7}, {Time: *now, Value: 8}, {Time: *now + 1, Value: 9}}
	correctnessUpdate(t, classic, input)
	correctnessUpdate(t, compressed, input)
	for _, age := range []int{60, 61, 600, 601, 3600} {
		correctnessCompare(t, classic, compressed, *now-age, *now)
	}
}

func TestCompressedPolicyChangePreservesBufferedHistory(t *testing.T) {
	now := correctnessClock(t)
	classic := correctnessFile(t, false, "1s:5m,10s:1h", 0.5)
	compressed := correctnessFile(t, true, "1s:5m,10s:1h", 0.5)
	var input []*TimeSeriesPoint
	for i := 0; i < 25; i++ {
		input = append(input, &TimeSeriesPoint{Time: *now - 100 + i, Value: float64(i)})
	}
	for _, w := range []*Whisper{classic, compressed} {
		correctnessUpdate(t, w, input)
		if err := w.UpdateConfig(MustParseRetentionDefs("1s:5m,10s:1h"), Sum, 0.5, w.opts); err != nil {
			t.Fatal(err)
		}
	}
	correctnessCompare(t, classic, compressed, *now-301, *now)
	input = []*TimeSeriesPoint{{Time: *now - 75, Value: 25}, {Time: *now - 74, Value: 26}}
	correctnessUpdate(t, classic, input)
	correctnessUpdate(t, compressed, input)
	correctnessCompare(t, classic, compressed, *now-301, *now)
	if err := compressed.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}
