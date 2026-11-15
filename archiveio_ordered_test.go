package whisper

import (
	"cmp"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// mapArchivePoints is the previous archivePoints merge: a timestamp map where
// the last stored point wins and sidecar points only fill missing timestamps,
// then a sort. It is the oracle for mergeOrderedArchivePoints.
func mapArchivePoints(stored []dataPoint, side []TimeSeriesPoint) []TimeSeriesPoint {
	byTime := make(map[int]float64, len(stored))
	for _, point := range stored {
		if point.interval > 0 {
			byTime[point.interval] = point.value
		}
	}
	for _, point := range side {
		if _, ok := byTime[point.Time]; !ok {
			byTime[point.Time] = point.Value
		}
	}
	result := make([]TimeSeriesPoint, 0, len(byTime))
	for timestamp, value := range byTime {
		result = append(result, TimeSeriesPoint{Time: timestamp, Value: value})
	}
	slices.SortFunc(result, func(a, b TimeSeriesPoint) int { return cmp.Compare(a.Time, b.Time) })
	return result
}

func sameSeriesBits(a, b []TimeSeriesPoint) bool {
	return slices.EqualFunc(a, b, func(x, y TimeSeriesPoint) bool {
		return x.Time == y.Time && math.Float64bits(x.Value) == math.Float64bits(y.Value)
	})
}

// TestMergeOrderedArchivePointsMatchesMap compares the ordered merge with the
// map oracle bit for bit, including duplicate timestamps, empty slots, NaN
// payloads and input that is not in time order.
func TestMergeOrderedArchivePointsMatchesMap(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	value := func() float64 {
		switch r.Intn(8) {
		case 0:
			return math.Float64frombits(0x7ff8000000000000 | uint64(r.Intn(1<<20)))
		case 1:
			return 0
		default:
			return r.NormFloat64() * 1e6
		}
	}
	for iteration := 0; iteration < 20000; iteration++ {
		span := 1 + r.Intn(200)
		stored := make([]dataPoint, r.Intn(64))
		t0 := 1700000000 - r.Intn(2)*1700000000 // sometimes include non-positive intervals
		for i := range stored {
			interval := t0 + r.Intn(span) - span/8
			if r.Intn(10) == 0 {
				interval = 0
			}
			stored[i] = dataPoint{interval: interval, value: value()}
		}
		if r.Intn(2) == 0 {
			slices.SortStableFunc(stored, func(a, b dataPoint) int { return cmp.Compare(a.interval, b.interval) })
		}
		side := make([]TimeSeriesPoint, r.Intn(32))
		for i := range side {
			side[i] = TimeSeriesPoint{Time: t0 + r.Intn(span) - span/8, Value: value()}
		}
		if r.Intn(2) == 0 {
			slices.SortStableFunc(side, func(a, b TimeSeriesPoint) int { return cmp.Compare(a.Time, b.Time) })
		}
		storedCopy, sideCopy := slices.Clone(stored), slices.Clone(side)
		want := mapArchivePoints(stored, side)
		got := mergeOrderedArchivePoints(stored, side)
		if !sameSeriesBits(want, got) {
			t.Fatalf("iteration %d: ordered merge differs\nstored=%v\nside=%v\nwant=%v\ngot=%v", iteration, stored, side, want, got)
		}
		if !slices.EqualFunc(storedCopy, stored, func(x, y dataPoint) bool {
			return x.interval == y.interval && math.Float64bits(x.value) == math.Float64bits(y.value)
		}) || !sameSeriesBits(sideCopy, side) {
			t.Fatalf("iteration %d: ordered merge modified its input", iteration)
		}
	}
}

// TestArchivePointsClassicAndCompressedSameSeries writes the same in-order and
// late points to a classic file and to a compressed file whose late points go
// to the out-of-order sidecar. Both layouts must read back the same series.
func TestArchivePointsClassicAndCompressedSameSeries(t *testing.T) {
	retentions := MustParseRetentionDefs("60s:2d")
	dir := t.TempDir()
	classic, err := CreateWithOptions(filepath.Join(dir, "classic.wsp"), retentions, Last, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer classic.Close()
	compressed, err := CreateWithOptions(filepath.Join(dir, "compressed.wsp"), retentions, Last, 0, &Options{Compressed: true, PointsPerBlock: 100, OutOfOrder: true})
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()
	r := rand.New(rand.NewSource(2))
	start := int(time.Now().Unix()) - 86400
	start -= start % 60
	var late []*TimeSeriesPoint
	for batch := 0; batch < 40; batch++ {
		var points []*TimeSeriesPoint
		for i := 0; i < 60; i++ {
			ts := start + (batch*60+i)*60
			if r.Intn(5) == 0 {
				continue // gaps
			}
			p := &TimeSeriesPoint{Time: ts, Value: math.Round(r.NormFloat64() * 1000)}
			if r.Intn(7) == 0 && batch > 0 {
				late = append(late, p)
				continue
			}
			points = append(points, p)
		}
		for _, w := range []*Whisper{classic, compressed} {
			if err := w.UpdateMany(points); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Late points arrive after newer blocks were written.
	for _, w := range []*Whisper{classic, compressed} {
		if err := w.UpdateMany(late); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "compressed.wsp") + oooSuffix); err != nil {
		t.Fatalf("no late point reached the compressed sidecar: %v", err)
	}
	want, err := classic.ArchivePoints(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, read := range []func(int) ([]TimeSeriesPoint, error){compressed.ArchivePoints, compressed.ArchivePointsWithRollups} {
		got, err := read(0)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) == 0 || !sameSeriesBits(want, got) {
			t.Fatalf("compressed series differs from classic: %d vs %d points", len(got), len(want))
		}
	}
}

// BenchmarkMergeOrderedArchivePoints measures the merge on a full, ordered
// one-minute archive with a small sidecar, the shape a transfer decodes.
func BenchmarkMergeOrderedArchivePoints(b *testing.B) {
	stored := make([]dataPoint, 43200)
	for i := range stored {
		stored[i] = dataPoint{interval: 1700000000 + 60*i, value: float64(i)}
	}
	side := []TimeSeriesPoint{{Time: 1700000000 + 30, Value: 1}, {Time: 1700000000 + 600, Value: 2}}
	b.Run("map", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mapArchivePoints(stored, side)
		}
	})
	b.Run("ordered", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mergeOrderedArchivePoints(stored, side)
		}
	})
}
