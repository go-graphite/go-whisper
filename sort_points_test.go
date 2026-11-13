package whisper

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

// sortDataPointsByInterval must return exactly what sort.Slice returned,
// including the order of duplicate intervals (told apart by value bits).
func TestSortDataPointsByIntervalMatchesSortSlice(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for c := 0; c < 20000; c++ {
		n := r.Intn(300)
		points := make([]dataPoint, n)
		base := 60 * (1 + r.Intn(1000))
		mode := r.Intn(5)
		for i := range points {
			interval := base + 60*i
			switch mode {
			case 0: // random with duplicates
				interval = 60 * r.Intn(n/2+1)
			case 1: // distinct, shuffled below
			}
			points[i] = dataPoint{interval: interval, value: math.Float64frombits(0x7ff8000000000000 | uint64(c)<<20 | uint64(i))}
		}
		switch mode {
		case 1:
			r.Shuffle(n, func(i, j int) { points[i], points[j] = points[j], points[i] })
		case 2, 3: // ring read: rotated, sometimes with a duplicate or overlap
			if n > 1 {
				k := r.Intn(n)
				points = append(points[k:], points[:k]...)
				if mode == 3 {
					points[r.Intn(n)].interval = points[r.Intn(n)].interval
				}
			}
		}
		want := append([]dataPoint(nil), points...)
		sort.Slice(want, func(i, j int) bool { return want[i].interval < want[j].interval })
		got := append([]dataPoint(nil), points...)
		sortDataPointsByInterval(got)
		for i := range want {
			if want[i].interval != got[i].interval || math.Float64bits(want[i].value) != math.Float64bits(got[i].value) {
				t.Fatalf("case %d mode %d n %d: index %d want %d/%x got %d/%x", c, mode, n, i, want[i].interval, math.Float64bits(want[i].value), got[i].interval, math.Float64bits(got[i].value))
			}
		}
	}
}

func BenchmarkReadRingSort(b *testing.B) {
	const n = 43200
	src := make([]dataPoint, n)
	for i := range src {
		src[i] = dataPoint{interval: 60 * (1 + (i+n/3)%n), value: float64(i)}
	}
	buf := make([]dataPoint, n)
	b.Run("sort.Slice", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			copy(buf, src)
			sort.Slice(buf, func(i, j int) bool { return buf[i].interval < buf[j].interval })
		}
	})
	b.Run("sortDataPointsByInterval", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			copy(buf, src)
			sortDataPointsByInterval(buf)
		}
	})
}
