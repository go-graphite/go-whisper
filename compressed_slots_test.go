package whisper

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
)

// Preserve the pre-optimisation map algorithm as the differential oracle.
func referenceCompressedSlotOwners(archive *archiveInfo, points, newer []dataPoint) []dataPoint {
	latest := make(map[int]int)
	for _, ps := range [][]dataPoint{points, newer} {
		for _, p := range ps {
			slot := mod(p.interval/archive.secondsPerPoint, archive.numberOfPoints)
			if p.interval > latest[slot] {
				latest[slot] = p.interval
			}
		}
	}
	kept := points[:0]
	for _, p := range points {
		if latest[mod(p.interval/archive.secondsPerPoint, archive.numberOfPoints)] == p.interval {
			kept = append(kept, p)
		}
	}
	return kept
}

func sameCompressedSlotPoints(a, b []dataPoint) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].interval != b[i].interval || math.Float64bits(a[i].value) != math.Float64bits(b[i].value) {
			return false
		}
	}
	return true
}

func TestCompressedSlotOwnersParity(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for _, slots := range []int{1, 2, 31, 63, 64, 65, 256, 1024, maxCompressedSlotOwners, maxCompressedSlotOwners + 1, 1 << 28} {
		t.Run(fmt.Sprint(slots), func(t *testing.T) {
			archive := &archiveInfo{Retention: Retention{secondsPerPoint: 60, numberOfPoints: slots}}
			count := slots * 2
			if slots > maxCompressedSlotOwners+1 {
				count = 256 // a huge sparse ring must not allocate dense scratch
			}
			points := make([]dataPoint, count)
			newer := make([]dataPoint, count/3)
			bound := slots
			if slots > maxCompressedSlotOwners+1 {
				bound = 10000
			}
			for i := range points {
				points[i] = dataPoint{interval: (rng.Intn(bound*6) - bound*2) * 60, value: math.Float64frombits(rng.Uint64())}
			}
			for i := range newer {
				newer[i] = dataPoint{interval: (rng.Intn(bound*6) - bound) * 60, value: float64(i)}
			}
			got := filterCompressedSlotOwners(archive, append([]dataPoint(nil), points...), newer)
			want := referenceCompressedSlotOwners(archive, append([]dataPoint(nil), points...), newer)
			if !sameCompressedSlotPoints(got, want) {
				t.Fatalf("slot filtering differs: got %d points, want %d", len(got), len(want))
			}
		})
	}
}

func TestCompressedSlotOwnersSparseParity(t *testing.T) {
	archive := &archiveInfo{Retention: Retention{secondsPerPoint: 60, numberOfPoints: 4096}}
	points := make([]dataPoint, 128)
	newer := make([]dataPoint, 8)
	for i := range points {
		points[i] = dataPoint{interval: i * 60, value: float64(i)}
	}
	for i := range newer {
		newer[i] = dataPoint{interval: (i + 4096) * 60, value: float64(-i)}
	}
	want := referenceCompressedSlotOwners(archive, append([]dataPoint(nil), points...), newer)
	got := filterCompressedSlotOwners(archive, points, newer)
	if !sameCompressedSlotPoints(got, want) {
		t.Fatal("sparse slot filtering differs from the reference")
	}
}

func TestCompressedSlotOwnersReusedStorage(t *testing.T) {
	for round := 0; round < 100; round++ {
		for _, slots := range []int{512, 31, 1024, 128} {
			archive := &archiveInfo{Retention: Retention{secondsPerPoint: 1, numberOfPoints: slots}}
			points := make([]dataPoint, slots*2)
			for i := range points {
				points[i] = dataPoint{interval: i + round*slots*2, value: float64(i)}
			}
			want := referenceCompressedSlotOwners(archive, append([]dataPoint(nil), points...), nil)
			got := filterCompressedSlotOwners(archive, points, nil)
			if !sameCompressedSlotPoints(got, want) {
				t.Fatalf("stale owners leaked into round %d, ring %d", round, slots)
			}
		}
	}
}

func TestCompressedSlotOwnersNoWarmAllocations(t *testing.T) {
	archive := &archiveInfo{Retention: Retention{secondsPerPoint: 1, numberOfPoints: 1024}}
	source := make([]dataPoint, 2048)
	for i := range source {
		source[i] = dataPoint{interval: i + 1, value: float64(i)}
	}
	points := make([]dataPoint, len(source))
	allocs := testing.AllocsPerRun(100, func() {
		copy(points, source)
		filterCompressedSlotOwners(archive, points, nil)
	})
	if allocs != 0 {
		t.Fatalf("warm dense filtering allocates: %g allocations per call", allocs)
	}
}

func TestCompressedSlotOwnersConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	for worker := 0; worker < 96; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(worker)))
			archive := &archiveInfo{Retention: Retention{secondsPerPoint: 10, numberOfPoints: 256}}
			for round := 0; round < 20; round++ {
				points := make([]dataPoint, 512)
				for i := range points {
					points[i] = dataPoint{interval: rng.Intn(1024) * 10, value: float64(worker)}
				}
				want := referenceCompressedSlotOwners(archive, append([]dataPoint(nil), points...), nil)
				got := filterCompressedSlotOwners(archive, points, nil)
				if !sameCompressedSlotPoints(got, want) {
					t.Errorf("worker %d round %d observed another filter's owners", worker, round)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
}
