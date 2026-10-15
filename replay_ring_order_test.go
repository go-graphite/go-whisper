package whisper

import (
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
)

// Keep the previous physical-order scan and sort as an independent oracle.
func replayArchivePointsOracle(w *Whisper, index int) []extraPoint {
	archive := w.archives[index]
	data := w.file.(*memFile).data[archive.Offset():archive.End()]
	count := 0
	for offset := 0; offset < len(data); offset += PointSize {
		if unpackInt(data[offset:]) > 0 {
			count++
		}
	}
	points := make([]extraPoint, 0, count)
	for offset := 0; offset < len(data); offset += PointSize {
		point := unpackDataPoint(data[offset:])
		if point.interval > 0 {
			points = append(points, extraPoint{dataPoint: point, replace: true})
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].interval < points[j].interval })
	return points
}

func replayRingFixture(tb testing.TB, slots int) *Whisper {
	tb.Helper()
	name := filepath.Join(tb.TempDir(), "replay.wsp")
	w, err := CreateWithOptions(name, Retentions{{secondsPerPoint: 1, numberOfPoints: slots}}, Last, 0,
		&Options{InMemory: true, Sparse: true})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = w.Close(); releaseMemFile(name) })
	return w
}

func assertReplayRingOracle(t *testing.T, w *Whisper) {
	t.Helper()
	want, got := replayArchivePointsOracle(w, 0), w.replayArchivePoints(0)
	if len(got) != len(want) {
		t.Fatalf("point count: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].interval != want[i].interval || got[i].replace != want[i].replace ||
			math.Float64bits(got[i].value) != math.Float64bits(want[i].value) {
			t.Fatalf("point %d: got %+v (%x), want %+v (%x)", i, got[i], math.Float64bits(got[i].value), want[i], math.Float64bits(want[i].value))
		}
	}
}

func TestReplayArchivePointsRingOracle(t *testing.T) {
	cases := [][]int{
		{}, {0}, {1}, {0, 0, 100, 0}, {103, 104, 100, 101, 102},
		{0, 104, 0, 101, 0}, {103, 4, 205, 106, 7}, {maxInt - 1, 0, 1, maxInt},
	}
	for i, intervals := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			slots := len(intervals)
			if slots == 0 {
				slots = 1
			}
			w := replayRingFixture(t, slots)
			archive := w.archives[0]
			data := w.file.(*memFile).data[archive.Offset():archive.End()]
			for j, interval := range intervals {
				packInt(data[j*PointSize:], interval, 0)
				packFloat64(data[j*PointSize:], math.Float64frombits(uint64(j)*0x7ff8000000000001), IntSize)
			}
			assertReplayRingOracle(t, w)
		})
	}
	rng := rand.New(rand.NewSource(1))
	w := replayRingFixture(t, 127)
	archive := w.archives[0]
	data := w.file.(*memFile).data[archive.Offset():archive.End()]
	for trial := 0; trial < 1000; trial++ {
		for slot := 0; slot < 127; slot++ {
			// Each slot can retain a different generation, including an empty
			// slot; this exercises the fallback for sparse, old ring contents.
			interval := 0
			if rng.Intn(4) != 0 {
				interval = 1 + slot + 127*rng.Intn(8)
			}
			packInt(data[slot*PointSize:], interval, 0)
			packFloat64(data[slot*PointSize:], math.Float64frombits(rng.Uint64()), IntSize)
		}
		assertReplayRingOracle(t, w)
	}
}

func BenchmarkReplayArchivePointsRing(b *testing.B) {
	for _, sparse := range []bool{false, true} {
		b.Run(fmt.Sprintf("sparse=%t", sparse), func(b *testing.B) {
			const slots = 21600
			w := replayRingFixture(b, slots)
			archive := w.archives[0]
			data := w.file.(*memFile).data[archive.Offset():archive.End()]
			for slot := 0; slot < slots; slot++ {
				interval := 1700000000 + (slot+slots/3)%slots
				if sparse && slot%3 == 0 {
					interval -= (1 + slot%5) * slots
				}
				packInt(data[slot*PointSize:], interval, 0)
				packFloat64(data[slot*PointSize:], float64(slot), IntSize)
			}
			for _, oracle := range []bool{true, false} {
				b.Run(fmt.Sprintf("oracle=%t", oracle), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if oracle {
							_ = replayArchivePointsOracle(w, 0)
						} else {
							_ = w.replayArchivePoints(0)
						}
					}
				})
			}
		})
	}
}
