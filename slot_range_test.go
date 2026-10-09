package whisper

import (
	"path/filepath"
	"testing"
)

func TestReadSlotIntervalsMatchesPointReads(t *testing.T) {
	w, err := Create(filepath.Join(t.TempDir(), "slots.wsp"), []*Retention{{secondsPerPoint: 1, numberOfPoints: 100}}, Average, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	const start = 1000
	var points []*TimeSeriesPoint
	for i := 0; i < 130; i++ { // wrap the 100-slot archive
		points = append(points, &TimeSeriesPoint{Time: start + i, Value: float64(i)})
	}
	if err := w.UpdateManyForArchive(points, 1); err != nil {
		t.Fatal(err)
	}
	archive := w.archives[0]
	base := w.getBaseInterval(archive)
	for _, intervals := range [][]int{
		{start + 120, start + 121, start + 125},    // one narrow range
		{start + 95, start + 99, start + 101},      // wraps the circular buffer
		{start + 129},                              // single point
		{start + 40, start + 129, start + 130 - 1}, // unwritten-lap aliases
	} {
		got, err := w.readSlotIntervals(archive, base, intervals)
		if err != nil {
			t.Fatal(err)
		}
		for i, interval := range intervals {
			want, err := w.readInt(archive.PointOffset(base, interval))
			if err != nil {
				t.Fatal(err)
			}
			if got[i] != want {
				t.Fatalf("slot of %d = %d, want %d", interval, got[i], want)
			}
		}
	}
}
