package whisper

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

// appendMergeWant is the importer's expected union: the newer copy wins equal
// timestamps, then a newer interval retires an older one in the same ring slot.
func appendMergeWant(base, newer []TimeSeriesPoint, step, count int) []TimeSeriesPoint {
	byTime := make(map[int]float64, len(base)+len(newer))
	for _, p := range base {
		byTime[p.Time] = p.Value
	}
	for _, p := range newer {
		byTime[p.Time] = p.Value
	}
	owner := make(map[int]int, len(byTime))
	for ts := range byTime {
		slot := ts / step % count
		if ts > owner[slot] {
			owner[slot] = ts
		}
	}
	var out []TimeSeriesPoint
	for ts, v := range byTime {
		if owner[ts/step%count] == ts {
			out = append(out, TimeSeriesPoint{Time: ts, Value: v})
		}
	}
	sortTimeSeriesPoints(out)
	return out
}

func sortTimeSeriesPoints(points []TimeSeriesPoint) {
	for i := 1; i < len(points); i++ {
		for j := i; j > 0 && points[j].Time < points[j-1].Time; j-- {
			points[j], points[j-1] = points[j-1], points[j]
		}
	}
}

func writeAppendFixture(t *testing.T, path string, r *rand.Rand, from, until int, now time.Time) *Whisper {
	t.Helper()
	w, err := CreateWithOptions(path, MustParseRetentionDefs("60s:6h,600s:2d,3600s:7d"), Average, 0, &Options{Compressed: true, PointsPerBlock: 64, OutOfOrder: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	var late []*TimeSeriesPoint
	for ts := from; ts < until; {
		var batch []*TimeSeriesPoint
		for i := 0; i < 30 && ts < until; i, ts = i+1, ts+60 {
			if r.Intn(6) == 0 {
				continue // gap
			}
			p := &TimeSeriesPoint{Time: ts, Value: math.Round(r.NormFloat64()*1000) / 8}
			if r.Intn(40) == 0 {
				p.Value = math.NaN()
			}
			if r.Intn(9) == 0 && ts > from+3600 {
				late = append(late, p) // arrives after newer points
				continue
			}
			batch = append(batch, p)
		}
		r.Shuffle(len(batch), func(i, j int) { batch[i], batch[j] = batch[j], batch[i] })
		if err := w.UpdateMany(batch); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.UpdateMany(late); err != nil {
		t.Fatal(err)
	}
	if err := w.MergeOutOfOrder(); err != nil {
		t.Fatal(err)
	}
	return w
}

func sameArchivePoints(a, b []TimeSeriesPoint) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Time != b[i].Time || math.Float64bits(a[i].Value) != math.Float64bits(b[i].Value) {
			return false
		}
	}
	return true
}

// TestAppendArchivePointsMergesNewerCopy merges a newer copy into an older
// one by appending and installing the newer buffers. With a handoff on the
// coarsest window boundary the result must equal the expected union bit for
// bit in every archive, including NaNs, gaps, late points and ring aliases.
func TestAppendArchivePointsMergesNewerCopy(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	const seeds = 40
	fallbacks := 0
	defer func() {
		t.Logf("%d/%d merges exact, %d need the importer's fallback", seeds-fallbacks, seeds, fallbacks)
		if fallbacks > seeds/4 {
			t.Errorf("%d/%d merges need a fallback", fallbacks, seeds)
		}
	}()
	for seed := int64(0); seed < seeds; seed++ {
		r := rand.New(rand.NewSource(seed))
		handoff := int(now.Unix()) - 3600*(1+r.Intn(30))
		handoff -= handoff % 3600
		dir := t.TempDir()
		base := writeAppendFixture(t, filepath.Join(dir, "base.wsp"), r, handoff-3600*(10+r.Intn(150)), handoff, now)
		newer := writeAppendFixture(t, filepath.Join(dir, "newer.wsp"), r, handoff, int(now.Unix()), now)
		var want, newerPoints [][]TimeSeriesPoint
		for i, a := range base.archives {
			bp, err := base.ArchivePointsWithRollups(i)
			if err != nil {
				t.Fatal(err)
			}
			np, err := newer.ArchivePointsWithRollups(i)
			if err != nil {
				t.Fatal(err)
			}
			newerPoints = append(newerPoints, np)
			want = append(want, appendMergeWant(bp, np, a.secondsPerPoint, a.numberOfPoints))
		}
		baseWatermarks := make([]int, len(base.archives))
		for i := range base.archives {
			from, err := base.ArchiveBlockWatermark(i)
			if err != nil {
				t.Fatal(err)
			}
			baseWatermarks[i] = from
			until, err := newer.ArchiveBlockWatermark(i)
			if err != nil {
				t.Fatal(err)
			}
			// Points above the newer copy's blocks come from its buffers,
			// which ReplaceBuffers installs; append everything else.
			virtual := map[int]bool{}
			for _, p := range newerPoints[i] {
				if p.Time > until {
					virtual[p.Time] = true
				}
			}
			var tail []TimeSeriesPoint
			for _, p := range want[i] {
				if p.Time > from && !virtual[p.Time] {
					tail = append(tail, p)
				}
			}
			if err := base.AppendArchivePoints(i, tail); err != nil {
				t.Fatalf("seed %d archive %d: %v", seed, i, err)
			}
		}
		if err := base.ReplaceBuffers(newer); err != nil {
			t.Fatal(err)
		}
		// Read back through a fresh handle: header, blocks and buffers persisted.
		path := base.file.Name()
		base.Close()
		newer.Close()
		merged, err := OpenWithOptions(path, &Options{OutOfOrder: true, Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		mismatch := false
		for i := range merged.archives {
			got, err := merged.ArchivePointsWithRollups(i)
			if err != nil {
				t.Fatal(err)
			}
			if sameArchivePoints(got, want[i]) {
				continue
			}
			// The only accepted difference: rollups or buffered points the
			// base held only in its buffers at or below its block watermark
			// cannot be appended. The importer detects it by reading back.
			for _, d := range diffArchiveTimes(got, want[i]) {
				if d > baseWatermarks[i] {
					t.Fatalf("seed %d archive %d: differs at %d above base watermark %d: %v", seed, i, d, baseWatermarks[i], diffArchivePoints(got, want[i]))
				}
			}
			mismatch = true
		}
		if mismatch {
			fallbacks++
		}
		merged.Close()
	}
}

func TestAppendArchivePointsRejectsOldOrOffGrid(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	w, err := CreateWithOptions(filepath.Join(t.TempDir(), "a.wsp"), MustParseRetentionDefs("60s:6h,600s:2d"), Average, 0, &Options{Compressed: true, PointsPerBlock: 64, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ts := int(now.Unix()) - 600
	ts -= ts % 600
	if err := w.AppendArchivePoints(1, []TimeSeriesPoint{{Time: ts, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendArchivePoints(1, []TimeSeriesPoint{{Time: ts, Value: 2}}); !errors.Is(err, ErrArchiveAppendOrder) {
		t.Fatalf("equal timestamp: %v", err)
	}
	if err := w.AppendArchivePoints(1, []TimeSeriesPoint{{Time: ts + 60, Value: 2}}); err == nil {
		t.Fatal("off-grid point accepted")
	}
	if err := w.AppendArchivePoints(0, []TimeSeriesPoint{{Time: ts + 120, Value: 2}, {Time: ts + 60, Value: 3}}); !errors.Is(err, ErrArchiveAppendOrder) {
		t.Fatalf("decreasing points: %v", err)
	}
	if got, _ := w.ArchivePoints(0); len(got) != 0 {
		t.Fatalf("rejected append wrote %v", got)
	}
	if wm, _ := w.ArchiveBlockWatermark(1); wm != ts {
		t.Fatalf("watermark %d, want %d", wm, ts)
	}
}

func diffArchivePoints(got, want []TimeSeriesPoint) []string {
	g := map[int]float64{}
	for _, p := range got {
		g[p.Time] = p.Value
	}
	w := map[int]float64{}
	for _, p := range want {
		w[p.Time] = p.Value
	}
	var out []string
	for ts, v := range w {
		if gv, ok := g[ts]; !ok {
			out = append(out, fmt.Sprintf("missing %d=%v", ts, v))
		} else if math.Float64bits(gv) != math.Float64bits(v) {
			out = append(out, fmt.Sprintf("value %d got %v want %v", ts, gv, v))
		}
	}
	for ts, v := range g {
		if _, ok := w[ts]; !ok {
			out = append(out, fmt.Sprintf("extra %d=%v", ts, v))
		}
	}
	return out
}

func diffArchiveTimes(got, want []TimeSeriesPoint) []int {
	g := map[int]uint64{}
	for _, p := range got {
		g[p.Time] = math.Float64bits(p.Value)
	}
	w := map[int]uint64{}
	for _, p := range want {
		w[p.Time] = math.Float64bits(p.Value)
	}
	var out []int
	for ts, v := range w {
		if gv, ok := g[ts]; !ok || gv != v {
			out = append(out, ts)
		}
	}
	for ts := range g {
		if _, ok := w[ts]; !ok {
			out = append(out, ts)
		}
	}
	return out
}
