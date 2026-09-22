package whisper

import (
	"errors"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func assertValues(t *testing.T, ts *TimeSeries, want []float64) {
	t.Helper()

	got := ts.Values()
	if len(got) != len(want) {
		t.Fatalf("got %d values %v; want %d values %v", len(got), got, len(want), want)
	}

	for i := range want {
		if math.IsNaN(want[i]) {
			if !math.IsNaN(got[i]) {
				t.Errorf("values[%d] = %v; want NaN", i, got[i])
			}
			continue
		}
		if got[i] != want[i] {
			t.Errorf("values[%d] = %v; want %v", i, got[i], want[i])
		}
	}
}

// newSingleRetentionOOO creates a 1s/7200 compressed file and writes three
// points two seconds apart, leaving holes between them and putting the block
// watermark at base+4.
func newSingleRetentionOOO(t *testing.T, outOfOrder bool) (cwhisper *Whisper, path string, base int) {
	t.Helper()

	path = filepath.Join(t.TempDir(), "ooo.cwsp")
	cwhisper, err := CreateWithOptions(
		path,
		[]*Retention{{secondsPerPoint: 1, numberOfPoints: 7200}},
		Sum, 0,
		&Options{
			Compressed: true, PointsPerBlock: 1200,
			IgnoreNowOnWrite: true, OutOfOrder: outOfOrder,
		},
	)
	if err != nil {
		t.Fatalf("create: %s", err)
	}

	base = int(time.Now().Unix()) - 3600
	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{
		{Time: base + 0, Value: 1},
		{Time: base + 2, Value: 2},
		{Time: base + 4, Value: 3},
	}); err != nil {
		t.Fatalf("update: %s", err)
	}

	return cwhisper, path, base
}

// newTwoRetentionOOO creates a 1s/300 + 10s/600 compressed file and fills five
// coarse windows starting at base with on-time data, leaving a single hole at
// base+hole.
//
// Five windows is not arbitrary. The base archive's buffer holds two units of
// one coarse window each, and a unit is only flushed - and only then propagated
// into the coarse archive - when the unit two windows later reuses it. So the
// last two windows are still buffered at the end of the fill, and only windows
// 0, 1 and 2 are actually stored in the coarse archive. Anything read from the
// last two comes from live aggregation over the buffer instead, which is a
// different code path and not what these tests are about.
func newTwoRetentionOOO(t *testing.T, hole int, xff float32) (cwhisper *Whisper, path string, base int) {
	t.Helper()

	p := filepath.Join(t.TempDir(), "coarse.cwsp")
	w, err := CreateWithOptions(
		p,
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 300},
			{secondsPerPoint: 10, numberOfPoints: 600},
		},
		Sum, xff,
		&Options{Compressed: true, PointsPerBlock: 100, OutOfOrder: true},
	)
	if err != nil {
		t.Fatalf("create: %s", err)
	}

	now := int(time.Now().Unix())
	b := now - 200
	b -= b % 10

	var points []*TimeSeriesPoint
	for i := 0; i < 50; i++ {
		if i == hole {
			continue
		}
		points = append(points, &TimeSeriesPoint{Time: b + i, Value: 1})
	}
	if err := w.UpdateMany(points); err != nil {
		t.Fatalf("fill: %s", err)
	}

	return w, p, b
}

// coarseValueAt returns the coarse-archive value covering interval want, read
// through a query old enough to resolve to that archive.
func coarseValueAt(t *testing.T, w *Whisper, want int) float64 {
	t.Helper()

	now := int(time.Now().Unix())
	ts, err := w.Fetch(now-500, now)
	if err != nil {
		t.Fatalf("fetch: %s", err)
	}
	if got := ts.Step(); got != 10 {
		t.Fatalf("step = %d; want 10 (query should resolve to the coarse archive)", got)
	}

	for _, p := range ts.Points() {
		if p.Time == want {
			return p.Value
		}
	}
	t.Fatalf("no coarse point at %d in %v", want, ts.Points())

	return 0
}

// A point older than the block watermark lands in a slot the compressed file
// never wrote. Without the sidecar it is lost; with it, it is served.
func TestOutOfOrderSidecarFillsHole(t *testing.T) {
	tests := []struct {
		name       string
		outOfOrder bool
		wantHole   float64
	}{
		{name: "disabled", outOfOrder: false, wantHole: math.NaN()},
		{name: "enabled", outOfOrder: true, wantHole: 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cwhisper, path, base := newSingleRetentionOOO(t, tt.outOfOrder)
			defer cwhisper.Close()

			if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 7}}); err != nil {
				t.Fatalf("late update: %s", err)
			}

			// the encoder rejects it either way
			if got := cwhisper.GetDiscardedPointsSinceOpen(); got != 1 {
				t.Errorf("GetDiscardedPointsSinceOpen() = %d; want 1", got)
			}

			wantDiverted := uint32(0)
			if tt.outOfOrder {
				wantDiverted = 1
			}
			if got := cwhisper.OutOfOrderPoints; got != wantDiverted {
				t.Errorf("OutOfOrderPoints = %d; want %d", got, wantDiverted)
			}

			_, err := os.Stat(path + oooSuffix)
			if gotSidecar := err == nil; gotSidecar != tt.outOfOrder {
				t.Errorf("sidecar exists = %v; want %v", gotSidecar, tt.outOfOrder)
			}

			ts, err := cwhisper.Fetch(base-1, base+5)
			if err != nil {
				t.Fatalf("fetch: %s", err)
			}
			// archiveInfo.Interval rounds up to the end of the bucket, so the
			// window is [base+0, base+6)
			assertValues(t, ts, []float64{1, tt.wantHole, 2, math.NaN(), 3, math.NaN()})
		})
	}
}

// Where the compressed file already holds a value, it stays authoritative: a
// late correction does not overwrite on-time data.
func TestOutOfOrderMainWinsOverSidecar(t *testing.T) {
	cwhisper, _, base := newSingleRetentionOOO(t, true)
	defer cwhisper.Close()

	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: base + 2, Value: 99}}); err != nil {
		t.Fatalf("late update: %s", err)
	}
	if got := cwhisper.OutOfOrderPoints; got != 1 {
		t.Fatalf("OutOfOrderPoints = %d; want 1", got)
	}

	ts, err := cwhisper.Fetch(base-1, base+5)
	if err != nil {
		t.Fatalf("fetch: %s", err)
	}
	assertValues(t, ts, []float64{1, math.NaN(), 2, math.NaN(), 3, math.NaN()})
}

// A sidecar written by one handle is picked up by the next one, including when
// that handle has out-of-order writes turned off.
func TestOutOfOrderSidecarSurvivesReopen(t *testing.T) {
	cwhisper, path, base := newSingleRetentionOOO(t, true)

	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 7}}); err != nil {
		t.Fatalf("late update: %s", err)
	}
	if err := cwhisper.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}

	reopened, err := OpenWithOptions(path, &Options{OutOfOrder: false})
	if err != nil {
		t.Fatalf("reopen: %s", err)
	}
	defer reopened.Close()

	if reopened.oooPath == "" {
		t.Fatal("reopened handle did not detect the sidecar")
	}

	ts, err := reopened.Fetch(base-1, base+5)
	if err != nil {
		t.Fatalf("fetch: %s", err)
	}
	assertValues(t, ts, []float64{1, 7, 2, math.NaN(), 3, math.NaN()})
}

func TestOutOfOrderReaderDiscoversSidecarCreatedAfterOpen(t *testing.T) {
	writer, path, base := newSingleRetentionOOO(t, true)
	defer writer.Close()

	reader, err := OpenWithOptions(path, &Options{})
	if err != nil {
		t.Fatalf("open reader: %s", err)
	}
	defer reader.Close()

	if err := writer.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 7}}); err != nil {
		t.Fatalf("late update: %s", err)
	}

	ts, err := reader.Fetch(base-1, base+5)
	if err != nil {
		t.Fatalf("fetch: %s", err)
	}
	assertValues(t, ts, []float64{1, 7, 2, math.NaN(), 3, math.NaN()})
}

func TestOutOfOrderReaderReportsSidecarStatError(t *testing.T) {
	reader, path, base := newSingleRetentionOOO(t, false)
	defer reader.Close()

	sidecarPath := path + oooSuffix
	if err := os.Symlink(sidecarPath, sidecarPath); err != nil {
		t.Fatalf("create looping sidecar symlink: %s", err)
	}

	if _, err := reader.Fetch(base-1, base+5); err == nil {
		t.Fatal("fetch succeeded despite sidecar stat error")
	}
}

// Backfill far enough into the past that UpdateMany routes the point to a
// coarser archive; it must be diverted against that archive, not re-routed by
// age into the base archive of the sidecar.
func TestOutOfOrderBackfillIntoCoarseArchive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "coarse.cwsp")
	cwhisper, err := CreateWithOptions(
		path,
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 120},
			{secondsPerPoint: 10, numberOfPoints: 1000},
		},
		Sum, 0,
		&Options{Compressed: true, PointsPerBlock: 1200, OutOfOrder: true},
	)
	if err != nil {
		t.Fatalf("create: %s", err)
	}
	defer cwhisper.Close()

	// fill the base archive so the buffer flushes and propagates into the
	// coarse archive, pushing its watermark up to roughly now
	now := int(time.Now().Unix())
	var points []*TimeSeriesPoint
	for i := 119; i >= 0; i-- {
		points = append(points, &TimeSeriesPoint{Time: now - i, Value: 1})
	}
	if err := cwhisper.UpdateMany(points); err != nil {
		t.Fatalf("fill: %s", err)
	}

	coarse := cwhisper.archives[1]
	if coarse.cblock.pn1.interval == 0 {
		t.Fatal("coarse archive got no propagated data; test setup is wrong")
	}

	// older than the base archive's 120s retention, so it routes to the
	// coarse archive, and older than that archive's watermark, so it is rejected
	backfill := now - 500
	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: backfill, Value: 42}}); err != nil {
		t.Fatalf("backfill: %s", err)
	}
	if got := cwhisper.OutOfOrderPoints; got != 1 {
		t.Fatalf("OutOfOrderPoints = %d; want 1", got)
	}

	ts, err := cwhisper.Fetch(now-600, now-400)
	if err != nil {
		t.Fatalf("fetch: %s", err)
	}
	if got := ts.Step(); got != 10 {
		t.Fatalf("step = %d; want 10 (query should resolve to the coarse archive)", got)
	}

	want := backfill - mod(backfill, 10)
	var found bool
	for _, p := range ts.Points() {
		if p.Time == want {
			found = true
			if p.Value != 42 {
				t.Errorf("value at %d = %v; want 42", p.Time, p.Value)
			}
		}
	}
	if !found {
		t.Errorf("backfilled point at %d not found in %v", want, ts.Points())
	}
}

// Compaction folds the sidecar into the compressed file and deletes it, and the
// data served must be identical before and after.
func TestOutOfOrderMergeIntoCompressedFile(t *testing.T) {
	cwhisper, path, base := newSingleRetentionOOO(t, true)
	defer cwhisper.Close()

	// two holes filled late, plus a correction the main file must keep winning
	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{
		{Time: base + 1, Value: 7},
		{Time: base + 3, Value: 8},
		{Time: base + 2, Value: 99},
	}); err != nil {
		t.Fatalf("late update: %s", err)
	}
	if got := cwhisper.OutOfOrderPoints; got != 3 {
		t.Fatalf("OutOfOrderPoints = %d; want 3", got)
	}

	want := []float64{1, 7, 2, 8, 3, math.NaN()}

	ts, err := cwhisper.Fetch(base-1, base+5)
	if err != nil {
		t.Fatalf("fetch before merge: %s", err)
	}
	assertValues(t, ts, want)

	if err := cwhisper.MergeOutOfOrder(); err != nil {
		t.Fatalf("merge: %s", err)
	}

	if _, err := os.Stat(path + oooSuffix); !os.IsNotExist(err) {
		t.Errorf("sidecar still present after merge (stat err = %v)", err)
	}
	if cwhisper.oooPath != "" {
		t.Errorf("oooPath = %q; want empty after merge", cwhisper.oooPath)
	}

	// same answer, now entirely out of the compressed file
	ts, err = cwhisper.Fetch(base-1, base+5)
	if err != nil {
		t.Fatalf("fetch after merge: %s", err)
	}
	assertValues(t, ts, want)

	// and it survives a reopen, i.e. it really is on disk
	if err := cwhisper.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}
	reopened, err := OpenWithOptions(path, &Options{})
	if err != nil {
		t.Fatalf("reopen: %s", err)
	}
	defer reopened.Close()

	ts, err = reopened.Fetch(base-1, base+5)
	if err != nil {
		t.Fatalf("fetch after reopen: %s", err)
	}
	assertValues(t, ts, want)
}

// MergeOutOfOrder on a file that never received a late point is a no-op.
func TestOutOfOrderMergeWithoutSidecar(t *testing.T) {
	cwhisper, _, base := newSingleRetentionOOO(t, true)
	defer cwhisper.Close()

	if err := cwhisper.MergeOutOfOrder(); err != nil {
		t.Fatalf("merge: %s", err)
	}

	ts, err := cwhisper.Fetch(base-1, base+5)
	if err != nil {
		t.Fatalf("fetch: %s", err)
	}
	assertValues(t, ts, []float64{1, math.NaN(), 2, math.NaN(), 3, math.NaN()})
}

// The oracle: classic whisper already accepts points in any order, so feeding
// the same shuffled set to a classic file and to a compressed file with a
// sidecar must produce the same series -- before and after compaction.
//
// Timestamps are distinct so that every slot has exactly one true value and the
// "main file wins" rule cannot mask a lost or misplaced point.
func TestOutOfOrderMatchesClassicWhisper(t *testing.T) {
	const (
		span      = 2000
		numPoints = 600
		seed      = 20240607
	)

	dir := t.TempDir()
	rets := []*Retention{{secondsPerPoint: 1, numberOfPoints: 7200}}

	classic, err := CreateWithOptions(
		filepath.Join(dir, "classic.wsp"), rets, Last, 0,
		&Options{IgnoreNowOnWrite: true},
	)
	if err != nil {
		t.Fatalf("create classic: %s", err)
	}
	defer classic.Close()

	cpath := filepath.Join(dir, "compressed.cwsp")
	cwhisper, err := CreateWithOptions(
		cpath, rets, Last, 0,
		// a small block forces the rewrite to merge across block boundaries
		&Options{Compressed: true, PointsPerBlock: 100, IgnoreNowOnWrite: true, OutOfOrder: true},
	)
	if err != nil {
		t.Fatalf("create compressed: %s", err)
	}
	defer cwhisper.Close()

	base := int(time.Now().Unix()) - 5000
	rnd := rand.New(rand.NewSource(seed))

	offsets := rnd.Perm(span)[:numPoints]
	points := make([]*TimeSeriesPoint, 0, numPoints)
	for i, off := range offsets {
		points = append(points, &TimeSeriesPoint{Time: base + off, Value: float64(i)})
	}

	for start := 0; start < len(points); start += 37 {
		end := start + 37
		if end > len(points) {
			end = len(points)
		}
		// UpdateMany sorts in place, so hand each file its own copy
		batch := make([]*TimeSeriesPoint, end-start)
		for i, p := range points[start:end] {
			cp := *p
			batch[i] = &cp
		}
		if err := classic.UpdateMany(points[start:end]); err != nil {
			t.Fatalf("classic update: %s", err)
		}
		if err := cwhisper.UpdateMany(batch); err != nil {
			t.Fatalf("compressed update: %s", err)
		}
	}

	if cwhisper.OutOfOrderPoints == 0 {
		t.Fatal("no points were diverted; the shuffle did not exercise the sidecar")
	}
	t.Logf("diverted %d of %d points to the sidecar", cwhisper.OutOfOrderPoints, numPoints)

	want, err := classic.Fetch(base-1, base+span)
	if err != nil {
		t.Fatalf("classic fetch: %s", err)
	}

	got, err := cwhisper.Fetch(base-1, base+span)
	if err != nil {
		t.Fatalf("compressed fetch: %s", err)
	}
	assertValues(t, got, want.Values())

	if err := cwhisper.MergeOutOfOrder(); err != nil {
		t.Fatalf("merge: %s", err)
	}

	got, err = cwhisper.Fetch(base-1, base+span)
	if err != nil {
		t.Fatalf("compressed fetch after merge: %s", err)
	}
	assertValues(t, got, want.Values())

	if _, err := os.Stat(cpath + oooSuffix); !os.IsNotExist(err) {
		t.Errorf("sidecar still present after merge (stat err = %v)", err)
	}
}

// A sidecar whose grid no longer matches the compressed file (retentions were
// changed underneath it) must be ignored rather than scattered into the wrong
// slots.
func TestOutOfOrderIncompatibleSidecarIgnored(t *testing.T) {
	cwhisper, path, base := newSingleRetentionOOO(t, true)

	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 7}}); err != nil {
		t.Fatalf("late update: %s", err)
	}
	if err := cwhisper.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}

	// replace the sidecar with one on a different grid
	sidecarPath := path + oooSuffix
	if err := os.Remove(sidecarPath); err != nil {
		t.Fatalf("remove sidecar: %s", err)
	}
	bad, err := Create(sidecarPath, []*Retention{{secondsPerPoint: 60, numberOfPoints: 100}}, Sum, 0)
	if err != nil {
		t.Fatalf("create incompatible sidecar: %s", err)
	}
	bad.Close()

	reopened, err := OpenWithOptions(path, &Options{})
	if err != nil {
		t.Fatalf("reopen: %s", err)
	}
	defer reopened.Close()

	ts, err := reopened.Fetch(base-1, base+5)
	if err != nil {
		t.Fatalf("fetch: %s", err)
	}
	assertValues(t, ts, []float64{1, math.NaN(), 2, math.NaN(), 3, math.NaN()})

	if len(reopened.NonFatalErrors) == 0 {
		t.Error("expected a non-fatal error recording the incompatible sidecar")
	}
}

// Recreating a metric whose compressed file was deleted must not resurrect the
// previous incarnation's out-of-order data.
func TestOutOfOrderOrphanedSidecarRemovedOnCreate(t *testing.T) {
	cwhisper, path, base := newSingleRetentionOOO(t, true)

	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 7}}); err != nil {
		t.Fatalf("late update: %s", err)
	}
	if err := cwhisper.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}
	if _, err := os.Stat(path + oooSuffix); err != nil {
		t.Fatalf("sidecar should exist: %s", err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatalf("remove main file: %s", err)
	}

	recreated, err := CreateWithOptions(
		path,
		[]*Retention{{secondsPerPoint: 1, numberOfPoints: 7200}},
		Sum, 0,
		&Options{Compressed: true, PointsPerBlock: 1200, OutOfOrder: true},
	)
	if err != nil {
		t.Fatalf("recreate: %s", err)
	}
	defer recreated.Close()

	if _, err := os.Stat(path + oooSuffix); !os.IsNotExist(err) {
		t.Errorf("orphaned sidecar still present (stat err = %v)", err)
	}
}

// Folding the sidecar into the base archive is not enough on its own: the
// coarse archives were aggregated without the late point and their slots are
// already encoded. MergeOutOfOrder must recompute the windows the backfill
// touches, or a backfill stays invisible at every resolution above the base.
func TestOutOfOrderMergeRecomputesCoarseAggregate(t *testing.T) {
	cwhisper, _, base := newTwoRetentionOOO(t, 5, 0)
	defer cwhisper.Close()

	// the on-time fill left base..base+9 one point short
	if got := coarseValueAt(t, cwhisper, base); got != 9 {
		t.Fatalf("coarse sum at %d = %v; want 9 before the backfill", base, got)
	}

	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: base + 5, Value: 1}}); err != nil {
		t.Fatalf("backfill: %s", err)
	}
	if got := cwhisper.OutOfOrderPoints; got != 1 {
		t.Fatalf("OutOfOrderPoints = %d; want 1", got)
	}

	// before the merge the stale aggregate still stands: the encoded slot
	// cannot be rewritten in place, and the sidecar's own propagated value
	// loses to the main file
	if got := coarseValueAt(t, cwhisper, base); got != 9 {
		t.Errorf("coarse sum at %d = %v; want 9 before the merge", base, got)
	}

	if err := cwhisper.MergeOutOfOrder(); err != nil {
		t.Fatalf("merge: %s", err)
	}

	if got := coarseValueAt(t, cwhisper, base); got != 10 {
		t.Errorf("coarse sum at %d = %v; want 10 after the merge", base, got)
	}
	// untouched windows must be left exactly as they were
	if got := coarseValueAt(t, cwhisper, base+10); got != 10 {
		t.Errorf("coarse sum at %d = %v; want 10 (untouched window)", base+10, got)
	}
}

// Recomputation obeys xFilesFactor, so a window that is still mostly empty
// after the backfill is left alone rather than written as a partial aggregate.
func TestOutOfOrderMergeRespectsXFilesFactor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "xff.cwsp")
	cwhisper, err := CreateWithOptions(
		path,
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 300},
			{secondsPerPoint: 10, numberOfPoints: 600},
		},
		Sum, 0.5,
		&Options{Compressed: true, PointsPerBlock: 100, OutOfOrder: true},
	)
	if err != nil {
		t.Fatalf("create: %s", err)
	}
	defer cwhisper.Close()

	now := int(time.Now().Unix())
	base := now - 200
	base -= base % 10

	// two points in the first window and ten in each of the rest, so only the
	// first falls short of an xFilesFactor of 0.5 (see newTwoRetentionOOO on
	// why five windows are written)
	var points []*TimeSeriesPoint
	for _, i := range []int{0, 1} {
		points = append(points, &TimeSeriesPoint{Time: base + i, Value: 1})
	}
	for i := 10; i < 50; i++ {
		points = append(points, &TimeSeriesPoint{Time: base + i, Value: 1})
	}
	if err := cwhisper.UpdateMany(points); err != nil {
		t.Fatalf("fill: %s", err)
	}
	if got := coarseValueAt(t, cwhisper, base); !math.IsNaN(got) {
		t.Fatalf("coarse value at %d = %v; want NaN before the backfill", base, got)
	}

	// backfill one point into the sparse window: 3/10 still fails xff
	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: base + 2, Value: 1}}); err != nil {
		t.Fatalf("backfill: %s", err)
	}
	if got := cwhisper.OutOfOrderPoints; got != 1 {
		t.Fatalf("OutOfOrderPoints = %d; want 1", got)
	}
	if err := cwhisper.MergeOutOfOrder(); err != nil {
		t.Fatalf("merge: %s", err)
	}

	if got := coarseValueAt(t, cwhisper, base); !math.IsNaN(got) {
		t.Errorf("coarse value at %d = %v; want NaN (3/10 is below xff 0.5)", base, got)
	}
	if got := coarseValueAt(t, cwhisper, base+10); got != 10 {
		t.Errorf("coarse value at %d = %v; want 10", base+10, got)
	}
}

// An unusable sidecar must not take the metric down. Writes carry on and the
// late points are discarded exactly as they would be with the option off, with
// the reason recorded once rather than once per operation.
func TestOutOfOrderIncompatibleSidecarDoesNotFailWrites(t *testing.T) {
	cwhisper, path, base := newSingleRetentionOOO(t, true)

	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 7}}); err != nil {
		t.Fatalf("late update: %s", err)
	}
	if err := cwhisper.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}

	// swap in a sidecar on a grid that no longer lines up, as UpdateConfig
	// would have left behind before it learned to fold sidecars in
	sidecarPath := path + oooSuffix
	if err := os.Remove(sidecarPath); err != nil {
		t.Fatalf("remove sidecar: %s", err)
	}
	bad, err := Create(sidecarPath, []*Retention{{secondsPerPoint: 60, numberOfPoints: 100}}, Sum, 0)
	if err != nil {
		t.Fatalf("create incompatible sidecar: %s", err)
	}
	bad.Close()

	w, err := OpenWithOptions(path, &Options{IgnoreNowOnWrite: true, OutOfOrder: true})
	if err != nil {
		t.Fatalf("reopen: %s", err)
	}
	defer w.Close()

	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: base + 10, Value: 4}}); err != nil {
		t.Fatalf("in-order write: %s", err)
	}
	for i := 0; i < 3; i++ {
		if err := w.UpdateMany([]*TimeSeriesPoint{{Time: base + 3, Value: 55}}); err != nil {
			t.Fatalf("late write %d: %s", i, err)
		}
	}
	if _, err := w.Fetch(base-1, base+11); err != nil {
		t.Fatalf("fetch: %s", err)
	}

	if got := len(w.NonFatalErrors); got != 1 {
		t.Errorf("len(NonFatalErrors) = %d; want 1 (latched, not one per operation): %v", got, w.NonFatalErrors)
	}
	if len(w.NonFatalErrors) > 0 && !errors.Is(w.NonFatalErrors[0], errOOOIncompatible) {
		t.Errorf("NonFatalErrors[0] = %v; want %v", w.NonFatalErrors[0], errOOOIncompatible)
	}
	if w.oooPath != "" {
		t.Errorf("oooPath = %q; want empty once the sidecar is known unusable", w.oooPath)
	}

	// the in-order point landed; the late ones were dropped, as without the option
	ts, err := w.Fetch(base-1, base+11)
	if err != nil {
		t.Fatalf("fetch: %s", err)
	}
	assertValues(t, ts, []float64{1, math.NaN(), 2, math.NaN(), 3, math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), 4, math.NaN()})
}

func TestOutOfOrderMergeRejectsIncompatibleSidecar(t *testing.T) {
	cwhisper, path, base := newSingleRetentionOOO(t, true)
	defer cwhisper.Close()

	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 7}}); err != nil {
		t.Fatalf("late update: %s", err)
	}
	if err := cwhisper.closeOOO(); err != nil {
		t.Fatalf("close sidecar: %s", err)
	}

	sidecarPath := path + oooSuffix
	if err := os.Remove(sidecarPath); err != nil {
		t.Fatalf("remove sidecar: %s", err)
	}
	bad, err := Create(sidecarPath, []*Retention{{secondsPerPoint: 60, numberOfPoints: 100}}, Sum, 0)
	if err != nil {
		t.Fatalf("create incompatible sidecar: %s", err)
	}
	if err := bad.Close(); err != nil {
		t.Fatalf("close incompatible sidecar: %s", err)
	}

	if err := cwhisper.MergeOutOfOrder(); !errors.Is(err, errOOOIncompatible) {
		t.Fatalf("merge error = %v; want %v", err, errOOOIncompatible)
	}
	if got := cwhisper.OutOfOrderPoints; got != 1 {
		t.Errorf("OutOfOrderPoints = %d; want 1 after failed merge", got)
	}
	if got := cwhisper.OutOfOrderPath(); got != sidecarPath {
		t.Errorf("OutOfOrderPath() = %q; want %q after failed merge", got, sidecarPath)
	}
	if _, err := os.Stat(sidecarPath); err != nil {
		t.Errorf("sidecar missing after failed merge: %s", err)
	}
}

// Callers are told to merge once enough has piled up, so the counter has to go
// back to zero; otherwise every later write re-triggers a full file rewrite.
// A second merge with nothing left to do is a no-op.
func TestOutOfOrderMergeResetsCounterAndIsIdempotent(t *testing.T) {
	cwhisper, path, base := newSingleRetentionOOO(t, true)
	defer cwhisper.Close()

	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{
		{Time: base + 1, Value: 7},
		{Time: base + 3, Value: 8},
	}); err != nil {
		t.Fatalf("late update: %s", err)
	}
	if got := cwhisper.OutOfOrderPoints; got != 2 {
		t.Fatalf("OutOfOrderPoints = %d; want 2", got)
	}

	want := []float64{1, 7, 2, 8, 3, math.NaN()}
	for i := 0; i < 2; i++ {
		if err := cwhisper.MergeOutOfOrder(); err != nil {
			t.Fatalf("merge %d: %s", i, err)
		}
		if got := cwhisper.OutOfOrderPoints; got != 0 {
			t.Errorf("merge %d: OutOfOrderPoints = %d; want 0", i, got)
		}
		if got := cwhisper.OutOfOrderPath(); got != "" {
			t.Errorf("merge %d: OutOfOrderPath() = %q; want empty", i, got)
		}
		if _, err := os.Stat(path + oooSuffix); !os.IsNotExist(err) {
			t.Errorf("merge %d: sidecar still present (stat err = %v)", i, err)
		}

		ts, err := cwhisper.Fetch(base-1, base+5)
		if err != nil {
			t.Fatalf("merge %d: fetch: %s", i, err)
		}
		assertValues(t, ts, want)
	}
}

// Changing retentions rewrites the file onto a new grid and does not read the
// sidecar, so the sidecar has to be folded in first or its points are stranded
// on a grid nothing will ever line up with again.
func TestOutOfOrderUpdateConfigFoldsInSidecar(t *testing.T) {
	cwhisper, path, base := newSingleRetentionOOO(t, true)

	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 7}}); err != nil {
		t.Fatalf("late update: %s", err)
	}
	if _, err := os.Stat(path + oooSuffix); err != nil {
		t.Fatalf("sidecar should exist: %s", err)
	}

	opts := &Options{Compressed: true, PointsPerBlock: 1200, IgnoreNowOnWrite: true, OutOfOrder: true}
	if err := cwhisper.UpdateConfig(
		[]*Retention{{secondsPerPoint: 1, numberOfPoints: 14400}},
		Sum, 0, opts,
	); err != nil {
		t.Fatalf("update config: %s", err)
	}

	if _, err := os.Stat(path + oooSuffix); !os.IsNotExist(err) {
		t.Errorf("sidecar survived the retention change (stat err = %v)", err)
	}

	reopened, err := OpenWithOptions(path, opts)
	if err != nil {
		t.Fatalf("reopen: %s", err)
	}
	defer reopened.Close()

	if got := reopened.Retentions()[0].numberOfPoints; got != 14400 {
		t.Fatalf("numberOfPoints = %d; want 14400 (retention change did not take)", got)
	}

	ts, err := reopened.Fetch(base-1, base+5)
	if err != nil {
		t.Fatalf("fetch: %s", err)
	}
	assertValues(t, ts, []float64{1, 7, 2, math.NaN(), 3, math.NaN()})
}

func TestOutOfOrderUpdateConfigFoldsInSidecarBeforeAggregationChange(t *testing.T) {
	cwhisper, path, base := newSingleRetentionOOO(t, true)
	defer cwhisper.Close()

	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 7}}); err != nil {
		t.Fatalf("late update: %s", err)
	}

	opts := &Options{Compressed: true, PointsPerBlock: 1200, IgnoreNowOnWrite: true, OutOfOrder: true}
	if err := cwhisper.UpdateConfig(NewRetentionsNoPointer(cwhisper.Retentions()), Average, 0.5, opts); err != nil {
		t.Fatalf("update config: %s", err)
	}
	if _, err := os.Stat(path + oooSuffix); !os.IsNotExist(err) {
		t.Fatalf("old sidecar survived the aggregation change (stat err = %v)", err)
	}

	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: base + 3, Value: 8}}); err != nil {
		t.Fatalf("second late update: %s", err)
	}
	if cwhisper.oooFile == nil {
		t.Fatal("second late update did not create a new sidecar")
	}
	if cwhisper.oooFile.aggregationMethod != Average || cwhisper.oooFile.xFilesFactor != 0.5 {
		t.Errorf("new sidecar config = (%v, %v); want (%v, %v)", cwhisper.oooFile.aggregationMethod, cwhisper.oooFile.xFilesFactor, Average, float32(0.5))
	}
}

// Nothing in this package deletes whisper files, so a sidecar outlives the
// metric unless the caller removes it too.
func TestOutOfOrderRemoveSidecar(t *testing.T) {
	cwhisper, path, base := newSingleRetentionOOO(t, true)

	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 7}}); err != nil {
		t.Fatalf("late update: %s", err)
	}
	if err := cwhisper.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}

	if got := OutOfOrderSidecarPath(path); got != path+oooSuffix {
		t.Errorf("OutOfOrderSidecarPath(%q) = %q; want %q", path, got, path+oooSuffix)
	}

	removed, err := RemoveOutOfOrderSidecar(path)
	if err != nil {
		t.Fatalf("remove: %s", err)
	}
	if !removed {
		t.Error("removed = false; want true")
	}
	if _, err := os.Stat(path + oooSuffix); !os.IsNotExist(err) {
		t.Errorf("sidecar still present (stat err = %v)", err)
	}

	// removing again, or removing one that never existed, is not an error
	removed, err = RemoveOutOfOrderSidecar(path)
	if err != nil {
		t.Fatalf("remove again: %s", err)
	}
	if removed {
		t.Error("removed = true on the second call; want false")
	}
}

func TestOutOfOrderLongFilenameWithFLock(t *testing.T) {
	tests := []struct {
		name              string
		filenameLength    int
		wantHashedSidecar bool
	}{
		{
			name:           "sidecar lock exceeds limit",
			filenameLength: maxFilenameLength - len(lockSuffix),
		},
		{
			name:              "sidecar exceeds limit",
			filenameLength:    maxFilenameLength,
			wantHashedSidecar: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename := strings.Repeat("m", tt.filenameLength-len(".wsp")) + ".wsp"
			path := filepath.Join(t.TempDir(), filename)
			w, err := CreateWithOptions(
				path,
				[]*Retention{{secondsPerPoint: 1, numberOfPoints: 7200}},
				Sum,
				0,
				&Options{
					Compressed:       true,
					FLock:            true,
					IgnoreNowOnWrite: true,
					OutOfOrder:       true,
					PointsPerBlock:   1200,
				},
			)
			if err != nil {
				t.Fatalf("create: %s", err)
			}
			defer w.Close()

			base := int(time.Now().Unix()) - 3600
			if err := w.UpdateMany([]*TimeSeriesPoint{
				{Time: base, Value: 1},
				{Time: base + 2, Value: 2},
				{Time: base + 4, Value: 3},
			}); err != nil {
				t.Fatalf("initial update: %s", err)
			}
			if err := w.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 7}}); err != nil {
				t.Fatalf("late update: %s", err)
			}

			sidecarPath := OutOfOrderSidecarPath(path)
			if got := sidecarPath != path+oooSuffix; got != tt.wantHashedSidecar {
				t.Errorf("hashed sidecar = %v; want %v (path %q)", got, tt.wantHashedSidecar, sidecarPath)
			}
			if _, err := os.Stat(sidecarPath); err != nil {
				t.Fatalf("stat sidecar: %s", err)
			}
			if _, err := os.Stat(auxiliaryPath(path, lockSuffix)); err != nil {
				t.Fatalf("stat main lock: %s", err)
			}
			if _, err := os.Stat(auxiliaryPath(sidecarPath, lockSuffix)); err != nil {
				t.Fatalf("stat sidecar lock: %s", err)
			}

			ts, err := w.Fetch(base-1, base+5)
			if err != nil {
				t.Fatalf("fetch: %s", err)
			}
			assertValues(t, ts, []float64{1, 7, 2, math.NaN(), 3, math.NaN()})
		})
	}
}

// The sidecar handle is opened once and reused: reopening it per fetch made
// every read of a metric that had ever seen one late point pay an open, a flock
// and a header parse. Close must still release it.
func TestOutOfOrderSidecarHandleIsCached(t *testing.T) {
	cwhisper, _, base := newSingleRetentionOOO(t, true)

	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 7}}); err != nil {
		t.Fatalf("late update: %s", err)
	}
	if cwhisper.oooFile == nil {
		t.Fatal("diverting did not cache the sidecar handle")
	}
	first := cwhisper.oooFile

	for i := 0; i < 3; i++ {
		if _, err := cwhisper.Fetch(base-1, base+5); err != nil {
			t.Fatalf("fetch %d: %s", i, err)
		}
		if cwhisper.oooFile != first {
			t.Fatalf("fetch %d reopened the sidecar instead of reusing the handle", i)
		}
	}

	if err := cwhisper.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}
	if cwhisper.oooFile != nil {
		t.Error("Close did not release the cached sidecar handle")
	}
}

func TestOutOfOrderSidecarHonorsReadOnlyOpen(t *testing.T) {
	cwhisper, path, base := newSingleRetentionOOO(t, true)
	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 7}}); err != nil {
		t.Fatalf("late update: %s", err)
	}
	if err := cwhisper.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}

	readOnly := os.O_RDONLY
	reader, err := OpenWithOptions(path, &Options{OpenFileFlag: &readOnly})
	if err != nil {
		t.Fatalf("open reader: %s", err)
	}
	defer reader.Close()

	ts, err := reader.Fetch(base-1, base+5)
	if err != nil {
		t.Fatalf("fetch: %s", err)
	}
	assertValues(t, ts, []float64{1, 7, 2, math.NaN(), 3, math.NaN()})

	if flag := reader.oooFile.opts.OpenFileFlag; flag == nil || *flag != os.O_RDONLY {
		t.Errorf("sidecar open flag = %v; want O_RDONLY", flag)
	}
}

// A point past the archive's retention never reaches the encoder, so there is
// nothing to divert and no sidecar is created for it.
func TestOutOfOrderBeyondRetentionNotDiverted(t *testing.T) {
	cwhisper, path, base := newSingleRetentionOOO(t, true)
	defer cwhisper.Close()

	// the file keeps 7200s and base is 3600s old, so this is well outside it
	if err := cwhisper.UpdateMany([]*TimeSeriesPoint{{Time: base - 7200, Value: 7}}); err != nil {
		t.Fatalf("stale update: %s", err)
	}

	if got := cwhisper.OutOfOrderPoints; got != 0 {
		t.Errorf("OutOfOrderPoints = %d; want 0", got)
	}
	if _, err := os.Stat(path + oooSuffix); !os.IsNotExist(err) {
		t.Errorf("sidecar created for a point beyond retention (stat err = %v)", err)
	}
}

// A point can be diverted at any archive, not only the base one, and the
// recomputation has to cascade down from wherever the change landed. Here
// nothing at all changes archive 0, so a cascade that starts from - or stops at
// - the base archive never reaches archive 2.
//
// The window under test must already hold a value in the main file, otherwise
// the sidecar's own propagated value fills it by gap-fill and the test passes
// whether or not anything was recomputed.
func TestOutOfOrderMergeCascadesFromCoarseArchive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cascade.cwsp")
	cwhisper, err := CreateWithOptions(
		path,
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 60},
			{secondsPerPoint: 10, numberOfPoints: 600},
			{secondsPerPoint: 60, numberOfPoints: 600},
		},
		Sum, 0,
		&Options{Compressed: true, PointsPerBlock: 100, OutOfOrder: true},
	)
	if err != nil {
		t.Fatalf("create: %s", err)
	}
	defer cwhisper.Close()

	stored := func(index, interval int) (float64, bool) {
		t.Helper()

		ps, err := cwhisper.storedPoints(cwhisper.archives[index], interval, interval)
		if err != nil {
			t.Fatalf("storedPoints(%d): %s", index, err)
		}
		for _, p := range ps {
			if p.interval == interval {
				return p.value, true
			}
		}

		return 0, false
	}

	// write straight into archive 1, leaving one of the six slots of the first
	// 60s window empty. Four windows, because archive 1's buffer holds two and
	// a unit only propagates once the unit two windows later reuses it.
	now := int(time.Now().Unix())
	base := now - 240
	base -= base % 60
	hole := base + 30

	var points []*TimeSeriesPoint
	for tick := base; tick < base+240; tick += 10 {
		if tick == hole {
			continue
		}
		points = append(points, &TimeSeriesPoint{Time: tick, Value: 1})
	}
	if err := cwhisper.UpdateManyForArchive(points, cwhisper.archives[1].MaxRetention()); err != nil {
		t.Fatalf("fill: %s", err)
	}

	if got, ok := stored(2, base); !ok || got != 5 {
		t.Fatalf("archive 2 at %d = %v (present %v); want 5 of 6 slots. Test setup is wrong", base, got, ok)
	}
	if _, ok := stored(1, hole); ok {
		t.Fatalf("archive 1 at %d should be empty; test setup is wrong", hole)
	}

	// backfill the hole: older than archive 1's watermark, so it is diverted
	// against archive 1. Archive 0 sees nothing at all.
	if err := cwhisper.UpdateManyForArchive(
		[]*TimeSeriesPoint{{Time: hole, Value: 1}},
		cwhisper.archives[1].MaxRetention(),
	); err != nil {
		t.Fatalf("backfill: %s", err)
	}
	if got := cwhisper.OutOfOrderPoints; got != 1 {
		t.Fatalf("OutOfOrderPoints = %d; want 1", got)
	}

	if err := cwhisper.MergeOutOfOrder(); err != nil {
		t.Fatalf("merge: %s", err)
	}

	if got, ok := stored(1, hole); !ok || got != 1 {
		t.Errorf("archive 1 at %d = %v (present %v); want 1", hole, got, ok)
	}
	if got, ok := stored(2, base); !ok || got != 6 {
		t.Errorf("archive 2 at %d = %v (present %v); want 6 after the cascade", base, got, ok)
	}
	// a window the backfill did not touch keeps its value
	if got, ok := stored(2, base+60); !ok || got != 6 {
		t.Errorf("archive 2 at %d = %v (present %v); want 6 unchanged", base+60, got, ok)
	}
}
