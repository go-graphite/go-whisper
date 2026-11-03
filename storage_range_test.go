package whisper

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

type storageBlockReadFile struct {
	file
	blocks map[ContinuationBlock]ownedPlanReader
	reads  []ContinuationBlock
}

func (f *storageBlockReadFile) ReadStorageBlock(block ContinuationBlock) ([]TimeSeriesPoint, error) {
	f.reads = append(f.reads, block)
	reader, ok := f.blocks[block]
	if !ok {
		return nil, fmt.Errorf("unexpected block: %+v", block)
	}
	return reader.ReadStorageBlock(block)
}

type storageBlockRangeRead struct {
	block       ContinuationBlock
	from, until int
}

type storageBlockRangeReadFile struct {
	*storageBlockReadFile
	ranges   []storageBlockRangeRead
	superset bool
	err      error
}

func (f *storageBlockRangeReadFile) ReadStorageBlockRange(block ContinuationBlock, from, until int) ([]TimeSeriesPoint, error) {
	f.ranges = append(f.ranges, storageBlockRangeRead{block, from, until})
	if f.err != nil {
		return nil, f.err
	}
	reader, ok := f.blocks[block]
	if !ok {
		return nil, fmt.Errorf("unexpected block: %+v", block)
	}
	points, err := reader.ReadStorageBlock(block)
	if f.superset || err != nil {
		return points, err
	}
	kept := points[:0]
	for _, p := range points {
		if from <= p.Time && p.Time <= until {
			kept = append(kept, p)
		}
	}
	return kept, nil
}

// Snapshot real continuation blocks so typed readers share the byte reader's
// physical data, including records hidden by circular slots or live rollups.
func storageBlockReadFixture(t *testing.T, w *Whisper) *storageBlockReadFile {
	t.Helper()
	layout, err := w.ContinuationLayout()
	if err != nil {
		t.Fatal(err)
	}
	f := &storageBlockReadFile{file: w.file, blocks: make(map[ContinuationBlock]ownedPlanReader)}
	for _, block := range layout.Blocks {
		points, err := w.ContinuationBlockPoints(block)
		if err != nil {
			t.Fatal(err)
		}
		f.blocks[block] = ownedPlanReader{points}
	}
	if len(f.blocks) == 0 {
		t.Fatal("fixture has no physical blocks")
	}
	return f
}

// Compare ordinary byte reads, full typed reads and both allowed range-reader
// responses through the public Fetch API, including its aligned request bounds.
func checkStorageBlockRangeFetch(t *testing.T, w *Whisper, from, until int) *TimeSeries {
	t.Helper()
	want, err := w.Fetch(from, until)
	if err != nil || want == nil {
		t.Fatalf("byte Fetch: %v, %v", want, err)
	}
	original := w.file
	defer func() { w.file = original }()
	full := storageBlockReadFixture(t, w)
	for _, mode := range []string{"full", "range", "superset"} {
		t.Run(mode, func(t *testing.T) {
			full.reads = nil
			ranged := &storageBlockRangeReadFile{storageBlockReadFile: full, superset: mode == "superset"}
			if mode == "full" {
				w.file = full
			} else {
				w.file = ranged
			}
			got, err := w.Fetch(from, until)
			if err != nil || got == nil {
				t.Fatalf("Fetch: %v, %v", got, err)
			}
			if got.fromTime != want.fromTime || got.untilTime != want.untilTime || got.step != want.step {
				t.Fatalf("Fetch bounds: got %+v, want %+v", got, want)
			}
			assertValues(t, got, want.Values())
			if mode == "full" {
				if len(full.reads) == 0 {
					t.Fatal("Fetch did not use ReadStorageBlock")
				}
				return
			}
			if len(full.reads) != 0 {
				t.Fatalf("range Fetch made %d full block reads", len(full.reads))
			}
			for _, read := range ranged.ranges {
				if read.block.Step == got.step && read.from == got.fromTime && read.until == got.untilTime {
					return
				}
			}
			t.Fatalf("no range read with Fetch bounds [%d, %d]: %+v", got.fromTime, got.untilTime, ranged.ranges)
		})
	}
	return want
}

func TestStorageBlockRangeFetch(t *testing.T) {
	w := compressedSnapshotFixture(t, 64, 1)
	w.opts.Now = func() time.Time { return time.Unix(1700000064, 0) }
	series := checkStorageBlockRangeFetch(t, w, 1700000009, 1700000019)
	assertValues(t, series, []float64{10, 11, 12, 13, 14, 15, 16, 0, 1, 2})
}

func TestStorageBlockRangeClipsSuperset(t *testing.T) {
	w := compressedSnapshotFixture(t, 64, 1)
	original := w.file
	defer func() { w.file = original }()
	full := storageBlockReadFixture(t, w)
	ranged := &storageBlockRangeReadFile{storageBlockReadFile: full, superset: true}
	w.file = ranged
	const from, until = 1700000010, 1700000020
	points, err := w.storedPoints(w.archives[0], from, until)
	if err != nil {
		t.Fatal(err)
	}
	if len(ranged.ranges) == 0 || len(points) != until-from+1 {
		t.Fatalf("superset read: %d calls, %d points", len(ranged.ranges), len(points))
	}
	for i, p := range points {
		if p.interval != from+i || p.value != float64((10+i)%17) {
			t.Fatalf("clipped point %d: %+v", i, p)
		}
	}
	// Continuation/verification still requires complete physical blocks even
	// when the file also implements the range hook.
	block := ranged.ranges[0].block
	all, err := w.ContinuationBlockPoints(block)
	if err != nil || len(all) != block.Count || len(full.reads) != 1 || len(ranged.ranges) != 1 {
		t.Fatalf("full continuation read: %d points, %v, %d full calls, %d range calls", len(all), err, len(full.reads), len(ranged.ranges))
	}
}

func TestStorageBlockRangeFetchRollupsAndOOO(t *testing.T) {
	w, _, base := newTwoRetentionOOO(t, -1, 0)
	defer w.Close()
	w.opts.Now = func() time.Time { return time.Unix(int64(base+200), 0) }
	// The first three windows are stored; the last two exist only as live
	// rollups over the fine archive's buffer.
	stored, err := w.storedPoints(w.archives[1], base+30, base+49)
	if err != nil || len(stored) != 0 {
		t.Fatalf("expected unstored live windows: %v, %v", stored, err)
	}
	check := func(t *testing.T, windows []float64) {
		t.Helper()
		series := checkStorageBlockRangeFetch(t, w, base-110, base+49)
		if series.step != 10 {
			t.Fatalf("Fetch selected step %d, want 10", series.step)
		}
		assertValues(t, &TimeSeries{values: series.Values()[10:]}, windows)
	}
	t.Run("live", func(t *testing.T) { check(t, []float64{10, 10, 10, 10, 10}) })
	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: base + 5, Value: 7}, {Time: base + 35, Value: 9}}); err != nil {
		t.Fatal(err)
	}
	if w.oooFile == nil {
		t.Fatal("late corrections did not create an OOO sidecar")
	}
	// Corrections replace a stored sample and a buffered sample, so OOO must
	// override both the stored coarse value and the live coarse value.
	t.Run("ooo", func(t *testing.T) { check(t, []float64{16, 10, 10, 18, 10}) })
}

func TestStorageBlockRangeFetchError(t *testing.T) {
	w := compressedSnapshotFixture(t, 64, 1)
	w.opts.Now = func() time.Time { return time.Unix(1700000064, 0) }
	original := w.file
	defer func() { w.file = original }()
	sentinel := errors.New("range read failed")
	full := storageBlockReadFixture(t, w)
	ranged := &storageBlockRangeReadFile{storageBlockReadFile: full, err: sentinel}
	w.file = ranged
	if _, err := w.Fetch(1700000009, 1700000019); !errors.Is(err, sentinel) {
		t.Fatalf("Fetch error: got %v, want %v", err, sentinel)
	}
	if len(ranged.ranges) != 1 || len(full.reads) != 0 {
		t.Fatalf("failed range read: %d range calls, %d full calls", len(ranged.ranges), len(full.reads))
	}
}
