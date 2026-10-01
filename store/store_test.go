package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	whisper "github.com/go-graphite/go-whisper"
)

func TestWALRecoveryAfterAbruptExit(t *testing.T) {
	if os.Getenv("STORE_CRASH_HELPER") == "1" {
		dir := os.Getenv("STORE_CRASH_DIR")
		s, err := Open(dir, Options{Now: func() time.Time { return time.Unix(1000, 0) }})
		if err != nil {
			os.Exit(2)
		}
		_, err = s.Create(context.Background(), config("crash", whisper.Average))
		if err == nil {
			err = s.UpdateMany(context.Background(), "crash", []whisper.TimeSeriesPoint{{Time: 990, Value: 7}})
		}
		if err != nil {
			os.Exit(3)
		}
		os.Exit(0) // deliberately skip Close: recovery must replay the synced WAL
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestWALRecoveryAfterAbruptExit$")
	cmd.Env = append(os.Environ(), "STORE_CRASH_HELPER=1", "STORE_CRASH_DIR="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash writer: %v: %s", err, output)
	}
	s, err := Open(dir, Options{Now: func() time.Time { return time.Unix(1000, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	series, err := s.Fetch(context.Background(), "crash", 980, 999)
	if err != nil {
		t.Fatal(err)
	}
	if len(series.Values) != 1 || series.Values[0] != 7 {
		t.Fatalf("recovered series = %#v", series)
	}
}

func TestConcurrentMetricUpdates(t *testing.T) {
	s := testStore(t, 10_000)
	ctx := context.Background()
	if _, err := s.Create(ctx, config("concurrent", whisper.Sum)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := s.UpdateMany(ctx, "concurrent", []whisper.TimeSeriesPoint{{Time: 9990 - i%6*10, Value: float64(i)}}); err != nil {
				t.Errorf("update: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if _, err := s.Snapshot(ctx, "concurrent"); err != nil {
		t.Fatal(err)
	}
}

func testStore(t *testing.T, now int) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), Options{MemTableSize: 1 << 20, Now: func() time.Time { return time.Unix(int64(now), 0) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func config(name string, method whisper.AggregationMethod) MetricConfig {
	return MetricConfig{Name: name, Retentions: []whisper.Retention{whisper.NewRetention(10, 12), whisper.NewRetention(60, 12)}, AggregationMethod: method, XFilesFactor: 0.5}
}

func TestUpdateManyCircularSlotsAndRecovery(t *testing.T) {
	const now = 10_000
	dir := t.TempDir()
	s, err := Open(dir, Options{MemTableSize: 1 << 20, Now: func() time.Time { return time.Unix(now, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := s.Create(ctx, config("a.b", whisper.Average)); err != nil {
		t.Fatal(err)
	}
	points := []whisper.TimeSeriesPoint{{Time: 9_990, Value: 1}, {Time: 9_980, Value: 2}, {Time: 9_990, Value: 3}, {Time: 10_010, Value: 4}, {Time: 9_800, Value: 9}}
	if err := s.UpdateMany(ctx, "a.b", points); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir, Options{MemTableSize: 1 << 20, Now: func() time.Time { return time.Unix(now, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	series, err := s.Fetch(ctx, "a.b", 9_970, now)
	if err != nil {
		t.Fatal(err)
	}
	if series.Step != 10 || series.FromTime != 9_980 {
		t.Fatalf("unexpected series header: %#v", series)
	}
	if !math.IsNaN(series.Values[0]) || series.Values[1] != 3 {
		t.Fatalf("values = %v, want [NaN 3]", series.Values)
	}
	if err := s.Update(ctx, "a.b", 1, now+1); err == nil {
		t.Fatal("Update accepted a future point")
	}
}

func TestRollupAggregation(t *testing.T) {
	methods := []struct {
		method whisper.AggregationMethod
		want   float64
	}{
		{whisper.Average, 3.5}, {whisper.Sum, 21}, {whisper.First, 1}, {whisper.Last, 6}, {whisper.Min, 1}, {whisper.Max, 6},
	}
	for _, tt := range methods {
		t.Run(tt.method.String(), func(t *testing.T) {
			const now = 10_000
			s := testStore(t, now)
			ctx := context.Background()
			if _, err := s.Create(ctx, config("aggregate", tt.method)); err != nil {
				t.Fatal(err)
			}
			points := make([]whisper.TimeSeriesPoint, 6)
			for i := range points {
				points[i] = whisper.TimeSeriesPoint{Time: 9_900 + i*10, Value: float64(i + 1)}
			}
			if err := s.UpdateMany(ctx, "aggregate", points); err != nil {
				t.Fatal(err)
			}
			snapshot, err := s.Snapshot(ctx, "aggregate")
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Archives[1].Points) != 1 || snapshot.Archives[1].Points[0].Value != tt.want {
				t.Fatalf("coarse archive = %#v, want %v", snapshot.Archives[1], tt.want)
			}
		})
	}
}

func TestArchivePreservingExportImport(t *testing.T) {
	const now = 10_000
	ctx := context.Background()
	fixture := filepath.Join(t.TempDir(), "fixture.wsp")
	rets := []whisper.Retention{whisper.NewRetention(10, 12), whisper.NewRetention(60, 12)}
	w, err := whisper.Create(fixture, whisper.NewRetentionsNoPointer(rets), whisper.Sum, 0.5)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.ReplaceArchivePoints(0, []whisper.TimeSeriesPoint{{Time: 1_000, Value: 1}, {Time: 1_010, Value: 2}}); err != nil {
		t.Fatal(err)
	}
	if err := w.ReplaceArchivePoints(1, []whisper.TimeSeriesPoint{{Time: 960, Value: 7}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	s := testStore(t, now)
	if _, err := s.ImportWSP(ctx, "historic", fixture, true); err != nil {
		t.Fatal(err)
	}
	export := filepath.Join(t.TempDir(), "export.wsp")
	if err := s.ExportWSP(ctx, "historic", export); err != nil {
		t.Fatal(err)
	}
	w, err = whisper.Open(export)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i, want := range [][]whisper.TimeSeriesPoint{{{Time: 1_000, Value: 1}, {Time: 1_010, Value: 2}}, {{Time: 960, Value: 7}}} {
		got, err := w.ArchivePoints(i)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) {
			t.Fatalf("archive %d has %v, want %v", i, got, want)
		}
		for j := range got {
			if got[j] != want[j] {
				t.Fatalf("archive %d point %d = %#v, want %#v", i, j, got[j], want[j])
			}
		}
	}
}

func TestMissingValueIsNaN(t *testing.T) {
	s := testStore(t, 1000)
	ctx := context.Background()
	if _, err := s.Create(ctx, config("missing", whisper.Average)); err != nil {
		t.Fatal(err)
	}
	series, err := s.Fetch(ctx, "missing", 900, 999)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range series.Values {
		if !math.IsNaN(v) {
			t.Fatalf("missing value = %v", v)
		}
	}
}

func TestUpdateManyKeepsExactRetentionBoundaryInHigherPrecision(t *testing.T) {
	s := testStore(t, 10_000)
	ctx := context.Background()
	c := config("boundary", whisper.Average)
	c.Retentions = []whisper.Retention{whisper.NewRetention(10, 6), whisper.NewRetention(60, 10)}
	if _, err := s.Create(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateMany(ctx, "boundary", []whisper.TimeSeriesPoint{{Time: 9_940, Value: 7}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.Snapshot(ctx, "boundary")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Archives[0].Points) != 1 || snapshot.Archives[0].Points[0].Time != 9_940 {
		t.Fatalf("boundary was routed to %#v", snapshot.Archives)
	}
}

func TestUpdateManyPreservesClassicMixedAgeBoundaryQuirk(t *testing.T) {
	s := testStore(t, 10_000)
	ctx := context.Background()
	if _, err := s.Create(ctx, config("quirk", whisper.Average)); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateMany(ctx, "quirk", []whisper.TimeSeriesPoint{{Time: 9990, Value: 1}, {Time: 9900, Value: 2}, {Time: 9800, Value: 3}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.Snapshot(ctx, "quirk")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Archives[0].Points) != 1 || snapshot.Archives[0].Points[0].Time != 9990 {
		t.Fatalf("fine archive = %#v", snapshot.Archives[0])
	}
}

func TestZeroLengthFetchFromEmptyArchiveIsEmpty(t *testing.T) {
	s := testStore(t, 10000)
	ctx := context.Background()
	if _, err := s.Create(ctx, config("zero", whisper.Average)); err != nil {
		t.Fatal(err)
	}
	series, err := s.Fetch(ctx, "zero", 9950, 9950)
	if err != nil {
		t.Fatal(err)
	}
	if series.FromTime != 9960 || series.UntilTime != 9960 || len(series.Values) != 0 {
		t.Fatalf("series = %#v", series)
	}
}

func TestImportCompressedOutOfOrderNormalizesCopiedSidecar(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "coarse.cwsp")
	r0, r1 := whisper.NewRetention(1, 300), whisper.NewRetention(10, 600)
	w, err := whisper.CreateWithOptions(path, whisper.Retentions{&r0, &r1}, whisper.Sum, 0, &whisper.Options{Compressed: true, PointsPerBlock: 100, OutOfOrder: true})
	if err != nil {
		t.Fatal(err)
	}
	base := int(time.Now().Unix()) - 200
	base -= base % 10
	points := make([]*whisper.TimeSeriesPoint, 0, 49)
	for i := 0; i < 50; i++ {
		if i != 5 {
			points = append(points, &whisper.TimeSeriesPoint{Time: base + i, Value: 1})
		}
	}
	if err := w.UpdateMany(points); err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: base + 5, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	mainBefore, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sidePath := whisper.OutOfOrderSidecarPath(path)
	sideBefore, err := os.ReadFile(sidePath)
	if err != nil {
		t.Fatal(err)
	}
	s := testStore(t, int(time.Now().Unix()))
	if _, err := s.ImportWSP(ctx, "compressed", path, true); err != nil {
		t.Fatal(err)
	}
	mainAfter, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sideAfter, err := os.ReadFile(sidePath)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(mainBefore) != sha256.Sum256(mainAfter) || sha256.Sum256(sideBefore) != sha256.Sum256(sideAfter) {
		t.Fatal("compressed source changed during import")
	}
	snapshot, err := s.Snapshot(ctx, "compressed")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, p := range snapshot.Archives[1].Points {
		if p.Time == base {
			found = true
			if p.Value != 10 {
				t.Fatalf("coarse value=%v want 10", p.Value)
			}
		}
	}
	if !found {
		t.Fatalf("missing normalized coarse point at %d", base)
	}
}

func TestImportHistoricCompressedOutOfOrderDoesNotExpireSnapshot(t *testing.T) {
	oldNow := whisper.Now
	whisper.Now = func() time.Time { return time.Unix(1589728200, 0) }
	defer func() { whisper.Now = oldNow }()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "historic.cwsp")
	r0, r1 := whisper.NewRetention(1, 300), whisper.NewRetention(10, 600)
	w, err := whisper.CreateWithOptions(path, whisper.Retentions{&r0, &r1}, whisper.Sum, 0, &whisper.Options{Compressed: true, PointsPerBlock: 100, OutOfOrder: true, IgnoreNowOnWrite: true})
	if err != nil {
		t.Fatal(err)
	}
	base := 1589728000
	points := make([]*whisper.TimeSeriesPoint, 0, 49)
	for i := 0; i < 50; i++ {
		if i != 5 {
			points = append(points, &whisper.TimeSeriesPoint{Time: base + i, Value: 1})
		}
	}
	if err := w.UpdateMany(points); err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: base + 5, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	side := whisper.OutOfOrderSidecarPath(path)
	sideBefore, err := os.ReadFile(side)
	if err != nil {
		t.Fatal(err)
	}
	s := testStore(t, int(time.Now().Unix()))
	if _, err := s.ImportWSP(ctx, "historic", path, true); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sideAfter, err := os.ReadFile(side)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(before) != sha256.Sum256(after) || sha256.Sum256(sideBefore) != sha256.Sum256(sideAfter) {
		t.Fatal("historic source changed")
	}
	snapshot, err := s.Snapshot(ctx, "historic")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range snapshot.Archives[1].Points {
		if p.Time == base && p.Value == 10 {
			return
		}
	}
	t.Fatalf("historic normalized coarse point missing: %#v", snapshot.Archives[1].Points)
}

func TestReplaceRejectsMalformedSnapshots(t *testing.T) {
	ctx := context.Background()
	rets := []whisper.Retention{whisper.NewRetention(10, 12), whisper.NewRetention(60, 12)}
	tests := []struct {
		name     string
		archives []Archive
	}{{"missing", []Archive{{Retention: rets[0]}}}, {"extra", []Archive{{Retention: rets[0]}, {Retention: rets[1]}, {Retention: rets[1]}}}, {"mismatch", []Archive{{Retention: whisper.NewRetention(5, 24)}, {Retention: rets[1]}}}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := testStore(t, 10000)
			_, err := s.Replace(ctx, Snapshot{Metadata: Metadata{MetricConfig: MetricConfig{Name: "bad", Retentions: rets, AggregationMethod: whisper.Sum}}, Archives: tt.archives})
			if err == nil {
				t.Fatal("accepted malformed snapshot")
			}
			if _, err := s.Metadata(ctx, "bad"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("published bad snapshot: %v", err)
			}
		})
	}
}

func TestImportWithoutReplaceHasSingleWinner(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rets := []whisper.Retention{whisper.NewRetention(10, 12)}
	paths := make([]string, 6)
	for i := range paths {
		paths[i] = filepath.Join(dir, time.Unix(int64(i), 0).Format("150405")+".wsp")
		w, err := whisper.Create(paths[i], whisper.NewRetentionsNoPointer(rets), whisper.Sum, .5)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.ReplaceArchivePoints(0, []whisper.TimeSeriesPoint{{Time: 9990, Value: float64(i)}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	s, err := Open(filepath.Join(dir, "db"), Options{Now: func() time.Time { return time.Unix(10000, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	results := make(chan error, len(paths))
	var wg sync.WaitGroup
	for _, path := range paths {
		wg.Add(1)
		go func(path string) { defer wg.Done(); _, err := s.ImportWSP(ctx, "metric", path, false); results <- err }(path)
	}
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else if !errors.Is(err, ErrExists) {
			t.Fatal(err)
		}
	}
	if success != 1 {
		t.Fatalf("successes=%d", success)
	}
}

func TestReplaceAndDeleteReclaimGenerations(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rets := []whisper.Retention{whisper.NewRetention(10, 12)}
	s, err := Open(dir, Options{Now: func() time.Time { return time.Unix(10000, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	makeSnapshot := func(points ...whisper.TimeSeriesPoint) Snapshot {
		return Snapshot{Metadata: Metadata{MetricConfig: MetricConfig{Name: "metric", Retentions: rets, AggregationMethod: whisper.Sum}}, Archives: []Archive{{Retention: rets[0], Points: points}}}
	}
	if _, err := s.Replace(ctx, makeSnapshot(whisper.TimeSeriesPoint{Time: 9970, Value: 1}, whisper.TimeSeriesPoint{Time: 9980, Value: 2}, whisper.TimeSeriesPoint{Time: 9990, Value: 3})); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Replace(ctx, makeSnapshot(whisper.TimeSeriesPoint{Time: 9980, Value: 20}, whisper.TimeSeriesPoint{Time: 9990, Value: 30})); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	count := func() int {
		db, err := pebble.Open(dir, &pebble.Options{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		it, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte{'p'}, UpperBound: []byte{'q'}})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()
		n := 0
		for it.First(); it.Valid(); it.Next() {
			n++
		}
		return n
	}
	if got := count(); got != 2 {
		t.Fatalf("keys after replace=%d", got)
	}
	s, err = Open(dir, Options{Now: func() time.Time { return time.Unix(10000, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "metric"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if got := count(); got != 0 {
		t.Fatalf("keys after delete=%d", got)
	}
}

func TestImportReadsReadOnlySourceAndRejectsIncompatibleSidecar(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rets := []whisper.Retention{whisper.NewRetention(10, 12)}
	classic := filepath.Join(dir, "readonly.wsp")
	w, err := whisper.Create(classic, whisper.NewRetentionsNoPointer(rets), whisper.Sum, .5)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(classic, 0444); err != nil {
		t.Fatal(err)
	}
	s := testStore(t, 10000)
	if _, err := s.ImportWSP(ctx, "readonly", classic, true); err != nil {
		t.Fatalf("read-only import: %v", err)
	}
	compressed := filepath.Join(dir, "bad.cwsp")
	r0 := whisper.NewRetention(1, 300)
	cw, err := whisper.CreateWithOptions(compressed, whisper.Retentions{&r0}, whisper.Sum, 0, &whisper.Options{Compressed: true, PointsPerBlock: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := cw.Close(); err != nil {
		t.Fatal(err)
	}
	wrong := whisper.NewRetention(2, 300)
	side, err := whisper.Create(whisper.OutOfOrderSidecarPath(compressed), whisper.Retentions{&wrong}, whisper.Sum, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := side.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ImportWSP(ctx, "bad", compressed, true); err == nil {
		t.Fatal("accepted incompatible sidecar")
	}
}
