package store

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func TestPagedCatalogAndConditionalDelete(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	opts := Options{Now: func() time.Time { return time.Unix(1000, 0) }}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a.one", "a.two", "b.one"} {
		if _, err := db.Create(ctx, config(name, whisper.Sum)); err != nil {
			t.Fatal(err)
		}
	}
	first, err := db.ListPage(ctx, "a.", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.ListPage(ctx, "a.", first[0].Name, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]string{first[0].Name, second[0].Name}, []string{"a.one", "a.two"}) {
		t.Fatal(first, second)
	}
	end, err := db.ListPage(ctx, "a.", second[0].Name, 1)
	if err != nil || len(end) != 0 {
		t.Fatal(end, err)
	}
	old := first[0]
	if err := db.UpdateMany(ctx, old.Name, []whisper.TimeSeriesPoint{{Time: 990, Value: 3}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.DeleteIfUnchanged(ctx, old.Name, old); !errors.Is(err, ErrConflict) {
		t.Fatalf("delete old revision: %v", err)
	}
	current, err := db.Metadata(ctx, old.Name)
	if err != nil {
		t.Fatal(err)
	}
	if current.Revision <= old.Revision {
		t.Fatalf("revision not recovered: old=%+v now=%+v", old, current)
	}
	if err := db.DeleteIfUnchanged(ctx, current.Name, current); err != nil {
		t.Fatal(err)
	}
	recreated, err := db.Create(ctx, config(current.Name, whisper.Sum))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteIfUnchanged(ctx, current.Name, current); !errors.Is(err, ErrConflict) {
		t.Fatalf("ABA deletion: %v recreated=%+v", err, recreated)
	}
}

func TestFillWSPPreservesDestinationAndConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	db := testStore(t, 1000)
	cfg := config("fill", whisper.Sum)
	if _, err := db.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMany(ctx, cfg.Name, []whisper.TimeSeriesPoint{{Time: 990, Value: 7}}); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "source.wsp")
	w, err := whisper.Create(source, whisper.NewRetentionsNoPointer(cfg.Retentions), cfg.AggregationMethod, cfg.XFilesFactor)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.ReplaceArchivePoints(0, []whisper.TimeSeriesPoint{{Time: 990, Value: 9}, {Time: 980, Value: 11}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.FillWSP(ctx, cfg.Name, source); err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Archives[0].Points) != 2 || snap.Archives[0].Points[0].Value != 11 || snap.Archives[0].Points[1].Value != 7 {
		t.Fatalf("fill overwrote data: %+v", snap)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if _, err := db.FillWSP(ctx, cfg.Name, source); err != nil {
				t.Error(err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if err := db.UpdateMany(ctx, cfg.Name, []whisper.TimeSeriesPoint{{Time: 990, Value: float64(i)}}); err != nil {
				t.Error(err)
			}
		}
	}()
	wg.Wait()
	snap, err = db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Archives[0].Points[1].Value != 19 {
		t.Fatalf("fill lost acknowledged update: %+v", snap)
	}
}

func TestImportRejectsOversizedArchiveBeforeAllocation(t *testing.T) {
	db := testStore(t, 1000)
	path := filepath.Join(t.TempDir(), "bad.wsp")
	cfg := config("bad", whisper.Sum)
	w, err := whisper.Create(path, whisper.NewRetentionsNoPointer(cfg.Retentions), cfg.AggregationMethod, cfg.XFilesFactor)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], 20_000_000)
	_, err = file.WriteAt(count[:], int64(whisper.MetadataSize+8))
	closeErr := file.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if _, err := db.ImportWSP(context.Background(), "bad", path, false); err == nil {
		t.Fatal("oversized truncated archive imported")
	}
	if _, err := db.Metadata(context.Background(), "bad"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed import published catalog: %v", err)
	}
}

func TestFillRejectsDifferentPolicyWithoutPublishing(t *testing.T) {
	db := testStore(t, 1000)
	ctx := context.Background()
	before, err := db.Create(ctx, config("policy", whisper.Average))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config("policy", whisper.Sum)
	path := filepath.Join(t.TempDir(), "sum.wsp")
	w, err := whisper.Create(path, whisper.NewRetentionsNoPointer(cfg.Retentions), cfg.AggregationMethod, cfg.XFilesFactor)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.FillWSP(ctx, "policy", path); !errors.Is(err, ErrConflict) {
		t.Fatalf("different policy filled: %v", err)
	}
	after, err := db.Metadata(ctx, "policy")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("failed fill published: before=%+v after=%+v", before, after)
	}
}
