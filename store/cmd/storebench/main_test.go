package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
	"github.com/go-graphite/go-whisper/store"
)

func TestMakeUpdatesUsesOnlyMissingSlotsFromBlankFixtures(t *testing.T) {
	retention := whisper.NewRetention(1, 10)
	snapshots := []snapshot{
		{name: "blank", data: store.Snapshot{Archives: []store.Archive{{Retention: retention}}}},
		{name: "existing", data: store.Snapshot{Archives: []store.Archive{{Retention: retention, Points: []whisper.TimeSeriesPoint{{Time: 99, Value: 1}}}}}},
	}
	got := makeUpdates(snapshots, time.Unix(100, 0), 3)
	points := got["blank"]
	if len(points) != 3 || points[0].Time != 99 || points[1].Time != 98 || points[2].Time != 97 {
		t.Fatalf("updates = %#v, want newest-first missing slots", points)
	}
	if _, ok := got["existing"]; ok {
		t.Fatal("existing fixture received an update workload")
	}
}

func TestQueryChecksumCoversCoarseArchives(t *testing.T) {
	oldNow := whisper.Now
	whisper.Now = func() time.Time { return time.Unix(100, 0) }
	defer func() { whisper.Now = oldNow }()
	dir := t.TempDir()
	rets := []whisper.Retention{whisper.NewRetention(1, 10), whisper.NewRetention(10, 10)}
	base := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: "metric", Retentions: rets, AggregationMethod: whisper.Average}}, Archives: []store.Archive{{Retention: rets[0], Points: []whisper.TimeSeriesPoint{{Time: 99, Value: 1}}}, {Retention: rets[1], Points: []whisper.TimeSeriesPoint{{Time: 90, Value: 1}}}}}
	firstDir, secondDir := dir+"/first", dir+"/second"
	if err := os.Mkdir(firstDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(secondDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeWhisper(firstDir+"/metric.wsp", base, false); err != nil {
		t.Fatal(err)
	}
	first, err := queryChecksumWhisper(firstDir, []snapshot{{name: "metric", data: base}}, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	base.Archives[1].Points[0].Value = 2
	if err := writeWhisper(secondDir+"/metric.wsp", base, false); err != nil {
		t.Fatal(err)
	}
	second, err := queryChecksumWhisper(secondDir, []snapshot{{name: "metric", data: base}}, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("coarse archive change did not alter query checksum")
	}
}

func TestOOOScenarioSurvivesReopenWithCoarseRollup(t *testing.T) {
	result, err := runOOOScenario(context.Background(), filepath.Join(t.TempDir(), "scenario"))
	if err != nil {
		t.Fatal(err)
	}
	if !result.SidecarBeforeMerge || !result.Parity || result.CoarseSum != 51 {
		t.Fatalf("scenario did not exercise sidecar and rollup: %+v", result)
	}
}
