// storebench compares the shared store with fresh classic and compressed
// Whisper files made from the same archive-preserving corpus snapshots.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	whisper "github.com/go-graphite/go-whisper"
	"github.com/go-graphite/go-whisper/store"
)

const (
	defaultBatch  = 64
	defaultUpdate = 128
)

type report struct {
	Corpus       string      `json:"corpus"`
	GeneratedAt  time.Time   `json:"generated_at"`
	FrozenNow    time.Time   `json:"frozen_now"`
	Durability   string      `json:"durability"`
	Caveats      []string    `json:"caveats"`
	Logical      logical     `json:"logical"`
	Classic      engine      `json:"classic"`
	Compressed   engine      `json:"compressed"`
	SharedPebble engine      `json:"shared_pebble"`
	OOO          oooScenario `json:"ooo_scenario"`
}
type oooScenario struct {
	ClassicWrite       time.Duration `json:"classic_write"`
	CompressedWrite    time.Duration `json:"compressed_write"`
	PebbleWrite        time.Duration `json:"pebble_write"`
	CoarseSum          float64       `json:"coarse_sum"`
	SidecarBeforeMerge bool          `json:"sidecar_before_merge"`
	ClassicValue       float64       `json:"classic_value"`
	CompressedValue    float64       `json:"compressed_value"`
	PebbleValue        float64       `json:"pebble_value"`
	Merge              time.Duration `json:"merge"`
	Parity             bool          `json:"parity"`
}

type logical struct {
	Metrics  int    `json:"metrics"`
	Points   int    `json:"points"`
	Checksum string `json:"checksum"`
}

type engine struct {
	Import            timing `json:"import"`
	Update            timing `json:"update"`
	Merge             timing `json:"merge,omitempty"`
	Read              timing `json:"read"`
	Reopen            timing `json:"reopen"`
	Parity            bool   `json:"parity"`
	Checksum          string `json:"import_checksum"`
	ChecksumKind      string `json:"import_checksum_kind"`
	PostChecksum      string `json:"post_update_query_checksum"`
	LogicalBytes      int64  `json:"logical_bytes"`
	AllocatedBytes    int64  `json:"allocated_bytes"`
	TransientBytes    int64  `json:"transient_bytes"`
	FileCount         int    `json:"file_count"`
	WALFileCount      int    `json:"wal_file_count"`
	WALAllocatedBytes int64  `json:"wal_allocated_bytes"`
}

type timing struct {
	Operations int           `json:"operations"`
	Duration   time.Duration `json:"duration"`
	P50        time.Duration `json:"p50"`
	P95        time.Duration `json:"p95"`
	P99        time.Duration `json:"p99"`
}

type snapshot struct {
	name string
	data store.Snapshot
}

type footprint struct {
	logical, allocated, walAllocated int64
	files, walFiles                  int
}

func main() {
	corpus := flag.String("corpus", "", "directory containing classic .wsp corpus files (required)")
	output := flag.String("output", "", "new directory for benchmark output (required)")
	format := flag.String("format", "json", "report format: json")
	updates := flag.Int("updates", defaultUpdate, "out-of-order updates per metric")
	batch := flag.Int("batch", defaultBatch, "points per update batch")
	reads := flag.Int("read-repetitions", 20, "warm-cache reads per metric")
	flag.Parse()
	if *corpus == "" || *output == "" || *updates < 0 || *batch < 1 || *reads < 1 || *format != "json" {
		fmt.Fprintln(os.Stderr, "-corpus and -output are required; -updates >= 0, -batch and -read-repetitions >= 1, and -format=json")
		os.Exit(2)
	}
	if err := run(*corpus, *output, *updates, *batch, *reads); err != nil {
		fmt.Fprintln(os.Stderr, "storebench:", err)
		os.Exit(1)
	}
}

func run(corpus, output string, updateCount, batchSize, reads int) error {
	if err := os.Mkdir(output, 0o700); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	ctx := context.Background()
	snapshots, frozen, err := loadCorpus(ctx, corpus)
	if err != nil {
		return err
	}
	oldNow := whisper.Now
	whisper.Now = func() time.Time { return frozen }
	defer func() { whisper.Now = oldNow }()

	want := checksumSnapshots(snapshots)
	logicalPoints := pointCount(snapshots)
	r := report{
		Corpus: corpus, GeneratedAt: time.Now().UTC(), FrozenNow: frozen.UTC(),
		Durability: "Shared Pebble writes use synchronous WAL commits. Classic and compressed baseline writes use the go-whisper file write path; this command does not add fsync.",
		Caveats: []string{
			"Read samples are warm-cache full-coarsest-retention queries; correctness separately checks every retention. No cold-cache, concurrency or sustained-throughput measurement is claimed.",
			"Reopen times open and close all seven classic/compressed handles versus reopening one shared store; import timing includes format conversion overhead.",
			"Allocated bytes use filesystem blocks (stat.Blocks*512); logical bytes use file lengths.",
			"Pebble transient bytes include WAL and files before Flush and Compact; steady bytes are measured after both.",
			"The corpus is test-generated data frozen around 2020, not a production workload. Same-name .wsp and .cwsp source peers are intentionally not compared directly.",
			"Corpus write timings use only blank classic fixtures because populated test fixtures may contain stale historic aggregates. OOO sidecar behavior is measured separately with current-time synthetic data.",
		},
		Logical: logical{Metrics: len(snapshots), Points: logicalPoints, Checksum: want},
	}

	if r.Classic, err = runClassic(ctx, filepath.Join(output, "classic"), snapshots, frozen, false, updateCount, batchSize, reads, want, ""); err != nil {
		return fmt.Errorf("classic: %w", err)
	}
	if r.Compressed, err = runClassic(ctx, filepath.Join(output, "compressed"), snapshots, frozen, true, updateCount, batchSize, reads, want, filepath.Join(output, "classic")); err != nil {
		return fmt.Errorf("compressed: %w", err)
	}
	if r.SharedPebble, err = runPebble(ctx, filepath.Join(output, "pebble"), snapshots, frozen, updateCount, batchSize, reads, want, filepath.Join(output, "classic")); err != nil {
		return fmt.Errorf("shared Pebble: %w", err)
	}
	if r.OOO, err = runOOOScenario(ctx, filepath.Join(output, "ooo")); err != nil {
		return fmt.Errorf("OOO scenario: %w", err)
	}

	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	return os.WriteFile(filepath.Join(output, "report.json"), append(b, '\n'), 0o600)
}

func loadCorpus(ctx context.Context, dir string) ([]snapshot, time.Time, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.wsp"))
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("list corpus: %w", err)
	}
	if len(paths) == 0 {
		return nil, time.Time{}, errors.New("corpus has no classic .wsp files")
	}
	result := make([]snapshot, 0, len(paths))
	var newest int
	for _, path := range paths {
		w, err := whisper.Open(path)
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("open %s: %w", path, err)
		}
		cfg := store.MetricConfig{Name: strings.TrimSuffix(filepath.Base(path), ".wsp"), Retentions: w.Retentions(), AggregationMethod: w.AggregationMethod(), XFilesFactor: w.XFilesFactor()}
		s := store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: make([]store.Archive, len(cfg.Retentions))}
		for i, retention := range cfg.Retentions {
			points, e := w.ArchivePoints(i)
			if e != nil {
				_ = w.Close()
				return nil, time.Time{}, fmt.Errorf("snapshot %s archive %d: %w", path, i, e)
			}
			s.Archives[i] = store.Archive{Retention: retention, Points: points}
			for _, p := range points {
				if !math.IsNaN(p.Value) && p.Time > newest {
					newest = p.Time
				}
			}
		}
		if err := w.Close(); err != nil {
			return nil, time.Time{}, fmt.Errorf("close %s: %w", path, err)
		}
		result = append(result, snapshot{name: cfg.Name, data: s})
	}
	if newest == 0 {
		return nil, time.Time{}, errors.New("corpus contains no points to freeze time")
	}
	return result, time.Unix(int64(newest), 0), nil
}

func runClassic(ctx context.Context, dir string, snapshots []snapshot, frozen time.Time, compressed bool, updates, batch, reads int, want, postReference string) (engine, error) {
	if err := os.Mkdir(dir, 0o700); err != nil {
		return engine{}, err
	}
	start := time.Now()
	for _, s := range snapshots {
		if err := writeWhisper(filepath.Join(dir, s.name+".wsp"), s.data, compressed); err != nil {
			return engine{}, err
		}
	}
	imported := time.Since(start)
	got, err := readWhisperSnapshots(dir, snapshots, frozen, compressed)
	if err != nil {
		return engine{}, err
	}
	checksum := checksumSnapshots(got)
	e := engine{ChecksumKind: "archive_snapshot", Import: timing{Operations: len(snapshots), Duration: imported}, Parity: checksum == want, Checksum: checksum}
	if compressed {
		checksum, err = queryChecksumWhisper(dir, snapshots, frozen)
		if err != nil {
			return engine{}, err
		}
		expected, err := queryChecksumSnapshots(dir, snapshots, frozen)
		if err != nil {
			return engine{}, err
		}
		e.Checksum, e.Parity, e.ChecksumKind = checksum, checksum == expected, "retention_queries"
	}
	if !e.Parity {
		return engine{}, errors.New("snapshot parity mismatch before updates")
	}
	readTimes, err := measureWhisperReads(dir, snapshots, frozen, reads)
	if err != nil {
		return engine{}, err
	}
	e.Read = summarize(readTimes)
	updatesByMetric := makeUpdates(snapshots, frozen, updates)
	updateTimes, err := updateWhisper(dir, updatesByMetric, compressed, batch)
	if err != nil {
		return engine{}, err
	}
	e.Update = summarize(updateTimes)
	if compressed {
		mergeTimes, err := mergeCompressed(dir, snapshots)
		if err != nil {
			return engine{}, err
		}
		e.Merge = summarize(mergeTimes)
	}
	if postReference == "" {
		e.PostChecksum, err = queryChecksumWhisper(dir, snapshots, frozen)
		if err != nil {
			return engine{}, err
		}
	}
	if postReference != "" {
		got, err := queryChecksumWhisper(dir, snapshots, frozen)
		if err != nil {
			return engine{}, err
		}
		expected, err := queryChecksumWhisper(postReference, snapshots, frozen)
		if err != nil {
			return engine{}, err
		}
		e.PostChecksum = got
		if got != expected {
			return engine{}, errors.New("post-update query parity mismatch")
		}
	}
	e.Reopen = timing{Operations: 1}
	start = time.Now()
	for _, snap := range snapshots {
		w, err := whisper.Open(filepath.Join(dir, snap.name+".wsp"))
		if err != nil {
			return engine{}, err
		}
		if err := w.Close(); err != nil {
			return engine{}, err
		}
	}
	e.Reopen.Duration = time.Since(start)
	f, err := inspect(dir)
	if err != nil {
		return engine{}, err
	}
	e.LogicalBytes, e.AllocatedBytes, e.FileCount, e.WALFileCount, e.WALAllocatedBytes = f.logical, f.allocated, f.files, f.walFiles, f.walAllocated
	return e, nil
}

func writeWhisper(path string, s store.Snapshot, compressed bool) error {
	if compressed {
		classicPath := path + ".classic"
		if err := writeWhisper(classicPath, s, false); err != nil {
			return err
		}
		w, err := whisper.Open(classicPath)
		if err != nil {
			return err
		}
		err = w.CompressTo(path)
		closeErr := w.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		return os.Remove(classicPath)
	}
	opts := &whisper.Options{Compressed: compressed, OutOfOrder: compressed}
	w, err := whisper.CreateWithOptions(path, whisper.NewRetentionsNoPointer(s.Metadata.Retentions), s.Metadata.AggregationMethod, s.Metadata.XFilesFactor, opts)
	if err != nil {
		return err
	}
	defer w.Close()
	for i, a := range s.Archives {
		if len(a.Points) == 0 {
			continue
		}
		if err := w.ReplaceArchivePoints(i, a.Points); err != nil {
			return err
		}
	}
	return nil
}

func readWhisperSnapshots(dir string, source []snapshot, frozen time.Time, compressed bool) ([]snapshot, error) {
	result := make([]snapshot, 0, len(source))
	for _, expected := range source {
		path := filepath.Join(dir, expected.name+".wsp")
		w, err := whisper.Open(path)
		if err != nil {
			return nil, err
		}
		s := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: expected.name, Retentions: w.Retentions(), AggregationMethod: w.AggregationMethod(), XFilesFactor: w.XFilesFactor()}}, Archives: make([]store.Archive, len(w.Retentions()))}
		for i, retention := range w.Retentions() {
			if compressed {
				break
			}
			points, e := w.ArchivePoints(i)
			if e != nil {
				_ = w.Close()
				return nil, e
			}
			s.Archives[i] = store.Archive{Retention: retention, Points: points}
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		result = append(result, snapshot{name: expected.name, data: s})
	}
	return result, nil
}

func queryChecksumWhisper(dir string, snapshots []snapshot, frozen time.Time) (string, error) {
	h := sha256.New()
	for _, s := range snapshots {
		w, err := whisper.Open(filepath.Join(dir, s.name+".wsp"))
		if err != nil {
			return "", err
		}
		for _, retention := range w.Retentions() {
			from := int(frozen.Unix()) - retention.MaxRetention() + retention.SecondsPerPoint()
			ts, fetchErr := w.Fetch(from, int(frozen.Unix()))
			if fetchErr != nil {
				_ = w.Close()
				return "", fetchErr
			}
			if ts == nil {
				fmt.Fprintf(h, "%s:nil:%d;", s.name, retention.SecondsPerPoint())
				continue
			}
			fmt.Fprintf(h, "%s:%d:%d:", s.name, ts.Step(), ts.FromTime())
			for _, p := range ts.Points() {
				fmt.Fprintf(h, "%d:%x;", p.Time, math.Float64bits(p.Value))
			}
		}
		if err := w.Close(); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func queryChecksumStore(ctx context.Context, db *store.Store, snapshots []snapshot, frozen time.Time) (string, error) {
	h := sha256.New()
	for _, snap := range snapshots {
		for _, retention := range snap.data.Metadata.Retentions {
			from := int(frozen.Unix()) - retention.MaxRetention() + retention.SecondsPerPoint()
			ts, err := db.Fetch(ctx, snap.name, from, int(frozen.Unix()))
			if err != nil {
				return "", err
			}
			if ts == nil {
				fmt.Fprintf(h, "%s:nil:%d;", snap.name, retention.SecondsPerPoint())
				continue
			}
			fmt.Fprintf(h, "%s:%d:%d:", snap.name, ts.Step, ts.FromTime)
			for i, v := range ts.Values {
				fmt.Fprintf(h, "%d:%x;", ts.FromTime+i*ts.Step, math.Float64bits(v))
			}
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func queryChecksumSnapshots(dir string, snapshots []snapshot, frozen time.Time) (string, error) {
	tmp := filepath.Join(dir, ".query-reference")
	if err := os.Mkdir(tmp, 0o700); err != nil {
		return "", err
	}
	defer os.RemoveAll(tmp)
	for _, s := range snapshots {
		if err := writeWhisper(filepath.Join(tmp, s.name+".wsp"), s.data, false); err != nil {
			return "", err
		}
	}
	return queryChecksumWhisper(tmp, snapshots, frozen)
}

func measureWhisperReads(dir string, snapshots []snapshot, frozen time.Time, repetitions int) ([]time.Duration, error) {
	times := make([]time.Duration, 0, len(snapshots)*repetitions)
	for _, s := range snapshots {
		w, err := whisper.Open(filepath.Join(dir, s.name+".wsp"))
		if err != nil {
			return nil, err
		}
		for i := 0; i < repetitions; i++ {
			start := time.Now()
			_, err = w.Fetch(0, int(frozen.Unix()))
			times = append(times, time.Since(start))
			if err != nil {
				_ = w.Close()
				return nil, err
			}
		}
		closeErr := w.Close()
		if closeErr != nil {
			return nil, closeErr
		}
	}
	return times, nil
}

func updateWhisper(dir string, updates map[string][]whisper.TimeSeriesPoint, compressed bool, batch int) ([]time.Duration, error) {
	times := make([]time.Duration, 0, len(updates))
	for name, points := range updates {
		w, err := whisper.OpenWithOptions(filepath.Join(dir, name+".wsp"), &whisper.Options{Compressed: compressed, OutOfOrder: compressed})
		if err != nil {
			return nil, err
		}
		start := time.Now()
		for from := 0; from < len(points); from += batch {
			until := from + batch
			if until > len(points) {
				until = len(points)
			}
			ptrs := make([]*whisper.TimeSeriesPoint, until-from)
			for i := range points[from:until] {
				p := points[from+i]
				ptrs[i] = &p
			}
			if err := w.UpdateMany(ptrs); err != nil {
				_ = w.Close()
				return nil, err
			}
		}
		elapsed := time.Since(start)
		if err := w.Close(); err != nil {
			return nil, err
		}
		times = append(times, elapsed)
	}
	return times, nil
}

func mergeCompressed(dir string, snapshots []snapshot) ([]time.Duration, error) {
	times := make([]time.Duration, 0, len(snapshots))
	for _, s := range snapshots {
		path := filepath.Join(dir, s.name+".wsp")
		w, err := whisper.OpenWithOptions(path, &whisper.Options{Compressed: true, OutOfOrder: true})
		if err != nil {
			return nil, err
		}
		start := time.Now()
		err = w.MergeOutOfOrder()
		elapsed := time.Since(start)
		closeErr := w.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		times = append(times, elapsed)
	}
	return times, nil
}

func sidecarHasPoints(path string) (bool, error) {
	w, err := whisper.Open(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer w.Close()
	for i := range w.Retentions() {
		points, err := w.ArchivePoints(i)
		if err != nil {
			return false, err
		}
		if len(points) > 0 {
			return true, nil
		}
	}
	return false, nil
}

func runOOOScenario(ctx context.Context, dir string) (oooScenario, error) {
	if err := os.Mkdir(dir, 0700); err != nil {
		return oooScenario{}, err
	}
	now := time.Now().Truncate(time.Second)
	oldNow := whisper.Now
	whisper.Now = func() time.Time { return now }
	defer func() { whisper.Now = oldNow }()
	rets := []whisper.Retention{whisper.NewRetention(1, 300), whisper.NewRetention(10, 600)}
	base := int(now.Unix()) - 200
	base -= base % 10
	fill := make([]whisper.TimeSeriesPoint, 0, 49)
	for i := 0; i < 50; i++ {
		if i != 5 {
			fill = append(fill, whisper.TimeSeriesPoint{Time: base + i, Value: 1})
		}
	}
	late := []whisper.TimeSeriesPoint{{Time: base + 5, Value: 42}}
	result := oooScenario{}
	for _, compressed := range []bool{false, true} {
		name := "classic"
		if compressed {
			name = "compressed"
		}
		path := filepath.Join(dir, name+".wsp")
		w, err := whisper.CreateWithOptions(path, whisper.NewRetentionsNoPointer(rets), whisper.Sum, 0, &whisper.Options{Compressed: compressed, OutOfOrder: compressed, PointsPerBlock: 100})
		if err != nil {
			return result, fmt.Errorf("create %s: %w", name, err)
		}
		err = w.UpdateMany(pointPointers(fill))
		start := time.Now()
		if err == nil {
			err = w.UpdateMany(pointPointers(late))
		}
		elapsed := time.Since(start)
		closeErr := w.Close()
		if err != nil {
			return result, fmt.Errorf("write %s: %w", name, err)
		}
		if closeErr != nil {
			return result, closeErr
		}
		if !compressed {
			result.ClassicWrite = elapsed
			continue
		}
		result.CompressedWrite = elapsed
		result.SidecarBeforeMerge, err = sidecarHasPoints(whisper.OutOfOrderSidecarPath(path))
		if err != nil {
			return result, fmt.Errorf("read sidecar: %w", err)
		}
		if !result.SidecarBeforeMerge {
			return result, errors.New("OOO workload did not produce populated sidecar")
		}
		w, err = whisper.Open(path)
		if err != nil {
			return result, fmt.Errorf("reopen compressed: %w", err)
		}
		start = time.Now()
		err = w.MergeOutOfOrder()
		result.Merge = time.Since(start)
		closeErr = w.Close()
		if err != nil {
			return result, fmt.Errorf("merge compressed: %w", err)
		}
		if closeErr != nil {
			return result, closeErr
		}
	}
	dbPath := filepath.Join(dir, "pebble")
	opts := store.Options{Now: func() time.Time { return now }}
	db, err := store.Open(dbPath, opts)
	if err != nil {
		return result, err
	}
	_, err = db.Create(ctx, store.MetricConfig{Name: "metric", Retentions: rets, AggregationMethod: whisper.Sum})
	if err == nil {
		err = db.UpdateMany(ctx, "metric", fill)
	}
	start := time.Now()
	if err == nil {
		err = db.UpdateMany(ctx, "metric", late)
	}
	result.PebbleWrite = time.Since(start)
	closeErr := db.Close()
	if err != nil {
		return result, err
	}
	if closeErr != nil {
		return result, closeErr
	}
	db, err = store.Open(dbPath, opts)
	if err != nil {
		return result, err
	}
	defer db.Close()
	classic, err := whisper.Open(filepath.Join(dir, "classic.wsp"))
	if err != nil {
		return result, err
	}
	defer classic.Close()
	compressed, err := whisper.Open(filepath.Join(dir, "compressed.wsp"))
	if err != nil {
		return result, err
	}
	defer compressed.Close()
	// The coarse window stops before cwhisper's two live buffer windows.
	for i, window := range [][2]int{{base - 1, base + 49}, {int(now.Unix()) - 301, base + 20}} {
		want, err := classic.Fetch(window[0], window[1])
		if err != nil {
			return result, err
		}
		cw, err := compressed.Fetch(window[0], window[1])
		if err != nil {
			return result, err
		}
		got, err := db.Fetch(ctx, "metric", window[0], window[1])
		if err != nil {
			return result, err
		}
		if want == nil || cw == nil || got == nil {
			return result, errors.New("missing OOO query result")
		}
		if want.FromTime() != cw.FromTime() || want.UntilTime() != cw.UntilTime() || want.Step() != cw.Step() || want.FromTime() != got.FromTime || want.UntilTime() != got.UntilTime || want.Step() != got.Step || len(want.Values()) != len(got.Values) || len(want.Values()) != len(cw.Values()) {
			return result, errors.New("OOO query grid mismatch")
		}
		for j, v := range want.Values() {
			same := func(a, b float64) bool {
				return (math.IsNaN(a) && math.IsNaN(b)) || math.Float64bits(a) == math.Float64bits(b)
			}
			if !same(v, cw.Values()[j]) || !same(v, got.Values[j]) {
				return result, fmt.Errorf("OOO value mismatch at %d: classic=%v compressed=%v pebble=%v", got.FromTime+j*got.Step, v, cw.Values()[j], got.Values[j])
			}
		}
		if i == 0 {
			result.ClassicValue = valueAt(want.Points(), base+5)
			result.CompressedValue = valueAt(cw.Points(), base+5)
			result.PebbleValue = valueAtSeries(got, base+5)
		} else {
			result.CoarseSum = valueAt(want.Points(), base)
		}
	}
	result.Parity = result.ClassicValue == 42 && result.CompressedValue == 42 && result.PebbleValue == 42 && result.CoarseSum == 51
	if !result.Parity {
		return result, fmt.Errorf("unexpected OOO result: %+v", result)
	}
	return result, nil
}

func valueAt(points []whisper.TimeSeriesPoint, t int) float64 {
	for _, p := range points {
		if p.Time == t {
			return p.Value
		}
	}
	return math.NaN()
}
func valueAtSeries(s *store.Series, t int) float64 {
	if s == nil {
		return math.NaN()
	}
	i := (t - s.FromTime) / s.Step
	if i < 0 || i >= len(s.Values) {
		return math.NaN()
	}
	return s.Values[i]
}

func runPebble(ctx context.Context, dir string, snapshots []snapshot, frozen time.Time, updates, batch, reads int, want, postReference string) (engine, error) {
	if err := os.Mkdir(dir, 0o700); err != nil {
		return engine{}, err
	}
	dbDir := filepath.Join(dir, "db")
	importsDir := filepath.Join(dir, "imports")
	s, err := store.Open(dbDir, store.Options{CacheSize: 8 << 20, MemTableSize: 4 << 20, Now: func() time.Time { return frozen }})
	if err != nil {
		return engine{}, err
	}
	start := time.Now()
	for _, snap := range snapshots {
		if err := os.MkdirAll(importsDir, 0o700); err != nil {
			_ = s.Close()
			return engine{}, err
		}
		wsp := filepath.Join(importsDir, snap.name+".wsp")
		if err := writeWhisper(wsp, snap.data, false); err != nil {
			_ = s.Close()
			return engine{}, err
		}
		if _, err := s.ImportWSP(ctx, snap.name, wsp, false); err != nil {
			_ = s.Close()
			return engine{}, err
		}
	}
	imported := time.Since(start)
	got, err := readPebbleSnapshots(ctx, s, snapshots)
	if err != nil {
		_ = s.Close()
		return engine{}, err
	}
	e := engine{ChecksumKind: "archive_snapshot", Import: timing{Operations: len(snapshots), Duration: imported}, Parity: checksumSnapshots(got) == want, Checksum: checksumSnapshots(got)}
	if !e.Parity {
		_ = s.Close()
		return engine{}, errors.New("snapshot parity mismatch before updates")
	}
	exportDir := filepath.Join(dir, "exports")
	for _, snap := range snapshots {
		if err := s.ExportWSP(ctx, snap.name, filepath.Join(exportDir, snap.name+".wsp")); err != nil {
			_ = s.Close()
			return engine{}, err
		}
	}
	readTimes := make([]time.Duration, 0, len(snapshots)*reads)
	for _, snap := range snapshots {
		for i := 0; i < reads; i++ {
			start = time.Now()
			_, err := s.Fetch(ctx, snap.name, 0, int(frozen.Unix()))
			if err != nil {
				_ = s.Close()
				return engine{}, err
			}
			readTimes = append(readTimes, time.Since(start))
		}
	}
	e.Read = summarize(readTimes)
	transient, err := inspect(dbDir)
	if err != nil {
		_ = s.Close()
		return engine{}, err
	}
	updateTimes := make([]time.Duration, 0, len(snapshots))
	for name, points := range makeUpdates(snapshots, frozen, updates) {
		start = time.Now()
		for from := 0; from < len(points); from += batch {
			until := from + batch
			if until > len(points) {
				until = len(points)
			}
			if err := s.UpdateMany(ctx, name, points[from:until]); err != nil {
				_ = s.Close()
				return engine{}, err
			}
		}
		updateTimes = append(updateTimes, time.Since(start))
	}
	e.Update = summarize(updateTimes)
	e.TransientBytes = transient.allocated
	if err := s.Flush(); err != nil {
		_ = s.Close()
		return engine{}, err
	}
	if err := s.Compact(); err != nil {
		_ = s.Close()
		return engine{}, err
	}
	if err := s.Close(); err != nil {
		return engine{}, err
	}
	e.Reopen.Operations = 1
	start = time.Now()
	s, err = store.Open(dbDir, store.Options{CacheSize: 8 << 20, MemTableSize: 4 << 20, Now: func() time.Time { return frozen }})
	if err != nil {
		return engine{}, err
	}
	e.Reopen.Duration = time.Since(start)
	postExportDir := filepath.Join(dir, "exports-after")
	for _, snap := range snapshots {
		if err := s.ExportWSP(ctx, snap.name, filepath.Join(postExportDir, snap.name+".wsp")); err != nil {
			_ = s.Close()
			return engine{}, err
		}
	}
	gotPost, err := queryChecksumStore(ctx, s, snapshots, frozen)
	if err != nil {
		_ = s.Close()
		return engine{}, err
	}
	wantPost, err := queryChecksumWhisper(postReference, snapshots, frozen)
	if err != nil {
		_ = s.Close()
		return engine{}, err
	}
	e.PostChecksum = gotPost
	if gotPost != wantPost {
		_ = s.Close()
		return engine{}, errors.New("post-update query parity mismatch")
	}
	if err := s.Close(); err != nil {
		return engine{}, err
	}
	f, err := inspect(dbDir)
	if err != nil {
		return engine{}, err
	}
	e.LogicalBytes, e.AllocatedBytes, e.FileCount, e.WALFileCount, e.WALAllocatedBytes = f.logical, f.allocated, f.files, f.walFiles, f.walAllocated
	return e, nil
}

func readPebbleSnapshots(ctx context.Context, s *store.Store, source []snapshot) ([]snapshot, error) {
	out := make([]snapshot, 0, len(source))
	for _, expected := range source {
		snap, err := s.Snapshot(ctx, expected.name)
		if err != nil {
			return nil, err
		}
		out = append(out, snapshot{name: expected.name, data: snap})
	}
	return out, nil
}

func makeUpdates(snapshots []snapshot, frozen time.Time, count int) map[string][]whisper.TimeSeriesPoint {
	result := make(map[string][]whisper.TimeSeriesPoint, len(snapshots))
	for metricIndex, s := range snapshots {
		if len(s.data.Archives) == 0 || count == 0 {
			continue
		}
		// A corpus fixture can contain stale coarse archives. Restrict the shared
		// OOO write oracle to blank fixtures so all engines start with identical
		// aggregation inputs rather than measuring historical stale aggregates.
		hasExisting := false
		for _, a := range s.data.Archives {
			if len(a.Points) > 0 {
				hasExisting = true
				break
			}
		}
		if hasExisting {
			continue
		}
		archive := s.data.Archives[0]
		present := make(map[int]struct{}, len(archive.Points))
		for _, p := range archive.Points {
			present[p.Time] = struct{}{}
		}
		step, latest := archive.Retention.SecondsPerPoint(), int(frozen.Unix())
		points := make([]whisper.TimeSeriesPoint, 0, count)
		for t := latest - step; t >= latest-archive.Retention.MaxRetention() && len(points) < count; t -= step {
			if _, exists := present[t]; !exists {
				points = append(points, whisper.TimeSeriesPoint{Time: t, Value: float64(metricIndex*count+len(points)) + 0.5})
			}
		}
		// Newest first forces the remaining missing slots through cwhisper's OOO sidecar.
		result[s.name] = points
	}
	return result
}

func checksumSnapshots(snapshots []snapshot) string {
	h := sha256.New()
	ordered := append([]snapshot(nil), snapshots...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].name < ordered[j].name })
	for _, s := range ordered {
		fmt.Fprintf(h, "name:%d:%s;", len(s.name), s.name)
		for archiveIndex, a := range s.data.Archives {
			fmt.Fprintf(h, "archive:%d:%d:%d;", archiveIndex, a.Retention.SecondsPerPoint(), a.Retention.NumberOfPoints())
			for _, p := range a.Points {
				fmt.Fprintf(h, "%d:%x;", p.Time, math.Float64bits(p.Value))
			}
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}
func pointPointers(points []whisper.TimeSeriesPoint) []*whisper.TimeSeriesPoint {
	result := make([]*whisper.TimeSeriesPoint, len(points))
	for i := range points {
		result[i] = &points[i]
	}
	return result
}
func pointCount(snapshots []snapshot) int {
	n := 0
	for _, s := range snapshots {
		for _, a := range s.data.Archives {
			for _, p := range a.Points {
				if !math.IsNaN(p.Value) {
					n++
				}
			}
		}
	}
	return n
}
func summarize(values []time.Duration) timing {
	if len(values) == 0 {
		return timing{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	total := time.Duration(0)
	for _, v := range values {
		total += v
	}
	at := func(q float64) time.Duration { return values[int(math.Ceil(float64(len(values))*q))-1] }
	return timing{Operations: len(values), Duration: total, P50: at(.50), P95: at(.95), P99: at(.99)}
}
func inspect(root string) (footprint, error) {
	var f footprint
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		f.files++
		f.logical += info.Size()
		allocated := info.Sys().(*syscall.Stat_t).Blocks * 512
		f.allocated += allocated
		base := filepath.Base(path)
		if strings.HasSuffix(base, ".log") || strings.Contains(strings.ToLower(base), "wal") {
			f.walFiles++
			f.walAllocated += allocated
		}
		return nil
	})
	return f, err
}
