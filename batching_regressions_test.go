package whisper

import (
	"bufio"
	"context"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

// These tests exercise the library at the same write boundaries as the
// go-carbon cache batching can produce.  They deliberately use a fixed clock:
// retention selection must not depend on when a test happened to run.
func TestBatchBoundariesPreserveCompressedOOOData(t *testing.T) {
	const now = 1700000000
	nativeFreezeNow(t, now)
	base := now - 20
	inputs := []*TimeSeriesPoint{
		{Time: base - 40, Value: 10},
		{Time: base - 30, Value: 20},
		{Time: base - 20, Value: 30},
		{Time: base - 10, Value: 40},
		{Time: base - 8, Value: 50},
		{Time: base - 6, Value: 60},
		{Time: base - 4, Value: 70},
		{Time: base - 3, Value: 70},
		// Later samples at existing intervals are last-write-wins.
		{Time: base - 30, Value: 21},
		{Time: base - 8, Value: 51},
		{Time: base - 3, Value: 71},
		{Time: base - 1, Value: 81},
	}

	type result struct {
		before nativeReplaySnapshot
		after  nativeReplaySnapshot
	}
	results := make(map[int]result)
	for _, batchSize := range []int{1, 4, 8} {
		path := filepath.Join(t.TempDir(), "metric.wsp")
		w, err := CreateWithOptions(path, MustParseRetentionDefs("1s:40s,10s:5m"), Average, 0,
			&Options{Compressed: true, FLock: true, OutOfOrder: true})
		if err != nil {
			t.Fatalf("batch %d create: %v", batchSize, err)
		}
		for start := 0; start < len(inputs); start += batchSize {
			end := start + batchSize
			if end > len(inputs) {
				end = len(inputs)
			}
			if err := w.UpdateMany(append([]*TimeSeriesPoint(nil), inputs[start:end]...)); err != nil {
				_ = w.Close()
				t.Fatalf("batch %d update %d:%d: %v", batchSize, start, end, err)
			}
		}
		before := nativeSnapshot(t, w, now)
		nativeAssertExpectedReplay(t, before, base)
		if err := w.MergeOutOfOrder(); err != nil {
			_ = w.Close()
			t.Fatalf("batch %d compact: %v", batchSize, err)
		}
		after := nativeSnapshot(t, w, now)
		nativeAssertExpectedReplay(t, after, base)
		if err := w.Close(); err != nil {
			t.Fatalf("batch %d close: %v", batchSize, err)
		}
		results[batchSize] = result{before: before, after: after}
	}

	for _, batchSize := range []int{4, 8} {
		nativeAssertSnapshotEqual(t, "before batch 1 and batch "+strconv.Itoa(batchSize), results[1].before, results[batchSize].before)
		nativeAssertSnapshotEqual(t, "after batch 1 and batch "+strconv.Itoa(batchSize), results[1].after, results[batchSize].after)
	}
	for _, batchSize := range []int{1, 4, 8} {
		nativeAssertSnapshotEqual(t, "compaction batch "+strconv.Itoa(batchSize), results[batchSize].before, results[batchSize].after)
	}
}

func TestCompressedRetentionCutoffKeepsExactBoundary(t *testing.T) {
	const now = 1700000000
	nativeFreezeNow(t, now)
	path := filepath.Join(t.TempDir(), "retention.wsp")
	w, err := CreateWithOptions(path, MustParseRetentionDefs("1s:10s,10s:1m"), Sum, 0,
		&Options{Compressed: true, OutOfOrder: true})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: now - 10, Value: 1}, {Time: now - 11, Value: 2}}); err != nil {
		t.Fatal(err)
	}
	if got := nativeArchiveValue(t, w, 0, now-10); got != 1 {
		t.Fatalf("raw cutoff value = %v; want 1", got)
	}
	if got := nativeArchiveValue(t, w, 1, now-20); got != 2 {
		t.Fatalf("coarse older value = %v; want 2", got)
	}
}

func TestHistoricalCoarseCorrectionPropagatesAllArchives(t *testing.T) {
	const now = 1700000000
	nativeFreezeNow(t, now)
	base := now - 180
	base -= base % 60
	path := filepath.Join(t.TempDir(), "three-archive.wsp")
	w, err := CreateWithOptions(path, MustParseRetentionDefs("1s:40s,10s:5m,60s:1h"), Sum, 0.5,
		&Options{Compressed: true, FLock: true, OutOfOrder: true})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	var initial []*TimeSeriesPoint
	for window := 0; window < 3; window++ {
		for slot := 0; slot < 6; slot++ {
			initial = append(initial, &TimeSeriesPoint{Time: base + window*60 + slot*10, Value: float64(slot + 1)})
		}
	}
	if err := w.UpdateMany(initial); err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: base + 20, Value: 100}}); err != nil {
		t.Fatal(err)
	}
	if got := nativeArchiveValue(t, w, 1, base+20); got != 100 {
		t.Fatalf("direct 10-second correction = %v; want 100", got)
	}
	if got := nativeArchiveValue(t, w, 2, base); got != 118 {
		t.Fatalf("recomputed 60-second sum = %v; want 118", got)
	}
}

func TestHistoricalCorrectionMergesExistingSidecarGap(t *testing.T) {
	const start = 1699999800
	const now = start + 300
	nativeFreezeNow(t, now)
	path := filepath.Join(t.TempDir(), "metric.wsp")
	rets := MustParseRetentionDefs("1s:20s,10s:10m,60s:1h")
	opts := &Options{Compressed: true, FLock: true, OutOfOrder: true}
	w, err := CreateWithOptions(path, rets, Average, 0.25, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: start, Value: 10}, {Time: start + 20, Value: 30}, {Time: start + 180, Value: 1}, {Time: start + 240, Value: 1}}); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	// Model an already-persisted sparse aggregate which fills a main-file gap.
	side, err := Create(OutOfOrderSidecarPath(path), rets, Average, 0.25)
	if err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := side.ReplaceArchivePoints(1, []TimeSeriesPoint{{Time: start + 10, Value: 20}}); err != nil {
		_ = side.Close()
		_ = w.Close()
		t.Fatal(err)
	}
	if err := side.Close(); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = OpenWithOptions(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: start, Value: 100}}); err != nil {
		t.Fatal(err)
	}
	if got := nativeArchiveValue(t, w, 1, start); got != 100 {
		t.Fatalf("explicit correction = %v; want 100", got)
	}
	if got := nativeArchiveValue(t, w, 1, start+10); got != 20 {
		t.Fatalf("sidecar gap fill = %v; want 20", got)
	}
	if got := nativeArchiveValue(t, w, 2, start); got != 50 {
		t.Fatalf("coarse aggregate = %v; want 50", got)
	}
}

func TestHistoricalCorrectionFailureCanRetry(t *testing.T) {
	const now = 1700000000
	nativeFreezeNow(t, now)
	path := filepath.Join(t.TempDir(), "metric.wsp")
	opts := &Options{Compressed: true, FLock: true, OutOfOrder: true}
	w, err := CreateWithOptions(path, MustParseRetentionDefs("1s:40s,10s:5m"), Average, 0.5, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: now - 60, Value: 10}, {Time: now - 50, Value: 20}}); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	blocked := path + ".correct"
	if err := os.Mkdir(blocked, 0700); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blocked, "block"), []byte("block"), 0600); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: now - 60, Value: 99}}); err == nil {
		_ = w.Close()
		t.Fatal("correction rewrite unexpectedly succeeded")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(blocked); err != nil {
		t.Fatal(err)
	}
	w, err = OpenWithOptions(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if got := nativeArchiveValue(t, w, 1, now-60); got != 10 {
		t.Fatalf("failed rewrite changed data to %v; want 10", got)
	}
	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: now - 60, Value: 99}}); err != nil {
		t.Fatal(err)
	}
	if got := nativeArchiveValue(t, w, 1, now-60); got != 99 {
		t.Fatalf("retry correction = %v; want 99", got)
	}
}

func TestPathLockConcurrentCreateAndRename(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metric.wsp")
	const creators = 12
	start := make(chan struct{})
	errs := make(chan error, creators)
	var wg sync.WaitGroup
	wg.Add(creators)
	for i := 0; i < creators; i++ {
		go func() {
			defer wg.Done()
			<-start
			w, err := CreateWithOptions(path, MustParseRetentionDefs("1s:1h"), Average, 0.5, &Options{FLock: true})
			if err == nil {
				err = w.Close()
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		if !os.IsExist(err) {
			t.Fatalf("concurrent create error = %v; want already-exists", err)
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent creates succeeded %d times; want 1", successes)
	}
	if _, err := os.Stat(auxiliaryPath(path, lockSuffix)); err != nil {
		t.Fatalf("persistent path lock: %v", err)
	}

	holder, err := OpenWithOptions(path, &Options{FLock: true})
	if err != nil {
		t.Fatal(err)
	}
	replacement := filepath.Join(filepath.Dir(path), "replacement.wsp")
	replaced, err := Create(replacement, MustParseRetentionDefs("1s:1h"), Average, 0.5)
	if err != nil {
		_ = holder.Close()
		t.Fatal(err)
	}
	if err := replaced.Close(); err != nil {
		_ = holder.Close()
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		_ = holder.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		w, err := OpenWithOptions(path, &Options{FLock: true})
		if err == nil {
			err = w.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		_ = holder.Close()
		t.Fatalf("open after rename completed while path lock held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := holder.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("open did not resume after path lock release")
	}
}

func TestPathLockBlocksSeparateProcess(t *testing.T) {
	if os.Getenv("GO_WHISPER_LOCK_HELPER") == "1" {
		nativePathLockHelper(t)
		return
	}
	path := filepath.Join(t.TempDir(), "metric.wsp")
	w, err := Create(path, MustParseRetentionDefs("1s:1h"), Average, 0.5)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	holder, err := OpenWithOptions(path, &Options{FLock: true})
	if err != nil {
		t.Fatal(err)
	}
	// Replace the data inode while retaining the stable path lock.
	replacement := filepath.Join(filepath.Dir(path), "replacement.wsp")
	replacementFile, err := Create(replacement, MustParseRetentionDefs("1s:1h"), Average, 0.5)
	if err != nil {
		_ = holder.Close()
		t.Fatal(err)
	}
	if err := replacementFile.Close(); err != nil {
		_ = holder.Close()
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		_ = holder.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPathLockBlocksSeparateProcess$")
	cmd.Env = append(os.Environ(), "GO_WHISPER_LOCK_HELPER=1", "GO_WHISPER_LOCK_PATH="+path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = holder.Close()
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		_ = holder.Close()
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "blocked" {
		_ = holder.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("helper did not become ready: %v", scanner.Err())
	}
	if err := holder.Close(); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal(err)
	}
	if !scanner.Scan() || scanner.Text() != "acquired" {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("helper did not acquire released lock: %v", scanner.Err())
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("helper: %v", err)
	}
}

func TestFailedFlockedOpenDoesNotLeakDescriptor(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("descriptor count is checked through /proc/self/fd")
	}
	before := nativeFDCount(t)
	path := filepath.Join(t.TempDir(), "not-a-whisper.wsp")
	if err := os.WriteFile(path, []byte("not a whisper file"), 0600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 32; i++ {
		if _, err := OpenWithOptions(path, &Options{FLock: true}); err == nil {
			t.Fatal("opening an invalid file unexpectedly succeeded")
		}
	}
	after := nativeFDCount(t)
	if after > before {
		t.Fatalf("failed flocked opens leaked descriptors: before=%d after=%d", before, after)
	}
}

func nativePathLockHelper(t *testing.T) {
	path := os.Getenv("GO_WHISPER_LOCK_PATH")
	if path == "" {
		t.Fatal("missing GO_WHISPER_LOCK_PATH")
	}
	// Verify the separate process is excluded even after the data inode changed.
	lock, err := acquirePathLock(path, syscall.LOCK_EX|syscall.LOCK_NB)
	if lock != nil {
		_ = lock.Close()
	}
	if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("nonblocking lock while parent holds path: %v", err)
	}
	if _, err := os.Stdout.WriteString("blocked\n"); err != nil {
		t.Fatal(err)
	}
	w, err := OpenWithOptions(path, &Options{FLock: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stdout.WriteString("acquired\n"); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

type nativeReplaySnapshot struct {
	raw    nativeSeries
	coarse nativeSeries
}

type nativeSeries struct {
	from   int
	step   int
	values []float64
}

func nativeFreezeNow(t *testing.T, now int) {
	t.Helper()
	previous := Now
	Now = func() time.Time { return time.Unix(int64(now), 0) }
	t.Cleanup(func() { Now = previous })
}

func nativeSnapshot(t *testing.T, w *Whisper, now int) nativeReplaySnapshot {
	t.Helper()
	return nativeReplaySnapshot{
		raw:    nativeFetch(t, w, now-35, now),
		coarse: nativeFetch(t, w, now-180, now-45),
	}
}

func nativeFetch(t *testing.T, w *Whisper, from, until int) nativeSeries {
	t.Helper()
	ts, err := w.Fetch(from, until)
	if err != nil {
		t.Fatalf("fetch %d..%d: %v", from, until, err)
	}
	if ts == nil {
		t.Fatalf("fetch %d..%d returned nil", from, until)
	}
	return nativeSeries{from: ts.FromTime(), step: ts.Step(), values: append([]float64(nil), ts.Values()...)}
}

func nativeAssertExpectedReplay(t *testing.T, snapshot nativeReplaySnapshot, base int) {
	t.Helper()
	for _, want := range []struct {
		timestamp int
		value     float64
	}{
		{base - 10, 40},
		{base - 8, 51},
		{base - 6, 60},
		{base - 4, 70},
		{base - 3, 71},
		{base - 1, 81},
	} {
		if got := nativeSeriesValue(snapshot.raw, want.timestamp); got != want.value {
			t.Errorf("raw value at %d = %v; want %v", want.timestamp, got, want.value)
		}
	}
	if got := nativeSeriesValue(snapshot.coarse, base-30); got != 21 {
		t.Errorf("coarse correction at %d = %v; want 21", base-30, got)
	}
}

func nativeArchiveValue(t *testing.T, w *Whisper, archive, timestamp int) float64 {
	t.Helper()
	points, err := w.ArchivePoints(archive)
	if err != nil {
		t.Fatal(err)
	}
	for _, point := range points {
		if point.Time == timestamp {
			return point.Value
		}
	}
	return math.NaN()
}

func nativeSeriesValue(series nativeSeries, timestamp int) float64 {
	index := (timestamp - series.from) / series.step
	if index < 0 || index >= len(series.values) || series.from+index*series.step != timestamp {
		return math.NaN()
	}
	return series.values[index]
}

func nativeAssertSnapshotEqual(t *testing.T, name string, want, got nativeReplaySnapshot) {
	t.Helper()
	nativeAssertSeriesEqual(t, name+" raw", want.raw, got.raw)
	nativeAssertSeriesEqual(t, name+" coarse", want.coarse, got.coarse)
}

func nativeAssertSeriesEqual(t *testing.T, name string, want, got nativeSeries) {
	t.Helper()
	if want.from != got.from || want.step != got.step || len(want.values) != len(got.values) {
		t.Fatalf("%s shape got=(%d,%d,%d) want=(%d,%d,%d)", name, got.from, got.step, len(got.values), want.from, want.step, len(want.values))
	}
	for i := range want.values {
		if want.values[i] == got.values[i] || math.IsNaN(want.values[i]) && math.IsNaN(got.values[i]) {
			continue
		}
		t.Fatalf("%s value at %d = %v; want %v", name, want.from+i*want.step, got.values[i], want.values[i])
	}
}

func nativeFDCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

func TestSidecarRetentionBoundarySurvivesRewrite(t *testing.T) {
	const now = 1700000000
	previousNow := Now
	Now = func() time.Time { return time.Unix(now, 0) }
	defer func() { Now = previousNow }()
	for _, correction := range []bool{false, true} {
		name := "compaction"
		if correction {
			name = "coarse-correction"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "metric.wsp")
			opts := &Options{Compressed: true, FLock: true, OutOfOrder: true}
			w, err := CreateWithOptions(path, MustParseRetentionDefs("1s:40s,10s:5m"), Average, 0.5, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			var input []*TimeSeriesPoint
			for timestamp := now - 39; timestamp < now; timestamp++ {
				input = append(input, &TimeSeriesPoint{Time: timestamp, Value: 1})
			}
			input = append(input, &TimeSeriesPoint{Time: now - 100, Value: 10}, &TimeSeriesPoint{Time: now - 60, Value: 20})
			if err := w.UpdateMany(input); err != nil {
				t.Fatal(err)
			}
			if err := w.UpdateMany([]*TimeSeriesPoint{{Time: now - 40, Value: 42}}); err != nil {
				t.Fatal(err)
			}
			if w.OutOfOrderPath() == "" {
				t.Fatal("boundary point was not diverted to sidecar")
			}
			if got := nativeArchiveValue(t, w, 0, now-40); got != 42 {
				t.Fatalf("retention boundary value = %v; want 42", got)
			}
			if correction {
				if err := w.UpdateMany([]*TimeSeriesPoint{{Time: now - 100, Value: 99}}); err != nil {
					t.Fatal(err)
				}
			} else if err := w.MergeOutOfOrder(); err != nil {
				t.Fatal(err)
			}
			if got := nativeArchiveValue(t, w, 0, now-40); got != 42 {
				t.Fatalf("retention boundary value = %v; want 42", got)
			}
			if _, err := os.Stat(OutOfOrderSidecarPath(path)); !os.IsNotExist(err) {
				t.Fatalf("sidecar was not removed: %v", err)
			}
		})
	}
}
