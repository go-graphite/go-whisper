package whisper

import (
	"math"
	"path/filepath"
	"testing"
	"time"
)

// Daily state publishers resend the same timestamp after a DAG changes state.
// Match classic Whisper even when the original sample has left the compressed
// archive's buffer, and keep the correction through repeated compactions.
func TestOutOfOrderHistoricalCorrectionsMatchClassic(t *testing.T) {
	retentions, err := ParseRetentionDefs("1m:14d,30m:2y")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	classicPath := filepath.Join(dir, "classic.wsp")
	compressedPath := filepath.Join(dir, "compressed.wsp")
	classic, err := CreateWithOptions(classicPath, retentions, Average, 0, &Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = classic.Close() }()
	compressed, err := CreateWithOptions(compressedPath, retentions, Average, 0, &Options{Compressed: true, OutOfOrder: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = compressed.Close() }()
	base := int(time.Now().Add(-10 * 24 * time.Hour).Truncate(24 * time.Hour).Unix())
	for day := 0; day < 8; day++ {
		for _, w := range []*Whisper{classic, compressed} {
			if err := w.UpdateMany([]*TimeSeriesPoint{{Time: base + day*86400, Value: 0}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	check := func(stage string, from, until int) {
		t.Helper()
		want, err := classic.Fetch(from, until)
		if err != nil {
			t.Fatal(err)
		}
		got, err := compressed.Fetch(from, until)
		if err != nil {
			t.Fatal(err)
		}
		if got.Step() != want.Step() {
			t.Fatalf("%s: step=%d, want %d", stage, got.Step(), want.Step())
		}
		if len(got.Values()) != len(want.Values()) {
			t.Fatalf("%s: got %d samples, want %d", stage, len(got.Values()), len(want.Values()))
		}
		for i, v := range want.Values() {
			if actual := got.Values()[i]; !(actual == v || math.IsNaN(actual) && math.IsNaN(v)) {
				t.Fatalf("%s: value[%d]=%v, classic=%v", stage, i, actual, v)
			}
		}
	}
	// Include a downward correction: this is last-write-wins, not max(value).
	for _, value := range []float64{1, 3, 0, 1} {
		for _, correction := range []float64{5, value} {
			for _, w := range []*Whisper{classic, compressed} {
				if err := w.UpdateMany([]*TimeSeriesPoint{{Time: base + 2*86400, Value: correction}}); err != nil {
					t.Fatal(err)
				}
			}
		}
		if compressed.OutOfOrderPoints == 0 {
			t.Fatal("correction was not diverted; test did not exercise an encoded sample")
		}
		check("before reopen", base-60, base+8*86400)
		if err := compressed.Close(); err != nil {
			t.Fatal(err)
		}
		compressed, err = OpenWithOptions(compressedPath, &Options{OutOfOrder: true})
		if err != nil {
			t.Fatal(err)
		}
		check("after reopen", base-60, base+8*86400)
		for merge := 0; merge < 2; merge++ {
			if err := compressed.MergeOutOfOrder(); err != nil {
				t.Fatal(err)
			}
			check("after compaction", base-60, base+8*86400)
			// A dashboard looking back beyond the base retention reads the
			// coarse archive, whose values must incorporate the correction.
			check("coarse after compaction", base-5*86400, base+5*86400)
		}
	}
}

func TestOutOfOrderCorrectionRecomputesCompleteAggregate(t *testing.T) {
	w, _, base := newTwoRetentionOOO(t, -1, 0)
	defer w.Close()
	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: base + 2, Value: 4}}); err != nil {
		t.Fatal(err)
	}
	ts, err := w.Fetch(base+1, base+2)
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, ts, []float64{4})
	// The sidecar's partial sum is 4; retain the complete on-time sum until
	// compaction can recompute all ten samples as 9*1 + 4 = 13.
	if got := coarseValueAt(t, w, base); got != 10 {
		t.Fatalf("coarse before compaction=%v, want 10", got)
	}
	if err := w.MergeOutOfOrder(); err != nil {
		t.Fatal(err)
	}
	if got := coarseValueAt(t, w, base); got != 13 {
		t.Fatalf("coarse after compaction=%v, want 13", got)
	}
}
