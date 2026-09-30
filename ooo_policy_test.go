package whisper

import (
	"os"
	"testing"
	"time"
)

func TestOutOfOrderMergePolicySurvivesReopen(t *testing.T) {
	w, path, base := newSingleRetentionOOO(t, true)
	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 7}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := OpenWithOptions(path, &Options{OutOfOrder: true})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if w.OutOfOrderPoints != 0 {
		t.Fatal("expected a fresh handle counter")
	}
	options := OutOfOrderMergeOptions{MinPoints: 2, MaxPointAge: 2 * time.Hour, RetentionMargin: time.Minute}
	merged, err := w.MergeOutOfOrderWithOptions(options)
	if err != nil || merged {
		t.Fatalf("one pending point: merged=%v, err=%v", merged, err)
	}
	// Overwriting a pending timestamp must not inflate the live-record count.
	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 8}}); err != nil {
		t.Fatal(err)
	}
	merged, err = w.MergeOutOfOrderWithOptions(options)
	if err != nil || merged {
		t.Fatalf("duplicate pending point: merged=%v, err=%v", merged, err)
	}
	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: base + 3, Value: 9}}); err != nil {
		t.Fatal(err)
	}
	merged, err = w.MergeOutOfOrderWithOptions(options)
	if err != nil || !merged {
		t.Fatalf("two pending points: merged=%v, err=%v", merged, err)
	}
	ts, err := w.Fetch(base-1, base+4)
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, ts, []float64{1, 8, 2, 9, 3})
	if _, err := os.Stat(OutOfOrderSidecarPath(path)); !os.IsNotExist(err) {
		t.Fatalf("sidecar after merge: %v", err)
	}
}

func TestOutOfOrderMergePolicyLimits(t *testing.T) {
	for _, tt := range []struct {
		name    string
		options OutOfOrderMergeOptions
		want    bool
	}{
		{"below limits", OutOfOrderMergeOptions{MinPoints: 100, MaxPointAge: 2 * time.Hour}, false},
		{"sample age", OutOfOrderMergeOptions{MinPoints: 100, MaxPointAge: time.Minute}, true},
		{"retention urgency", OutOfOrderMergeOptions{MinPoints: 100, RetentionMargin: 2 * time.Hour}, true},
		{"unconditional", OutOfOrderMergeOptions{}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w, _, base := newSingleRetentionOOO(t, true)
			defer w.Close()
			if err := w.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 7}}); err != nil {
				t.Fatal(err)
			}
			merged, err := w.MergeOutOfOrderWithOptions(tt.options)
			if err != nil || merged != tt.want {
				t.Fatalf("merged=%v, want %v; err=%v", merged, tt.want, err)
			}
		})
	}
}

func TestOutOfOrderMergePolicyValidation(t *testing.T) {
	w, _, _ := newSingleRetentionOOO(t, true)
	defer w.Close()
	for _, options := range []OutOfOrderMergeOptions{{MinPoints: -1}, {MaxPointAge: -1}, {RetentionMargin: -1}} {
		if _, err := w.MergeOutOfOrderWithOptions(options); err == nil {
			t.Fatalf("accepted %+v", options)
		}
	}
	merged, err := w.MergeOutOfOrderWithOptions(OutOfOrderMergeOptions{MinPoints: 100})
	if err != nil || merged {
		t.Fatalf("absent sidecar: merged=%v, err=%v", merged, err)
	}
}
