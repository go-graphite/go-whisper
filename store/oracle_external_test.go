package store_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
	"github.com/go-graphite/go-whisper/store"
)

// Compare operation traces, including the pinned library's boundary behavior,
// rather than deriving expected results from the shared store implementation.
func TestClassicOracleTraces(t *testing.T) {
	oldNow := whisper.Now
	defer func() { whisper.Now = oldNow }()
	ctx := context.Background()
	for _, method := range []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.First, whisper.Last, whisper.Min, whisper.Max} {
		for _, xff := range []float32{0, .5, 1} {
			t.Run(fmt.Sprintf("%s/xff=%g", method, xff), func(t *testing.T) {
				now := 1700000400
				clock := func() time.Time { return time.Unix(int64(now), 0) }
				whisper.Now = clock
				rets := []whisper.Retention{whisper.NewRetention(1, 60), whisper.NewRetention(5, 36), whisper.NewRetention(30, 30)}
				legacy, err := whisper.Create(filepath.Join(t.TempDir(), "oracle.wsp"), whisper.NewRetentionsNoPointer(rets), method, xff)
				if err != nil {
					t.Fatal(err)
				}
				defer legacy.Close()
				db, err := store.Open(t.TempDir(), store.Options{MemTableSize: 1 << 20, CacheSize: 1 << 20, Now: clock})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if _, err = db.Create(ctx, store.MetricConfig{Name: "oracle.metric", Retentions: rets, AggregationMethod: method, XFilesFactor: xff}); err != nil {
					t.Fatal(err)
				}
				rng := rand.New(rand.NewSource(42))
				check := func(op int) {
					t.Helper()
					windows := [][2]int{{now - 30, now}, {now - 59, now}, {now - 60, now}, {now - 61, now}, {now - 179, now}, {now - 180, now}, {now - 899, now}, {now - 10, now - 10}, {now + 1, now + 10}, {now - 1100, now - 950}, {now, now - 1}}
					for _, w := range windows {
						expected, ee := legacy.Fetch(w[0], w[1])
						actual, ae := db.Fetch(ctx, "oracle.metric", w[0], w[1])
						if (ee == nil) != (ae == nil) {
							t.Fatalf("op %d window %v: errors legacy=%v store=%v", op, w, ee, ae)
						}
						if ee != nil {
							continue
						}
						if (expected == nil) != (actual == nil) {
							t.Fatalf("op %d window %v: nil legacy=%v store=%v", op, w, expected, actual)
						}
						if expected == nil {
							continue
						}
						if expected.FromTime() != actual.FromTime || expected.UntilTime() != actual.UntilTime || expected.Step() != actual.Step || len(expected.Values()) != len(actual.Values) {
							t.Fatalf("op %d window %v: grid legacy=(%d,%d,%d,%d) store=(%d,%d,%d,%d)", op, w, expected.FromTime(), expected.UntilTime(), expected.Step(), len(expected.Values()), actual.FromTime, actual.UntilTime, actual.Step, len(actual.Values))
						}
						for i, want := range expected.Values() {
							got := actual.Values[i]
							if math.IsNaN(want) && math.IsNaN(got) {
								continue
							}
							if math.Float64bits(want) != math.Float64bits(got) {
								t.Fatalf("op %d window %v timestamp %d: want %g got %g", op, w, actual.FromTime+i*actual.Step, want, got)
							}
						}
					}
				}
				check(-1)
				for op := 0; op < 80; op++ {
					now += 3
					if op%7 == 0 {
						ts := now - rng.Intn(930)
						v := float64(rng.Intn(21) - 10)
						ee := legacy.Update(v, ts)
						ae := db.Update(ctx, "oracle.metric", v, ts)
						if (ee == nil) != (ae == nil) {
							t.Fatalf("op %d Update timestamp %d: legacy=%v store=%v", op, ts, ee, ae)
						}
					} else {
						points := make([]whisper.TimeSeriesPoint, 0, 12)
						for i := 0; i < 8; i++ {
							age := []int{rng.Intn(50), 59, 60, 61, 179, 180, 899, 930}[i]
							points = append(points, whisper.TimeSeriesPoint{Time: now - age, Value: float64(rng.Intn(21) - 10)})
						}
						points = append(points, whisper.TimeSeriesPoint{Time: now - 3, Value: 7}, whisper.TimeSeriesPoint{Time: now - 3, Value: 11})
						if op%5 == 0 {
							points = append(points, whisper.TimeSeriesPoint{Time: now + 2, Value: 99})
						}
						rng.Shuffle(len(points), func(i, j int) { points[i], points[j] = points[j], points[i] })
						copies := append([]whisper.TimeSeriesPoint(nil), points...)
						ptrs := make([]*whisper.TimeSeriesPoint, len(copies))
						for i := range copies {
							ptrs[i] = &copies[i]
						}
						ee := legacy.UpdateMany(ptrs)
						ae := db.UpdateMany(ctx, "oracle.metric", points)
						if ee != nil || ae != nil {
							t.Fatalf("op %d UpdateMany: legacy=%v store=%v", op, ee, ae)
						}
					}
					check(op)
				}
			})
		}
	}
}
