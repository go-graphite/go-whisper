package whisper

import (
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkCompressedOverlapReplay(b *testing.B) {
	const now = 1791198000
	previousNow := Now
	Now = func() time.Time { return time.Unix(now, 0) }
	b.Cleanup(func() { Now = previousNow })
	rets, err := ParseRetentionDefs("1s:6h,1m:30d")
	if err != nil {
		b.Fatal(err)
	}
	name := filepath.Join(b.TempDir(), "replay.wsp")
	w, err := CreateWithOptions(name, rets, Average, .5, &Options{Compressed: true, InMemory: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = w.Close(); releaseMemFile(name) })
	var seed []*TimeSeriesPoint
	for ts := now - 30*86400 + 60; ts < now-6*3600; ts += 60 {
		seed = append(seed, &TimeSeriesPoint{Time: ts, Value: float64(ts % 97)})
	}
	for ts := now - 6*3600; ts < now; ts++ {
		seed = append(seed, &TimeSeriesPoint{Time: ts, Value: float64(ts % 97)})
	}
	if err := w.UpdateMany(seed); err != nil {
		b.Fatal(err)
	}
	point := &TimeSeriesPoint{Time: now - 100}
	input := []*TimeSeriesPoint{point}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		point.Value = float64(i + 1)
		if err := w.updateCompressedOverlappingBatch(input); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	series, err := w.Fetch(now-101, now-99)
	if err != nil {
		b.Fatal(err)
	}
	found := false
	for i, value := range series.Values() {
		if series.FromTime()+i*series.Step() == now-100 {
			found = true
			if value != float64(b.N) {
				b.Fatalf("replay value = %v, want %v", value, b.N)
			}
		}
	}
	if !found {
		b.Fatal("replayed timestamp absent")
	}
}
