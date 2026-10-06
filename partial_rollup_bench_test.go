package whisper

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkCompressedPartialRollupContinuation(b *testing.B) {
	for _, policy := range []struct {
		method AggregationMethod
		xff    float32
	}{{Last, 0}, {Average, .5}} {
		b.Run(fmt.Sprintf("%s/xff=%g", policy.method, policy.xff), func(b *testing.B) {
			benchmarkCompressedPartialRollupContinuation(b, policy.method, policy.xff)
		})
	}
}

func benchmarkCompressedPartialRollupContinuation(b *testing.B, method AggregationMethod, xff float32) {
	now := 1791198055
	previousNow := Now
	Now = func() time.Time { return time.Unix(int64(now), 0) }
	b.Cleanup(func() { Now = previousNow })
	// The same four archive grids seen on affected secondly files on 6301.
	rets := MustParseRetentionDefs("1s:1d,1m:30d,1h:1y,1d:10y")
	path := filepath.Join(b.TempDir(), "metric.wsp")
	opts := &Options{Compressed: true, OutOfOrder: true, FLock: true}
	w, err := CreateWithOptions(path, rets, method, xff, opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = w.Close() })
	for _, archive := range w.archives {
		last := archive.Interval(now - 1)
		points := make([]dataPoint, archive.numberOfPoints)
		for i := range points {
			points[i] = dataPoint{last - (len(points)-1-i)*archive.secondsPerPoint, 1}
		}
		if _, err := archive.appendToBlockAndRotate(points); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.WriteHeaderCompressed(); err != nil {
		b.Fatal(err)
	}
	const batchSize = 16
	var values [batchSize]TimeSeriesPoint
	var points [batchSize]*TimeSeriesPoint
	for i := range points {
		points[i] = &values[i]
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range points {
			points[j] = &values[j]
			values[j] = TimeSeriesPoint{Time: now + j, Value: float64((i*batchSize + j) % 97)}
		}
		now += batchSize
		if err := w.UpdateMany(points[:]); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
		w, err = OpenWithOptions(path, opts)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batchSize), "ns/point")
	series, err := w.Fetch(now-2, now)
	if err != nil {
		b.Fatal(err)
	}
	if got, want := series.Values()[0], float64((b.N*batchSize-1)%97); got != want {
		b.Fatalf("last point = %v, want %v", got, want)
	}
}
