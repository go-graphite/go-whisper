package whisper

import (
	"math"
	"testing"
)

// Frozen classic reader from d84403499e32. It remains independent of the
// optimized raw-buffer reader so classic itself has an unchanged oracle.
func (whisper *Whisper) readSeriesBeforeOptimization(start, end int64, archive *archiveInfo) ([]dataPoint, error) {
	var b []byte
	if start < end {
		b = make([]byte, end-start)
		err := whisper.fileReadAt(b, start)
		if err != nil {
			return nil, err
		}
	} else {
		b = make([]byte, archive.End()-start)
		err := whisper.fileReadAt(b, start)
		if err != nil {
			return nil, err
		}
		b2 := make([]byte, end-archive.Offset())
		err = whisper.fileReadAt(b2, archive.Offset())
		if err != nil {
			return nil, err
		}
		b = append(b, b2...)
	}
	return unpackDataPoints(b), nil
}

func (whisper *Whisper) fetchBeforeReadOptimization(archive *archiveInfo, fromTime, untilTime int) (*TimeSeries, error) {
	fromInterval := archive.Interval(fromTime)
	untilInterval := archive.Interval(untilTime)

	baseInterval := whisper.getBaseInterval(archive)

	if baseInterval == 0 {
		step := archive.secondsPerPoint
		points := (untilInterval - fromInterval) / step
		values := make([]float64, points)
		for i := range values {
			values[i] = math.NaN()
		}
		return &TimeSeries{fromInterval, untilInterval, step, values}, nil
	}

	// Zero-length time range: always include the next point
	if fromInterval == untilInterval {
		untilInterval += archive.SecondsPerPoint()
	}

	fromOffset := archive.PointOffset(baseInterval, fromInterval)
	untilOffset := archive.PointOffset(baseInterval, untilInterval)

	series, err := whisper.readSeriesBeforeOptimization(fromOffset, untilOffset, archive)
	if err != nil {
		return nil, err
	}

	values := make([]float64, len(series))
	for i := range values {
		values[i] = math.NaN()
	}
	currentInterval := fromInterval
	step := archive.secondsPerPoint

	for i, dPoint := range series {
		if dPoint.interval == currentInterval {
			values[i] = dPoint.value
		}
		currentInterval += step
	}
	return &TimeSeries{fromInterval, untilInterval, step, values}, nil
}

func TestClassicReadMatchesFrozenReader(t *testing.T) {
	now := correctnessClock(t)
	w := correctnessFile(t, false, "1s:1m,10s:1h", 0)
	values := []float64{0, math.Copysign(0, -1), math.Inf(1), math.Inf(-1), math.Float64frombits(0x7ff8000000000123), 1.5, -17}
	for _, archive := range w.archives {
		for round := 0; round < 3; round++ {
			var input []*TimeSeriesPoint
			for i := 0; i < archive.numberOfPoints; i++ {
				if i%7 == 0 {
					continue
				}
				input = append(input, &TimeSeriesPoint{Time: *now - archive.MaxRetention() + i*archive.secondsPerPoint, Value: values[i%len(values)]})
			}
			if err := w.archiveUpdateMany(archive, input); err != nil {
				t.Fatal(err)
			}
			for from := *now - archive.MaxRetention(); from <= *now; from += archive.secondsPerPoint {
				for _, width := range []int{0, 1, archive.secondsPerPoint, 17 * archive.secondsPerPoint, archive.MaxRetention()} {
					until := from + width
					if until > *now {
						until = *now
					}
					want, err := w.fetchBeforeReadOptimization(archive, from, until)
					if err != nil {
						t.Fatal(err)
					}
					got, err := w.fetchFromArchive(archive, from, until)
					if err != nil {
						t.Fatal(err)
					}
					if got.fromTime != want.fromTime || got.untilTime != want.untilTime || got.step != want.step || len(got.values) != len(want.values) {
						t.Fatalf("grid differs: got=%+v want=%+v", got, want)
					}
					for i, v := range want.values {
						if math.Float64bits(got.values[i]) != math.Float64bits(v) {
							t.Fatalf("from=%d until=%d index=%d bits=%x want=%x", from, until, i, math.Float64bits(got.values[i]), math.Float64bits(v))
						}
					}
				}
			}
			*now += archive.MaxRetention()
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.readSeries(w.archives[0].Offset(), w.archives[0].End(), w.archives[0]); err == nil {
		t.Fatal("expected read error from closed file")
	}
}
