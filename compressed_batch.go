package whisper

import (
	"fmt"
	"sort"
)

// Materialize buffered rollups under the old policy before changing it. Reading
// those buffers under the new aggregation would retroactively change history.
func (whisper *Whisper) materializeCompressedRollups() error {
	extras := make([][]extraPoint, len(whisper.archives))
	rets := make([]*Retention, len(whisper.archives))
	for i, archive := range whisper.archives {
		points, err := whisper.fetchCompressed(1, int64(maxInt), archive)
		if err != nil {
			return err
		}
		values := make(map[int]dataPoint, len(points))
		for _, point := range points {
			values[point.interval] = point
		}
		for _, point := range values {
			extras[i] = append(extras[i], extraPoint{dataPoint: point, replace: true})
		}
		sort.Slice(extras[i], func(a, b int) bool { return extras[i][a].interval < extras[i][b].interval })
		ret := archive.Retention
		rets[i] = &ret
	}
	return whisper.rewrite(rets, "batch", func(i int) []extraPoint { return extras[i] })
}

// compressedBatchOverlaps reports a cross-resolution circular-slot collision.
// Buffering propagation until read time cannot preserve the write order when
// a later explicit coarse write replaces a freshly propagated finer sample.
func (whisper *Whisper) compressedBatchOverlaps(points []*TimeSeriesPoint, now int) bool {
	if !whisper.compressed || whisper.aggregationMethod == Mix {
		return false
	}
	var finer []*TimeSeriesPoint
	remaining := points
	for _, archive := range whisper.archives {
		current, rest := extractPoints(remaining, now, archive.MaxRetention())
		remaining = rest
		slots := make(map[int]int, len(finer))
		for _, point := range finer {
			interval := point.Time - mod(point.Time, archive.secondsPerPoint)
			slots[mod(interval/archive.secondsPerPoint, archive.numberOfPoints)] = interval
		}
		for _, point := range current {
			interval := point.Time - mod(point.Time, archive.secondsPerPoint)
			if previous, ok := slots[mod(interval/archive.secondsPerPoint, archive.numberOfPoints)]; ok && previous != interval {
				return true
			}
		}
		finer = append(finer, current...)
	}
	return false
}

// updateCompressedOverlappingBatch uses the classic circular write/propagation
// order for an exceptional batch that overlaps slots across resolutions. The
// scratch file is in memory; only the finished compressed replacement is
// published, under the caller's existing path lock. Ordinary writes keep the
// incremental compressed path.
func (whisper *Whisper) updateCompressedOverlappingBatch(points []*TimeSeriesPoint) error {
	if err := whisper.MergeOutOfOrder(); err != nil {
		return err
	}
	name := auxiliaryPath(whisper.file.Name(), ".batch")
	replay, err := CreateWithOptions(name, NewRetentionsNoPointer(whisper.Retentions()), whisper.aggregationMethod, whisper.xFilesFactor, &Options{InMemory: true})
	if err != nil {
		return fmt.Errorf("create overlapping batch scratch: %w", err)
	}
	defer func() {
		_ = replay.Close()
		releaseMemFile(name)
	}()
	for i, archive := range whisper.archives {
		stored, err := whisper.fetchCompressed(1, int64(maxInt), archive)
		if err != nil {
			return err
		}
		values := make([]TimeSeriesPoint, len(stored))
		for j, point := range stored {
			values[j] = TimeSeriesPoint{Time: point.interval, Value: point.value}
		}
		if err := replay.ReplaceArchivePoints(i, values); err != nil {
			return err
		}
	}
	// Restore the caller order that UpdateMany's reverse/stable sort expects.
	input := append([]*TimeSeriesPoint(nil), points...)
	reversePoints(input)
	if err := replay.UpdateMany(input); err != nil {
		return err
	}
	extras := make([][]extraPoint, len(whisper.archives))
	for i := range extras {
		stored, err := replay.ArchivePoints(i)
		if err != nil {
			return err
		}
		for _, point := range stored {
			extras[i] = append(extras[i], extraPoint{dataPoint: dataPoint{point.Time, point.Value}, replace: true})
		}
	}
	rets := make([]*Retention, len(whisper.archives))
	for i, archive := range whisper.archives {
		ret := archive.Retention
		rets[i] = &ret
	}
	return whisper.rewrite(rets, "batch", func(i int) []extraPoint { return extras[i] })
}
