package whisper

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"syscall"
	"testing"
)

// Frozen reader from the preceding revision: the oracle deliberately does not
// share the extent walker or its range/record-boundary calculations.
func referenceArchivePointsInRange(w *Whisper, index, from, until int) ([]dataPoint, error) {
	archive := w.archives[index]
	if until < from {
		return nil, nil
	}
	from += mod(-from, archive.secondsPerPoint)
	if until < from {
		return nil, nil
	}
	offset, remaining := archive.Offset(), archive.Size()
	if from >= 0 && until-from < archive.MaxRetention()-archive.secondsPerPoint {
		base := w.getBaseInterval(archive)
		if base == 0 {
			return nil, nil
		}
		offset = archive.PointOffset(base, from)
		remaining = ((until-from)/archive.secondsPerPoint + 1) * PointSize
	}
	bufferSize := 4096 * PointSize
	if remaining < bufferSize {
		bufferSize = remaining
	}
	buf := make([]byte, bufferSize)
	var points []dataPoint
	for remaining > 0 {
		size := len(buf)
		if size > remaining {
			size = remaining
		}
		if int64(size) > archive.End()-offset {
			size = int(archive.End() - offset)
		}
		chunk := buf[:size]
		if err := w.fileReadAt(chunk, offset); err != nil {
			return nil, fmt.Errorf("read archive %d: %w", index, err)
		}
		for i := 0; i < len(chunk); i += PointSize {
			p := unpackDataPoint(chunk[i : i+PointSize])
			if p.interval > 0 && p.interval >= from && p.interval <= until {
				points = append(points, p)
			}
		}
		remaining -= size
		offset += int64(size)
		if offset == archive.End() {
			offset = archive.Offset()
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].interval < points[j].interval })
	return points, nil
}

// OutOfOrderPath returns the path of this file's out-of-order sidecar if one
// exists, otherwise the empty string.

func assertArchivePointBits(t *testing.T, got, want []dataPoint) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("points: got %d, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i].interval != want[i].interval || math.Float64bits(got[i].value) != math.Float64bits(want[i].value) {
			t.Fatalf("point %d: got %+v/%x, want %+v/%x", i, got[i], math.Float64bits(got[i].value), want[i], math.Float64bits(want[i].value))
		}
	}
}

func TestArchiveExtentRecordBoundaries(t *testing.T) {
	// Every possible alignment of a 12-byte record against a block edge.
	for archiveStart := int64(0); archiveStart < PointSize; archiveStart++ {
		calls := 0
		r := &archiveExtentReader{seek: func(offset int64, whence int) (int64, error) {
			calls++
			switch whence {
			case io.SeekCurrent:
				return 37, nil
			case io.SeekStart:
				if offset != 37 {
					t.Fatalf("offset not restored: %d", offset)
				}
				return offset, nil
			case seekArchiveData:
				return 4096, nil
			case seekArchiveHole:
				return 8192, nil
			}
			panic("unexpected seek")
		}}
		from, to, err := r.next(archiveStart, archiveStart+12000, archiveStart)
		if err != nil || from > 4096 || to < 8192 || (from-archiveStart)%PointSize != 0 || (to-archiveStart)%PointSize != 0 || 4096-from >= PointSize || to-8192 >= PointSize {
			t.Fatalf("alignment %d: %d..%d, %v", archiveStart, from, to, err)
		}
		n := calls
		a, b, err := r.next(from+PointSize, to, archiveStart)
		if err != nil || a != from+PointSize || b != to || calls != n {
			t.Fatalf("extent cache: %d..%d, calls %d -> %d, %v", a, b, n, calls, err)
		}
	}
}

func TestArchiveExtentFallbacks(t *testing.T) {
	for _, tc := range []struct {
		name             string
		fail             int
		data, hole, size int64
		err, errorStat   error
		from, to         int64
		disabled         bool
	}{
		{name: "unsupported", fail: seekArchiveData, err: syscall.EINVAL, from: 100, to: 1000, disabled: true},
		{name: "seek-current-failed", fail: io.SeekCurrent, err: syscall.EBADF, from: 100, to: 1000, disabled: true},
		{name: "hole-seek-failed", fail: seekArchiveHole, err: syscall.EINVAL, data: 200, from: 100, to: 1000, disabled: true},
		{name: "all-hole", fail: seekArchiveData, err: syscall.ENXIO, size: 1000, from: 1000, to: 1000},
		{name: "truncated", fail: seekArchiveData, err: syscall.ENXIO, size: 999, from: 100, to: 1000, disabled: true},
		{name: "stat-failed", fail: seekArchiveData, err: syscall.ENXIO, errorStat: syscall.EBADF, from: 100, to: 1000, disabled: true},
		{name: "data-outside-range", data: 2000, from: 1000, to: 1000},
		{name: "invalid-data", data: 99, from: 100, to: 1000, disabled: true},
		{name: "invalid-hole", data: 200, hole: 200, from: 100, to: 1000, disabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restored := false
			r := &archiveExtentReader{seek: func(offset int64, whence int) (int64, error) {
				if whence == tc.fail && tc.err != nil {
					return 0, tc.err
				}
				switch whence {
				case io.SeekCurrent:
					return 37, nil
				case io.SeekStart:
					restored = offset == 37
					return offset, nil
				case seekArchiveData:
					return tc.data, nil
				case seekArchiveHole:
					return tc.hole, nil
				}
				panic("unexpected seek")
			}, size: func() (int64, error) { return tc.size, tc.errorStat }}
			a, b, err := r.next(100, 1000, 100)
			if err != nil || a != tc.from || b != tc.to || r.disabled != tc.disabled {
				t.Fatalf("got %d..%d disabled=%v err=%v", a, b, r.disabled, err)
			}
			if tc.fail != io.SeekCurrent && !restored {
				t.Fatal("seek position not restored")
			}
		})
	}
	boom := errors.New("restore failed")
	r := &archiveExtentReader{seek: func(offset int64, whence int) (int64, error) {
		if whence == io.SeekStart {
			return 0, boom
		}
		if whence == seekArchiveData {
			return 2000, nil
		}
		return 37, nil
	}}
	if _, _, err := r.next(100, 1000, 100); !errors.Is(err, boom) {
		t.Fatalf("restore error: %v", err)
	}
	var empty *archiveExtentReader
	if a, b, err := empty.next(100, 1000, 100); a != 100 || b != 1000 || err != nil {
		t.Fatal("nil fallback")
	}
}
