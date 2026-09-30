package whisper

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
)

// The reference operation always decodes and re-encodes. "compact" enables
// prefix copying; compare bit-identical points after reopen and further writes.
func TestCompactionPrefixRewriteOracle(t *testing.T) {
	for _, wrap := range []bool{false, true} {
		for _, placement := range []string{"none", "head", "middle", "tail", "after", "resize"} {
			t.Run(fmt.Sprintf("wrap=%v/%s", wrap, placement), func(t *testing.T) {
				dir := t.TempDir()
				ret := &Retention{secondsPerPoint: 1, numberOfPoints: 4096, avgCompressedPointSize: 14}
				opts := &Options{Compressed: true, PointsPerBlock: 64, IgnoreNowOnWrite: true}
				source, err := CreateWithOptions(filepath.Join(dir, "source"), Retentions{ret}, Last, 0, opts)
				if err != nil {
					t.Fatal(err)
				}
				defer source.Close()
				count := 2048
				if wrap {
					count = 20000
				}
				base := int(Now().Unix()) - count*2 - 100
				points := make([]*TimeSeriesPoint, count)
				for i := range points {
					value := math.Float64frombits(0x3ff0000000000000 | uint64(i*7919))
					switch i % 257 {
					case 0:
						value = math.Copysign(0, -1)
					case 1:
						value = math.Inf(1)
					case 2:
						value = math.Inf(-1)
					}
					points[i] = &TimeSeriesPoint{Time: base + 2*i, Value: value}
				}
				if err := source.UpdateMany(points); err != nil {
					t.Fatal(err)
				}
				stored, err := source.storedPoints(source.archives[0], 1, maxInt)
				if err != nil {
					t.Fatal(err)
				}
				var pending []extraPoint
				if placement != "none" {
					index := 0
					switch placement {
					case "middle", "resize":
						index = len(stored) / 2
					case "tail", "after":
						index = len(stored) - 1
					}
					stamp := stored[index].interval - 1
					if placement == "after" {
						stamp = stored[index].interval + 1
					}
					pending = []extraPoint{{dataPoint: dataPoint{stamp, -17}}}
				}
				outputs := make([]*Whisper, 2)
				image, err := os.ReadFile(source.file.Name())
				if err != nil {
					t.Fatal(err)
				}
				for i := range outputs {
					path := filepath.Join(dir, fmt.Sprint(i))
					if err := os.WriteFile(path, image, 0600); err != nil {
						t.Fatal(err)
					}
					outputs[i], err = OpenWithOptions(path, opts)
					if err != nil {
						t.Fatal(err)
					}
					op := "reference"
					if i == 1 {
						op = "compact"
					}
					newRet := *ret
					if placement == "resize" {
						newRet.avgCompressedPointSize = 7
					}
					if err := outputs[i].rewrite(Retentions{&newRet}, op, func(int) []extraPoint { return pending }); err != nil {
						t.Fatal(err)
					}
					if err := outputs[i].Close(); err != nil {
						t.Fatal(err)
					}
					outputs[i], err = OpenWithOptions(path, opts)
					if err != nil {
						t.Fatal(err)
					}
					defer outputs[i].Close()
				}
				compare := func() {
					t.Helper()
					for _, w := range outputs {
						if err := w.CheckIntegrity(); err != nil {
							t.Fatal(err)
						}
					}
					left, err := outputs[0].storedPoints(outputs[0].archives[0], 1, maxInt)
					if err != nil {
						t.Fatal(err)
					}
					right, err := outputs[1].storedPoints(outputs[1].archives[0], 1, maxInt)
					if err != nil {
						t.Fatal(err)
					}
					if len(left) != len(right) {
						t.Fatalf("point counts differ: %d != %d", len(left), len(right))
					}
					for j := range left {
						if left[j].interval != right[j].interval || math.Float64bits(left[j].value) != math.Float64bits(right[j].value) {
							t.Fatalf("point %d: %v != %v", j, left[j], right[j])
						}
					}
				}
				compare()
				// Continue writing through the copied tail and then wrap the ring.
				points = make([]*TimeSeriesPoint, 10000)
				for j := range points {
					points[j] = &TimeSeriesPoint{Time: base + 2*count + j + 2, Value: float64(j%311) / 7}
				}
				for _, w := range outputs {
					if err := w.UpdateMany(points); err != nil {
						t.Fatal(err)
					}
				}
				compare()
			})
		}
	}
}

type rejectBlockReadFile struct {
	file
	offset int64
}

func (f *rejectBlockReadFile) ReadAt(b []byte, off int64) (int, error) {
	if off == f.offset {
		return 0, io.ErrUnexpectedEOF
	}
	return f.file.ReadAt(b, off)
}

func TestCompactionBlockReadFailure(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty=%v", empty), func(t *testing.T) {
			w, path, _ := newSingleRetentionOOO(t, true)
			defer w.Close()
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var selected *blockRange
			for i := range w.archives[0].blockRanges {
				b := &w.archives[0].blockRanges[i]
				if (b.start == 0) == empty {
					selected = b
					break
				}
			}
			if selected == nil {
				t.Fatal("fixture lacks requested block")
			}
			w.file = &rejectBlockReadFile{file: w.file, offset: int64(w.archives[0].blockOffset(selected.index))}
			ret := w.archives[0].Retention
			err = w.rewrite(Retentions{&ret}, "compact", func(int) []extraPoint { return nil })
			if empty {
				if err != nil {
					t.Fatalf("read unused capacity: %v", err)
				}
				if err := w.CheckIntegrity(); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("ignored occupied-block read failure")
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("failed rewrite modified main file")
			}
			if _, err := os.Stat(path + ".compact"); !os.IsNotExist(err) {
				t.Fatalf("temporary file leaked: %v", err)
			}
		})
	}
}
