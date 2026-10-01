package whisper

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestBlockStatsBeforeCurrentOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for trial := 0; trial < 1000; trial++ {
		size := rng.Intn(100)
		arc := &archiveInfo{blockRanges: make([]blockRange, size)}
		arc.cblock.index = rng.Intn(size+2) - 1
		for i := range arc.blockRanges {
			arc.blockRanges[i] = blockRange{index: i, start: rng.Intn(10), count: rng.Intn(1000)}
			if i%13 == 0 {
				arc.blockRanges[i].start = maxInt
			}
		}
		rng.Shuffle(size, func(i, j int) { arc.blockRanges[i], arc.blockRanges[j] = arc.blockRanges[j], arc.blockRanges[i] })
		before := append([]blockRange{}, arc.blockRanges...)
		wantPoints, wantBlocks := 0, 0
		for _, block := range arc.getSortedBlockRanges() {
			if block.index == arc.cblock.index {
				break
			}
			wantPoints += block.count
			wantBlocks++
		}
		points, blocks := arc.blockStatsBeforeCurrent()
		if points != wantPoints || blocks != wantBlocks {
			t.Fatalf("trial %d: got (%d,%d), want (%d,%d)", trial, points, blocks, wantPoints, wantBlocks)
		}
		if !reflect.DeepEqual(before, arc.blockRanges) {
			t.Fatal("modified block ranges")
		}
		if allocs := testing.AllocsPerRun(10, func() { arc.blockStatsBeforeCurrent() }); allocs != 0 {
			t.Fatalf("stats allocated %g times", allocs)
		}
	}
}

// Keep the original merge as a differential oracle for scratch-buffer reuse.
func mergeExtraReference(block []dataPoint, extra []extraPoint, end int) (merged []dataPoint, rest []extraPoint) {
	if end <= 0 {
		return block, extra
	}
	n := 0
	for n < len(extra) && extra[n].interval <= end {
		n++
	}
	head, rest := extra[:n], extra[n:]
	if len(head) == 0 {
		return block, rest
	}
	merged = make([]dataPoint, 0, len(block)+len(head))
	i, j := 0, 0
	for i < len(block) && j < len(head) {
		switch {
		case block[i].interval < head[j].interval:
			merged = append(merged, block[i])
			i++
		case block[i].interval > head[j].interval:
			merged = append(merged, head[j].dataPoint)
			j++
		case head[j].replace:
			merged = append(merged, head[j].dataPoint)
			i++
			j++
		default:
			merged = append(merged, block[i])
			i++
			j++
		}
	}
	merged = append(merged, block[i:]...)
	merged = append(merged, dataPointsOf(head[j:])...)
	return merged, rest
}

func equalPointBits(a, b []dataPoint) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].interval != b[i].interval || math.Float64bits(a[i].value) != math.Float64bits(b[i].value) {
			return false
		}
	}
	return true
}

func TestMergeExtraScratchOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	var scratch []dataPoint
	for trial := 0; trial < 1000; trial++ {
		var block []dataPoint
		var extra []extraPoint
		for stamp := 1; stamp <= 100; stamp++ {
			if rng.Intn(3) == 0 {
				block = append(block, dataPoint{stamp, math.Float64frombits(rng.Uint64())})
			}
			if rng.Intn(3) == 0 {
				extra = append(extra, extraPoint{dataPoint: dataPoint{stamp, float64(stamp)}, replace: rng.Intn(2) == 0})
			}
		}
		before := append([]dataPoint(nil), block...)
		end := rng.Intn(110) - 5
		want, wantRest := mergeExtraReference(block, extra, end)
		got, rest := mergeExtraWithBuffer(block, extra, end, &scratch)
		if !equalPointBits(got, want) || !reflect.DeepEqual(rest, wantRest) {
			t.Fatalf("trial %d: merge differs from reference", trial)
		}
		if !equalPointBits(block, before) {
			t.Fatal("merge mutated decoder buffer")
		}
	}
	block := []dataPoint{{1, 1}, {3, 3}}
	extra := []extraPoint{{dataPoint: dataPoint{2, 2}}}
	if allocs := testing.AllocsPerRun(100, func() { mergeExtraWithBuffer(block, extra, 3, &scratch) }); allocs != 0 {
		t.Fatalf("warm merge allocated %g times", allocs)
	}
}

func TestCompressedHeaderScratchOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "header.wsp")
	w, err := CreateWithOptions(path, MustParseRetentionDefs("1s:2d,1m:35d,1h:2y"), Average, 0.5, &Options{Compressed: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: int(Now().Unix()) - 60, Value: 42}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ranges := append([]blockRange(nil), w.archives[0].blockRanges...)
	buffer := append([]byte(nil), w.archives[0].buffer...)
	image, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	broken := path + ".broken"
	if err := os.WriteFile(broken, image[:64], 0600); err != nil {
		t.Fatal(err)
	}
	t.Run("parallel opens and failed reads", func(t *testing.T) {
		for i := 0; i < 8; i++ {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				t.Parallel()
				for j := 0; j < 20; j++ {
					if bad, err := Open(broken); err == nil {
						bad.Close()
						t.Fatal("accepted truncated header")
					}
					handle, err := Open(path)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(handle.archives[0].blockRanges, ranges) || !bytes.Equal(handle.archives[0].buffer, buffer) {
						t.Error("header changed across opens")
					}
					if err := handle.Close(); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	})
	if !reflect.DeepEqual(w.archives[0].blockRanges, ranges) || !bytes.Equal(w.archives[0].buffer, buffer) {
		t.Fatal("handle retained pooled scratch")
	}
}

func BenchmarkCompressedExtensionCheck(b *testing.B) {
	for _, size := range []int{32, 1024} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			arc := &archiveInfo{Retention: Retention{secondsPerPoint: 1, numberOfPoints: size * 1024, avgCompressedPointSize: 14, blockCount: size}, blockSize: 4096}
			arc.blockRanges = make([]blockRange, size)
			arc.cblock.index = size - 1
			for i := range arc.blockRanges {
				arc.blockRanges[i] = blockRange{index: i, start: 1000 + i*1024, count: 1023}
			}
			w := &Whisper{archives: []*archiveInfo{arc}}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, extend, _ := w.computeExtendedRetentions()
				if extend {
					b.Fatal("unexpected extension")
				}
			}
		})
	}
}

func BenchmarkCompressedHeaderOpen(b *testing.B) {
	path := filepath.Join(b.TempDir(), "header.wsp")
	w, err := CreateWithOptions(path, MustParseRetentionDefs("1s:2d,1m:35d,1h:2y"), Average, 0.5, &Options{Compressed: true})
	if err != nil {
		b.Fatal(err)
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w, err := Open(path)
		if err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
