package whisper

import (
	"errors"
	"reflect"
	"testing"
)

type matchingPlanReader struct {
	*bufferedPlanReader
	matched bool
	err     error
	calls   int
	block   ContinuationBlock
	points  []TimeSeriesPoint
}

func (r *matchingPlanReader) MatchesStoragePoints(block ContinuationBlock, points []TimeSeriesPoint) (bool, error) {
	r.calls++
	r.block = block
	r.points = append([]TimeSeriesPoint(nil), points...)
	return r.matched, r.err
}

func TestPlanInPlaceMatchingBlockSkipsRead(t *testing.T) {
	archive, block, points := planScratchFixture(4)
	w := &Whisper{archives: []*archiveInfo{archive}}
	reader := &matchingPlanReader{bufferedPlanReader: &bufferedPlanReader{}, matched: true}
	// Reading would fail: a successful proof must avoid materializing the block.
	reader.bufferedPlanReader.err = errors.New("unexpected block read")
	pending := [][]extraPoint{markExtras([]dataPoint{{interval: points[1].Time, value: points[1].Value}}, true)}
	blocks, weighted := InPlaceBlocks(true), InPlaceBlockPoints(true)
	plan, result, err := w.planInPlace(reader, pending)
	if err != nil || result != InPlaced || plan == nil {
		t.Fatalf("plan: %v, %v", result, err)
	}
	if reader.calls != 1 || reader.reads != 0 || reader.block != archive.continuationBlock(0, block) ||
		!reflect.DeepEqual(reader.points, points[1:2]) {
		t.Fatalf("matcher boundary: %+v", reader)
	}
	if len(plan.blocks) != 0 || len(plan.appends[0]) != 0 {
		t.Fatalf("matching block produced changes: %+v", plan)
	}
	if InPlaceBlocks(true)-blocks != 1 || InPlaceBlockPoints(true)-weighted != uint64(len(points)) {
		t.Fatal("skipped plan counters must count one whole physical block")
	}
}

func TestPlanInPlaceMatcherFallback(t *testing.T) {
	for _, scenario := range []string{"changed", "missing", "unchanged"} {
		t.Run(scenario, func(t *testing.T) {
			archive, _, points := planScratchFixture(4)
			stored := append([]TimeSeriesPoint(nil), points...)
			incoming := points[1]
			switch scenario {
			case "changed":
				incoming.Value = -1
			case "missing":
				stored = append(stored[:1], stored[2:]...)
			}
			w := &Whisper{archives: []*archiveInfo{archive}}
			pending := [][]extraPoint{markExtras([]dataPoint{{interval: incoming.Time, value: incoming.Value}}, true)}
			// A reader without the optional interface is the compatibility oracle.
			plain := &bufferedPlanReader{ownedPlanReader: ownedPlanReader{stored}}
			want, result, err := w.planInPlace(plain, pending)
			if err != nil || result != InPlaced || plain.reads != 1 {
				t.Fatalf("ordinary plan: %v, %v, reads=%d", result, err, plain.reads)
			}
			reader := &matchingPlanReader{bufferedPlanReader: &bufferedPlanReader{ownedPlanReader: ownedPlanReader{stored}}}
			got, result, err := w.planInPlace(reader, pending)
			if err != nil || result != InPlaced || reader.calls != 1 || reader.reads != 1 || !reflect.DeepEqual(got, want) {
				t.Fatalf("fallback differs: result=%v, err=%v, matcher=%d, reads=%d", result, err, reader.calls, reader.reads)
			}
			if scenario == "unchanged" {
				if len(got.blocks) != 0 {
					t.Fatal("ordinary planning changed an unchanged block")
				}
				return
			}
			expected := append([]TimeSeriesPoint(nil), points...)
			expected[1] = incoming
			if len(got.blocks) != 1 || !reflect.DeepEqual(got.blocks[0].points, expected) || len(got.appends[0]) != 0 {
				t.Fatalf("correction or missing point was lost: %+v", got)
			}
		})
	}
}

func TestPlanInPlaceMatcherError(t *testing.T) {
	archive, _, points := planScratchFixture(4)
	w := &Whisper{archives: []*archiveInfo{archive}}
	sentinel := errors.New("match failed")
	reader := &matchingPlanReader{bufferedPlanReader: &bufferedPlanReader{}, err: sentinel}
	plan, _, err := w.planInPlace(reader, [][]extraPoint{markExtras([]dataPoint{{interval: points[1].Time}}, true)})
	if !errors.Is(err, sentinel) || plan != nil || reader.reads != 0 {
		t.Fatalf("matcher error was not propagated: plan=%+v, err=%v, reads=%d", plan, err, reader.reads)
	}
}

func TestPlanInPlaceMatcherCannotSkipEarlierPoints(t *testing.T) {
	archive, block, points := planScratchFixture(4)
	w := &Whisper{archives: []*archiveInfo{archive}}
	reader := &matchingPlanReader{bufferedPlanReader: &bufferedPlanReader{ownedPlanReader: ownedPlanReader{points}}, matched: true}
	before := dataPoint{interval: block.start - 60, value: -1}
	plan, result, err := w.planInPlace(reader, [][]extraPoint{markExtras([]dataPoint{before}, true)})
	if err != nil || result != InPlaced || reader.calls != 0 || reader.reads != 1 {
		t.Fatalf("earlier-point plan: result=%v, err=%v, matcher=%d, reads=%d", result, err, reader.calls, reader.reads)
	}
	expected := append([]TimeSeriesPoint{{Time: before.interval, Value: before.value}}, points...)
	if len(plan.blocks) != 1 || !reflect.DeepEqual(plan.blocks[0].points, expected) {
		t.Fatalf("point preceding the block was lost: %+v", plan)
	}
}
