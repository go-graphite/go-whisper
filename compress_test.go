package whisper

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/kr/pretty"
)

func TestBitsReadWrite(t *testing.T) {
	buf := make([]byte, 256)

	var bw bitsWriter
	bw.buf = buf
	bw.bitPos = 7

	var br bitsReader
	br.buf = buf
	br.bitPos = 7

	input := []struct {
		val uint64
		len int
	}{
		{len: 8, val: 5},
		{len: 16, val: 97},
		{len: 32, val: 123},
		{len: 64, val: math.Float64bits(95.1)},
		{len: 64, val: 0xf1f2f3f4f5f6f7f8},

		{len: 1, val: 1},
		{len: 15, val: 0x7f7f},
		{len: 55, val: 0x7f2f3f4f5f6f7f},
	}
	for _, d := range input {
		bw.Write(d.len, d.val)
	}
	for i, d := range input {
		if got, want := br.Read(d.len), d.val; got != want {
			t.Errorf("%d: br.Read(%d) = %x; want %b", i, d.len, got, want)
		}
	}
}

func init() {
	log.SetFlags(log.Lshortfile)
	rand.Seed(time.Now().UnixNano())
}

func TestBlockReadWrite1(t *testing.T) {
	for i := 0; i < 1; i++ {
		var acv archiveInfo
		acv.secondsPerPoint = 1
		acv.numberOfPoints = 64
		acv.cblock.lastByteBitPos = 7
		acv.blockSize = 64 * PointSize
		acv.blockRanges = make([]blockRange, 1)

		ts := 1543689630
		var delta int
		next := func(incs ...int) int {
			for _, i := range incs {
				delta += i
			}
			return ts + delta
		}

		var input []dataPoint
		{
			rand.Seed(time.Now().Unix())
			input = append(input, dataPoint{interval: next(1), value: 1})
			input = append(input, dataPoint{interval: next(1), value: 1})
			input = append(input, dataPoint{interval: next(1), value: 1})
			for i := 0; i < 200; i++ {
				input = append(input, dataPoint{interval: next(rand.Intn(10)), value: rand.NormFloat64()})
			}
		}

		buf := make([]byte, acv.blockSize)
		_, left, _ := acv.AppendPointsToBlock(buf, input)

		points := make([]dataPoint, 0, 200)
		points, _, err := acv.ReadFromBlock(buf, points, ts, ts+60*60*60)
		if err != nil {
			t.Error(err)
		}

		if !reflect.DeepEqual(input, append(points, left...)) {
			if diff := cmp.Diff(input, points, cmp.AllowUnexported(dataPoint{})); diff != "" {
				t.Error(diff)
			}

			t.FailNow()
		}
	}
}

func TestBlockReadWrite2(t *testing.T) {
	for i := 0; i < 1; i++ {
		var acv archiveInfo
		acv.secondsPerPoint = 1
		acv.numberOfPoints = 100
		acv.cblock.lastByteBitPos = 7
		acv.blockSize = int(float32(acv.numberOfPoints) * avgCompressedPointSize)
		acv.blockRanges = make([]blockRange, 1)

		var input []dataPoint = []dataPoint{
			{interval: 1544456874, value: 12},
			{interval: 1544456875, value: 24},
			{interval: 1544456876, value: 15},
			{interval: 1544456877, value: 1},
			{interval: 1544456878, value: 2},
			{interval: 1544456888, value: 3},
			{interval: 1544456889, value: 4},
			{interval: 1544457000, value: 4},
			{interval: 1544458000, value: 4},
			{interval: 1544476889, value: 4},
		}

		buf := make([]byte, acv.blockSize)
		acv.AppendPointsToBlock(buf, input[:1])
		acv.AppendPointsToBlock(buf[acv.cblock.lastByteOffset:], input[1:5])
		acv.AppendPointsToBlock(buf[acv.cblock.lastByteOffset:], input[5:])

		points, _, err := acv.ReadFromBlock(buf, make([]dataPoint, 0, 200), 1544456874, 1544477000)
		if err != nil {
			t.Error(err)
		}

		if !reflect.DeepEqual(input, points) {
			pretty.Printf("%# v\n", input)
			pretty.Printf("%# v\n", points)
			if diff := cmp.Diff(input, points, cmp.AllowUnexported(dataPoint{})); diff != "" {
				t.Error(diff)
			}

			t.FailNow()
		}
	}
}

func TestCompressedWhisperReadWrite1(t *testing.T) {
	fpath := "comp.whisper"
	os.Remove(fpath)
	whisper, err := CreateWithOptions(
		fpath,
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 100},
			{secondsPerPoint: 5, numberOfPoints: 100},
		},
		Sum,
		0.7,
		&Options{Compressed: true, PointsPerBlock: 7200},
	)
	if err != nil {
		panic(err)
	}

	ts := int(Now().Add(time.Second * -60).Unix())
	var delta int
	next := func(incs int) int {
		delta += incs
		return ts + delta
	}
	input := []*TimeSeriesPoint{
		{Time: next(1), Value: 12},
		{Time: next(1), Value: 24},
		{Time: next(1), Value: 15},
		{Time: next(1), Value: 1},
		{Time: next(1), Value: 2},
		{Time: next(10), Value: 3},
		{Time: next(1), Value: 4},
		{Time: next(1), Value: 15.5},
		{Time: next(1), Value: 14.0625},
		{Time: next(1), Value: 3.25},
		{Time: next(1), Value: 8.625},
		{Time: next(1), Value: 13.1},
	}

	if err := whisper.UpdateMany(input); err != nil {
		t.Error(err)
	}

	// this negative data points should be ignored
	if err := whisper.UpdateMany([]*TimeSeriesPoint{{Time: next(0) - 10, Value: 12}}); err != nil {
		t.Error(err)
	}
	if got, want := whisper.archives[0].stats.discard.oldInterval, uint32(1); got != want {
		t.Errorf("whisper.archives[0].stats.discard.oldInterval = %d; want %d", got, want)
	}

	whisper.Close()

	whisper, err = OpenWithOptions(fpath, &Options{Compressed: true, PointsPerBlock: 7200})
	if err != nil {
		t.Fatal(err)
	}

	expectVals := make([]float64, 60)
	for i := 0; i < 60; i++ {
		expectVals[i] = math.NaN()
	}
	for _, p := range input {
		expectVals[p.Time-ts-1] = p.Value
	}
	expect := &TimeSeries{
		fromTime:  ts + 1,
		untilTime: ts + 61,
		step:      1,
		values:    expectVals,
	}
	if ts, err := whisper.Fetch(ts, ts+300); err != nil {
		t.Error(err)
	} else if diff := cmp.Diff(ts, expect, cmp.AllowUnexported(TimeSeries{}), cmpopts.EquateNaNs()); diff != "" {
		t.Error(diff)
	}

	t.Run("buffer_overflow", func(t *testing.T) {
		// fmt.Println("---")
		// whisper.archives[0].dumpDataPointsCompressed()
		if err := whisper.UpdateMany([]*TimeSeriesPoint{
			{Time: next(5), Value: 10},
			{Time: next(5), Value: 11},
			{Time: next(5), Value: 12},
		}); err != nil {
			t.Error(err)
		}
		// fmt.Println("---")
		// whisper.archives[0].dumpDataPointsCompressed()

		if err := whisper.UpdateMany([]*TimeSeriesPoint{
			{Time: next(0) - 15, Value: 13},
			{Time: next(0) - 10, Value: 13},
		}); err != nil {
			t.Error(err)
		}
		// fmt.Println("---")
		// whisper.archives[0].dumpDataPointsCompressed()

		// debugCompress = true
		if err := whisper.UpdateMany([]*TimeSeriesPoint{
			{Time: next(0) - 5, Value: 14},
			{Time: next(0) - 0, Value: 15},
		}); err != nil {
			t.Error(err)
		}
		// debugCompress = false

		// fmt.Println("---")
		// whisper.archives[0].dumpDataPointsCompressed()

		expect := []TimeSeriesPoint{
			{Time: next(0) - 14, Value: math.NaN()},
			{Time: next(0) - 13, Value: math.NaN()},
			{Time: next(0) - 12, Value: math.NaN()},
			{Time: next(0) - 11, Value: math.NaN()},
			{Time: next(0) - 10, Value: 10},
			{Time: next(0) - 9, Value: math.NaN()},
			{Time: next(0) - 8, Value: math.NaN()},
			{Time: next(0) - 7, Value: math.NaN()},
			{Time: next(0) - 6, Value: math.NaN()},
			{Time: next(0) - 5, Value: 14},
			{Time: next(0) - 4, Value: math.NaN()},
			{Time: next(0) - 3, Value: math.NaN()},
			{Time: next(0) - 2, Value: math.NaN()},
			{Time: next(0) - 1, Value: math.NaN()},
			{Time: next(0) - 0, Value: 15},
		}
		if ts, err := whisper.Fetch(next(0)-15, next(0)); err != nil {
			t.Error(err)
		} else if diff := cmp.Diff(ts.Points(), expect, cmp.AllowUnexported(TimeSeries{}), cmpopts.EquateNaNs()); diff != "" {
			t.Error(diff)
		}
	})
	whisper.Close()
}

func TestCompressedWhisperReadWrite2(t *testing.T) {
	fpath := "comp.whisper"
	os.Remove(fpath)
	whisper, err := CreateWithOptions(
		fpath,
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 100},
			{secondsPerPoint: 5, numberOfPoints: 100},
		},
		Sum,
		0.7,
		&Options{Compressed: true, PointsPerBlock: 7200},
	)
	if err != nil {
		panic(err)
	}

	nowTs := 1544478230
	Now = func() time.Time { return time.Unix(int64(nowTs), 0) }
	defer func() { Now = func() time.Time { return time.Now() } }()

	input := []*TimeSeriesPoint{
		{Time: nowTs - 300, Value: 666},

		{Time: 1544478201, Value: 12},

		{Time: 1544478211, Value: 24},
		{Time: 1544478212, Value: 15},
		{Time: 1544478213, Value: 1},
		{Time: 1544478214, Value: 2},

		{Time: 1544478224, Value: 3},
		{Time: 1544478225, Value: 4},
		{Time: 1544478226, Value: 15.5},
		{Time: 1544478227, Value: 14.0625},
		{Time: 1544478228, Value: 3.25},
		{Time: 1544478229, Value: 8.625},
		{Time: nowTs, Value: 13.1},
	}

	for _, p := range input {
		if err := whisper.UpdateMany([]*TimeSeriesPoint{p}); err != nil {
			t.Error(err)
		}
	}
	whisper.Close()

	whisper, err = OpenWithOptions(fpath, &Options{Compressed: true, PointsPerBlock: 7200})
	if err != nil {
		t.Fatal(err)
	}

	{
		expectVals := make([]float64, 4)
		for i := 0; i < 4; i++ {
			expectVals[i] = math.NaN()
		}
		expectVals[1] = input[0].Value
		expect := &TimeSeries{
			fromTime:  1544477925,
			untilTime: 1544477945,
			step:      5,
			values:    expectVals,
		}
		if ts, err := whisper.Fetch(nowTs-310, nowTs-290); err != nil {
			t.Error(err)
		} else if diff := cmp.Diff(ts, expect, cmp.AllowUnexported(TimeSeries{}), cmpopts.EquateNaNs()); diff != "" {
			t.Error(diff)
		}
	}

	{
		expectVals := make([]float64, 30)
		for i := 0; i < 30; i++ {
			expectVals[i] = math.NaN()
		}
		for _, p := range input[1:] {
			expectVals[29-(nowTs-p.Time)] = p.Value
		}
		expect := &TimeSeries{
			fromTime:  1544478201,
			untilTime: 1544478231,
			step:      1,
			values:    expectVals,
		}
		if ts, err := whisper.Fetch(nowTs-30, nowTs); err != nil {
			t.Error(err)
		} else if diff := cmp.Diff(ts, expect, cmp.AllowUnexported(TimeSeries{}), cmpopts.EquateNaNs()); diff != "" {
			t.Error(diff)
		}
	}
}

var fullTest3 = flag.Bool("full-test3", false, "run a full test of TestCompressedWhisperReadWrite3")

// To run a full test of TestCompressedWhisperReadWrite3, it would take about 10
// minutes, the slowness comes from standard whisper file propagation (around 10
// times slower and comsume much more memory than compressed format).
//
// Parallel is disabled because we need to manipulate Now in order to simulate
// updates.
func TestCompressedWhisperReadWrite3(t *testing.T) {
	// TODO: add a test case of mixing random and sequential values/times
	inputs := []struct {
		name      string
		randLimit func() int
		gen       func(prevTime time.Time, index int) *TimeSeriesPoint
	}{
		{
			name: "random_time",
			gen: func(prevTime time.Time, index int) *TimeSeriesPoint {
				return &TimeSeriesPoint{
					Value: 0,
					Time:  int(prevTime.Add(time.Duration(rand.Intn(4096)+1) * time.Second).Unix()),
				}
			},
		},
		{
			name: "random_time_value",
			gen: func(prevTime time.Time, index int) *TimeSeriesPoint {
				return &TimeSeriesPoint{
					Value: rand.NormFloat64(),
					Time:  int(prevTime.Add(time.Duration(rand.Intn(3600*24)+1) * time.Second).Unix()),
				}
			},
		},
		{
			name: "less_random_time_value",
			gen: func(prevTime time.Time, index int) *TimeSeriesPoint {
				return &TimeSeriesPoint{
					Value: 2000.0 + float64(rand.Intn(1000)),
					Time:  int(prevTime.Add(time.Duration(rand.Intn(60)) * time.Second).Unix()),
				}
			},
		},

		{
			name: "random_value",
			gen: func(prevTime time.Time, index int) *TimeSeriesPoint {
				return &TimeSeriesPoint{
					Value: rand.NormFloat64(),
					Time:  int(prevTime.Add(time.Second).Unix()),
				}
			},
		},
		{
			name:      "random_value2",
			randLimit: func() int { return rand.Intn(300) + (60 * 60 * 24) },
			gen: func(prevTime time.Time, index int) *TimeSeriesPoint {
				return &TimeSeriesPoint{
					Value: 2000.0 + float64(rand.Intn(1000)),
					Time:  int(prevTime.Add(time.Second).Unix()),
				}
			},
		},
		{
			name: "simple",
			gen: func(prevTime time.Time, index int) *TimeSeriesPoint {
				return &TimeSeriesPoint{Value: 0, Time: int(prevTime.Add(time.Second).Unix())}
			},
		},
	}

	os.MkdirAll("tmp", 0755)
	inMemory := true
	for i := range inputs {
		input := inputs[i]
		if input.randLimit == nil {
			input.randLimit = func() int { return rand.Intn(300) }
		}

		t.Run(input.name, func(t *testing.T) {
			// can't run tests parallel here because they modify Now
			// t.Parallel()
			t.Logf("case: %s\n", input.name)

			fpath := fmt.Sprintf("tmp/test3_%s.wsp", input.name)
			os.Remove(fpath)
			os.Remove(fpath + ".cwsp")

			cwhisper, err := CreateWithOptions(
				fpath+".cwsp",
				[]*Retention{
					{secondsPerPoint: 1, numberOfPoints: 172800},   // 1s:2d
					{secondsPerPoint: 60, numberOfPoints: 40320},   // 1m:28d
					{secondsPerPoint: 3600, numberOfPoints: 17520}, // 1h:2y
				},
				Average,
				0,
				&Options{Compressed: true, PointsPerBlock: 7200, InMemory: inMemory},
			)
			if err != nil {
				panic(err)
			}
			ncwhisper, err := CreateWithOptions(
				fpath,
				[]*Retention{
					{secondsPerPoint: 1, numberOfPoints: 172800},   // 1s:2d
					{secondsPerPoint: 60, numberOfPoints: 40320},   // 1m:28d
					{secondsPerPoint: 3600, numberOfPoints: 17520}, // 1h:2y
				},
				Average,
				0,
				&Options{Compressed: false, PointsPerBlock: 7200, InMemory: inMemory},
			)
			if err != nil {
				panic(err)
			}
			cwhisper.Close()
			ncwhisper.Close()

			var now = time.Now()
			var total = 60*60*24*365*2 + 37
			var start = now.Add(time.Second * time.Duration(total) * -1)
			Now = func() time.Time { return start }
			defer func() { Now = func() time.Time { return time.Now() } }()

			// var psArr [][]*TimeSeriesPoint
			var ps []*TimeSeriesPoint
			var limit = input.randLimit()
			var statTotalUpdates, extended, totalPoints int
			firstArchiveBound := cwhisper.Retentions()[0].MaxRetention()
			for i := 0; i < total; i++ {
				p := input.gen(start, i)
				var toAppend = true
				if len(ps) == 0 || p.Time-ps[0].Time < firstArchiveBound {
					ps = append(ps, p)
					start = time.Unix(int64(p.Time), 0)
					toAppend = false
				}

				if toAppend || len(ps) >= limit || start.After(now) {
					// fmt.Printf("%d toAppend = %v\r", start.Unix(), len(ps))
					// fmt.Printf("progress: %.2f%% len(points): %d\r", 100-float64(now.Unix()-start.Unix())*100/float64(total), len(ps))
					limit = input.randLimit()
					statTotalUpdates++
					totalPoints += len(ps)
					// psArr = append(psArr, ps)

					cwhisper, err = OpenWithOptions(fpath+".cwsp", &Options{InMemory: inMemory})
					if err != nil {
						t.Fatal(err)
					}
					if err := cwhisper.UpdateMany(ps); err != nil {
						t.Fatal(err)
					}
					if err := cwhisper.Close(); err != nil {
						t.Fatal(err)
					}

					if cwhisper.Extended {
						// for _, a := range cwhisper.archives {
						// 	t.Logf("extended: %s: %d\n", a.Retention, a.totalPoints())
						// }
						extended++
					}

					if *fullTest3 {
						ncwhisper, err = OpenWithOptions(fpath, &Options{InMemory: inMemory})
						if err != nil {
							t.Fatal(err)
						}
						if err := ncwhisper.UpdateMany(ps); err != nil {
							t.Fatal(err)
						}
						if err := ncwhisper.Close(); err != nil {
							t.Fatal(err)
						}
					}

					ps = ps[:0]
				}
				if start.After(now) {
					break
				}
			}

			t.Logf("statTotalUpdates: %d extended: %d totalPoints: %d\n", statTotalUpdates, extended, totalPoints)
			// for _, a := range cwhisper.archives {
			// 	t.Logf("%s: %d\n", a.Retention, a.totalPoints())
			// }

			if inMemory {
				if err := newMemFile(fpath).dumpOnDisk(fpath); err != nil {
					t.Fatal(err)
				}
				if err := newMemFile(fpath + ".cwsp").dumpOnDisk(fpath + ".cwsp"); err != nil {
					t.Fatal(err)
				}
			}

			// {
			// 	data, err := json.Marshal(psArr)
			// 	if err != nil {
			// 		panic(err)
			// 	}
			// 	if err := ioutil.WriteFile("test3.json", data, 0644); err != nil {
			// 		panic(err)
			// 	}
			// }

			if *fullTest3 {
				t.Log("go", "run", "cmd/compare.go", "-v", "-now", fmt.Sprintf("%d", now.Unix()), fpath, fpath+".cwsp")
				output, err := exec.Command("go", "run", "cmd/compare.go", "-now", fmt.Sprintf("%d", now.Unix()), fpath, fpath+".cwsp").CombinedOutput()
				if err != nil {
					t.Log(string(output))
					t.Error(err)
				}
			}

			std, err := os.Stat(fpath)
			if err != nil {
				t.Error(err)
			}
			cmp, err := os.Stat(fpath + ".cwsp")
			if err != nil {
				t.Error(err)
			}
			t.Logf("compression ratio %s: %.2f%%\n", input.name, float64(cmp.Size()*100)/float64(std.Size()))
		})
	}
}

func TestCompressTo(t *testing.T) {
	fpath := "compress_to.wsp"
	os.Remove(fpath)

	whisper, err := CreateWithOptions(
		fpath,
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 172800},   // 1s:2d
			{secondsPerPoint: 60, numberOfPoints: 40320},   // 1m:28d
			{secondsPerPoint: 3600, numberOfPoints: 17520}, // 1h:2y
		},
		Average,
		0,
		&Options{Compressed: false, PointsPerBlock: 7200, InMemory: true},
	)
	if err != nil {
		panic(err)
	}
	whisper.Close()

	for _, archive := range whisper.archives {
		var ps []*TimeSeriesPoint
		for i := 0; i < archive.numberOfPoints; i++ {
			start := Now().Add(time.Second * time.Duration(archive.secondsPerPoint*i) * -1)
			ps = append(ps, &TimeSeriesPoint{
				// Time: int(start.Add(time.Duration(i) * time.Second).Unix()),
				Time: int(start.Unix()),
				// Value: float64(i),
				// Value: 2000.0 + float64(rand.Intn(100000))/100.0,
				// Value: rand.NormFloat64(),
				Value: float64(rand.Intn(100000)),
			})
		}
		whisper, err = OpenWithOptions(fpath, &Options{InMemory: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := whisper.UpdateMany(ps); err != nil {
			t.Fatal(err)
		}
		if err := whisper.Close(); err != nil {
			t.Fatal(err)
		}
	}
	whisper.file.(*memFile).dumpOnDisk(fpath)

	whisper, err = OpenWithOptions(fpath, &Options{})
	if err != nil {
		t.Fatal(err)
	}
	os.Remove(fpath + ".cwsp")
	if err := whisper.CompressTo(fpath + ".cwsp"); err != nil {
		t.Fatal(err)
	}

	output, err := exec.Command("go", "run", "cmd/compare.go", fpath, fpath+".cwsp").CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %s", err, output)
	}
}

func TestRandomReadWrite(t *testing.T) {
	// os.Remove("test_random_read_write.wsp")
	fileTs := time.Now().Unix()
	cwhisper, err := CreateWithOptions(
		fmt.Sprintf("test_random_read_write.%d.wsp", fileTs),
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 1728000},
			// {secondsPerPoint: 60, numberOfPoints: 40320},
			// {secondsPerPoint: 3600, numberOfPoints: 17520},
		},
		Sum,
		0,
		&Options{Compressed: true, PointsPerBlock: 7200},
	)
	if err != nil {
		panic(err)
	}

	start := Now()
	ptime := start
	var ps []*TimeSeriesPoint
	var vals []float64
	var entropy int
	for i := 0; i < cwhisper.Retentions()[0].numberOfPoints; i++ {
		gap := rand.Intn(10) + 1
		ptime = ptime.Add(time.Second * time.Duration(gap))
		if ptime.After(start.Add(time.Duration(cwhisper.Retentions()[0].numberOfPoints) * time.Second)) {
			break
		}
		for j := gap; j > 1; j-- {
			vals = append(vals, math.NaN())
		}
		ts := &TimeSeriesPoint{
			Time:  int(ptime.Unix()),
			Value: rand.NormFloat64(),
			// Value: 2000.0 + float64(rand.Intn(100000))/100.0,
			// Value: float64(rand.Intn(100000)),
		}
		ps = append(ps, ts)
		vals = append(vals, ts.Value)
		entropy++
	}

	if err := cwhisper.UpdateMany(ps); err != nil {
		t.Fatal(err)
	}

	Now = func() time.Time { return time.Unix(int64(ps[len(ps)-1].Time), 0) }
	defer func() { Now = func() time.Time { return time.Now() } }()

	ts, err := cwhisper.Fetch(int(start.Unix()), int(ptime.Unix()))
	if err != nil {
		t.Fatal(err)
	}

	// log.Printf("entropy = %+v\n", entropy)
	// log.Printf("len(vals) = %+v\n", len(vals))
	// log.Printf("len(ts.Values()) = %+v\n", len(ts.Values()))

	if diff := cmp.Diff(ts.Values(), vals, cmp.AllowUnexported(dataPoint{}), cmpopts.EquateNaNs()); diff != "" {
		// t.Error(diff)
		t.Error("mismatch")
		cache, err := os.Create(fmt.Sprintf("test_random_read_write.%d.json", fileTs))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewEncoder(cache).Encode(ps); err != nil {
			t.Fatal(err)
		}
		cache.Close()
	}

	if err := cwhisper.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFillCompressed(t *testing.T) {
	fpath := "fill.wsp"
	os.Remove(fpath)
	os.Remove(fpath + ".cwsp")

	standard, err := CreateWithOptions(
		fpath,
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 172800},   // 1s:2d
			{secondsPerPoint: 60, numberOfPoints: 40320},   // 1m:28d
			{secondsPerPoint: 3600, numberOfPoints: 17520}, // 1h:2y
		},
		Average,
		0,
		&Options{Compressed: false, PointsPerBlock: 7200, InMemory: true},
	)
	if err != nil {
		panic(err)
	}

	points := []*TimeSeriesPoint{}
	twoYearsAgo := Now().Add(time.Hour * 24 * 365 * -2)
	for i := 0; i < 2*365*24-28*24; i++ {
		points = append(points, &TimeSeriesPoint{
			Time:  int(twoYearsAgo.Add(time.Hour * time.Duration(i)).Unix()),
			Value: rand.NormFloat64(),
		})
	}
	if err := standard.UpdateMany(points); err != nil {
		t.Error(err)
	}

	points = []*TimeSeriesPoint{}
	oneMonthAgo := Now().Add(time.Hour * 24 * -28)
	for i := 0; i < 28*24*60-2*24*60; i++ {
		points = append(points, &TimeSeriesPoint{
			Time:  int(oneMonthAgo.Add(time.Minute * time.Duration(i)).Unix()),
			Value: rand.NormFloat64(),
		})
	}
	if err := standard.UpdateMany(points); err != nil {
		t.Error(err)
	}

	compressed, err := CreateWithOptions(
		fpath+".cwsp",
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 172800, avgCompressedPointSize: 9},   // 1s:2d
			{secondsPerPoint: 60, numberOfPoints: 40320, avgCompressedPointSize: 9},   // 1m:28d
			{secondsPerPoint: 3600, numberOfPoints: 17520, avgCompressedPointSize: 9}, // 1h:2y
		},
		Average,
		0,
		&Options{Compressed: true, PointsPerBlock: 7200, InMemory: true},
	)
	if err != nil {
		panic(err)
	}
	points = []*TimeSeriesPoint{}
	twoDaysAgo := Now().Add(time.Hour * 24 * -2)
	for i := 0; i < 60*60*24*2; i++ {
		points = append(points, &TimeSeriesPoint{
			Time:  int(twoDaysAgo.Add(time.Second * time.Duration(i)).Unix()),
			Value: rand.NormFloat64(),
		})
	}
	if err := compressed.UpdateMany(points); err != nil {
		t.Error(err)
	}

	if err := compressed.file.(*memFile).dumpOnDisk(fpath + ".original.cwsp"); err != nil {
		t.Error(err)
	}

	if err := compressed.FillCompressed(standard); err != nil {
		t.Error(err)
	}

	if err := compressed.file.(*memFile).dumpOnDisk(fpath + ".cwsp"); err != nil {
		t.Error(err)
	}
	if err := standard.file.(*memFile).dumpOnDisk(fpath); err != nil {
		t.Error(err)
	}

	compare := func(w1, w2 *Whisper, from, until int) {
		valsc, err := w1.Fetch(from, until)
		if err != nil {
			t.Error(err)
		}
		valss, err := w2.Fetch(from, until)
		if err != nil {
			t.Error(err)
		}
		var diff, same int
		for i := 0; i < len(valsc.values); i++ {
			vc := valsc.values[i]
			vs := valss.values[i]
			if math.IsNaN(vc) && math.IsNaN(vs) {
				same++
			} else if vc != vs {
				t.Errorf("%d/%d %d: %v != %v\n", i, len(valsc.values), valsc.fromTime+i*valsc.step, vc, vs)
				diff++
			} else {
				same++
			}
		}
		if diff > 0 {
			t.Errorf("diff = %d", diff)
			t.Errorf("same = %d", same)
		}
	}

	t.Log("comparing 2 years archive")
	compare(compressed, standard, int(twoYearsAgo.Unix()), int(Now().Add(time.Hour*24*-28).Unix()))
	t.Log("comparing 1 month archive")
	compare(compressed, standard, int(oneMonthAgo.Add(time.Hour).Unix()), int(Now().Add(time.Hour*24*-2-time.Hour).Unix()))

	oldCompressed, err := OpenWithOptions(fpath+".original.cwsp", &Options{})
	if err != nil {
		t.Error(err)
	}
	t.Log("comparing 2 days archive")
	compare(compressed, oldCompressed, int(Now().Add(time.Hour*24*-2+time.Hour).Unix()), int(Now().Unix()))
}

func TestSanitizeAvgCompressedPointSizeOnCreate(t *testing.T) {
	var cases = []struct {
		rets   []*Retention
		expect float32
	}{
		{
			rets: []*Retention{
				{secondsPerPoint: 1, numberOfPoints: 100, avgCompressedPointSize: math.Float32frombits(0xffc00000)}, // 32bits nan
				{secondsPerPoint: 5, numberOfPoints: 100},
			},
			expect: avgCompressedPointSize,
		},
		{
			rets: []*Retention{
				{secondsPerPoint: 1, numberOfPoints: 100, avgCompressedPointSize: float32(math.NaN())}, // 62 bits nan
				{secondsPerPoint: 5, numberOfPoints: 100},
			},
			expect: avgCompressedPointSize,
		},
		{
			rets: []*Retention{
				{secondsPerPoint: 1, numberOfPoints: 100, avgCompressedPointSize: 0},
				{secondsPerPoint: 5, numberOfPoints: 100},
			},
			expect: avgCompressedPointSize,
		},
		{
			rets: []*Retention{
				{secondsPerPoint: 1, numberOfPoints: 100, avgCompressedPointSize: -10},
				{secondsPerPoint: 5, numberOfPoints: 100},
			},
			expect: avgCompressedPointSize,
		},
		{
			rets: []*Retention{
				{secondsPerPoint: 1, numberOfPoints: 100, avgCompressedPointSize: 65536},
				{secondsPerPoint: 5, numberOfPoints: 100},
			},
			expect: MaxCompressedPointSize,
		},
	}
	for _, c := range cases {
		fpath := "extend.whisper"
		os.Remove(fpath)
		whisper, err := CreateWithOptions(
			fpath,
			c.rets,
			Sum,
			0.7,
			&Options{Compressed: true, PointsPerBlock: 7200},
		)
		if err != nil {
			panic(err)
		}
		whisper.Close()

		whisper, err = OpenWithOptions(fpath, &Options{Compressed: true, PointsPerBlock: 7200})
		if err != nil {
			t.Fatal(err)
		}
		if got, want := whisper.archives[0].avgCompressedPointSize, c.expect; got != want {
			t.Errorf("whisper.archives[0].avgCompressedPointSize = %f; want %f", got, want)
		}
	}
}

func TestEstimatePointSize(t *testing.T) {
	cases := []struct {
		input  []dataPoint
		expect float32
	}{
		// 0 datapoints
		{input: []dataPoint{}, expect: avgCompressedPointSize},
		// not enough datapoints
		{
			input: []dataPoint{
				{interval: 1543449600, value: 5},
				{interval: 1543478400, value: 5},
			},
			expect: avgCompressedPointSize,
		},
	}
	for _, c := range cases {
		// not enough datapoints
		size := estimatePointSize(c.input, &Retention{secondsPerPoint: 10, numberOfPoints: 17280}, DefaultPointsPerBlock)
		if got, want := size, c.expect; got != want {
			t.Errorf("size = %f; want %f", got, want)
		}
	}

	// for i := 0; i < 500; i += 10 {
	// 	var ds []dataPoint
	// 	var start = 1543449600
	// 	for j := 0; j < i; j++ {
	// 		ds = append(ds, dataPoint{interval: start, value: rand.NormFloat64()})
	// 		// ds = append(ds, dataPoint{interval: start, value: 10})
	//
	// 		start += 1
	// 		// start += rand.Int()
	// 	}
	// 	size := estimatePointSize(ds, &Retention{secondsPerPoint: 10, numberOfPoints: 17280}, DefaultPointsPerBlock)
	// 	fmt.Printf("%d: %f\n", i, size)
	// }

	return
}

func TestFillCompressedMix(t *testing.T) {
	srcPath := "fill-mix.src.cwsp"
	dstPath := "fill-mix.dst.cwsp"
	os.Remove(srcPath)
	os.Remove(dstPath)

	srcMix, err := CreateWithOptions(
		srcPath,
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 172800},   // 1s:2d
			{secondsPerPoint: 60, numberOfPoints: 40320},   // 1m:28d
			{secondsPerPoint: 3600, numberOfPoints: 17520}, // 1h:2y
		},
		Mix,
		0,
		&Options{
			Compressed: true, PointsPerBlock: 7200, InMemory: true,
			MixAggregationSpecs: []MixAggregationSpec{
				{Method: Average, Percentile: 0},
				{Method: Sum, Percentile: 0},
				{Method: Last, Percentile: 0},
				{Method: Max, Percentile: 0},
				{Method: Min, Percentile: 0},
				{Method: Percentile, Percentile: 50},
				{Method: Percentile, Percentile: 95},
				{Method: Percentile, Percentile: 99},
			},
		},
	)
	if err != nil {
		panic(err)
	}

	var points []*TimeSeriesPoint
	var limit int
	var start = 1544478600
	var now = start
	Now = func() time.Time { return time.Unix(int64(now), 0) }
	nowNext := func() time.Time { now++; return Now() }
	defer func() { Now = func() time.Time { return time.Now() } }()

	limit = 300 + rand.Intn(100)
	for i, end := 0, 60*60*24*80; i < end; i++ {
		points = append(points, &TimeSeriesPoint{
			Time:  int(nowNext().Unix()),
			Value: rand.NormFloat64(),
		})

		if len(points) > limit || i == end-1 {
			limit = 300 + rand.Intn(100)
			if err := srcMix.UpdateMany(points); err != nil {
				t.Error(err)
			}
			points = points[:0]
		}
	}

	dstMix, err := CreateWithOptions(
		dstPath,
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 172800},   // 1s:2d
			{secondsPerPoint: 60, numberOfPoints: 40320},   // 1m:28d
			{secondsPerPoint: 3600, numberOfPoints: 17520}, // 1h:2y
		},
		Mix,
		0,
		&Options{
			Compressed: true, PointsPerBlock: 7200, InMemory: true,
			MixAggregationSpecs: []MixAggregationSpec{
				{Method: Average, Percentile: 0},
				{Method: Sum, Percentile: 0},
				{Method: Last, Percentile: 0},
				{Method: Max, Percentile: 0},
				{Method: Min, Percentile: 0},
				{Method: Percentile, Percentile: 50},
				{Method: Percentile, Percentile: 95},
				{Method: Percentile, Percentile: 99},
			},
		},
	)
	if err != nil {
		panic(err)
	}
	points = []*TimeSeriesPoint{}
	limit = 300 + rand.Intn(100)
	for i, end := 0, 60*60*24*2; i < end; i++ {
		points = append(points, &TimeSeriesPoint{
			Time:  int(nowNext().Unix()),
			Value: rand.NormFloat64(),
		})

		if len(points) > limit || i == end-1 {
			limit = 300 + rand.Intn(100)
			if err := dstMix.UpdateMany(points); err != nil {
				t.Error(err)
			}
			points = points[:0]
		}
	}
	if err := dstMix.UpdateMany(points); err != nil {
		t.Error(err)
	}

	if err := dstMix.file.(*memFile).dumpOnDisk(dstPath + ".bak"); err != nil {
		t.Error(err)
	}

	if err := dstMix.FillCompressed(srcMix); err != nil {
		t.Error(err)
	}

	if err := dstMix.file.(*memFile).dumpOnDisk(dstPath); err != nil {
		t.Error(err)
	}
	if err := srcMix.file.(*memFile).dumpOnDisk(srcPath); err != nil {
		t.Error(err)
	}

	compare := func(w1, w2 *Whisper, from, until int) {
		valsc, err := w1.Fetch(from, until)
		if err != nil {
			t.Error(err)
		}
		valss, err := w2.Fetch(from, until)
		if err != nil {
			t.Error(err)
		}
		t.Logf("  dst %d src %d", len(valsc.values), len(valss.values))
		var diff, same, nonNans int
		for i := 0; i < len(valsc.values); i++ {
			vc := valsc.values[i]
			vs := valss.values[i]
			if math.IsNaN(vc) && math.IsNaN(vs) {
				same++
			} else if vc != vs {
				t.Errorf("%d/%d %d: %v != %v\n", i, len(valsc.values), valsc.fromTime+i*valsc.step, vc, vs)
				diff++
				nonNans++
			} else {
				same++
				nonNans++
			}
		}
		if diff > 0 {
			t.Errorf("  diff %d", diff)
			t.Errorf("  same %d", same)
		}
		t.Logf("  non-nans %d", nonNans)
	}

	t.Log("comparing 2 years archive")
	compare(dstMix, srcMix, now-365*24*60*60, now-28*24*60*60)
	t.Log("comparing 1 month archive")
	compare(dstMix, srcMix, now-28*24*60*60, now-30*2*60*60)

	oldDstMix, err := OpenWithOptions(dstPath+".bak", &Options{})
	if err != nil {
		t.Error(err)
	}
	t.Log("comparing 2 days archive")
	compare(dstMix, oldDstMix, int(Now().Add(time.Hour*24*-2+time.Hour).Unix()), int(Now().Unix()))
}

func TestFetchCompressedMix(t *testing.T) {
	srcPath := "fetch-mix.cwsp"
	os.Remove(srcPath)

	srcMix, err := CreateWithOptions(
		srcPath,
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 60 * 60}, // 1s:1h
			{secondsPerPoint: 60, numberOfPoints: 3 * 60}, // 1m:3h
			{secondsPerPoint: 600, numberOfPoints: 6 * 6}, // 10m:6h
		},
		Mix,
		0,
		&Options{
			Compressed: true, PointsPerBlock: 7200, InMemory: true,
			MixAggregationSpecs: []MixAggregationSpec{
				{Method: Average, Percentile: 0},
				{Method: Sum, Percentile: 0},
				{Method: Last, Percentile: 0},
				{Method: Max, Percentile: 0},
				{Method: Min, Percentile: 0},
				{Method: Percentile, Percentile: 50},
				{Method: Percentile, Percentile: 95},
				{Method: Percentile, Percentile: 99},
			},
		},
	)
	if err != nil {
		panic(err)
	}

	points := []*TimeSeriesPoint{}
	start := 1544478600
	now := start
	Now = func() time.Time { return time.Unix(int64(now), 0) }
	defer func() { Now = func() time.Time { return time.Now() } }()

	for i, total := 0, 4*60*60; i < total; i++ {
		points = append(points, &TimeSeriesPoint{
			Time:  int(Now().Unix()),
			Value: float64(i),
		})
		now++

		// To trigger frequent aggregations. Because of the current
		// implementation logics if all data points are updated in a single
		// function call, only one aggregation is triggered.
		if len(points) > 1000 || i == total-1 {
			if err := srcMix.UpdateMany(points); err != nil {
				t.Error(err)
			}
			points = points[:0]
		}
	}

	if err := srcMix.file.(*memFile).dumpOnDisk(srcPath); err != nil {
		t.Error(err)
	}

	t.Run("Check1stArchive", func(t *testing.T) {
		data, err := srcMix.FetchByAggregation(now-10, now, &MixAggregationSpec{Method: Min})
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(data.Points(), []TimeSeriesPoint{
			{Time: 1544492991, Value: 14391}, {Time: 1544492992, Value: 14392}, {Time: 1544492993, Value: 14393},
			{Time: 1544492994, Value: 14394}, {Time: 1544492995, Value: 14395}, {Time: 1544492996, Value: 14396},
			{Time: 1544492997, Value: 14397}, {Time: 1544492998, Value: 14398}, {Time: 1544492999, Value: 14399},
			{Time: 1544493000, Value: math.NaN()},
		}, cmp.AllowUnexported(TimeSeriesPoint{}), cmpopts.EquateNaNs()); diff != "" {
			t.Error(diff)
		}
	})
	t.Run("Check2ndArchiveMin", func(t *testing.T) {
		data, err := srcMix.FetchByAggregation(now-2*60*60, now, &MixAggregationSpec{Method: Min})
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(data.Points()[len(data.Points())-42:], []TimeSeriesPoint{
			{Time: 1544490540, Value: 11940}, {Time: 1544490600, Value: 12000}, {Time: 1544490660, Value: 12060},
			{Time: 1544490720, Value: 12120}, {Time: 1544490780, Value: 12180}, {Time: 1544490840, Value: 12240},
			{Time: 1544490900, Value: 12300}, {Time: 1544490960, Value: 12360}, {Time: 1544491020, Value: 12420},
			{Time: 1544491080, Value: 12480}, {Time: 1544491140, Value: 12540}, {Time: 1544491200, Value: 12600},
			{Time: 1544491260, Value: 12660}, {Time: 1544491320, Value: 12720}, {Time: 1544491380, Value: 12780},
			{Time: 1544491440, Value: 12840}, {Time: 1544491500, Value: 12900}, {Time: 1544491560, Value: 12960},
			{Time: 1544491620, Value: 13020}, {Time: 1544491680, Value: 13080}, {Time: 1544491740, Value: 13140},
			{Time: 1544491800, Value: 13200}, {Time: 1544491860, Value: 13260}, {Time: 1544491920, Value: 13320},
			{Time: 1544491980, Value: 13380}, {Time: 1544492040, Value: 13440}, {Time: 1544492100, Value: 13500},
			{Time: 1544492160, Value: 13560}, {Time: 1544492220, Value: 13620}, {Time: 1544492280, Value: 13680},
			{Time: 1544492340, Value: 13740}, {Time: 1544492400, Value: 13800}, {Time: 1544492460, Value: 13860},
			{Time: 1544492520, Value: 13920}, {Time: 1544492580, Value: 13980}, {Time: 1544492640, Value: 14040},
			{Time: 1544492700, Value: 14100}, {Time: 1544492760, Value: 14160}, {Time: 1544492820, Value: 14220},
			{Time: 1544492880, Value: 14280}, {Time: 1544492940, Value: 14340}, {Time: 1544493000, Value: math.NaN()},
		}, cmp.AllowUnexported(TimeSeriesPoint{}), cmpopts.EquateNaNs()); diff != "" {
			t.Error(diff)
		}
	})
	t.Run("Check3rdArchiveMin", func(t *testing.T) {
		data, err := srcMix.FetchByAggregation(start, now, &MixAggregationSpec{Method: Min})
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(data.Points(), []TimeSeriesPoint{
			{Time: 1544479200, Value: 600}, {Time: 1544479800, Value: 1200}, {Time: 1544480400, Value: 1800},
			{Time: 1544481000, Value: 2400}, {Time: 1544481600, Value: 3000}, {Time: 1544482200, Value: 3600},
			{Time: 1544482800, Value: 4200}, {Time: 1544483400, Value: 4800}, {Time: 1544484000, Value: 5400},
			{Time: 1544484600, Value: 6000}, {Time: 1544485200, Value: 6600}, {Time: 1544485800, Value: 7200},
			{Time: 1544486400, Value: 7800}, {Time: 1544487000, Value: 8400}, {Time: 1544487600, Value: 9000},
			{Time: 1544488200, Value: 9600}, {Time: 1544488800, Value: 10200}, {Time: 1544489400, Value: 10800},
			{Time: 1544490000, Value: 11400}, {Time: 1544490600, Value: 12000}, {Time: 1544491200, Value: 12600},
			{Time: 1544491800, Value: 13200}, {Time: 1544492400, Value: 13800}, {Time: 1544493000, Value: math.NaN()},
		}, cmp.AllowUnexported(TimeSeriesPoint{}), cmpopts.EquateNaNs()); diff != "" {
			t.Error(diff)
		}
	})
	t.Run("Check3rdArchiveSum", func(t *testing.T) {
		data, err := srcMix.FetchByAggregation(start, now, &MixAggregationSpec{Method: Sum})
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(data.Points(), []TimeSeriesPoint{
			{Time: 1544479200, Value: 539700}, {Time: 1544479800, Value: 899700},
			{Time: 1544480400, Value: 1.2597e+06}, {Time: 1544481000, Value: 1.6197e+06},
			{Time: 1544481600, Value: 1.9797e+06}, {Time: 1544482200, Value: 2.3397e+06},
			{Time: 1544482800, Value: 2.6997e+06}, {Time: 1544483400, Value: 3.0597e+06},
			{Time: 1544484000, Value: 3.4197e+06}, {Time: 1544484600, Value: 3.7797e+06},
			{Time: 1544485200, Value: 4.1397e+06}, {Time: 1544485800, Value: 4.4997e+06},
			{Time: 1544486400, Value: 4.8597e+06}, {Time: 1544487000, Value: 5.2197e+06},
			{Time: 1544487600, Value: 5.5797e+06}, {Time: 1544488200, Value: 5.9397e+06},
			{Time: 1544488800, Value: 6.2997e+06}, {Time: 1544489400, Value: 6.6597e+06},
			{Time: 1544490000, Value: 7.0197e+06}, {Time: 1544490600, Value: 7.3797e+06},
			{Time: 1544491200, Value: 7.7397e+06}, {Time: 1544491800, Value: 8.0997e+06},
			{Time: 1544492400, Value: 8.4597e+06}, {Time: 1544493000, Value: math.NaN()},
		}, cmp.AllowUnexported(TimeSeriesPoint{}), cmpopts.EquateNaNs()); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("CheckDuplicateDataPoints", func(t *testing.T) {
		for i, arc := range srcMix.archives {
			m := map[int]bool{}
			for _, block := range arc.blockRanges {
				if block.start == 0 {
					continue
				}

				buf := make([]byte, arc.blockSize)
				if err := arc.whisper.fileReadAt(buf, int64(arc.blockOffset(block.index))); err != nil {
					panic(err)
				}

				dps, _, err := arc.ReadFromBlock(buf, []dataPoint{}, block.start, block.end)
				if err != nil {
					panic(err)
				}

				for _, dp := range dps {
					if m[dp.interval] {
						var spec string
						if i > 0 {
							spec = " " + arc.aggregationSpec.String()
						}
						t.Errorf("archive %d %s%s contains a duplicate timestamp: %d", i, arc.String(), spec, dp.interval)
					} else {
						m[dp.interval] = true
					}
				}
			}
		}
	})
}

func BenchmarkWriteCompressed(b *testing.B) {
	fpath := "benchmark_write.cwsp"
	os.Remove(fpath)
	cwhisper, err := CreateWithOptions(
		fpath,
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 172800},   // 1s:2d
			{secondsPerPoint: 60, numberOfPoints: 40320},   // 1m:28d
			{secondsPerPoint: 3600, numberOfPoints: 17520}, // 1h:2y
		},
		Sum,
		0,
		&Options{Compressed: true, PointsPerBlock: 7200},
	)
	if err != nil {
		b.Fatal(err)
	}

	start := Now()
	var ps, history []*TimeSeriesPoint
	for i := 0; i < b.N; i++ {
		p := &TimeSeriesPoint{
			Time:  int(start.Add(time.Duration(i) * time.Second).Unix()),
			Value: rand.NormFloat64(),
		}
		history = append(history, p)
		ps = append(ps, p)

		if len(ps) >= 300 {
			if err := cwhisper.UpdateMany(ps); err != nil {
				b.Fatal(err)
			}
			ps = ps[:0]
		}
	}
	if err := cwhisper.Close(); err != nil {
		b.Fatal(err)
	}
	td, err := os.Create("test_data")
	if err != nil {
		b.Fatal(err)
	}
	if err := json.NewEncoder(td).Encode(history); err != nil {
		b.Fatal(err)
	}
	if err := td.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkReadCompressed(b *testing.B) {
	fpath := "benchmark_write.cwsp"
	cwhisper, err := OpenWithOptions(fpath, &Options{})
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		_, err := cwhisper.Fetch(int(time.Now().Add(-48*time.Hour).Unix()), int(time.Now().Unix()))
		if err != nil {
			b.Fatal(err)
		}
	}
	if err := cwhisper.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkReadStandard(b *testing.B) {
	fpath := "benchmark_write.wsp"
	cwhisper, err := OpenWithOptions(fpath, &Options{})
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		_, err := cwhisper.Fetch(int(time.Now().Add(-48*time.Hour).Unix()), int(time.Now().Unix()))
		if err != nil {
			b.Fatal(err)
		}
	}
	if err := cwhisper.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkWriteStandard(b *testing.B) {
	fpath := "benchmark_write.wsp"
	os.Remove(fpath)
	cwhisper, err := CreateWithOptions(
		fpath,
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 172800},   // 1s:2d
			{secondsPerPoint: 60, numberOfPoints: 40320},   // 1m:28d
			{secondsPerPoint: 3600, numberOfPoints: 17520}, // 1h:2y
		},
		Sum,
		0,
		&Options{Compressed: false, PointsPerBlock: 7200},
	)
	if err != nil {
		b.Fatal(err)
	}

	start := Now()
	var ps []*TimeSeriesPoint
	for i := 0; i < b.N; i++ {
		ps = append(ps, &TimeSeriesPoint{
			Time:  int(start.Add(time.Duration(i) * time.Second).Unix()),
			Value: rand.NormFloat64(),
		})

		if len(ps) >= 300 {
			if err := cwhisper.UpdateMany(ps); err != nil {
				b.Fatal(err)
			}
			ps = ps[:0]

		}
	}
	if err := cwhisper.Close(); err != nil {
		b.Fatal(err)
	}
}

func TestAggregatePercentile(t *testing.T) {
	for _, c := range []struct {
		percentile float32
		expect     float64
		vals       []float64
	}{
		{50, 4.0, []float64{1, 2, 3, 4, 5, 6, 7}},
		{50, 4.5, []float64{1, 2, 3, 4, 5, 6, 7, 8}},
		{50, 1.0, []float64{1}},
		{50, math.NaN(), []float64{}},
		{90, 9.1, []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
		{99, 99.01, func() []float64 {
			var vals []float64
			for i := 0; i < 100; i++ {
				vals = append(vals, float64(i+1))
			}
			return vals
		}()},
		{99.9, 999.001015, func() []float64 {
			var vals []float64
			for i := 0; i < 1000; i++ {
				vals = append(vals, float64(i+1))
			}
			return vals
		}()},
	} {
		if got, want := aggregatePercentile(c.percentile, c.vals), c.expect; math.Trunc(got*10000) != math.Trunc(want*10000) && !(math.IsNaN(got) && math.IsNaN(want)) {
			t.Errorf("aggregatePercentile(%.2f, %v) = %f; want %f", c.percentile, c.vals, got, want)
		}
	}
}
