// +build ignore

package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func main() {
	ignoreNow := flag.Bool("ignore-now", false, "ignore now on write (always write to the base/first archive)")
	schema := flag.String("schema", "", "create a new whisper file using the schema if file not found: 1s2d:1m:31d:1h:10y;avg")
	xFilesFactor := flag.Float64("xfiles-factor", 0.0, "xfiles factor used for creating new whisper file")
	delimiter := flag.String("d", ",", "delimiter of data points")
	compressed := flag.Bool("compressed", false, "use compressed format")
	randChunk := flag.Int("rand-chunk", 0, "randomize input size with limit for triggering extensions and simulating real life writes.")
	ppb := flag.Int("ppb", whisper.DefaultPointsPerBlock, "points per block")
	flag.Parse()

	var body string
	if len(flag.Args()) < 1 {
		fmt.Println("write: write data points to a whisper file.\nwrite file.wsp [1572940800:3,1572940801:5]\n")
		os.Exit(1)
	} else if len(flag.Args()) > 1 {
		body = flag.Args()[1]
	} else {
		in, err := io.ReadAll(os.Stdin)
		if err != nil {
			panic(err)
		}
		body = string(in)
	}

	filename := flag.Args()[0]
	db, err := whisper.OpenWithOptions(filename, &whisper.Options{FLock: true, IgnoreNowOnWrite: *ignoreNow})
	if err != nil {
		if !os.IsNotExist(err) {
			fmt.Printf("failed to open file: %s\n", err)
			os.Exit(1)
		}
		if *schema == "" {
			fmt.Println("file not found")
			os.Exit(1)
		}

		specs := strings.Split(*schema, ";")
		if len(specs) != 2 {
			fmt.Printf("illegal schema: %s example: retention;aggregation\n", *schema)
			os.Exit(1)
		}
		rets, err := whisper.ParseRetentionDefs(specs[0])
		if err != nil {
			fmt.Printf("failed to parse retentions: %s\n", err)
			os.Exit(1)
		}
		aggregationMethod := whisper.ParseAggregationMethod(specs[1])
		if aggregationMethod == whisper.Unknown {
			fmt.Printf("unknow aggregation method: %s\n", specs[1])
			os.Exit(1)
		}

		db, err = whisper.CreateWithOptions(
			filename, rets, aggregationMethod, float32(*xFilesFactor),
			&whisper.Options{
				Compressed:       *compressed,
				IgnoreNowOnWrite: *ignoreNow,
				PointsPerBlock:   *ppb,
			},
		)
		if err != nil {
			fmt.Printf("failed to create new whisper file: %s\n", err)
			os.Exit(1)
		}
	}

	rand.Seed(time.Now().Unix())

	dps := parse(body, *delimiter)
	if *randChunk > 0 {
		for i := 0; i < len(dps); {
			// end := i + rand.Intn(*randChunk) + 1
			end := i + *randChunk + 1
			if end > len(dps) {
				end = len(dps)
			}
			if err := db.UpdateMany(dps[i:end]); err != nil {
				panic(err)
			}
			i = end
		}
	} else {
		if err := db.UpdateMany(dps); err != nil {
			panic(err)
		}
	}

	if err := db.Close(); err != nil {
		panic(err)
	}

	if db.Extended {
		fmt.Println("file is extended.")
	}
}

func parse(str, delimiter string) []*whisper.TimeSeriesPoint {
	var ps []*whisper.TimeSeriesPoint
	for _, p := range strings.Split(str, delimiter) {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		pp := strings.Split(p, ":")
		t, err := strconv.Atoi(pp[0])
		if err != nil {
			panic(err)
		}
		v, err := strconv.ParseFloat(pp[1], 64)
		if err != nil {
			panic(err)
		}
		ps = append(ps, &whisper.TimeSeriesPoint{Time: t, Value: v})
	}
	return ps
}
