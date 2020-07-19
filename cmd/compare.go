// +build ignore

package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func init() { log.SetFlags(log.Lshortfile) }

func main() {
	now := flag.Int("now", 0, "specify the current time")
	ignoreBuffer := flag.Bool("ignore-buffer", false, "ignore points in buffer that haven't been propagated")
	quarantinesRaw := flag.String("quarantines", "", "ignore data started from this point. e.g. 2019-02-21,2019-02-22")
	verbose := flag.Bool("verbose", false, "be overly and nicely talkive")
	strict := flag.Bool("strict", false, "exit 1 whenever there are discrepancies between between the files")
	muteThreshold := flag.Int("mute-if-less", 2, "do not alarm if diff of points is less than specified.")
	flag.BoolVar(verbose, "v", false, "be overly and nicely talkive")
	flag.Parse()
	if len(flag.Args()) != 2 {
		fmt.Println("usage: cverify metric.wsp metric.cwsp")
		fmt.Println("purpose: check if two whisper files are containing the same data, made for verify migration result.")
		flag.PrintDefaults()
		os.Exit(1)
	}

	var quarantines [][2]int
	if *quarantinesRaw != "" {
		for _, q := range strings.Split(*quarantinesRaw, ";") {
			var quarantine [2]int
			for i, t := range strings.Split(q, ",") {
				tim, err := time.Parse("2006-01-02", t)
				if err != nil {
					panic(err)
				}
				quarantine[i] = int(tim.Unix())
			}
			quarantines = append(quarantines, quarantine)
		}
	}

	if *now > 0 {
		whisper.Now = func() time.Time {
			return time.Unix(int64(*now), 0)
		}
	}

	file1 := flag.Args()[0]
	file2 := flag.Args()[1]
	oflag := os.O_RDONLY

	db1, err := whisper.OpenWithOptions(file1, &whisper.Options{OpenFileFlag: &oflag})
	if err != nil {
		panic(err)
	}
	db2, err := whisper.OpenWithOptions(file2, &whisper.Options{OpenFileFlag: &oflag})
	if err != nil {
		panic(err)
	}

	var bad bool
	for index, ret := range db1.Retentions() {
		from := int(whisper.Now().Unix()) - ret.MaxRetention() + ret.SecondsPerPoint()*60
		until := int(whisper.Now().Unix()) - 3600*8

		if *verbose {
			fmt.Printf("%d %s: from = %+v until = %+v\n", index, ret, from, until)
		}

		var dps1, dps2 *whisper.TimeSeries
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()

			var err error
			dps1, err = db1.Fetch(from, until)
			if err != nil {
				panic(err)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			var err error
			dps2, err = db2.Fetch(from, until)
			if err != nil {
				panic(err)
			}
		}()

		wg.Wait()

		if *ignoreBuffer {
			{
				vals := dps1.Values()
				vals[len(vals)-1] = math.NaN()
				vals[len(vals)-2] = math.NaN()
			}
			{
				vals := dps2.Values()
				vals[len(vals)-1] = math.NaN()
				vals[len(vals)-2] = math.NaN()
			}
		}

		for _, quarantine := range quarantines {
			qfrom := quarantine[0]
			quntil := quarantine[1]
			if from <= qfrom && qfrom <= until {
				qfromIndex := (qfrom - from) / ret.SecondsPerPoint()
				quntilIndex := (quntil - from) / ret.SecondsPerPoint()
				{
					vals := dps1.Values()
					for i := qfromIndex; i <= quntilIndex && i < len(vals); i++ {
						vals[i] = math.NaN()
					}
				}
				{
					vals := dps2.Values()
					for i := qfromIndex; i <= quntilIndex && i < len(vals); i++ {
						vals[i] = math.NaN()
					}
				}
			}
		}

		var vals1, vals2 int
		for _, p := range dps1.Values() {
			if !math.IsNaN(p) {
				vals1++
			}
		}
		for _, p := range dps2.Values() {
			if !math.IsNaN(p) {
				vals2++
			}
		}

		fmt.Printf("  len1 = %d len2 = %d vals1 = %d vals2 = %d\n", len(dps1.Values()), len(dps2.Values()), vals1, vals2)

		if len(dps1.Values()) != len(dps2.Values()) {
			bad = true
			fmt.Printf("  size doesn't match: %d != %d\n", len(dps1.Values()), len(dps2.Values()))
		}
		if vals1 != vals2 {
			bad = true
			fmt.Printf("  values doesn't match: %d != %d (%d)\n", vals1, vals2, vals1-vals2)
		}
		var ptDiff int
		for i, p1 := range dps1.Values() {
			if len(dps2.Values()) < i {
				break
			}
			p2 := dps2.Values()[i]
			if !((math.IsNaN(p1) && math.IsNaN(p2)) || p1 == p2) {
				bad = true
				ptDiff++
				if *verbose {
					fmt.Printf("    %d: %d %v != %v\n", i, dps1.FromTime()+i*ret.SecondsPerPoint(), p1, p2)
				}
			}
		}
		fmt.Printf("  point mismatches: %d\n", ptDiff)
		if ptDiff <= *muteThreshold && !*strict {
			bad = false
		}
	}
	if db1.IsCompressed() {
		if err := db1.CheckIntegrity(); err != nil {
			fmt.Printf("integrity: %s\n%s", file1, err)
			bad = true
		}
	}
	if db2.IsCompressed() {
		if err := db2.CheckIntegrity(); err != nil {
			fmt.Printf("integrity: %s\n%s", file2, err)
			bad = true
		}
	}

	if bad {
		os.Exit(1)
	}
}
