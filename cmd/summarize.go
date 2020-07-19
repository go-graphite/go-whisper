// +build ignore

package main

import (
	"crypto/md5"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func main() {
	now := flag.Int("now", int(time.Now().Add(0*time.Hour).Unix()), "specify the until value")
	offset := flag.Int("offset", 3600*12, "until = now - offset (unit: hour)")
	quarantinesRaw := flag.String("quarantines", "2019-02-21,2019-02-22", "ignore data started from this point")
	flag.Parse()

	var quarantines [][2]int
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

	path := flag.Args()[0]
	oflag := os.O_RDONLY
	db, err := whisper.OpenWithOptions(path, &whisper.Options{OpenFileFlag: &oflag})
	if err != nil {
		panic(err)
	}

	var sums []string
	for _, ret := range db.Retentions() {
		// from := int(whisper.Now().Unix()) - ret.MaxRetention()
		// until := int(whisper.Now().Add(time.Hour * time.Duration(-1*(*endOffset))).Unix())
		from := *now - ret.MaxRetention() + ret.SecondsPerPoint()*60
		until := *now - *offset
		dps, err := db.Fetch(from, until)
		if err != nil {
			panic(err)
		}

		vals := dps.Values()
		data := make([]byte, 8*len(vals))

		for _, quarantine := range quarantines {
			qfrom := quarantine[0]
			quntil := quarantine[1]
			if from <= qfrom && qfrom <= until {
				qfromIndex := (qfrom - from) / ret.SecondsPerPoint()
				quntilIndex := (quntil - from) / ret.SecondsPerPoint()
				for i := qfromIndex; i <= quntilIndex && i < len(vals); i++ {
					vals[i] = math.NaN()
				}
			}
		}

		var nonNans, nonZeros uint
		for i, p := range vals {
			if math.IsNaN(p) {
				binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(0))
			} else {
				binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(p))
				nonNans++
				if p != 0.0 {
					nonZeros++
				}
			}
		}

		sum := md5.Sum(data)
		sums = append(sums, fmt.Sprintf("%x,%d,%d,%d", sum[:], nonNans, nonZeros, len(vals)))
	}
	fmt.Printf("%s,%x,%s\n", path, md5.Sum([]byte(strings.Join(sums, ","))), strings.Join(sums, ","))
}
