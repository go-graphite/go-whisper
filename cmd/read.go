// +build ignore

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func main() {
	archive := flag.Int("archive", -1, "read data from specified archive (0-indexed).")
	from := flag.String("from", "", "read data from the specified timestamp.")
	until := flag.String("until", "", "read data until the specified timestamp.")
	format := flag.String("format", "txt", "specify the format of the data returned: json, txt.")
	archiveNum := flag.Bool("archive-num", false, "check the number of archives")
	timezone := flag.String("timezone", "Local", `timezone to parse from/until timestamps: Local, UTC, or a location name corresponding to a file in the IANA Time Zone database, such as "America/New_York". https://golang.org/pkg/time/#LoadLocation.`)
	help := flag.Bool("help", false, "print help messages.")
	keepNaN := flag.Bool("nan", false, "Show NaN/absent data points. If false it will be returned as 0.")
	quiet := flag.Bool("quiet", false, "print only data, no other messages.")
	flag.BoolVar(help, "h", false, "print help messages.")

	flag.Parse()

	location, err := time.LoadLocation(*timezone)
	if err != nil {
		panic(err)
	}

	if len(flag.Args()) == 0 || *help {
		fmt.Printf(
			"usage: read -from %q -until %q whisper_filename\n",
			time.Now().In(location).Add(-4*time.Hour).Format("2006-01-02 15:03:06"),
			time.Now().In(location).Add(-5*time.Minute).Format("2006-01-02 15:03:06"))
		flag.PrintDefaults()
		os.Exit(0)
	}

	openOption := os.O_RDONLY
	db, err := whisper.OpenWithOptions(flag.Arg(0), &whisper.Options{FLock: true, OpenFileFlag: &openOption})
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			panic(err)
		}
	}()

	if *archiveNum {
		fmt.Println(len(db.Retentions()))
		os.Exit(0)
	}

	if *archive == -1 && (*from == "" || *until == "") {
		flag.PrintDefaults()
		os.Exit(1)
	}

	var fromTime, untilTime int
	if *archive != -1 {
		rets := db.Retentions()
		if *archive >= len(rets) {
			fmt.Fprintf(os.Stderr, "The whisper file only has %d archives.\n", len(rets))
			os.Exit(2)
		}

		if *until == "" {
			untilTime = int(time.Now().Unix())
		} else {
			t, err := time.ParseInLocation("2006-01-02 15:04:05", *until, location)
			if err != nil {
				panic(err)
			}
			untilTime = int(t.Unix())
		}

		fromTime = untilTime - rets[*archive].MaxRetention()
	} else {
		t1, err := time.ParseInLocation("2006-01-02 15:04:05", *from, location)
		if err != nil {
			panic(err)
		}
		fromTime = int(t1.Unix())

		t2, err := time.ParseInLocation("2006-01-02 15:04:05", *until, location)
		if err != nil {
			panic(err)
		}
		untilTime = int(t2.Unix())
	}

	if !*quiet {
		fmt.Printf("reading data from %d (%s) to %d (%s)\n", fromTime, time.Unix(int64(fromTime), 0), untilTime, time.Unix(int64(untilTime), 0))
	}

	data, err := db.Fetch(fromTime, untilTime)
	if err != nil {
		panic(err)
	} else if data == nil {
		fmt.Println("No data is fetched. Bad timerange (from/until)?")
		return
	}

	var ps []whisper.TimeSeriesPoint
	for _, p := range data.Points() {
		if math.IsNaN(p.Value) {
			if !*keepNaN {
				continue
			}
			p.Value = 0
		}

		ps = append(ps, p)
	}

	switch *format {
	case "json":
		bytes, err := json.Marshal(ps)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(bytes))
	default:
		for _, p := range ps {
			fmt.Printf("%d: %v\n", p.Time, p.Value)
		}
	}
}

func parse(str string) []*whisper.TimeSeriesPoint {
	var ps []*whisper.TimeSeriesPoint
	for _, p := range strings.Split(str, ",") {
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
