// +build ignore

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	whisper "github.com/go-graphite/go-whisper"
)

func main() {
	var body string
	if len(os.Args) < 2 {
		fmt.Println("write: write data points to a whisper file.\nwrite file.wsp [1572940800:3,1572940801:5]\n")
		os.Exit(1)
	} else if len(os.Args) > 2 {
		body = os.Args[2]
	} else {
		in, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			panic(err)
		}
		body = string(in)
	}

	db, err := whisper.OpenWithOptions(os.Args[1], &whisper.Options{FLock: true})
	if err != nil {
		panic(err)
	}

	if err := db.UpdateMany(parse(body)); err != nil {
		panic(err)
	}

	if err := db.Close(); err != nil {
		panic(err)
	}

	if db.Extended {
		fmt.Println("file has been extended.")
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
