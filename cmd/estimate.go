// +build ignore

package main

import (
	"fmt"
	"os"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func help() {
	fmt.Printf("estimate: to estimate the file compression ratio for specific retention policy on different compressed data point sizes.\nusage: estimate 1s:2d,1m:31d,1h:2y\n")
	os.Exit(1)
}

func main() {
	if len(os.Args) == 1 {
		help()
	}
	rets, err := whisper.ParseRetentionDefs(os.Args[1])
	if err != nil {
		help()
	}

	swhisper, err := whisper.CreateWithOptions(
		fmt.Sprintf("estimate.%d.wsp", time.Now().Unix()), rets, whisper.Sum, 0,
		&whisper.Options{Compressed: false, PointsPerBlock: 7200, InMemory: true},
	)
	if err != nil {
		fmt.Printf("failed to estimate whisper file size: %s\n", err)
		os.Exit(1)
	}

	fmt.Println("compressed whisper file sizes using different size per data points and comparing to standard whisper file")
	fmt.Printf("standard whisper file size = %s (%d)\n", toString(swhisper.Size()), swhisper.Size())
	fmt.Println("point size, file size, ratio")
	for _, size := range []float32{0.2, 2, 7, 14} {
		for _, ret := range rets {
			ret.SetAvgCompressedPointSize(size)
		}

		cwhisper, err := whisper.CreateWithOptions(
			fmt.Sprintf("estimate.%d.cwsp", time.Now().Unix()), rets, whisper.Sum, 0,
			&whisper.Options{Compressed: true, PointsPerBlock: 7200, InMemory: true},
		)
		if err != nil {
			fmt.Printf("failed to estimate cwhisper file with point size %f: %s\n", size, err)
			os.Exit(1)
		}

		fmt.Printf("%.1f, %s (%d), %.2f%%\n", size, toString(cwhisper.Size()), cwhisper.Size(), float32(cwhisper.Size()*100)/float32(swhisper.Size()))
	}
}

func toString(size int) string {
	switch {
	case size < 1024:
		return fmt.Sprintf("%dB", size)
	case size < 1024*1024:
		return fmt.Sprintf("%.2fKB", float32(size)/1024)
	default:
		return fmt.Sprintf("%.2fMB", float32(size)/1024/1024)
	}
}
