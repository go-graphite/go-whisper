// +build ignore

package main

import (
	"fmt"
	"log"
	"os"

	whisper "github.com/go-graphite/go-whisper"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage: icheck metric.wsp")
		fmt.Println("purpose: checks intergiry of a compressed file, including")
		fmt.Println("         - crc32 values saved in the headers matched the content")
		fmt.Println("         - no invalid data ponit size like 0 or NAN")
		os.Exit(1)
	}
	file1 := os.Args[1]

	oflag := os.O_RDONLY
	db1, err := whisper.OpenWithOptions(file1, &whisper.Options{OpenFileFlag: &oflag})
	if err != nil {
		panic(err)
	}

	if err := db1.CheckIntegrity(); err != nil {
		fmt.Printf("integrity: %s\n%s", file1, err)
		os.Exit(1)
	}
}
