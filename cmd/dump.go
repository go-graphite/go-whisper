// +build ignore

package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"

	whisper "github.com/go-graphite/go-whisper"
)

func main() {
	header := flag.Bool("header", false, "show only file header")
	debug := flag.Bool("debug", false, "show decompression debug info")
	noLess := flag.Bool("no-less", false, "Don't use less, print everything to stdout.")
	flag.Parse()

	oflag := os.O_RDONLY
	db, err := whisper.OpenWithOptions(flag.Args()[0], &whisper.Options{OpenFileFlag: &oflag})
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	less := exec.Command("less")
	if !*noLess {
		less.Stdout = os.Stdout
		temp, err := os.CreateTemp("", "")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
		os.Stdout = temp
	}

	db.Dump(!*header, *debug)

	if !*noLess {
		if _, err := os.Stdout.Seek(0, 0); err != nil {
			panic(err)
		}
		less.Stderr = os.Stderr
		less.Stdin = os.Stdout
		if err := less.Run(); err != nil {
			panic(err)
		}
	}
}
