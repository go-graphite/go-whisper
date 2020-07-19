// +build ignore

package main

import (
	"flag"
	"io/ioutil"
	"os"
	"os/exec"

	whisper "github.com/go-graphite/go-whisper"
)

func main() {
	header := flag.Bool("header", false, "show only file header")
	debug := flag.Bool("debug", false, "show decompression debug info")
	noLess := flag.Bool("no-less", false, "Don't use less, print everything to stdout.")
	flag.Parse()

	less := exec.Command("less")
	if !*noLess {
		less.Stdout = os.Stdout
		temp, err := ioutil.TempFile("", "")
		if err != nil {
			panic(err)
		}
		os.Stdout = temp
	}

	oflag := os.O_RDONLY
	db, err := whisper.OpenWithOptions(flag.Args()[0], &whisper.Options{OpenFileFlag: &oflag})
	if err != nil {
		panic(err)
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
