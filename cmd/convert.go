// +build ignore

package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

var helpMessage = `convert: convert standard whisper files to compressed format

Start a normal conversion:
    convert -home /var/lib/carbon/convert -store /var/lib/carbon/whisper -rate 2 -one-off

Run a one-off to convert a file:
    convert -home /var/lib/carbon/convert -store metric.wsp -rate 2 -debug -one-off -force

Or files:
    convert -home /var/lib/carbon/convert -store *.wsp -rate 2 -debug -one-off -force
`

func main() {
	rand.Seed(time.Now().Unix())

	var storeDir = flag.String("store", "/var/lib/carbon/whisper", "path to whisper data files")
	var homdDir = flag.String("home", "/var/lib/carbon/convert", "home directory for saving conversion progress")
	var rate = flag.Int("rate", runtime.NumCPU(), "count of concurrent conversion per second")
	var debug = flag.Bool("debug", false, "show debug info")
	var force = flag.Bool("force", false, "ignore records progress.db and convert the files")
	var oneoff = flag.Bool("one-off", false, "only scan once")
	var keepOriginal = flag.Bool("keep-original", false, "keep both the original and compressed whisper files")
	var now = flag.Int64("now", 0, "specify the current time")
	var help = flag.Bool("help", false, "show help message")
	flag.BoolVar(help, "h", false, "show help message")
	flag.Parse()
	if *help {
		fmt.Println(helpMessage)
		flag.PrintDefaults()
		os.Exit(0)
	}
	if err := os.MkdirAll(*homdDir, 0755); err != nil {
		panic(err)
	}

	if *now > 0 {
		whisper.Now = func() time.Time {
			return time.Unix(*now, 0)
		}
	}

	var progressDB = *homdDir + "/progress.db"
	var pidFile = *homdDir + "/pid"
	var taskc = make(chan string, *rate)
	var convertingFiles sync.Map
	var convertingCount int64
	var progressc = make(chan string, *rate)
	var exitc = make(chan struct{})
	var shutdownc = make(chan os.Signal, 1)

	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", os.Getpid())), 0644); err != nil {
		fmt.Printf("main: failed to save pid: %s\n", err)
	}

	go schedule(*rate, taskc, progressDB, progressc, &convertingCount, &convertingFiles, exitc, *debug, *keepOriginal)
	go logProgress(progressDB, progressc)
	go func() {
		for {
			if err := scanAndDispatch(*storeDir, progressDB, taskc, *force); err != nil {
				fmt.Printf("error: %s", err)
			}

			if *oneoff {
				for len(taskc) > 0 {
					time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)))
				}
				time.Sleep(time.Second * 3)
				shutdownc <- syscall.SIGUSR2
			}

			time.Sleep(time.Hour)
		}
	}()
	onExit(&convertingCount, &convertingFiles, progressc, taskc, exitc, shutdownc)
}

func onExit(convertingCount *int64, convertingFiles *sync.Map, progressc chan string, taskc chan string, exitc chan struct{}, shutdownc chan os.Signal) {
	signal.Notify(shutdownc, os.Interrupt)
	signal.Notify(shutdownc, syscall.SIGTERM)
	signal.Notify(shutdownc, syscall.SIGUSR2)

	<-shutdownc
	close(exitc)
	fmt.Printf("exit: enter shutting down process\n")
	for {
		if c := atomic.LoadInt64(convertingCount); c > 0 {
			fmt.Printf("exit: %d files are still converting\n", c)
			convertingFiles.Range(func(k, v interface{}) bool {
				fmt.Printf("\t%s\n", k)
				return true
			})

			time.Sleep(time.Second * time.Duration(rand.Intn(10)))
			continue
		}
		if len(progressc) > 0 {
			fmt.Printf("exit: flushing progress records\n")
			time.Sleep(time.Second)
			continue
		}
		fmt.Printf("exit: progrem shutting down in 3 seconds\n")
		time.Sleep(time.Second * 3)
		// close(progressc)
		os.Exit(0)
	}
}

func schedule(rate int, taskc chan string, db string, progressc chan string, convertingCount *int64, convertingFiles *sync.Map, exitc chan struct{}, debug, keepOriginal bool) {
	ticker := time.NewTicker(time.Second / time.Duration(rate))
	for range ticker.C {
		if atomic.LoadInt64(convertingCount) >= int64(rate) {
			continue
		}

		var metric string
		select {
		case metric = <-taskc:
		case <-exitc:
			fmt.Printf("schedule: stopped\n")
			return
		}
		atomic.AddInt64(convertingCount, 1)
		convertingFiles.Store(metric, struct{}{})
		go func() {
			err := convert(metric, progressc, convertingCount, convertingFiles, debug, keepOriginal)
			if err != nil {
				fmt.Printf("error: %s", err)
			}
		}()
	}
}

func readProgress(db string) (files map[string]struct{}, err error) {
	file, err := os.Open(db)
	if err != nil {
		return
	}
	defer file.Close()

	files = map[string]struct{}{}
	var line []byte
	r := bufio.NewReader(file)
	for {
		line, _, err = r.ReadLine()
		if err == io.EOF {
			err = nil
			break
		}
		if len(line) == 0 {
			continue
		}
		a := bytes.Split(line, []byte(","))
		files[string(a[0])] = struct{}{}
	}

	return
}

func logProgress(progressDB string, progressc chan string) {
	logf, err := os.OpenFile(progressDB, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(fmt.Sprintf("log: failed to open %s: %s\n", progressDB, err))
		return
	}

	for file := range progressc {
		if _, err := logf.WriteString(file); err != nil {
			fmt.Printf("log: failed to log %s: %s\n", file, err)
		}
		if err := logf.Sync(); err != nil {
			fmt.Printf("log: failed to sync log: %s\n", err)
		}
	}

	// if err := logf.Close(); err != nil {
	// 	fmt.Printf("log: failed to close db: %s\n", err)
	// }
}

func convert(path string, progressc chan string, convertingCount *int64, convertingFiles *sync.Map, debugf, keepOriginal bool) error {
	defer func() {
		atomic.AddInt64(convertingCount, -1)
		convertingFiles.Delete(path)
	}()

	if debugf {
		fmt.Printf("convert: handling %s\n", path)
	}

	db, err := whisper.OpenWithOptions(path, &whisper.Options{FLock: true})
	if err != nil {
		return fmt.Errorf("convert: failed to open %s: %s", path, err)
	}
	defer func() {
		if err := db.File().Close(); err != nil {
			fmt.Printf("convert: failed to close converted file %s: %s", path, err)
		}
	}()

	if db.IsCompressed() {
		if debugf {
			fmt.Printf("convert: %s is already compressed\n", path)
		}
		return nil
	}

	var start = time.Now()
	var tmpPath = path + ".cwsp"
	var oldSize int64
	if stat, err := os.Stat(path); err == nil {
		oldSize = stat.Size()
	}
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("convert: %s panicked: %s\n", path, err)
			debug.PrintStack()
			return
		}

		var cpath = path
		if keepOriginal {
			cpath = tmpPath
		}
		stat, err := os.Stat(cpath)
		if err != nil {
			fmt.Printf("convert: failed to stat new size of %s: %s\n", cpath, err)
		}
		newSize := stat.Size()
		progressc <- fmt.Sprintf(
			"%s,%d,%d,%d,%d\n",
			path, oldSize, newSize, time.Now().Sub(start), start.Unix(),
		)
	}()

	os.Remove(tmpPath)
	if err := db.CompressTo(tmpPath); err != nil {
		return fmt.Errorf("convert: failed to compress %s: %s", path, err)
	}

	if keepOriginal {
		return nil
	}

	defer os.Remove(tmpPath)

	cfile, err := os.Open(tmpPath)
	if err != nil {
		return fmt.Errorf("convert: failed to open %s: %s", tmpPath, err)
	}
	defer cfile.Close()

	cstat, err := cfile.Stat()
	if err != nil {
		return fmt.Errorf("convert: failed to stat %s: %s", tmpPath, err)
	}

	if err := db.File().Truncate(cstat.Size()); err != nil {
		fmt.Printf("convert: failed to truncate %s: %s", path, err)
	}

	if _, err := db.File().Seek(0, 0); err != nil {
		return fmt.Errorf("convert: failed to seek %s: %s", path, err)
	}
	if _, err := io.Copy(db.File(), cfile); err != nil {
		os.Remove(path) // original file is most likely corrupted
		return fmt.Errorf("convert: failed to copy compressed data for %s: %s", path, err)
	}

	return nil
}

func scanAndDispatch(storeDir, progressDB string, taskc chan string, force bool) error {
	fmt.Printf("sd: start new conversion cycle\n")
	files, dur, err := scan(storeDir)
	if err != nil {
		return err
	}
	fmt.Printf("sd: scan %d files took %s\n", len(files), dur)

	cwsps, err := readProgress(progressDB)
	var tasks []string
	for _, file := range files {
		if _, ok := cwsps[file]; ok && !force {
			continue
		}
		tasks = append(tasks, file)
	}

	fmt.Printf("sd: uncompressed %d total %d pct %.2f%%\n", len(tasks), len(files), float64(len(tasks))/float64(len(files))*100)
	start := time.Now()
	for _, task := range tasks {
		taskc <- task
	}

	fmt.Printf("sd: done took %s\n", time.Now().Sub(start))
	return nil
}

func scan(dir string) (files []string, dur time.Duration, err error) {
	if stat, e := os.Stat(dir); e != nil {
		err = e
		return
	} else if !stat.IsDir() {
		files = append(files, dir)
		return
	}

	start := time.Now()
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".wsp") {
			files = append(files, path)
		}
		return nil
	})
	dur = time.Now().Sub(start)
	return
}
