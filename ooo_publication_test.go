package whisper

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
)

// Pause after the complete classic header is written, before sparse allocation.
// This makes the publication window deterministic without a production hook.
type publicationFS struct {
	atomicOSFileSystem
	header, resume chan struct{}
}

type publicationFile struct {
	StorageFile
	fs *publicationFS
}

func (fs *publicationFS) OpenFile(path string, flags int, mode os.FileMode) (StorageFile, error) {
	f, err := os.OpenFile(path, flags, mode)
	if err != nil {
		return nil, err
	}
	if flags&os.O_CREATE != 0 {
		return &publicationFile{f, fs}, nil
	}
	return f, nil
}

func (f *publicationFile) Write(b []byte) (int, error) {
	n, err := f.StorageFile.Write(b)
	if err == nil {
		close(f.fs.header)
		<-f.fs.resume
	}
	return n, err
}

func TestOOOPublicationDuringFetch(t *testing.T) {
	w, path, base := newSingleRetentionOOO(t, true)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := OpenWithOptions(path, &Options{OutOfOrder: true})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	fs := &publicationFS{header: make(chan struct{}), resume: make(chan struct{})}
	w.opts.FileSystem = fs
	done := make(chan error, 1)
	go func() {
		_, err := w.oooSidecar(true)
		done <- err
	}()
	<-fs.header
	func() {
		// Always release the writer, including when the old reader panics.
		defer func() {
			close(fs.resume)
			if err := <-done; err != nil {
				t.Error(err)
			}
		}()
		if _, err := r.Fetch(base, base+5); err != nil {
			t.Fatal(err)
		}
	}()
	if err := w.UpdateMany([]*TimeSeriesPoint{{Time: base + 1, Value: 42}}); err != nil {
		t.Fatal(err)
	}
	ts, err := r.Fetch(base, base+5)
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, ts, []float64{42, 2, math.NaN(), 3, math.NaN()})
	files, err := filepath.Glob(filepath.Join(filepath.Dir(path), "*.tmp"))
	if err != nil || len(files) != 0 {
		t.Fatalf("temporary files remain: %v, %v", files, err)
	}
}

func TestOOOShortSidecarReturnsError(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, size := range []int64{0, MetadataSize - 1, MetadataSize + ArchiveInfoSize, -1} {
			t.Run(fmt.Sprintf("cached=%t/size=%d", cached, size), func(t *testing.T) {
				w, path, base := newSingleRetentionOOO(t, true)
				if _, err := w.oooSidecar(true); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				r, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				if cached {
					if _, err := r.Fetch(base, base+5); err != nil {
						t.Fatal(err)
					}
				}
				if size == -1 {
					info, err := os.Stat(OutOfOrderSidecarPath(path))
					if err != nil {
						t.Fatal(err)
					}
					size = info.Size() - 1
				}
				if err := os.Truncate(OutOfOrderSidecarPath(path), size); err != nil {
					t.Fatal(err)
				}
				// A cached, empty sidecar can answer from its intact base slot
				// when only its unused tail is removed. Reopening validates all of it.
				if cached && size > MetadataSize+ArchiveInfoSize {
					return
				}
				if ts, err := r.Fetch(base, base+5); err == nil || ts != nil {
					t.Fatalf("short sidecar: got %v, %v", ts, err)
				}
				if size == MetadataSize+ArchiveInfoSize && !cached {
					_, err := r.Fetch(base, base+5)
					if !errors.Is(err, io.ErrUnexpectedEOF) {
						t.Fatalf("missing short-file error: %v", err)
					}
				}
			})
		}
	}
}

type failedPublicationFS struct {
	atomicOSFileSystem
	err error
}

func (fs failedPublicationFS) Rename(string, string) error { return fs.err }

func TestOOOFailedPublicationRemovesTemporaryFile(t *testing.T) {
	w, path, _ := newSingleRetentionOOO(t, true)
	defer w.Close()
	want := errors.New("publication failed")
	w.opts.FileSystem = failedPublicationFS{err: want}
	if sidecar, err := w.openOOO(true); sidecar != nil || !errors.Is(err, want) {
		t.Fatalf("publication: %v, %v", sidecar, err)
	}
	if _, err := os.Stat(OutOfOrderSidecarPath(path)); !os.IsNotExist(err) {
		t.Fatalf("published failed sidecar: %v", err)
	}
	files, err := filepath.Glob(filepath.Join(filepath.Dir(path), "*.tmp"))
	if err != nil || len(files) != 0 {
		t.Fatalf("temporary files remain: %v, %v", files, err)
	}
}
