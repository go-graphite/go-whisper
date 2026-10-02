package whisper

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

// Preserve the previous on-disk checksum algorithm as an independent oracle.
func referenceCRC32(data []byte, previous uint32) uint32 {
	crc := previous ^ 0xffffffff
	for _, value := range data {
		crc ^= uint32(value)
		for bit := 0; bit < 8; bit++ {
			crc = (crc >> 1) ^ (uint32(-int32(crc&1)) & 0xedb88320)
		}
	}
	return crc ^ 0xffffffff
}

func TestCRC32Compatibility(t *testing.T) {
	if got := crc32([]byte("123456789"), 0); got != 0xcbf43926 {
		t.Fatalf("known IEEE vector: got %08x", got)
	}
	random := rand.New(rand.NewSource(42))
	data := make([]byte, 65536+16)
	random.Read(data)
	lengths := []int{0, 1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 1023, 1024, 4095, 4096, 65536}
	for _, length := range lengths {
		for offset := 0; offset < 16; offset++ {
			input := data[offset : offset+length]
			for _, previous := range []uint32{0, 1, 0xffffffff, 0x12345678, random.Uint32()} {
				want := referenceCRC32(input, previous)
				if got := crc32(input, previous); got != want {
					t.Fatalf("length=%d offset=%d previous=%08x: got %08x, want %08x", length, offset, previous, got, want)
				}
				split := length / 2
				if got := crc32(input[split:], crc32(input[:split], previous)); got != want {
					t.Fatalf("incremental checksum differs at length=%d offset=%d", length, offset)
				}
			}
		}
	}
}

var crc32BenchmarkSink uint32

func BenchmarkCRC32(b *testing.B) {
	for _, size := range []int{8, 64, 256, 4096, 65536} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			input := make([]byte, size)
			rand.New(rand.NewSource(42)).Read(input)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			var sum uint32
			for i := 0; i < b.N; i++ {
				sum = crc32(input, sum)
			}
			crc32BenchmarkSink = sum
		})
	}
}

func BenchmarkCompressedHeaderRewrite(b *testing.B) {
	path := filepath.Join(b.TempDir(), "header.wsp")
	w, err := CreateWithOptions(path, MustParseRetentionDefs("1s:2d,1m:35d,1h:2y"), Average, 0.5, &Options{Compressed: true})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.WriteHeaderCompressed(); err != nil {
			b.Fatal(err)
		}
	}
}
