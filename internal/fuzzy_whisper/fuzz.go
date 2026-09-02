package fuzzy_whisper

import (
	"os"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

// TODO: not implemented yet?
//
// skipcq: RVV-B0012
func Fuzz(data []byte) int {
	f, err := os.CreateTemp("cwhisper", "*")
	if err != nil {
		panic("failed to create tempfile: " + err.Error())
	}
	db, err := whisper.OpenWithOptions(f.Name(), &whisper.Options{FLock: true})
	if err != nil {
		panic(err)
	}
	if _, err := db.Fetch(int(time.Now().Add(time.Hour*24*-60).Unix()), int(time.Now().Unix())); err != nil {
		panic(err)
	}
	return 0
}
