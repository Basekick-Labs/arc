package fieldschema

import (
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
)

// A footer that makes the Parquet library panic is an undecodable anchor.
func TestDecodeAnchorFooterPanic(t *testing.T) {
	prev := openAnchorFooter
	openAnchorFooter = func(parquet.ReaderAtSeeker, ...file.ReadOption) (*file.Reader, error) {
		panic("makeslice: cap out of range")
	}
	t.Cleanup(func() { openAnchorFooter = prev })
	if _, _, err := DecodeAnchorComplete([]byte("PAR1")); err == nil {
		t.Fatal("a panicking footer read was not an error")
	}
}
