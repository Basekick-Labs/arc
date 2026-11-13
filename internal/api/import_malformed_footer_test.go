package api

import (
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/gofiber/fiber/v2"
)

// A footer that makes the Parquet library panic is a corrupt upload: 422,
// not a panic (GHSA-mw72-qxq2-vxhr).
func TestOpenImportParquetFooterPanic(t *testing.T) {
	prev := openParquetFooter
	openParquetFooter = func(parquet.ReaderAtSeeker, ...file.ReadOption) (*file.Reader, error) {
		panic("makeslice: cap out of range")
	}
	t.Cleanup(func() { openParquetFooter = prev })
	fr, ierr := openImportParquet([]byte("PAR1"))
	if fr != nil || ierr == nil || ierr.StatusCode != fiber.StatusUnprocessableEntity {
		t.Fatalf("openImportParquet = %v, %+v; want a 422", fr, ierr)
	}
}
