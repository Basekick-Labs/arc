package fieldschema

import "github.com/apache/arrow-go/v18/parquet/compress"

// compressionNone is the codec for an anchor: it holds no data pages, so a
// codec only adds a dependency on the reader having it available.
func compressionNone() compress.Compression { return compress.Codecs.Uncompressed }
