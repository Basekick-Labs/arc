package iceberg

import (
	"context"
	"strconv"

	iceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"

	// Registers the s3:// FileIO scheme, and no other blob driver: iceberg-go
	// opens table metadata, manifests and added data files through it (#939).
	_ "github.com/apache/iceberg-go/io/gocloud/s3"

	"github.com/basekick-labs/arc/internal/storage"
)

// s3Settings is the subset of the S3 backend the exporter reads; the
// interface keeps tests free to supply settings without a live bucket.
type s3Settings interface {
	ClientSettings() storage.S3ClientSettings
}

// s3CatalogProps maps the S3 backend's settings onto the catalog properties
// iceberg-go's S3 FileIO reads. No credential goes in: the FileIO takes the
// backend's aws.Config from the context (withAWS), so metadata, the SQLite
// catalog and logs never hold a key, and a renewable provider (IRSA, an
// assumed role) renews for both clients through one cache.
//
//   - s3.endpoint is always present. Absent, the FileIO would read
//     AWS_S3_ENDPOINT from the environment and could silently talk to
//     another endpoint than Arc; present and empty, it leaves the SDK's own
//     resolution alone (FIPS, dual-stack, partitions), as Arc's client does.
//   - s3.force-virtual-addressing is always present: iceberg-go defaults to
//     path style whenever an endpoint is set, Arc to virtual-hosted style
//     unless storage.s3_path_style is on.
func s3CatalogProps(set storage.S3ClientSettings) iceberg.Properties {
	props := iceberg.Properties{
		iceio.S3EndpointURL:            set.Endpoint,
		iceio.S3ForceVirtualAddressing: strconv.FormatBool(!set.PathStyle),
	}
	if set.Region != "" {
		props[iceio.S3Region] = set.Region
	}
	return props
}

// withAWS puts the S3 backend's aws.Config on ctx for iceberg-go's FileIO,
// which loads a filesystem per table load and commit from the context it is
// given. A no-op without an S3 backend. Every exported Exporter entry point
// calls it, since any of them can reach the FileIO.
func (e *Exporter) withAWS(ctx context.Context) context.Context {
	if e.awsCfg == nil || utils.GetAwsConfig(ctx) != nil {
		return ctx
	}
	return utils.WithAwsConfig(ctx, e.awsCfg)
}

// awsConfigOf returns a copy of the backend's aws.Config, or nil without an
// S3 backend.
func awsConfigOf(backend storage.Backend) (*aws.Config, iceberg.Properties) {
	sb, ok := backend.(s3Settings)
	if !ok {
		return nil, nil
	}
	set := sb.ClientSettings()
	cfg := set.AWS.Copy()
	return &cfg, s3CatalogProps(set)
}
