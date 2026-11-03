package storage

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// gcsS3Signer excludes Accept-Encoding from the signature for the GCS XML API.
// Keep the header on the wire: the SDK's identity encoding prevents transparent
// decompression from changing stored object bytes on reads.
type gcsS3Signer struct{ signer s3.HTTPSignerV4 }

func (s *gcsS3Signer) SignHTTP(ctx context.Context, credentials aws.Credentials, request *http.Request, payloadHash, service, region string, signingTime time.Time, options ...func(*v4.SignerOptions)) error {
	values, present := request.Header["Accept-Encoding"]
	delete(request.Header, "Accept-Encoding")
	defer func() {
		if present {
			request.Header["Accept-Encoding"] = values
		}
	}()
	return s.signer.SignHTTP(ctx, credentials, request, payloadHash, service, region, signingTime, options...)
}

// configureGCSS3Options confines XML interop differences to Google's endpoint.
func configureGCSS3Options(endpoint string, options *s3.Options) {
	parsed, err := url.Parse(endpoint)
	if err != nil || !strings.EqualFold(parsed.Hostname(), "storage.googleapis.com") {
		return
	}
	// Optional streaming checksum trailers are rejected by GCS XML interop.
	options.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	options.HTTPSignerV4 = &gcsS3Signer{signer: v4.NewSigner(func(o *v4.SignerOptions) {
		o.DisableURIPathEscaping = true
	})}
}
