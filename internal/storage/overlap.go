package storage

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

// Destination is one storage location reduced to the few things that decide
// whether two locations are the same place.
//
// It exists for ONE caller shape: refusing a configuration in which two
// subsystems that both write would write over each other. The backup
// destination (#1085 stage B) is the case that forced it, because a backup
// destination that contains the primary storage root is backed up by the next
// backup and swept by reconciliation, and a backup destination that shares a
// bucket prefix with the cold tier makes the backup listing see cold-tier
// objects.
//
// Comparison is deliberately COARSE and errs toward "these overlap". Two
// locations that are not provably distinct are reported as overlapping,
// because the caller turns the answer into a startup refusal: a false refusal
// is a config file an operator edits once, and a false clearance is silent
// data loss.
//
// Built as a function over two Destinations rather than over one pair of
// config blocks because stage B2b-2 compares N targets against each other as
// well as against these two, and N-squared calls of a two-argument function is
// the shape that extends without rewriting.
type Destination struct {
	// Kind is "local", "s3" or "azure". The spelling aliases (minio for s3,
	// azblob for azure) are normalised away, so two differently-spelled
	// configurations of one store compare equal.
	Kind string

	// Store identifies the object store instance, CANONICALISED so that two
	// spellings of one store compare equal. Empty on a local destination.
	//
	// Canonicalising is load bearing, not tidiness. An operator may leave
	// s3_endpoint empty for AWS on one side and spell it
	// "s3.us-east-1.amazonaws.com" on the other, or name an Azure account on
	// one side and give its "<account>.blob.core.windows.net" endpoint on the
	// other. Those are the same store, and comparing the raw strings reported
	// them as different — so the refusal was MISSED for the same bucket and
	// the same prefix. See canonicalS3Endpoint and azureStore.
	//
	// For s3 it is the endpoint with any AWS spelling folded to "", so "" is
	// a real identity (AWS) rather than a missing one. For azure it is the
	// storage account where one can be resolved, and the endpoint otherwise
	// (Azurite, a custom host). StoreUnknown is how "could not be determined"
	// is said.
	Store string

	// StoreUnknown says the store identity could not be resolved from the
	// configuration, which is reachable only for an Azure config carrying
	// neither an endpoint, an account name, nor a parseable connection string.
	// Such a destination compares equal to every store of its kind, so the
	// answer errs toward refusal — this check's whole job is upholding
	// cleanupPartialBackupWrite's premise, and a false clearance there is
	// deletion of objects the backup does not own. Note that config.Load
	// requires one of those three for an Azure block, so an unknown identity
	// is a configuration Arc would already have refused.
	StoreUnknown bool

	// Bucket is the S3 bucket or the Azure container. Empty on a local
	// destination.
	Bucket string

	// Prefix is the object-key prefix with no leading or trailing separator,
	// so the three spellings "", "arc" and "arc/" of the same prefix compare
	// equal. "" means the bucket or container root, which contains every
	// other prefix in that bucket.
	Prefix string

	// Path is the resolved absolute local directory (symlinks resolved
	// through the deepest existing ancestor). Empty on an object destination.
	Path string
}

// LocalDestination describes a local directory. The path is resolved, so a
// relative spelling, a trailing slash and a symlinked parent all land on the
// same value.
func LocalDestination(path string) Destination {
	return Destination{Kind: "local", Path: ResolveExistingPath(path)}
}

// DestinationFromSpec describes the location a BackendSpec names, without
// constructing the backend or touching the network.
//
// The prefix goes through ValidateObjectPrefix, the same function the backends
// themselves use, so a prefix this refuses would have failed construction
// anyway and a prefix it accepts is normalised exactly as the backend will
// normalise it. The trailing separator ValidateObjectPrefix adds is stripped
// again here: Destination.Prefix is separator-free so that "arc" and "arc/"
// cannot compare unequal (#534's corollary — the shape whose own fix shipped a
// mid-segment HasPrefix bug).
func DestinationFromSpec(spec BackendSpec) (Destination, error) {
	switch strings.ToLower(strings.TrimSpace(spec.Type)) {
	case "local":
		return LocalDestination(spec.LocalPath), nil

	case "s3", "minio":
		prefix, err := ValidateObjectPrefix(spec.S3.Prefix)
		if err != nil {
			return Destination{}, err
		}
		bucket := strings.TrimSpace(spec.S3.Bucket)
		return Destination{
			Kind:   "s3",
			Store:  canonicalS3Endpoint(spec.S3.Endpoint, bucket),
			Bucket: bucket,
			Prefix: strings.Trim(prefix, "/"),
		}, nil

	case "azure", "azblob":
		prefix, err := ValidateObjectPrefix(spec.Azure.Prefix)
		if err != nil {
			return Destination{}, err
		}
		store := azureStore(spec.Azure)
		return Destination{
			Kind:         "azure",
			Store:        store,
			StoreUnknown: store == "",
			Bucket:       strings.TrimSpace(spec.Azure.ContainerName),
			Prefix:       strings.Trim(prefix, "/"),
		}, nil

	default:
		return Destination{}, fmt.Errorf("unsupported storage backend %q (use local, s3, minio, azure or azblob)", spec.Type)
	}
}

// Overlaps reports whether writing to d could touch objects that belong to
// other, or the reverse.
//
// True when either destination CONTAINS the other, not only when they are
// identical: a backup destination at the bucket root and a cold tier at
// "cold/" in that bucket are one listing, and the backup's own objects appear
// under the cold tier's parent. Containment is tested at a separator boundary
// in both the local and the object arm, so "wh-other" is not inside "wh"
// (#534).
//
// Two locations of DIFFERENT kinds never overlap: a local directory is not
// inside an S3 bucket. Same bucket with disjoint prefixes does not overlap
// either — that is the legitimate shape an operator chooses on purpose, and
// refusing it would be a false positive on a check whose whole value is being
// believed.
func (d Destination) Overlaps(other Destination) bool {
	if d.Kind != other.Kind {
		return false
	}
	if d.Kind == "local" {
		return PathWithin(d.Path, other.Path) || PathWithin(other.Path, d.Path)
	}
	// Both Stores are already CANONICAL, which is what makes a string compare
	// sound here: two spellings of one store were the way this check got
	// missed, and the fix is to fold the spellings rather than to treat an
	// empty Store as unknown. Those are different things and conflating them
	// regresses the opposite case — on s3 an empty Store means AWS, a
	// determinate place, and treating it as a wildcard made an AWS bucket
	// overlap a MinIO bucket of the same name.
	//
	// An UNRESOLVED identity does match every store of its kind, so that
	// answer errs toward refusal (see Destination.StoreUnknown).
	if d.Store != other.Store && !d.StoreUnknown && !other.StoreUnknown {
		return false
	}
	if d.Bucket != other.Bucket {
		return false
	}
	return keyPrefixWithin(d.Prefix, other.Prefix) || keyPrefixWithin(other.Prefix, d.Prefix)
}

// String renders the destination the way an operator spells it, for the
// refusal message. Credentials are never part of a Destination, so this is
// always safe to put in an error.
func (d Destination) String() string {
	if d.Kind == "local" {
		return d.Path
	}
	loc := d.Kind + "://" + d.Bucket
	if d.Prefix != "" {
		loc += "/" + d.Prefix
	}
	if d.Store != "" {
		loc += " at " + d.Store
	}
	return loc
}

// keyPrefixWithin reports whether the object-key prefix p is dir itself or
// lies under it. Both arguments are separator-free (Destination.Prefix), and
// the empty prefix is the bucket root, which contains everything.
//
// Matched at a separator boundary, not with a bare HasPrefix: "arcx" is not
// under "arc" even though its first three bytes say otherwise. This is the
// object-key twin of PathWithin and exists for the same #534 reason.
func keyPrefixWithin(p, dir string) bool {
	if dir == "" {
		return true
	}
	return p == dir || strings.HasPrefix(p, dir+"/")
}

// awsS3Host matches every spelling of an AWS S3 regional endpoint, with the
// virtual-hosted bucket label already removed: "s3.amazonaws.com",
// "s3.us-east-1.amazonaws.com", "s3-us-east-1.amazonaws.com" and the dualstack
// and FIPS variants that insert further labels.
//
// Anchored on ".amazonaws.com" so the China partition
// ("s3.cn-north-1.amazonaws.com.cn") does NOT fold: it is a separate partition
// with separate credentials and separate buckets, so a bucket of the same name
// there is a different bucket.
var awsS3Host = regexp.MustCompile(`^s3(?:[.-][a-z0-9-]+)*\.amazonaws\.com$`)

// canonicalS3Endpoint folds every spelling of one S3 endpoint onto one value,
// with the AWS default spelled as "".
//
// Three spellings an operator may mix between two config blocks, all naming
// one store:
//
//	""                                  the default, by omission
//	"s3.us-east-1.amazonaws.com"        the regional endpoint, written out
//	"<bucket>.s3.amazonaws.com"         virtual-hosted addressing
//
// Comparing those raw made the overlap refusal MISS the same bucket at the
// same prefix, which is the one case the refusal exists for. Folding them is
// the fix; treating "" as a wildcard is not, because "" is AWS and a MinIO
// bucket of the same name is a different bucket.
//
// A non-AWS host (MinIO, SeaweedFS, an S3-compatible appliance) is returned
// normalised and unfolded, because nothing can be assumed about it.
func canonicalS3Endpoint(endpoint, bucket string) string {
	e := normalizeEndpoint(endpoint)
	if e == "" {
		return ""
	}
	// Virtual-hosted addressing puts the bucket in front of the host. Strip it
	// before matching, or "<bucket>.s3.amazonaws.com" would read as a custom
	// host and compare unequal to the same bucket addressed path-style.
	if b := strings.ToLower(strings.TrimSpace(bucket)); b != "" {
		e = strings.TrimPrefix(e, b+".")
	}
	if awsS3Host.MatchString(e) {
		return ""
	}
	return e
}

// azureBlobHost matches "<account>.blob.core.windows.net", capturing the
// account, so an explicit endpoint and a named account resolve to the same
// identity.
var azureBlobHost = regexp.MustCompile(`^([a-z0-9]{3,24})\.blob\.core\.windows\.net$`)

// azureAccountFromEndpoint returns the storage account an Azure blob endpoint
// addresses, or "" when the endpoint is not a public Azure one (Azurite, a
// private host).
func azureAccountFromEndpoint(endpoint string) string {
	if m := azureBlobHost.FindStringSubmatch(normalizeEndpoint(endpoint)); m != nil {
		return m[1]
	}
	return ""
}

// normalizeEndpoint reduces an object-store endpoint to a comparable identity:
// no scheme, no trailing slash, lowercased. "" stays "" and means the
// provider default.
//
// Scheme-stripping is what makes "localhost:9000" and
// "http://localhost:9000" one store, which matters because the S3 backend
// accepts both spellings and adds the scheme itself from UseSSL.
func normalizeEndpoint(endpoint string) string {
	e := strings.ToLower(strings.TrimSpace(endpoint))
	e = strings.TrimPrefix(e, "https://")
	e = strings.TrimPrefix(e, "http://")
	e = strings.TrimSuffix(e, "/")
	// A scheme's own default port is not part of the identity: "host" and
	// "host:443" are one host. Any other port IS part of it — "host:9000" and
	// "host:9001" are two stores on one machine, which is an ordinary
	// development layout.
	e = strings.TrimSuffix(e, ":443")
	e = strings.TrimSuffix(e, ":80")
	return e
}

// azureStore resolves which storage account an Azure config addresses.
//
// The account matters because two accounts can each hold a container of the
// same name, so the container name alone is not an identity.
//
// The ACCOUNT is preferred over the endpoint, in that order, because the two
// are two spellings of one thing: an operator may give
// azure_account_name = "acme" in one block and
// azure_endpoint = "https://acme.blob.core.windows.net" in another, and
// comparing those raw reported one store as two and MISSED the refusal. An
// endpoint that is not a public Azure host (Azurite, a private appliance) has
// no account to extract and is used as the identity itself.
//
// A connection-string deployment leaves AccountName empty —
// NewAzureBlobBackend authenticates from the string alone — so the string is
// parsed rather than treated as unknown, which would refuse a legitimate
// config.
//
// Returns "" when nothing resolves, which the caller turns into
// StoreUnknown — matching every other Azure store, so that answer errs toward
// refusal. Reachable only for an Azure config config.Load would already have
// refused (it requires an account name or a connection string).
func azureStore(cfg AzureBlobConfig) string {
	if name := strings.ToLower(strings.TrimSpace(cfg.AccountName)); name != "" {
		return name
	}
	if account := azureAccountFromEndpoint(cfg.Endpoint); account != "" {
		return account
	}
	if e := normalizeEndpoint(cfg.Endpoint); e != "" {
		return e
	}
	return parseAzureConnectionStringStore(cfg.ConnectionString)
}

// parseAzureConnectionStringStore pulls a store identity out of an Azure
// connection string. The format is fixed and semicolon-delimited with
// case-insensitive keys, so this reads the two keys that name a location and
// ignores every credential key.
func parseAzureConnectionStringStore(connStr string) string {
	var account, blobEndpoint string
	devStorage := false
	for _, part := range strings.Split(connStr, ";") {
		key, value, found := strings.Cut(part, "=")
		if !found {
			continue
		}
		value = strings.TrimSpace(value)
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "accountname":
			account = value
		case "blobendpoint":
			blobEndpoint = value
		case "usedevelopmentstorage":
			devStorage = strings.EqualFold(value, "true")
		}
	}
	// Account first, for the same reason azureStore prefers it: it is the
	// identity the other spellings resolve to.
	if account != "" {
		return strings.ToLower(account)
	}
	if a := azureAccountFromEndpoint(blobEndpoint); a != "" {
		return a
	}
	if blobEndpoint != "" {
		return normalizeEndpoint(blobEndpoint)
	}
	if devStorage {
		// Azurite's well-known local endpoint. A fixed identity rather than ""
		// so two Azurite configs compare as the same store, which they are.
		return "127.0.0.1:10000/devstoreaccount1"
	}
	return ""
}

// ResolveExistingPath returns p as an absolute, cleaned path with symlinks
// resolved. A path that does not exist yet is resolved through its deepest
// existing ancestor and the remainder is re-joined, so a fresh node whose
// directory sits under a symlinked parent still classifies correctly.
//
// An empty p resolves to the working directory, which is almost never what a
// caller means: callers that may hold "" must check for it before calling.
func ResolveExistingPath(p string) string {
	abs, err := filepath.Abs(p)
	if err != nil {
		return filepath.Clean(p)
	}
	abs = filepath.Clean(abs)
	var tail []string
	cur := abs
	for {
		if resolved, err := filepath.EvalSymlinks(cur); err == nil {
			for i := len(tail) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, tail[i])
			}
			return resolved
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return abs
		}
		tail = append(tail, filepath.Base(cur))
		cur = parent
	}
}

// PathWithin reports whether p is dir itself or lies beneath it, matching only
// at a path boundary: "/data/wh-other" is not within "/data/wh" (#534).
func PathWithin(p, dir string) bool {
	sep := string(filepath.Separator)
	dir = strings.TrimSuffix(filepath.Clean(dir), sep)
	p = filepath.Clean(p)
	return p == dir || strings.HasPrefix(p, dir+sep)
}
