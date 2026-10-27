package storage

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
)

// redactedMark replaces credential material in error text.
const redactedMark = "REDACTED"

var (
	// An http(s) address with anything after "?": the part from "?" on is
	// dropped. Stops at whitespace, quotes, angle brackets and a backslash,
	// which is where Go's error formats, XML bodies and JSON-escaped text end
	// an address (so a stored JSON document stays valid).
	// Inside the query, a JSON-escaped "&" (\u0026) is part of it, so
	// escaped text loses the same query as plain text.
	credURLWithQuery = regexp.MustCompile(`(https?://[^\s"'<>?\\]*)\?(?:[^\s"'<>\\]|\\u0026)*`)
	// A connection string's signature field: its value runs to the next ";"
	// (it contains "&" itself).
	credSignatureField = regexp.MustCompile(`(?i)(^|[^a-z])(SharedAccessSignature)=[^;\s"'<>\\]+`)
	// Credential fields that can appear outside an address: in a response
	// body, or in a connection string. Preceded by anything but a letter, so
	// a JSON-escaped "\u0026sig=" matches too.
	credKeyValue = regexp.MustCompile(`(?i)(^|[^a-z])(sig|AccountKey)=[^&;\s"'<>\\]+`)
)

// RedactCredentialText removes credential material from text that may reach
// a log, an API response or a stored record: the part of any http(s) address
// from "?" on, the values of sig=, SharedAccessSignature= and AccountKey=,
// and every non-empty literal in secrets. The host and path of an address
// stay, because they name the store and object, which an operator needs.
func RedactCredentialText(s string, secrets ...string) string {
	for _, sec := range secrets {
		if len(sec) >= 8 {
			s = strings.ReplaceAll(s, sec, redactedMark)
		}
	}
	s = credURLWithQuery.ReplaceAllString(s, "$1")
	s = credSignatureField.ReplaceAllString(s, "$1$2="+redactedMark)
	return credKeyValue.ReplaceAllString(s, "$1$2="+redactedMark)
}

// redactedError is an error whose text has been through RedactCredentialText.
// It keeps the original in its chain, so errors.Is and errors.As behave as
// before (not-found checks, ErrStoreNotFound, context cancellation).
type redactedError struct {
	text string
	err  error
}

func (e *redactedError) Error() string { return e.text }
func (e *redactedError) Unwrap() error { return e.err }

// redactError returns err with credential material removed from its text.
// A *url.Error in the chain also has its address trimmed in place, so a
// caller that reaches it with errors.As and prints it gets the trimmed form.
func redactError(err error, secrets ...string) error {
	if err == nil {
		return nil
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		ue.URL = trimAddress(ue.URL)
	}
	text := RedactCredentialText(err.Error(), secrets...)
	var already *redactedError
	if errors.As(err, &already) && already.text == text {
		return err
	}
	return &redactedError{text: text, err: err}
}

// trimAddress drops everything from "?" on. It does not parse: an address
// that fails to parse (a control character in it) is exactly the case that
// must still be trimmed.
func trimAddress(addr string) string {
	before, _, _ := strings.Cut(addr, "?")
	return before
}
