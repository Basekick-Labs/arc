package api

import (
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/rs/zerolog"
)

// clientErrText is the text of an error from DuckDB execution (or from
// reading storage) as a client may see it: the message and its hint lines,
// without DuckDB's statement context (Arc's rewritten SQL) and without
// storage paths (sqlutil.ClientErrText). The paths it removed are logged in
// their own field: the log sanitiser drops them from the error too, and
// support needs to know which file an error was about. Use it only for the
// response text; classify on the original error.
func clientErrText(logger zerolog.Logger, err error) string {
	text, paths := sqlutil.ClientErrText(err.Error())
	if len(paths) > 0 {
		logger.Warn().Strs("paths", paths).Msg("error named storage paths; removed from the response")
	}
	return text
}
