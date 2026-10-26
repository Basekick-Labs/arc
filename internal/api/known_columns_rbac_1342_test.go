package api

import (
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// #1342: a case-twin refusal depends on the columns a measurement already
// has, so a token learns of them only for measurements it may write.
func TestKnownColumnsOnlyForWritableMeasurements(t *testing.T) {
	known := func(string) map[string]string { return map[string]string{"temp": "Temp"} }
	rbac := &mockRBACChecker{enabled: true, allowedDBs: map[string]bool{"d": true}, deniedMeasurements: map[string]bool{"d.secret": true}}
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	var allowed, denied map[string]string
	app.Get("/", func(c *fiber.Ctx) error {
		c.Locals("token_info", &auth.TokenInfo{ID: 7, Name: "t"})
		k := writableKnownColumns(c, rbac, "d", known)
		allowed, denied = k("open"), k("secret")
		return nil
	})
	_, err := app.Test(httptest.NewRequest("GET", "/", nil))
	require.NoError(t, err)
	require.Equal(t, "Temp", allowed["temp"])
	require.Nil(t, denied, "a measurement the token may not write must not reveal its columns")
}
