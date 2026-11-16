package api

import (
	"errors"
	"strconv"

	"github.com/basekick-labs/arc/internal/audit"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
)

// Token writes on a cluster node for a token the cluster does not manage
// (#1630): see internal/auth/token_node_local.go.

// tokenWriteOptions reads ?node= from the request. Presence is kept apart
// from the value, so an empty node= is refused rather than read as absent.
func tokenWriteOptions(c *fiber.Ctx) auth.TokenWriteOptions {
	args := c.Context().QueryArgs()
	if !args.Has("node") {
		return auth.TokenWriteOptions{}
	}
	return auth.TokenWriteOptions{Node: string(args.Peek("node")), NodeSet: true}
}

// tokenScopeErrorResponse answers a TokenScopeError and reports true, or
// reports false for any other error.
func tokenScopeErrorResponse(c *fiber.Ctx, err error) (bool, error) {
	var se *auth.TokenScopeError
	if !errors.As(err, &se) {
		return false, nil
	}
	status := fiber.StatusConflict
	switch se.Code {
	case auth.TokenCodeNotCaughtUp, auth.TokenCodeNodeIDUnknown:
		status = fiber.StatusServiceUnavailable
		c.Set(fiber.HeaderRetryAfter, "5")
	case auth.TokenCodeInvalidNode:
		status = fiber.StatusBadRequest
	}
	body := fiber.Map{
		"success": false,
		"error":   se.Message,
		"code":    se.Code,
	}
	if se.NodeID != "" {
		body["node_id"] = se.NodeID
	}
	detail := map[string]string{"code": se.Code}
	if se.NodeID != "" {
		detail["node_id"] = se.NodeID
	}
	if se.TokenID != 0 {
		detail["token_id"] = strconv.FormatInt(se.TokenID, 10)
	}
	c.Locals(audit.DetailLocalsKey, detail)
	return true, c.Status(status).JSON(body)
}

// nodeLocalSuccess adds, to the 200 body of a write that changed a token on
// this node only, where it took effect and a warning that it did not take
// effect anywhere else. A cluster token's body is left as it was.
func nodeLocalSuccess(c *fiber.Ctx, body fiber.Map, res auth.TokenWriteResult, warning string) fiber.Map {
	if !res.NodeLocal {
		return body
	}
	on := "this node"
	if res.NodeID != "" {
		on = "node " + res.NodeID
	}
	body["scope"] = "node"
	if res.NodeID != "" {
		body["node_id"] = res.NodeID
	}
	body["token"] = fiber.Map{"id": res.TokenID, "name": res.Name, "fingerprint": res.Fingerprint}
	body["warning"] = "Changed on " + on + " only: the cluster does not manage this token. " + warning
	c.Locals(audit.DetailLocalsKey, map[string]string{
		"scope":      "node",
		"node_id":    res.NodeID,
		"token_id":   strconv.FormatInt(res.TokenID, 10),
		"token_name": res.Name,
	})
	return body
}

const sameTokenElsewhere = "If the same token exists on other nodes, repeat this request on each of them."
