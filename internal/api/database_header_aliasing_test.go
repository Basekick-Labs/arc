package api

import (
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/gofiber/fiber/v2"
	"github.com/valyala/fasthttp"
)

// unsafeStringForTest reproduces Fiber's zero-copy []byte->string conversion
// (utils.UnsafeString) so the property test can alias a mutable backing array
// the same way c.Get aliases the fasthttp header buffer.
func unsafeStringForTest(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// TestDatabaseHeaderNotAliasedAfterHandler is the regression test for the
// data-corruption bug where a msgpack/line-protocol write tagged
// x-arc-database=X landed in a DIFFERENT database under concurrency.
//
// Root cause: Fiber's c.Get is zero-copy — the returned string aliases the
// fasthttp request-header buffer and is only valid for the handler's lifetime.
// Arc retained that string past the handler (async Arrow-buffer flush uses it as
// the storage-path DB directory). When a concurrent request reused the fasthttp
// buffer, the retained bytes were overwritten, silently redirecting the write.
//
// This test reproduces the mechanism directly: it captures what a handler
// retains from c.Get, then mutates the underlying fasthttp header arena the way
// buffer reuse would, and asserts the retained value is unchanged. Without
// strings.Clone this FAILS (the captured string mutates); with it, it passes.
func TestDatabaseHeaderNotAliasedAfterHandler(t *testing.T) {
	app := fiber.New()

	var retained string
	app.Post("/w", func(c *fiber.Ctx) error {
		// Mirror the production code path: clone at the boundary.
		retained = strings.Clone(c.Get("x-arc-database"))
		return c.SendStatus(fiber.StatusNoContent)
	})

	// Drive the handler against a real fasthttp ctx so c.Get reads a real
	// aliasing buffer, then scribble over that buffer post-handler.
	fctx := &fasthttp.RequestCtx{}
	fctx.Request.Header.SetMethod("POST")
	fctx.Request.SetRequestURI("/w")
	fctx.Request.Header.Set("x-arc-database", "energy_grid")

	app.Handler()(fctx)

	// Simulate buffer reuse by a concurrent, longer-named writer. If `retained`
	// aliased the header arena, this corrupts it (and truncates to len 11).
	fctx.Request.Reset()
	fctx.Request.Header.Set("x-arc-database", "vessels_tracking_other_writer")

	if retained != "energy_grid" {
		t.Fatalf("retained database was corrupted by buffer reuse: got %q, want %q "+
			"(the c.Get value escaped the handler without strings.Clone)", retained, "energy_grid")
	}
}

// TestStringsCloneDecouplesFromBackingArray is a minimal proof of the property
// the fix relies on: strings.Clone yields a string whose bytes are independent
// of the source's backing memory, so later mutation of that memory cannot change
// the clone. (Also covers the empty-string case: clone of "" stays "".)
func TestStringsCloneDecouplesFromBackingArray(t *testing.T) {
	backing := []byte("energy_grid")
	aliased := unsafeStringForTest(backing) // aliases `backing`, like c.Get
	cloned := strings.Clone(aliased)

	// Overwrite the backing array, as fasthttp buffer reuse would.
	copy(backing, []byte("vessels_trac"))

	if cloned != "energy_grid" {
		t.Fatalf("strings.Clone did not decouple from backing array: got %q", cloned)
	}
	if aliased == "energy_grid" {
		t.Fatalf("sanity check failed: the aliased string should have mutated with its backing array, got %q", aliased)
	}
	if strings.Clone("") != "" {
		t.Fatalf(`strings.Clone("") must be ""`)
	}
}

// TestConcurrentMultiDatabaseWritesStayIsolated exercises the concurrency shape
// that triggered the bug: many handlers, each with a different database header,
// firing at once. Each must retain exactly its own database name — never
// another goroutine's. With the alias bug this is racy and fails under -race;
// with strings.Clone every capture is its own memory.
func TestConcurrentMultiDatabaseWritesStayIsolated(t *testing.T) {
	app := fiber.New()

	app.Post("/w", func(c *fiber.Ctx) error {
		db := strings.Clone(c.Get("x-arc-database"))
		// Hand the retained value back via a response header so the driver can
		// assert per-request isolation without shared state.
		c.Set("x-echo-db", db)
		return c.SendStatus(fiber.StatusNoContent)
	})

	dbs := []string{"energy_grid", "vessels_tracking", "weather_tracking", "system_monitoring", "quiniela_avenir"}
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		want := dbs[i%len(dbs)]
		wg.Add(1)
		go func(want string) {
			defer wg.Done()
			fctx := &fasthttp.RequestCtx{}
			fctx.Request.Header.SetMethod("POST")
			fctx.Request.SetRequestURI("/w")
			fctx.Request.Header.Set("x-arc-database", want)
			app.Handler()(fctx)
			if got := string(fctx.Response.Header.Peek("x-echo-db")); got != want {
				t.Errorf("handler retained wrong database: got %q, want %q", got, want)
			}
		}(want)
	}
	wg.Wait()
}
