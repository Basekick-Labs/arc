package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func newTestRouter(registry *Registry, localNode *Node) *Router {
	return NewRouter(&RouterConfig{
		Timeout:   5 * time.Second,
		Retries:   2,
		Strategy:  LoadBalanceRoundRobin,
		Registry:  registry,
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})
}

func TestRouterRouteWriteNoWriters(t *testing.T) {
	registry := newTestRegistry()
	localNode := NewNode("reader-1", "Reader", RoleReader, "test-cluster")
	router := newTestRouter(registry, localNode)

	req := httptest.NewRequest("POST", "/api/v1/write", nil)
	ctx := context.Background()

	_, err := router.RouteWrite(ctx, req)
	if err != ErrNoWriterAvailable {
		t.Errorf("Expected ErrNoWriterAvailable, got: %v", err)
	}
}

func TestRouterRouteWriteLocalCanHandle(t *testing.T) {
	registry := newTestRegistry()
	localNode := NewNode("writer-1", "Writer", RoleWriter, "test-cluster")
	localNode.UpdateState(StateHealthy)
	router := newTestRouter(registry, localNode)

	req := httptest.NewRequest("POST", "/api/v1/write", nil)
	ctx := context.Background()

	_, err := router.RouteWrite(ctx, req)
	if err != ErrLocalNodeCanHandle {
		t.Errorf("Expected ErrLocalNodeCanHandle, got: %v", err)
	}
}

func TestRouterRouteQueryNoReaders(t *testing.T) {
	registry := newTestRegistry()
	localNode := NewNode("compactor-1", "Compactor", RoleCompactor, "test-cluster")
	router := newTestRouter(registry, localNode)

	req := httptest.NewRequest("GET", "/api/v1/query", nil)
	ctx := context.Background()

	_, err := router.RouteQuery(ctx, req)
	if err != ErrNoReaderAvailable {
		t.Errorf("Expected ErrNoReaderAvailable, got: %v", err)
	}
}

func TestRouterRouteQueryLocalCanHandle(t *testing.T) {
	registry := newTestRegistry()
	localNode := NewNode("reader-1", "Reader", RoleReader, "test-cluster")
	localNode.UpdateState(StateHealthy)
	router := newTestRouter(registry, localNode)

	req := httptest.NewRequest("GET", "/api/v1/query", nil)
	ctx := context.Background()

	_, err := router.RouteQuery(ctx, req)
	if err != ErrLocalNodeCanHandle {
		t.Errorf("Expected ErrLocalNodeCanHandle, got: %v", err)
	}
}

func TestRouterForwardRequest(t *testing.T) {
	// Create a test server to act as the target node
	responseBody := `{"status": "ok"}`
	var receivedHeaders http.Header
	var receivedBody string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header
		body, _ := io.ReadAll(r.Body)
		receivedBody = string(body)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(responseBody))
	}))
	defer server.Close()

	// Parse server address
	addr := strings.TrimPrefix(server.URL, "http://")

	registry := newTestRegistry()

	// Create a writer node pointing to the test server
	writer := NewNode("writer-1", "Writer", RoleWriter, "test-cluster")
	writer.UpdateState(StateHealthy)
	writer.APIAddress = addr
	registry.Register(writer)

	// Create router with a reader as local node (so it needs to forward writes)
	localNode := NewNode("reader-1", "Reader", RoleReader, "test-cluster")
	router := newTestRouter(registry, localNode)

	// Create request
	reqBody := `{"logs": [{"message": "test"}]}`
	req := httptest.NewRequest("POST", "/api/v1/write", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer token123")
	ctx := context.Background()

	// Route the write
	resp, err := router.RouteWrite(ctx, req)
	if err != nil {
		t.Fatalf("RouteWrite failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got: %d", resp.StatusCode)
	}

	// Verify the request was forwarded correctly
	if receivedBody != reqBody {
		t.Errorf("Body not forwarded correctly. Got: %s, want: %s", receivedBody, reqBody)
	}

	if receivedHeaders.Get("Content-Type") != "application/json" {
		t.Error("Content-Type header not forwarded")
	}

	if receivedHeaders.Get("Authorization") != "Bearer token123" {
		t.Error("Authorization header not forwarded")
	}

	if receivedHeaders.Get("X-Arc-Forwarded-By") != localNode.ID {
		t.Error("X-Arc-Forwarded-By header not set correctly")
	}
}

func TestRouterRoundRobinSelection(t *testing.T) {
	registry := newTestRegistry()

	// Create multiple readers
	for i := 1; i <= 3; i++ {
		reader := NewNode(fmt.Sprintf("reader-%d", i), "Reader", RoleReader, "test-cluster")
		reader.UpdateState(StateHealthy)
		reader.APIAddress = fmt.Sprintf("10.0.0.%d:8000", i)
		registry.Register(reader)
	}

	localNode := NewNode("writer-1", "Writer", RoleWriter, "test-cluster")
	router := NewRouter(&RouterConfig{
		Strategy:  LoadBalanceRoundRobin,
		Registry:  registry,
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})

	// Get readers for selection testing
	readers := registry.GetReaders()

	// Select nodes multiple times and verify round-robin behavior
	selected := make(map[string]int)
	for i := 0; i < 9; i++ {
		node := router.selectNode(readers)
		selected[node.ID]++
	}

	// Each reader should be selected exactly 3 times
	for id, count := range selected {
		if count != 3 {
			t.Errorf("Reader %s selected %d times, expected 3", id, count)
		}
	}
}

func TestRouterLeastConnectionsSelection(t *testing.T) {
	registry := newTestRegistry()

	// Create multiple readers
	reader1 := NewNode("reader-1", "Reader", RoleReader, "test-cluster")
	reader1.UpdateState(StateHealthy)
	reader1.APIAddress = "10.0.0.1:8000"
	registry.Register(reader1)

	reader2 := NewNode("reader-2", "Reader", RoleReader, "test-cluster")
	reader2.UpdateState(StateHealthy)
	reader2.APIAddress = "10.0.0.2:8000"
	registry.Register(reader2)

	localNode := NewNode("writer-1", "Writer", RoleWriter, "test-cluster")
	router := NewRouter(&RouterConfig{
		Strategy:  LoadBalanceLeastConnections,
		Registry:  registry,
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})

	// Simulate reader-1 having more active connections
	router.activeConnsMu.Lock()
	router.activeConns["reader-1"] = &atomic.Int64{}
	router.activeConns["reader-1"].Store(5)
	router.activeConns["reader-2"] = &atomic.Int64{}
	router.activeConns["reader-2"].Store(2)
	router.activeConnsMu.Unlock()

	// Get readers for selection testing
	readers := registry.GetReaders()

	// Select node - should choose reader-2 (fewer connections)
	node := router.selectNode(readers)
	if node.ID != "reader-2" {
		t.Errorf("Expected reader-2 (fewer connections), got: %s", node.ID)
	}
}

func TestRouterActiveConnectionTracking(t *testing.T) {
	registry := newTestRegistry()
	localNode := NewNode("reader-1", "Reader", RoleReader, "test-cluster")
	router := newTestRouter(registry, localNode)

	// Test increment/decrement
	router.incrementConns("node-1")
	if count := router.GetActiveConnections("node-1"); count != 1 {
		t.Errorf("Expected 1 connection, got: %d", count)
	}

	router.incrementConns("node-1")
	if count := router.GetActiveConnections("node-1"); count != 2 {
		t.Errorf("Expected 2 connections, got: %d", count)
	}

	router.decrementConns("node-1")
	if count := router.GetActiveConnections("node-1"); count != 1 {
		t.Errorf("Expected 1 connection, got: %d", count)
	}

	// Test unknown node
	if count := router.GetActiveConnections("unknown"); count != 0 {
		t.Errorf("Expected 0 connections for unknown node, got: %d", count)
	}
}

func TestRouterCanRouteLocally(t *testing.T) {
	registry := newTestRegistry()

	tests := []struct {
		name      string
		role      NodeRole
		isWrite   bool
		canHandle bool
	}{
		{"writer can handle write", RoleWriter, true, true},
		{"writer can handle query", RoleWriter, false, true},
		{"reader cannot handle write", RoleReader, true, false},
		{"reader can handle query", RoleReader, false, true},
		{"compactor cannot handle write", RoleCompactor, true, false},
		{"compactor cannot handle query", RoleCompactor, false, false},
		{"standalone can handle write", RoleStandalone, true, true},
		{"standalone can handle query", RoleStandalone, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localNode := NewNode("node-1", "Node", tt.role, "test-cluster")
			router := newTestRouter(registry, localNode)

			if got := router.CanRouteLocally(tt.isWrite); got != tt.canHandle {
				t.Errorf("CanRouteLocally(%v) = %v, want %v", tt.isWrite, got, tt.canHandle)
			}
		})
	}
}

func TestRouterStats(t *testing.T) {
	registry := newTestRegistry()
	localNode := NewNode("reader-1", "Reader", RoleReader, "test-cluster")
	router := NewRouter(&RouterConfig{
		Timeout:   10 * time.Second,
		Retries:   5,
		Strategy:  LoadBalanceLeastConnections,
		Registry:  registry,
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})

	// Add some connection data
	router.incrementConns("node-1")
	router.incrementConns("node-1")
	router.incrementConns("node-2")

	stats := router.Stats()

	if stats["strategy"] != LoadBalanceLeastConnections {
		t.Error("Strategy not in stats")
	}
	if stats["timeout_ms"] != int64(10000) {
		t.Error("Timeout not in stats")
	}
	if stats["retries"] != 5 {
		t.Error("Retries not in stats")
	}

	connStats := stats["active_connections"].(map[string]int64)
	if connStats["node-1"] != 2 {
		t.Errorf("node-1 connections = %d, want 2", connStats["node-1"])
	}
	if connStats["node-2"] != 1 {
		t.Errorf("node-2 connections = %d, want 1", connStats["node-2"])
	}
}

func TestRouterFallbackToWriterForQuery(t *testing.T) {
	// Create a test server to act as the writer node
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"results": []}`))
	}))
	defer server.Close()

	addr := strings.TrimPrefix(server.URL, "http://")

	registry := newTestRegistry()

	// Only register a writer (no readers)
	writer := NewNode("writer-1", "Writer", RoleWriter, "test-cluster")
	writer.UpdateState(StateHealthy)
	writer.APIAddress = addr
	registry.Register(writer)

	// Create router with a compactor as local node (cannot handle queries)
	localNode := NewNode("compactor-1", "Compactor", RoleCompactor, "test-cluster")
	router := newTestRouter(registry, localNode)

	req := httptest.NewRequest("GET", "/api/v1/query?sql=SELECT%20*%20FROM%20logs", nil)
	ctx := context.Background()

	// Should fall back to writer for query
	resp, err := router.RouteQuery(ctx, req)
	if err != nil {
		t.Fatalf("RouteQuery failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got: %d", resp.StatusCode)
	}
}

func TestRouterContextCancellation(t *testing.T) {
	// Create a slow server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	addr := strings.TrimPrefix(server.URL, "http://")

	registry := newTestRegistry()

	writer := NewNode("writer-1", "Writer", RoleWriter, "test-cluster")
	writer.UpdateState(StateHealthy)
	writer.APIAddress = addr
	registry.Register(writer)

	localNode := NewNode("reader-1", "Reader", RoleReader, "test-cluster")
	router := NewRouter(&RouterConfig{
		Timeout:   10 * time.Second, // Long timeout
		Retries:   0,                // No retries
		Strategy:  LoadBalanceRoundRobin,
		Registry:  registry,
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})

	req := httptest.NewRequest("POST", "/api/v1/write", nil)

	// Create context that will be cancelled quickly
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := router.RouteWrite(ctx, req)
	if err == nil {
		t.Error("Expected error due to context cancellation")
	}
}

func TestRouterQueryStringPreserved(t *testing.T) {
	var receivedURL string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedURL = r.URL.String()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	addr := strings.TrimPrefix(server.URL, "http://")

	registry := newTestRegistry()

	writer := NewNode("writer-1", "Writer", RoleWriter, "test-cluster")
	writer.UpdateState(StateHealthy)
	writer.APIAddress = addr
	registry.Register(writer)

	localNode := NewNode("reader-1", "Reader", RoleReader, "test-cluster")
	router := newTestRouter(registry, localNode)

	req := httptest.NewRequest("POST", "/api/v1/write?db=test&precision=ns", nil)
	ctx := context.Background()

	resp, err := router.RouteWrite(ctx, req)
	if err != nil {
		t.Fatalf("RouteWrite failed: %v", err)
	}
	resp.Body.Close()

	expectedURL := "/api/v1/write?db=test&precision=ns"
	if receivedURL != expectedURL {
		t.Errorf("Query string not preserved. Got: %s, want: %s", receivedURL, expectedURL)
	}
}
