package mcp

import (
	"context"
	"testing"
	"time"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// SSE: a dropped event stream is repaired by the periodic checker, not by
// anything SSE-specific.
// =============================================================================

// TestSSEStreamDropped_CheckerReconnects covers the SSE half of the same gap
// the STDIO respawn test covers in the integration suite. When an SSE stream
// dies, handleSSEConnectionLost records the failure and marks the client
// Unstable but leaves the dead connection installed, because nothing anywhere
// converts transport death into Conn == nil. The client therefore lands in the
// checker's live-connection branch, and that branch's reconnect is the only
// thing that can heal it.
//
// httptest's CloseClientConnections drops the established stream while the
// server keeps accepting new ones, which is exactly the recoverable shape: an
// idle timeout or a proxy hang-up rather than a server that has gone away.
func TestSSEStreamDropped_CheckerReconnects(t *testing.T) {
	s := server.NewMCPServer("test-sse-reconnect", "1.0.0", server.WithToolCapabilities(true))
	s.AddTool(
		mcpgo.NewTool("echo", mcpgo.WithDescription("Echo tool")),
		func(_ context.Context, _ mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
			return mcpgo.NewToolResultText("ok"), nil
		},
	)
	ts := server.NewTestServer(s)
	t.Cleanup(ts.Close)

	m := NewMCPManager(context.Background(), schemas.MCPConfig{}, nil, &MockLogger{}, nil)
	config := &schemas.MCPClientConfig{
		ID:               "client-sse-reconnect",
		Name:             "sse-client",
		AuthType:         schemas.MCPAuthTypeNone,
		ConnectionType:   schemas.MCPConnectionTypeSSE,
		ConnectionString: schemas.NewSecretVar(ts.URL + "/sse"),
		ToolsToExecute:   []string{"*"},
	}
	require.NoError(t, m.connectToMCPClient(context.Background(), config))
	// Registered after ts.Close above so it runs BEFORE it (t.Cleanup is LIFO):
	// a live SSE stream keeps httptest.Server.Close blocked indefinitely.
	t.Cleanup(func() {
		_ = m.RemoveClient(config.ID)
		ts.CloseClientConnections()
	})

	before, ok := snapshotClientState(m, config.ID)
	require.True(t, ok)
	require.NotNil(t, before.Conn)
	require.Equal(t, schemas.MCPConnectionStateHealthy, before.State)

	// Drop every established stream. The server keeps listening, so a fresh
	// dial still succeeds.
	ts.CloseClientConnections()

	// The library's loss callback fires asynchronously; either way the entry
	// keeps its connection, which is the point.
	require.Eventually(t, func() bool {
		st, exists := snapshotClientState(m, config.ID)
		return exists && st.Conn != nil
	}, 5*time.Second, 50*time.Millisecond, "a lost transport is never detached, so the live-connection branch owns the repair")

	checker := NewClientConnectionChecker(m, config.ID, time.Minute, false, &MockLogger{})
	checker.performCheck()

	require.Eventually(t, func() bool {
		after, exists := snapshotClientState(m, config.ID)
		return exists &&
			after.State == schemas.MCPConnectionStateHealthy &&
			after.ConnGeneration > before.ConnGeneration
	}, 30*time.Second, 100*time.Millisecond, "the checker's reconnect must re-establish the stream")

	after, ok := snapshotClientState(m, config.ID)
	require.True(t, ok)
	assert.NotSame(t, before.Conn, after.Conn, "the dead stream must be replaced, not re-probed")
	assert.Contains(t, after.ToolMap, config.Name+"-echo", "and rediscovery must repopulate the tool map")
}
