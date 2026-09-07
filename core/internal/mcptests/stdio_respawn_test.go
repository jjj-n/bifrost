package mcptests

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/mcp"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readPID returns the pid recorded by the wrapper below, or 0 while the file
// has not been written yet.
func readPID(t *testing.T, pidFile string) int {
	t.Helper()
	raw, err := os.ReadFile(pidFile)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0
	}
	return pid
}

// TestSTDIOSubprocessKilled_CheckerRespawnsIt closes the one gap in this
// package's coverage of connection repair. A STDIO server's process exiting is
// the one transport death nothing observes: OnConnectionLost is registered for
// SSE only, and mcp-go owns the subprocess, so Bifrost holds no handle to
// notice it going away. The dead connection stays installed on the entry, so
// the periodic checker's live-connection branch is the only thing that can
// repair it, and until recently that branch never reconnected at all.
//
// TestHealthCheckSTDIOServerDropAndRecoverIn20Seconds simulates the drop with
// RemoveClient + AddClient, which exercises the manager rather than the
// failure. This kills the actual process.
//
// The wrapper records its own pid and then execs the server in place, so the
// pid in the file is the server process itself and a respawn is observable as
// a different pid rather than merely as a healthy state.
func TestSTDIOSubprocessKilled_CheckerRespawnsIt(t *testing.T) {
	InitMCPServerPaths(t)
	serverBin := mcpServerPaths.GoTestServer
	if _, err := os.Stat(serverBin); err != nil {
		t.Skipf("go-test-server binary not built (%v); run make setup-mcp-tests", err)
	}

	pidFile := filepath.Join(t.TempDir(), "server.pid")
	config := schemas.MCPClientConfig{
		ID:             "stdio-respawn-client",
		Name:           "StdioRespawnServer",
		ConnectionType: schemas.MCPConnectionTypeSTDIO,
		StdioConfig: &schemas.MCPStdioConfig{
			Command: "sh",
			Args:    []string{"-c", fmt.Sprintf("echo $$ > %s; exec %s", pidFile, serverBin)},
		},
		ToolsToExecute: []string{"*"},
	}

	manager := setupMCPManager(t, config)

	var originalPID int
	require.Eventually(t, func() bool {
		originalPID = readPID(t, pidFile)
		return originalPID > 0
	}, 15*time.Second, 50*time.Millisecond, "the STDIO server should have started and recorded its pid")

	clients := manager.GetClients()
	require.Len(t, clients, 1)
	require.Equal(t, schemas.MCPConnectionStateHealthy, clients[0].State, "sanity: connected before the kill")

	// Kill the server out from under Bifrost. Nothing in the process observes
	// this: there is no OnConnectionLost for STDIO and no exit handler.
	require.NoError(t, syscall.Kill(originalPID, syscall.SIGKILL))

	// A checker on a tight cadence, standing in for the ordinary periodic one
	// without waiting out its real interval.
	checker := mcp.NewClientConnectionChecker(manager, config.ID, 300*time.Millisecond, false, newTestLogger(t))
	checker.Start()
	defer checker.Stop()

	var respawnedPID int
	require.Eventually(t, func() bool {
		respawnedPID = readPID(t, pidFile)
		return respawnedPID > 0 && respawnedPID != originalPID
	}, 60*time.Second, 200*time.Millisecond, "the checker's reconnect should have spawned a replacement server process")

	require.Eventually(t, func() bool {
		for _, c := range manager.GetClients() {
			if c.ExecutionConfig.ID == config.ID {
				return c.State == schemas.MCPConnectionStateHealthy
			}
		}
		return false
	}, 30*time.Second, 200*time.Millisecond, "and the client should be healthy again on the replacement")

	assert.NoError(t, syscall.Kill(respawnedPID, 0), "the replacement process should be alive")
}
