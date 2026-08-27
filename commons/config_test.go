package commons

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogPaths(t *testing.T) {
	config := NewDefaultConfig()
	config.DataRootPath = "/var/lib/irodsfs-pool"

	assert.Equal(t, "/var/lib/irodsfs-pool", config.GetLogRootPath())
	assert.Equal(t, "/var/lib/irodsfs-pool/irodsfs-pool.log", config.GetLogFilePath())

	config.LogRootPath = "/var/log/irodsfs-pool"
	assert.Equal(t, "/var/log/irodsfs-pool", config.GetLogRootPath())
	assert.Equal(t, filepath.Join(config.LogRootPath, "irodsfs-pool.log"), config.GetLogFilePath())
}

func TestParsePoolServiceEndpoint(t *testing.T) {
	tests := []struct {
		endpoint       string
		expectedScheme string
		expectedAddr   string
		expectError    bool
	}{
		{"tcp://localhost:1247", "tcp", "localhost:1247", false},
		{"unix:///tmp/socket", "unix", "/tmp/socket", false},
		{"localhost:1247", "tcp", "localhost:1247", false},
		{"127.0.0.1:1247", "tcp", "127.0.0.1:1247", false},
		{"tcp://:1247", "tcp", ":1247", false},
		{"unix:/tmp/socket", "unix", "/tmp/socket", false},
		{"invalid://localhost:1247", "", "", true},
		{"", "", "", true},
	}

	for _, test := range tests {
		scheme, addr, err := ParsePoolServiceEndpoint(test.endpoint)
		t.Logf("Testing endpoint: %s -> scheme %q, addr %q", test.endpoint, scheme, addr)
		if test.expectError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, test.expectedScheme, scheme)
			assert.Equal(t, test.expectedAddr, addr)
		}
	}
}
