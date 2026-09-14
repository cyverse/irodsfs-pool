package commons

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

type trackingWriteCloser struct {
	bytes.Buffer
	closed bool
}

func (writer *trackingWriteCloser) Close() error {
	writer.closed = true
	return nil
}

func TestNonClosingWriter(t *testing.T) {
	underlying := &trackingWriteCloser{}
	writer := NewMultiWriteCloser(nonClosingWriter{Writer: underlying})

	_, err := io.WriteString(writer, "message")
	assert.NoError(t, err)
	assert.NoError(t, writer.Close())
	assert.Equal(t, "message", underlying.String())
	assert.False(t, underlying.closed)
}

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

func TestParseManagementServiceEndpoint(t *testing.T) {
	tests := []struct {
		endpoint  string
		wantAddr  string
		wantError bool
	}{
		{"", "", false},
		{"http://0.0.0.0:12021", "0.0.0.0:12021", false},
		{"http://127.0.0.1:12021", "127.0.0.1:12021", false},
		{"http://[::1]:12021", "[::1]:12021", false},
		{"127.0.0.1:12021", "127.0.0.1:12021", false},
		{"[::1]:12021", "[::1]:12021", false},
		{"https://127.0.0.1:12021", "", true},
		{"http://127.0.0.1", "", true},
		{"http://127.0.0.1:http", "", true},
		{"http://127.0.0.1:0", "", true},
		{"http://127.0.0.1:12021/monitor", "", true},
		{"http://user@127.0.0.1:12021", "", true},
	}

	for _, test := range tests {
		addr, err := ParseManagementServiceEndpoint(test.endpoint)
		if test.wantError {
			assert.Error(t, err, "endpoint = %q", test.endpoint)
			continue
		}
		assert.NoError(t, err, "endpoint = %q", test.endpoint)
		assert.Equal(t, test.wantAddr, addr, "endpoint = %q", test.endpoint)
	}
}

func TestGetManagementServiceEndpoint(t *testing.T) {
	config := NewDefaultConfig()
	config.ManagementServiceEndpoint = "127.0.0.1:12021"

	assert.Equal(t, "http://127.0.0.1:12021", config.GetManagementServiceEndpoint())
}
