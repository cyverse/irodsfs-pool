package commons

import (
	"bytes"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/natefinch/lumberjack.v2"
)

type trackingWriteCloser struct {
	bytes.Buffer
	closed bool
}

func (writer *trackingWriteCloser) Close() error {
	writer.closed = true
	return nil
}

// newValidatableConfig returns a config that passes Validate, so that a test
// can change the one field it is about
func newValidatableConfig(t *testing.T) *Config {
	t.Helper()

	dataRootPath := t.TempDir()

	config := NewDefaultConfig()
	config.DataRootPath = dataRootPath
	config.StagingRootPath = filepath.Join(dataRootPath, "staging")
	config.PIDFile = filepath.Join(dataRootPath, "irodsfs-pool.pid")
	// a base64-encoded 32-byte key, the only shape the config accepts
	config.RecoveryEncryptionKey = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="

	return config
}

// log_root_path is documented as optional and falls back to the data root path,
// so a config that leaves it out has to validate
func TestValidateAcceptsUnsetLogRootPath(t *testing.T) {
	config := newValidatableConfig(t)

	require.Empty(t, config.LogRootPath)
	require.NoError(t, config.Validate())
	assert.Equal(t, config.DataRootPath, config.GetLogRootPath())
}

func TestValidateRejectsRelativeLogRootPath(t *testing.T) {
	config := newValidatableConfig(t)
	config.LogRootPath = "relative/logs"

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "log_root_path")
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

func TestLogMaxBackupsConfigAndWriters(t *testing.T) {
	config := newValidatableConfig(t)
	assert.Equal(t, LogMaxBackupsDefault, config.LogMaxBackups)

	parsed, err := NewConfigFromYAML(config, []byte("log_max_backups: 42\n"))
	require.NoError(t, err)
	require.NoError(t, parsed.Validate())
	assert.Equal(t, 42, parsed.LogMaxBackups)

	foreground := getLogWriterForForegroundProcess(parsed.GetLogFilePath(), parsed.LogMaxBackups)
	assert.Equal(t, 42, foreground.(*lumberjack.Logger).MaxBackups)
	daemon := getLogWriterForDaemonProcess(parsed.GetLogFilePath(), parsed.LogMaxBackups)
	assert.Equal(t, 42, daemon.(*lumberjack.Logger).MaxBackups)
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

func TestPackedDirectoriesDefaults(t *testing.T) {
	config := NewDefaultConfig()

	assert.Equal(t, PackedDirectoriesEnabledDefault, config.PackedDirectories.Enabled)
	assert.Equal(t, PackedDirectorySuffixDefault, config.PackedDirectories.Suffix)
	assert.Equal(t, PackedDirectoryCompressionDefault, config.PackedDirectories.Compression)
	assert.Equal(t, MaxPackedDirectorySizeDefault, config.PackedDirectories.MaxPackedDirSize)
	assert.Equal(t, PackedSnapshotIntervalDefault, time.Duration(config.PackedDirectories.SnapshotInterval))
	assert.Equal(t, ConcurrentPackLimitDefault, config.PackedDirectories.ConcurrentPackLimit)

	// The shipped list is an operator decision, so assert what must hold of it
	// rather than pinning the exact membership: every name is a bare directory
	// name, and none collides with the archive suffix.
	assert.NotEmpty(t, config.PackedDirectories.Names)
	for _, name := range config.PackedDirectories.Names {
		assert.NotContains(t, name, "/", "%q must be a base name", name)
		assert.NotEqual(t, ".", name)
		assert.NotEqual(t, "..", name)
		assert.False(t, strings.HasSuffix(name, config.PackedDirectories.Suffix),
			"%q must not end with the archive suffix", name)
	}

	// Whatever the list holds, the defaults must form a usable configuration.
	require.NoError(t, config.PackedDirectories.ToPackedFSConfig().Validate())
}

func TestPackedDirectoriesConfigFromYAML(t *testing.T) {
	yamlConfig := []byte(`
packed_directories:
  enabled: true
  names: [".venv", ".tox"]
  compression: zstd
  max_packed_dir_size: 1073741824
  snapshot_interval: 15m
  concurrent_pack_limit: 4
`)

	config := NewDefaultConfig()
	config, err := NewConfigFromYAML(config, yamlConfig)
	require.NoError(t, err)

	assert.True(t, config.PackedDirectories.Enabled)
	assert.Equal(t, []string{".venv", ".tox"}, config.PackedDirectories.Names)
	assert.Equal(t, "zstd", config.PackedDirectories.Compression)
	assert.Equal(t, int64(1073741824), config.PackedDirectories.MaxPackedDirSize)
	assert.Equal(t, 15*time.Minute, time.Duration(config.PackedDirectories.SnapshotInterval))

	packedConfig := config.PackedDirectories.ToPackedFSConfig()
	require.NoError(t, packedConfig.Validate())
	// The codec's extension joins the configured suffix.
	assert.Equal(t, "/p/.venv.packedfs.tar.zst", packedConfig.ArchivePath("/p/.venv"))
}

func TestValidateRejectsBadPackedDirectoriesConfig(t *testing.T) {
	config := NewDefaultConfig()
	config.LogRootPath = "/var/log/irodsfs-pool"
	config.RecoveryEncryptionKey = "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="
	config.PackedDirectories.Enabled = true
	config.PackedDirectories.Compression = "lz4"

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid packed_directories configuration")
}

// NewConfigFromYAML and NewConfigFromJSON used to unmarshal into a local copy
// and then return the untouched base config, so every parsed value was
// discarded. The file-based variants were always correct; these two were not.
func TestNewConfigFromBytesReturnsTheParsedConfig(t *testing.T) {
	t.Run("yaml", func(t *testing.T) {
		base := NewDefaultConfig()
		parsed, err := NewConfigFromYAML(base, []byte("data_root_path: /custom/root\n"))
		require.NoError(t, err)
		assert.Equal(t, "/custom/root", parsed.DataRootPath)
		assert.Equal(t, DataRootPathDefault, base.DataRootPath, "the base config is left alone")
	})

	t.Run("json", func(t *testing.T) {
		base := NewDefaultConfig()
		parsed, err := NewConfigFromJSON(base, []byte(`{"data_root_path":"/custom/root"}`))
		require.NoError(t, err)
		assert.Equal(t, "/custom/root", parsed.DataRootPath)
		assert.Equal(t, DataRootPathDefault, base.DataRootPath)
	})
}
