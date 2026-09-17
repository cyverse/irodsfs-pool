package commons

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
	yaml "gopkg.in/yaml.v3"

	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/irods/packedfs"
)

// PackedDirectoriesConfig controls which directories are stored in iRODS as a
// single archive data object instead of as a collection.
//
// Directories such as .venv or .git hold tens of thousands of small files, and
// uploading them one at a time costs a round trip each. A packed directory is
// held on local staging disk for as long as a session uses it and crosses the
// wire only as one archive, so the per-file cost disappears. The trade is that
// the directory is not browsable in iRODS on its own, which suits directories
// that are not read directly there.
type PackedDirectoriesConfig struct {
	Enabled bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`

	// Names are directory base names that are packed, matched at any depth.
	Names []string `yaml:"names,omitempty" json:"names,omitempty"`

	// Suffix forms the data object name, so ".venv" becomes ".venv.mount.tar".
	// Compression appends its own extension on top of this.
	Suffix string `yaml:"suffix,omitempty" json:"suffix,omitempty"`

	// Compression is one of none, gzip or zstd. The default, none, keeps the
	// archive seekable and costs no CPU; the files in these directories are
	// usually already-compressed data, so a codec saves little.
	Compression string `yaml:"compression,omitempty" json:"compression,omitempty"`

	// MaxPackedDirSize caps the staging disk one packed directory may occupy.
	// A larger directory is refused rather than filling the staging area.
	MaxPackedDirSize int64 `yaml:"max_packed_dir_size,omitempty" json:"max_packed_dir_size,omitempty"`

	// SnapshotInterval bounds how much work a crash can lose, by uploading a
	// dirty directory this often even while the session stays open. Zero uses
	// the default; a negative value uploads at session release only.
	SnapshotInterval irodsclient_types.Duration `yaml:"snapshot_interval,omitempty" json:"snapshot_interval,omitempty"`

	// ConcurrentPackLimit caps how many directories are packed at once.
	ConcurrentPackLimit int `yaml:"concurrent_pack_limit,omitempty" json:"concurrent_pack_limit,omitempty"`
}

// ToPackedFSConfig converts to the form the common library consumes.
func (config *PackedDirectoriesConfig) ToPackedFSConfig() *packedfs.Config {
	packedConfig := &packedfs.Config{
		Enabled:             config.Enabled,
		Names:               config.Names,
		Suffix:              config.Suffix,
		Compression:         packedfs.Compression(config.Compression),
		MaxPackedDirSize:    config.MaxPackedDirSize,
		SnapshotInterval:    time.Duration(config.SnapshotInterval),
		ConcurrentPackLimit: config.ConcurrentPackLimit,
	}
	packedConfig.ApplyDefaults()
	return packedConfig
}

// Config holds the parameters list which can be configured
type Config struct {
	ServiceEndpoint string `yaml:"service_endpoint,omitempty" json:"service_endpoint,omitempty"`
	DataRootPath    string `yaml:"data_root_path,omitempty" json:"data_root_path,omitempty"`
	PIDFile         string `yaml:"pid_file,omitempty" json:"pid_file,omitempty"`

	RecoveryEncryptionKey string `yaml:"recovery_encryption_key,omitempty" json:"recovery_encryption_key,omitempty"`

	SessionTimeout                        irodsclient_types.Duration                   `yaml:"session_timeout,omitempty" json:"session_timeout,omitempty"`
	SessionTimeoutCheckInterval           irodsclient_types.Duration                   `yaml:"session_timeout_check_interval,omitempty" json:"session_timeout_check_interval,omitempty"`
	DataBlockSize                         int64                                        `yaml:"data_block_size,omitempty" json:"data_block_size,omitempty"`
	MaxDataMemCacheSize                   int64                                        `yaml:"max_data_mem_cache_size,omitempty" json:"max_data_mem_cache_size,omitempty"`
	MaxDataMemCacheBufferItems            int64                                        `yaml:"max_data_mem_cache_buffer_items,omitempty" json:"max_data_mem_cache_buffer_items,omitempty"`
	DataMemCacheTTL                       irodsclient_types.Duration                   `yaml:"data_mem_cache_ttl,omitempty" json:"data_mem_cache_ttl,omitempty"`
	MaxIOConnectionPerSession             int                                          `yaml:"max_io_connection_per_session,omitempty" json:"max_io_connection_per_session,omitempty"`
	MetadataCacheTimeoutSettings          []irodsclient_fs.MetadataCacheTimeoutSetting `yaml:"metadata_cache_timeout_settings,omitempty" json:"metadata_cache_timeout_settings,omitempty"`
	StartNewTransaction                   bool                                         `yaml:"start_new_transaction,omitempty" json:"start_new_transaction,omitempty"`
	MaxMetadataCacheEntriesPerSession     int64                                        `yaml:"max_metadata_cache_entries_per_session,omitempty" json:"max_metadata_cache_entries_per_session,omitempty"`
	MaxMetadataCacheSizePerSession        int64                                        `yaml:"max_metadata_cache_size_per_session,omitempty" json:"max_metadata_cache_size_per_session,omitempty"`
	MaxMetadataCacheBufferItemsPerSession int64                                        `yaml:"max_metadata_cache_buffer_items_per_session,omitempty" json:"max_metadata_cache_buffer_items_per_session,omitempty"`
	MetadataCacheTTL                      irodsclient_types.Duration                   `yaml:"metadata_cache_ttl,omitempty" json:"metadata_cache_ttl,omitempty"`
	StagingRootPath                       string                                       `yaml:"staging_root_path,omitempty" json:"staging_root_path,omitempty"`
	MaxStagingDataSize                    int64                                        `yaml:"max_staging_data_size,omitempty" json:"max_staging_data_size,omitempty"`
	MaxCacheFileSize                      int64                                        `yaml:"max_cache_file_size,omitempty" json:"max_cache_file_size,omitempty"`
	StagingDataGracePeriod                irodsclient_types.Duration                   `yaml:"staging_data_grace_period,omitempty" json:"staging_data_grace_period,omitempty"`
	SessionCloseGracePeriod               irodsclient_types.Duration                   `yaml:"session_close_grace_period,omitempty" json:"session_close_grace_period,omitempty"`
	OperationTimeout                      irodsclient_types.Duration                   `yaml:"operation_timeout,omitempty" json:"operation_timeout,omitempty"`

	PackedDirectories PackedDirectoriesConfig `yaml:"packed_directories,omitempty" json:"packed_directories,omitempty"`

	ManagementServiceEndpoint string `yaml:"management_service_endpoint,omitempty" json:"management_service_endpoint,omitempty"`

	Debug bool `yaml:"debug,omitempty" json:"debug,omitempty"`

	LogRootPath string `yaml:"log_root_path,omitempty" json:"log_root_path,omitempty"`
}

// NewDefaultConfig returns a default config
func NewDefaultConfig() *Config {
	return &Config{
		ServiceEndpoint: "",
		DataRootPath:    DataRootPathDefault,
		PIDFile:         PIDFilePathDefault,

		SessionTimeout:                        irodsclient_types.Duration(SessionTimeoutDefault),
		SessionTimeoutCheckInterval:           irodsclient_types.Duration(SessionTimeoutCheckIntervalDefault),
		DataBlockSize:                         DataBlockSizeDefault,
		MaxDataMemCacheSize:                   MaxDataMemCacheSizeDefault,
		MaxDataMemCacheBufferItems:            MaxDataMemCacheBufferItemsDefault,
		DataMemCacheTTL:                       irodsclient_types.Duration(DataMemCacheTTLDefault),
		MaxIOConnectionPerSession:             MaxIOConnectionPerSessionDefault,
		MetadataCacheTimeoutSettings:          []irodsclient_fs.MetadataCacheTimeoutSetting{},
		StartNewTransaction:                   StartNewTransactionDefault,
		MaxMetadataCacheEntriesPerSession:     MaxMetadataCacheEntriesPerSessionDefault,
		MaxMetadataCacheSizePerSession:        MaxMetadataCacheSizePerSessionDefault,
		MaxMetadataCacheBufferItemsPerSession: MaxMetadataCacheBufferItemsPerSessionDefault,
		MetadataCacheTTL:                      irodsclient_types.Duration(MetadataCacheTTLDefault),
		StagingRootPath:                       filepath.Join(DataRootPathDefault, StagingRootPathDefault),
		MaxStagingDataSize:                    MaxStagingDataSizeDefault,
		MaxCacheFileSize:                      MaxCacheFileSizeDefault,
		StagingDataGracePeriod:                irodsclient_types.Duration(StagingDataGracePeriodDefault),
		SessionCloseGracePeriod:               irodsclient_types.Duration(SessionCloseGracePeriodDefault),
		OperationTimeout:                      irodsclient_types.Duration(OperationTimeoutDefault),

		PackedDirectories: PackedDirectoriesConfig{
			Enabled: PackedDirectoriesEnabledDefault,
			// Tool directories that are rebuilt rather than read: an editor or
			// agent workspace, a virtualenv, a package cache. Removing a name
			// here is how a deployment keeps that directory a normal iRODS
			// collection.
			Names: []string{
				// git
				".git",
				// venv
				".venv",
				// Ai tools
				".claude", ".codex", ".copilot",
				// development
				".ansible", ".cache", ".docker", ".vscode", ".vscode-shared",
				// Jupyter
				".ipynb_checkpoints",
				// JupyterLab LSP
				".virtual_documents",
				// mypy, ruff
				".mypy_cache",
				".pytest_cache",
				".ruff_cache",
				".tox", ".nox",
				// pixi
				".pixi",
				// conda
				".conda",
				// RStudio
				".Rproj.user",
				// Spyder
				".spyproject",
				// Julia
				".julia",
				// DVC
				".dvc",
				// Snakemake
				".nextflow",
				// DataLad
				".datalad",
				// Hydra
				".hydra",
				// Metaflow
				".metaflow",
				// development tools
				".idea", ".vs", ".metadata", ".history", ".gradle", ".m2", ".cargo", ".rustup", ".npm", ".yarn", ".pnpm-store", ".terraform", ".vagrant", ".next", ".nuxt", ".svelte-kit", ".turbo", ".parcel-cache", ".svn", ".hg", ".jj",
				// HPC
				".local", ".apptainer", ".singularity", ".spack", ".lmod.d",
				// others
				"node_modules", "__pycache__", "mlruns", "wandb", "lightning_logs", "catboost_info", "site-packages",
			},
			Suffix:              PackedDirectorySuffixDefault,
			Compression:         PackedDirectoryCompressionDefault,
			MaxPackedDirSize:    MaxPackedDirectorySizeDefault,
			SnapshotInterval:    irodsclient_types.Duration(PackedSnapshotIntervalDefault),
			ConcurrentPackLimit: ConcurrentPackLimitDefault,
		},

		ManagementServiceEndpoint: ManagementServiceEndpointDefault,

		Debug: false,

		LogRootPath: "", // use default
	}
}

// NewConfigFromFile creates Config from file
func NewConfigFromFile(config *Config, filePath string) (*Config, error) {
	st, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, err
		}

		return nil, errors.Wrapf(err, "failed to stat file %q", filePath)
	}

	if st.IsDir() {
		return nil, errors.Newf("configuration must be a file %q", filePath)
	}

	dataBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read file %q", filePath)
	}

	format := DetectFormat(dataBytes)
	switch format {
	case FormatJSON:
		return NewConfigFromJSONFile(config, filePath)
	case FormatYAML:
		return NewConfigFromYAMLFile(config, filePath)
	default:
		return nil, errors.New("unknown file format")
	}
}

// NewConfigFromYAMLFile creates Config from YAML
func NewConfigFromYAMLFile(config *Config, yamlPath string) (*Config, error) {
	cfg := Config{}
	if config != nil {
		cfg = *config
	}

	yamlBytes, err := os.ReadFile(yamlPath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read YAML file %q", yamlPath)
	}

	err = yaml.Unmarshal(yamlBytes, &cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal YAML file %q to config", yamlPath)
	}

	return &cfg, nil
}

// NewConfigFromJSONFile creates Config from JSON
func NewConfigFromJSONFile(config *Config, jsonPath string) (*Config, error) {
	cfg := Config{}
	if config != nil {
		cfg = *config
	}

	jsonBytes, err := os.ReadFile(jsonPath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read JSON file %q", jsonPath)
	}

	err = json.Unmarshal(jsonBytes, &cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal JSON file %q to config", jsonPath)
	}

	return &cfg, nil
}

// NewConfigFromYAML creates Config from YAML
func NewConfigFromYAML(config *Config, yamlBytes []byte) (*Config, error) {
	cfg := Config{}
	if config != nil {
		cfg = *config
	}

	err := yaml.Unmarshal(yamlBytes, &cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal yaml into config")
	}

	return &cfg, nil
}

// NewConfigFromJSON creates Config from JSON
func NewConfigFromJSON(config *Config, jsonBytes []byte) (*Config, error) {
	cfg := Config{}
	if config != nil {
		cfg = *config
	}

	err := json.Unmarshal(jsonBytes, &cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal json into config")
	}

	return &cfg, nil
}

// GetLogRootPath returns the directory containing service and session logs.
func (config *Config) GetLogRootPath() string {
	if len(config.LogRootPath) > 0 {
		return config.LogRootPath
	}

	// default
	return config.DataRootPath
}

// GetLogFilePath returns the service log file path.
func (config *Config) GetLogFilePath() string {
	return filepath.Join(config.GetLogRootPath(), "irodsfs-pool.log")
}

func (config *Config) GetServiceEndpoint() string {
	if len(config.ServiceEndpoint) > 0 {
		return config.ServiceEndpoint
	}

	return fmt.Sprintf("unix://%s/comm.sock", config.DataRootPath)
}

// GetManagementServiceEndpoint returns the HTTP endpoint for the management service.
// An empty endpoint disables the service.
func (config *Config) GetManagementServiceEndpoint() string {
	endpoint := strings.TrimSpace(config.ManagementServiceEndpoint)
	if endpoint != "" && !strings.Contains(endpoint, "://") {
		return "http://" + endpoint
	}
	return endpoint
}

func (config *Config) GetDataStagingRootPath() string {
	return filepath.Join(config.DataRootPath, "staging")
}

func (config *Config) GetDataRootPath() string {
	return config.DataRootPath
}

func (config *Config) GetRecoveryEncryptionKey() ([]byte, error) {
	encodedKey := strings.TrimSpace(config.RecoveryEncryptionKey)
	if encodedKey == "" {
		return nil, errors.New("recovery encryption key must be given")
	}
	key, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil {
		return nil, errors.Wrap(err, "recovery encryption key must be valid base64")
	}
	if len(key) != 32 {
		return nil, errors.Newf("recovery encryption key must decode to exactly 32 bytes, got %d", len(key))
	}
	return key, nil
}

// MakeLogDir makes a log dir required
func (config *Config) MakeLogDir() error {
	return config.makeDir(config.GetLogRootPath())
}

// MakeWorkDirs makes dirs required
func (config *Config) MakeWorkDirs() error {
	dataRootPath := config.GetDataRootPath()
	err := config.makeDir(dataRootPath)
	if err != nil {
		return err
	}

	dataStagingRootPath := config.GetDataStagingRootPath()
	err = config.makeDir(dataStagingRootPath)
	if err != nil {
		return err
	}

	scheme, endpoint, err := ParsePoolServiceEndpoint(config.GetServiceEndpoint())
	if err != nil {
		return err
	}

	if scheme == "unix" {
		err = config.makeUnixSocketDir(endpoint)
		if err != nil {
			return err
		}
	}

	return nil
}

// makeDir makes a dir for use
func (config *Config) makeDir(path string) error {
	if len(path) == 0 {
		return errors.New("failed to create a dir with empty path")
	}

	dirInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			// make
			mkdirErr := os.MkdirAll(path, 0775)
			if mkdirErr != nil {
				return errors.Wrapf(mkdirErr, "making a dir %q error", path)
			}

			return nil
		}

		return errors.Wrapf(err, "stating a dir %q error", path)
	}

	if !dirInfo.IsDir() {
		return errors.Newf("a file %q exist, not a directory", path)
	}

	dirPerm := dirInfo.Mode().Perm()
	if dirPerm&0200 != 0200 {
		return errors.Newf("a dir %q exist, but does not have the write permission", path)
	}

	return nil
}

// makeUnixSocketDir makes unix socket dir
func (config *Config) makeUnixSocketDir(endpoint string) error {
	// endpoint is a file
	_, err := os.Stat(endpoint)
	if err != nil {
		if !os.IsNotExist(err) {
			return errors.Wrapf(err, "service unix socket file %q error", endpoint)
		}
	} else {
		// file exists
		// remove
		err2 := os.Remove(endpoint)
		if err2 != nil {
			return errors.Wrapf(err2, "failed to remove the existing unix socket file %q", endpoint)
		}
	}

	parentDir := filepath.Dir(endpoint)
	unixSocketDirInfo, err := os.Stat(parentDir)
	if err != nil {
		if os.IsNotExist(err) {
			err2 := os.MkdirAll(parentDir, os.FileMode(0777))
			if err2 != nil {
				return errors.Wrapf(err2, "failed to make a directory for unix socket %q", parentDir)
			}
			// ok - fall
		} else {
			return errors.Wrapf(err, "unix socket directory %q error", parentDir)
		}
	} else {
		unixSocketDirPerm := unixSocketDirInfo.Mode().Perm()
		if unixSocketDirPerm&0200 != 0200 {
			return errors.Newf("unix socket directory %q must have write permission", parentDir)
		}
		// ok - fall
	}

	return nil
}

// Validate validates configuration
func (config *Config) Validate() error {
	_, _, err := ParsePoolServiceEndpoint(config.GetServiceEndpoint())
	if err != nil {
		return err
	}

	if _, err := ParseManagementServiceEndpoint(config.GetManagementServiceEndpoint()); err != nil {
		return err
	}

	paths := map[string]string{
		"data_root_path": config.DataRootPath,
		"pid_file":       config.PIDFile,
		// an unset log root path falls back to the data root path, so the
		// effective path is what has to be absolute, not the field
		"log_root_path":     config.GetLogRootPath(),
		"staging_root_path": config.StagingRootPath,
	}

	for name, path := range paths {
		if !filepath.IsAbs(path) {
			return errors.Newf("%s %q must be an absolute path", name, path)
		}
	}

	if len(config.DataRootPath) == 0 {
		return errors.New("data root dir must be given")
	}

	if len(config.PIDFile) == 0 {
		return errors.New("pid file path must be given")
	}

	if _, err := config.GetRecoveryEncryptionKey(); err != nil {
		return err
	}

	// Validate the packed directory settings here so a bad name or codec fails
	// at startup rather than at the first access to such a directory.
	if err := config.PackedDirectories.ToPackedFSConfig().Validate(); err != nil {
		return errors.Wrap(err, "invalid packed_directories configuration")
	}

	return nil
}

// MultiWriteCloser writes to multiple writers and closes the ones that implement io.Closer.
type MultiWriteCloser struct {
	writers []io.Writer
}

// nonClosingWriter prevents MultiWriteCloser from closing a writer it does not own.
// In particular, foreground logging must never close os.Stderr.
type nonClosingWriter struct {
	io.Writer
}

func NewMultiWriteCloser(writers ...io.Writer) *MultiWriteCloser {
	return &MultiWriteCloser{writers: writers}
}

func (mw *MultiWriteCloser) Write(p []byte) (n int, err error) {
	for _, w := range mw.writers {
		n, err = w.Write(p)
		if err != nil {
			return n, err
		}
	}
	return len(p), nil
}

func (mw *MultiWriteCloser) Close() error {
	var firstErr error
	for _, w := range mw.writers {
		if closer, ok := w.(io.Closer); ok {
			if err := closer.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (config *Config) GetLogWriter(foregroundProcess bool) (io.WriteCloser, error) {
	logFilePath := config.GetLogFilePath()
	if logFilePath == "-" || len(logFilePath) == 0 {
		return os.Stderr, nil
	}

	err := config.MakeLogDir()
	if err != nil {
		return nil, err
	}

	if foregroundProcess {
		fileWriter := getLogWriterForForegroundProcess(logFilePath)
		return NewMultiWriteCloser(nonClosingWriter{Writer: os.Stderr}, fileWriter), nil
	}

	daemonWriter := getLogWriterForDaemonProcess(logFilePath)
	return daemonWriter, nil
}

func getLogWriterForForegroundProcess(logPath string) io.WriteCloser {
	logFilePath := fmt.Sprintf("%s.fg", logPath)
	return &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    50, // 50MB
		MaxBackups: 5,
		MaxAge:     30, // 30 days
		Compress:   false,
	}
}

func getLogWriterForDaemonProcess(logPath string) io.WriteCloser {
	logFilePath := fmt.Sprintf("%s", logPath)
	return &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    50, // 50MB
		MaxBackups: 10,
		MaxAge:     365, // 365 days
		Compress:   false,
	}
}
