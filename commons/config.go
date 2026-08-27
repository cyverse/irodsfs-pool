package commons

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"gopkg.in/natefinch/lumberjack.v2"
	yaml "gopkg.in/yaml.v2"

	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
)

// Config holds the parameters list which can be configured
type Config struct {
	ServiceEndpoint string `yaml:"service_endpoint,omitempty" json:"service_endpoint,omitempty"`
	DataRootPath    string `yaml:"data_root_path,omitempty" json:"data_root_path,omitempty"`
	PIDFile         string `yaml:"pid_file,omitempty" json:"pid_file,omitempty"`

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

	MonitoringServicePort int `yaml:"monitoring_service_port,omitempty" json:"monitoring_service_port,omitempty"`

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
		StagingRootPath:                       path.Join(DataRootPathDefault, StagingRootPathDefault),
		MaxStagingDataSize:                    MaxStagingDataSizeDefault,
		MaxCacheFileSize:                      MaxCacheFileSizeDefault,
		StagingDataGracePeriod:                irodsclient_types.Duration(StagingDataGracePeriodDefault),
		SessionCloseGracePeriod:               irodsclient_types.Duration(SessionCloseGracePeriodDefault),
		OperationTimeout:                      irodsclient_types.Duration(OperationTimeoutDefault),

		MonitoringServicePort: MonitoringServicePortDefault,

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
		return nil, errors.Newf("unknown file format")
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
		return nil, errors.Wrapf(err, "failed to unmarshal yaml into config")
	}

	return config, nil
}

// NewConfigFromJSON creates Config from JSON
func NewConfigFromJSON(config *Config, jsonBytes []byte) (*Config, error) {
	cfg := Config{}
	if config != nil {
		cfg = *config
	}

	err := json.Unmarshal(jsonBytes, &cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal json into config")
	}

	return config, nil
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
	return path.Join(config.GetLogRootPath(), "irodsfs-pool.log")
}

func (config *Config) GetServiceEndpoint() string {
	if len(config.ServiceEndpoint) > 0 {
		return config.ServiceEndpoint
	}

	return fmt.Sprintf("unix://%s/comm.sock", config.DataRootPath)
}

func (config *Config) GetDataStagingRootPath() string {
	return path.Join(config.DataRootPath, "staging")
}

func (config *Config) GetDataRootPath() string {
	return config.DataRootPath
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
		return errors.Errorf("failed to create a dir with empty path")
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
		return errors.Errorf("a file %q exist, not a directory", path)
	}

	dirPerm := dirInfo.Mode().Perm()
	if dirPerm&0200 != 0200 {
		return errors.Errorf("a dir %q exist, but does not have the write permission", path)
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
			return errors.Errorf("unix socket directory %q must have write permission", parentDir)
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

	if len(config.DataRootPath) == 0 {
		return errors.Errorf("data root dir must be given")
	}

	if len(config.PIDFile) == 0 {
		return errors.Errorf("pid file path must be given")
	}

	return nil
}

// MultiWriteCloser writes to multiple writers and closes the ones that implement io.Closer.
type MultiWriteCloser struct {
	writers []io.Writer
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
		return NewMultiWriteCloser(os.Stderr, fileWriter), nil
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
		MaxBackups: 1000,
		MaxAge:     365, // 365 days
		Compress:   false,
	}
}
