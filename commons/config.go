package commons

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"
	yaml "gopkg.in/yaml.v2"

	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	log "github.com/sirupsen/logrus"
)

// GetDefaultDataRootDirPath returns default data root path
func GetDefaultDataRootDirPath() string {
	dirPath, err := os.Getwd()
	if err != nil {
		return DataRootPathFallback
	}
	return dirPath
}

// Config holds the parameters list which can be configured
type Config struct {
	ServiceEndpoint string `yaml:"service_endpoint,omitempty" json:"service_endpoint,omitempty"`
	DataRootPath    string `yaml:"data_root_path,omitempty" json:"data_root_path,omitempty"`

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
	StagingDataGracePeriod                irodsclient_types.Duration                   `yaml:"staging_data_grace_period,omitempty" json:"staging_data_grace_period,omitempty"`
	OperationTimeout                      irodsclient_types.Duration                   `yaml:"operation_timeout,omitempty" json:"operation_timeout,omitempty"`

	MonitoringServicePort int `yaml:"monitoring_service_port,omitempty" json:"monitoring_service_port,omitempty"`

	Foreground bool `yaml:"foreground,omitempty" json:"foreground,omitempty"`
	Debug      bool `yaml:"debug,omitempty" json:"debug,omitempty"`

	LogPath string `yaml:"log_path,omitempty" json:"log_path,omitempty"`
}

// NewDefaultConfig returns a default config
func NewDefaultConfig() *Config {
	return &Config{
		ServiceEndpoint: "",
		DataRootPath:    GetDefaultDataRootDirPath(),

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
		StagingRootPath:                       path.Join(GetDefaultDataRootDirPath(), StagingRootPathDefault),
		StagingDataGracePeriod:                irodsclient_types.Duration(StagingDataGracePeriodDefault),
		OperationTimeout:                      irodsclient_types.Duration(OperationTimeoutDefault),

		MonitoringServicePort: MonitoringServicePortDefault,

		Foreground: false,
		Debug:      false,

		LogPath: "", // use default
	}
}

// NewConfigFromYAML creates Config from YAML
func NewConfigFromYAML(yamlBytes []byte) (*Config, error) {
	config := NewDefaultConfig()

	err := yaml.Unmarshal(yamlBytes, config)
	if err != nil {
		return nil, errors.Errorf("failed to unmarshal yaml into config: %w", err)
	}

	return config, nil
}

// NewConfigFromJSON creates Config from JSON
func NewConfigFromJSON(jsonBytes []byte) (*Config, error) {
	config := NewDefaultConfig()

	err := json.Unmarshal(jsonBytes, config)
	if err != nil {
		return nil, errors.Errorf("failed to unmarshal json into config: %w", err)
	}

	return config, nil
}

// GetLogFilePath returns log file path
func (config *Config) GetLogFilePath() string {
	if len(config.LogPath) > 0 {
		return config.LogPath
	}

	// default
	return path.Join(config.DataRootPath, "irodsfs-pool.log")
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
	logger := log.WithFields(log.Fields{
		"package":  "commons",
		"object":   "Config",
		"function": "MakeLogDir",
	})

	logFilePath := config.GetLogFilePath()
	logDirPath := filepath.Dir(logFilePath)

	logger.Debugf("making log dir %q", logDirPath)
	err := config.makeDir(logDirPath)
	if err != nil {
		return err
	}

	return nil
}

// MakeWorkDirs makes dirs required
func (config *Config) MakeWorkDirs() error {
	logger := log.WithFields(log.Fields{
		"package":  "commons",
		"object":   "Config",
		"function": "MakeWorkDirs",
	})

	dataRootPath := config.GetDataRootPath()
	logger.Debugf("making data root %q", dataRootPath)
	err := config.makeDir(dataRootPath)
	if err != nil {
		return err
	}

	dataStagingRootPath := config.GetDataStagingRootPath()
	logger.Debugf("making data staging root %q", dataStagingRootPath)
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

// CleanSocketFile
func (config *Config) CleanSocketFile() error {
	scheme, endpoint, err := ParsePoolServiceEndpoint(config.GetServiceEndpoint())
	if err != nil {
		return err
	}

	if scheme == "unix" {
		err = config.removeUnixSocketFile(endpoint)
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
				return errors.Errorf("making a dir %q error: %w", path, mkdirErr)
			}

			return nil
		}

		return errors.Errorf("stating a dir %q error: %w", path, err)
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
			return errors.Errorf("service unix socket file %q error: %w", endpoint, err)
		}
	} else {
		// file exists
		// remove
		err2 := os.Remove(endpoint)
		if err2 != nil {
			return errors.Errorf("failed to remove the existing unix socket file %q: %w", endpoint, err2)
		}
	}

	parentDir := filepath.Dir(endpoint)
	unixSocketDirInfo, err := os.Stat(parentDir)
	if err != nil {
		if os.IsNotExist(err) {
			err2 := os.MkdirAll(parentDir, os.FileMode(0777))
			if err2 != nil {
				return errors.Errorf("failed to make a directory for unix socket %q: %w", parentDir, err2)
			}
			// ok - fall
		} else {
			return errors.Errorf("unix socket directory %q error: %w", parentDir, err)
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

// removeUnixSocketFile removes unix socket file
func (config *Config) removeUnixSocketFile(endpoint string) error {
	if len(endpoint) == 0 {
		return nil
	}

	err := os.Remove(endpoint)
	if err != nil {
		return errors.Errorf("failed to remove unix socket file %q: %w", endpoint, err)
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
	logFilePath := fmt.Sprintf("%s.daemon", logPath)
	return &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    50, // 50MB
		MaxBackups: 1000,
		MaxAge:     365, // 365 days
		Compress:   false,
	}
}

func parseRawURL(rawurl string) (string, string, string, error) {
	if len(strings.TrimSpace(rawurl)) == 0 {
		return "", "", "", errors.Errorf("empty raw url")
	}

	u, err := url.ParseRequestURI(rawurl)
	if err != nil || (u.Host == "" && u.Path == "") {
		// try adding //
		u, repErr := url.ParseRequestURI("tcp://" + rawurl)
		if repErr != nil {
			return "", "", "", errors.Errorf("could not parse raw url: %s, error: %w", rawurl, err)
		}

		return "tcp", u.Host, "", nil
	}

	if u != nil {
		scheme := strings.ToLower(u.Scheme)
		if scheme == "unix" {
			return "unix", "", u.Path, nil
		} else if scheme == "tcp" {
			return "tcp", u.Host, "", nil
		}

		return u.Scheme, u.Host, u.Path, nil
	}

	return "", "", "", errors.Errorf("could not parse raw url: %s", rawurl)
}

// ParsePoolServiceEndpoint parses endpoint string
func ParsePoolServiceEndpoint(endpoint string) (string, string, error) {
	scheme, host, p, err := parseRawURL(endpoint)
	if err != nil {
		return "", "", err
	}

	scheme = strings.ToLower(scheme)
	switch scheme {
	case "tcp":
		return "tcp", host, nil
	case "unix":
		p = path.Join("/", strings.TrimPrefix(p, "/"))
		return "unix", p, nil
	case "":
		if len(host) > 0 {
			return "tcp", host, nil
		}
		return "", "", errors.Errorf("unknown host: %q", host)
	default:
		return "", "", errors.Errorf("unsupported protocol: %q", scheme)
	}
}
