package commons

import (
	"fmt"
	"time"

	"github.com/rs/xid"
	yaml "gopkg.in/yaml.v2"
)

const (
	ServicePortDefault             int           = 12020
	BufferSizeMaxDefault           int64         = 1024 * 1024 * 64        // 64MB
	DataCacheSizeMaxDefault        int64         = 1024 * 1024 * 1024 * 20 // 20GB
	DataCacheRootPathPrefixDefault string        = "/tmp/irodsfs_pool"
	DataCacheTimeoutDefault        time.Duration = 3 * time.Minute // 3min
	DataCacheCleanupTimeDefault    time.Duration = 3 * time.Minute // 3min
	LogFilePathPrefixDefault       string        = "/tmp/irodsfs_pool"
)

var (
	instanceID string
)

// getInstanceID returns instance ID
func getInstanceID() string {
	if len(instanceID) == 0 {
		instanceID = xid.New().String()
	}

	return instanceID
}

// GetDefaultLogFilePath returns default log file path
func GetDefaultLogFilePath() string {
	return fmt.Sprintf("%s_%s.log", LogFilePathPrefixDefault, getInstanceID())
}

// GetDefaultDataCacheRootPath returns default data cache root path
func GetDefaultDataCacheRootPath() string {
	return fmt.Sprintf("%s_%s", DataCacheRootPathPrefixDefault, getInstanceID())
}

// Config holds the parameters list which can be configured
type Config struct {
	ServicePort          int           `yaml:"service_port"`
	BufferSizeMax        int64         `yaml:"buffer_size_max"`
	DataCacheSizeMax     int64         `yaml:"data_cache_size_max"`
	DataCacheRootPath    string        `yaml:"data_cache_root_path"`
	DataCacheTimeout     time.Duration `yaml:"data_cache_timeout"`
	DataCacheCleanupTime time.Duration `yaml:"data_cache_cleanup_time"`

	LogPath string `yaml:"log_path,omitempty"`

	Foreground   bool `yaml:"foreground,omitempty"`
	ChildProcess bool `yaml:"childprocess,omitempty"`

	InstanceID string `yaml:"instanceid,omitempty"`
}

type configAlias struct {
	ServicePort          int    `yaml:"service_port"`
	BufferSizeMax        int64  `yaml:"buffer_size_max"`
	DataCacheSizeMax     int64  `yaml:"data_cache_size_max"`
	DataCacheRootPath    string `yaml:"data_cache_root_path"`
	DataCacheTimeout     string `yaml:"data_cache_timeout"`
	DataCacheCleanupTime string `yaml:"data_cache_cleanup_time"`

	LogPath string `yaml:"log_path,omitempty"`

	Foreground   bool `yaml:"foreground,omitempty"`
	ChildProcess bool `yaml:"childprocess,omitempty"`

	InstanceID string `yaml:"instanceid,omitempty"`
}

// NewDefaultConfig creates DefaultConfig
func NewDefaultConfig() *Config {
	return &Config{
		ServicePort:          ServicePortDefault,
		BufferSizeMax:        BufferSizeMaxDefault,
		DataCacheSizeMax:     DataCacheSizeMaxDefault,
		DataCacheRootPath:    GetDefaultDataCacheRootPath(),
		DataCacheTimeout:     DataCacheTimeoutDefault,
		DataCacheCleanupTime: DataCacheCleanupTimeDefault,

		LogPath: "",

		Foreground:   false,
		ChildProcess: false,

		InstanceID: getInstanceID(),
	}
}

// NewConfigFromYAML creates Config from YAML
func NewConfigFromYAML(yamlBytes []byte) (*Config, error) {

	alias := configAlias{
		ServicePort:       ServicePortDefault,
		BufferSizeMax:     BufferSizeMaxDefault,
		DataCacheSizeMax:  DataCacheSizeMaxDefault,
		DataCacheRootPath: GetDefaultDataCacheRootPath(),

		Foreground:   false,
		ChildProcess: false,

		InstanceID: getInstanceID(),
	}

	err := yaml.Unmarshal(yamlBytes, &alias)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML - %v", err)
	}

	var dataCacheTimeout time.Duration
	if len(alias.DataCacheTimeout) > 0 {
		dataCacheTimeout, err = time.ParseDuration(alias.DataCacheTimeout)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal YAML - %v", err)
		}
	} else {
		dataCacheTimeout = DataCacheTimeoutDefault
	}

	var dataCacheCleanupTime time.Duration
	if len(alias.DataCacheCleanupTime) > 0 {
		dataCacheCleanupTime, err = time.ParseDuration(alias.DataCacheCleanupTime)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal YAML - %v", err)
		}
	} else {
		dataCacheCleanupTime = DataCacheCleanupTimeDefault
	}

	return &Config{
		ServicePort:          alias.ServicePort,
		BufferSizeMax:        alias.BufferSizeMax,
		DataCacheSizeMax:     alias.BufferSizeMax,
		DataCacheRootPath:    alias.DataCacheRootPath,
		DataCacheTimeout:     dataCacheTimeout,
		DataCacheCleanupTime: dataCacheCleanupTime,

		LogPath: alias.LogPath,

		Foreground:   alias.Foreground,
		ChildProcess: alias.ChildProcess,

		InstanceID: alias.InstanceID,
	}, nil
}

// Validate validates configuration
func (config *Config) Validate() error {
	if config.ServicePort <= 0 {
		return fmt.Errorf("Service port must be given")
	}

	return nil
}
