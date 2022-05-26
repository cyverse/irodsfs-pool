package commons

import (
	"fmt"

	"github.com/rs/xid"
	yaml "gopkg.in/yaml.v2"

	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
)

const (
	ServicePortDefault             int    = 12020
	DataCacheSizeMaxDefault        int64  = 1024 * 1024 * 1024 * 20 // 20GB
	DataCacheRootPathPrefixDefault string = "/tmp/irodsfs_pool_cache"
	LogFilePathPrefixDefault       string = "/tmp/irodsfs_pool"
	TempRootPathPrefixDefault      string = "/tmp/irodsfs_pool_temp"
	ProfileServicePortDefault      int    = 12021
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

// GetDefaultTempRootPath returns default temp root path
func GetDefaultTempRootPath() string {
	return fmt.Sprintf("%s_%s", TempRootPathPrefixDefault, getInstanceID())
}

// Config holds the parameters list which can be configured
type Config struct {
	ServicePort          int                           `yaml:"service_port"`
	DataCacheSizeMax     int64                         `yaml:"data_cache_size_max"`
	DataCacheRootPath    string                        `yaml:"data_cache_root_path"`
	TempRootPath         string                        `yaml:"temp_root_path"`
	CacheTimeoutSettings []MetadataCacheTimeoutSetting `yaml:"cache_timeout_settings,omitempty"`

	LogPath string `yaml:"log_path,omitempty"`

	Profile            bool `yaml:"profile,omitempty"`
	ProfileServicePort int  `yaml:"profile_service_port,omitempty"`

	Foreground   bool `yaml:"foreground,omitempty"`
	ChildProcess bool `yaml:"childprocess,omitempty"`

	InstanceID string `yaml:"instanceid,omitempty"`
}

// MetadataCacheTimeoutSetting defines cache timeout for path
type MetadataCacheTimeoutSetting struct {
	Path    string                        `yaml:"path" json:"path"`
	Timeout irodsfs_common_utils.Duration `yaml:"timeout" json:"timeout"`
	Inherit bool                          `yaml:"inherit,omitempty" json:"inherit,omitempty"`
}

// NewDefaultConfig creates DefaultConfig
func NewDefaultConfig() *Config {
	return &Config{
		ServicePort:          ServicePortDefault,
		DataCacheSizeMax:     DataCacheSizeMaxDefault,
		DataCacheRootPath:    GetDefaultDataCacheRootPath(),
		TempRootPath:         GetDefaultTempRootPath(),
		CacheTimeoutSettings: []MetadataCacheTimeoutSetting{},

		LogPath: "",

		Profile:            false,
		ProfileServicePort: ProfileServicePortDefault,

		Foreground:   false,
		ChildProcess: false,

		InstanceID: getInstanceID(),
	}
}

// NewConfigFromYAML creates Config from YAML
func NewConfigFromYAML(yamlBytes []byte) (*Config, error) {
	config := Config{
		ServicePort:          ServicePortDefault,
		DataCacheSizeMax:     DataCacheSizeMaxDefault,
		DataCacheRootPath:    GetDefaultDataCacheRootPath(),
		TempRootPath:         GetDefaultTempRootPath(),
		CacheTimeoutSettings: []MetadataCacheTimeoutSetting{},

		Profile:            false,
		ProfileServicePort: ProfileServicePortDefault,

		Foreground:   false,
		ChildProcess: false,

		InstanceID: getInstanceID(),
	}

	err := yaml.Unmarshal(yamlBytes, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML - %v", err)
	}

	return &config, nil
}

// Validate validates configuration
func (config *Config) Validate() error {
	if config.ServicePort <= 0 {
		return fmt.Errorf("service port must be given")
	}

	if config.Profile && config.ProfileServicePort <= 0 {
		return fmt.Errorf("profile service port must be given")
	}

	return nil
}
