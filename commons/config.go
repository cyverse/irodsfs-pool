package commons

import (
	"fmt"

	"github.com/rs/xid"
	yaml "gopkg.in/yaml.v2"
)

const (
	ServicePortDefault             int    = 12020
	BufferSizeMaxDefault           int64  = 1024 * 1024 * 64        // 64MB
	DataCacheSizeMaxDefault        int64  = 1024 * 1024 * 1024 * 20 // 20GB
	DataCacheRootPathPrefixDefault string = "/tmp/irodsfs_pool"
	LogFilePathPrefixDefault       string = "/tmp/irodsfs_pool"
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

// Config holds the parameters list which can be configured
type Config struct {
	ServicePort       int    `yaml:"service_port"`
	BufferSizeMax     int64  `yaml:"buffer_size_max"`
	DataCacheSizeMax  int64  `yaml:"data_cache_size_max"`
	DataCacheRootPath string `yaml:"data_cache_root_path"`

	LogPath string `yaml:"log_path,omitempty"`

	Profile            bool `yaml:"profile,omitempty"`
	ProfileServicePort int  `yaml:"profile_service_port,omitempty"`

	Foreground   bool `yaml:"foreground,omitempty"`
	ChildProcess bool `yaml:"childprocess,omitempty"`

	InstanceID string `yaml:"instanceid,omitempty"`
}

// NewDefaultConfig creates DefaultConfig
func NewDefaultConfig() *Config {
	return &Config{
		ServicePort:       ServicePortDefault,
		BufferSizeMax:     BufferSizeMaxDefault,
		DataCacheSizeMax:  DataCacheSizeMaxDefault,
		DataCacheRootPath: GetDefaultDataCacheRootPath(),

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
		ServicePort:       ServicePortDefault,
		BufferSizeMax:     BufferSizeMaxDefault,
		DataCacheSizeMax:  DataCacheSizeMaxDefault,
		DataCacheRootPath: GetDefaultDataCacheRootPath(),

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
