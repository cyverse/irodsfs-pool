package commons

import (
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/rs/xid"
	yaml "gopkg.in/yaml.v2"

	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
)

const (
	DataCacheSizeMaxDefault        int64  = 1024 * 1024 * 1024 * 20 // 20GB
	DataCacheRootPathPrefixDefault string = "/tmp/irodsfs_pool_cache"
	LogFilePathPrefixDefault       string = "/tmp/irodsfs_pool"
	TempRootPathPrefixDefault      string = "/tmp/irodsfs_pool_temp"
	ServiceEndpointDefault         string = "unix:///tmp/irodsfs_pool_comm.sock"
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

// MetadataCacheTimeoutSetting defines cache timeout for path
type MetadataCacheTimeoutSetting struct {
	Path    string                        `yaml:"path" json:"path"`
	Timeout irodsfs_common_utils.Duration `yaml:"timeout" json:"timeout"`
	Inherit bool                          `yaml:"inherit,omitempty" json:"inherit,omitempty"`
}

// Config holds the parameters list which can be configured
type Config struct {
	ServiceEndpoint      string                        `yaml:"service_endpoint"`
	DataCacheSizeMax     int64                         `yaml:"data_cache_size_max"`
	DataCacheRootPath    string                        `yaml:"data_cache_root_path"`
	TempRootPath         string                        `yaml:"temp_root_path"`
	CacheTimeoutSettings []MetadataCacheTimeoutSetting `yaml:"cache_timeout_settings,omitempty"`

	LogPath string `yaml:"log_path,omitempty"`

	Profile            bool `yaml:"profile,omitempty"`
	ProfileServicePort int  `yaml:"profile_service_port,omitempty"`

	Foreground   bool `yaml:"foreground,omitempty"`
	Debug        bool `yaml:"debug,omitempty"`
	ChildProcess bool `yaml:"childprocess,omitempty"`

	InstanceID string `yaml:"instanceid,omitempty"`
}

// NewDefaultConfig creates DefaultConfig
func NewDefaultConfig() *Config {
	return &Config{
		ServiceEndpoint:      ServiceEndpointDefault,
		DataCacheSizeMax:     DataCacheSizeMaxDefault,
		DataCacheRootPath:    GetDefaultDataCacheRootPath(),
		TempRootPath:         GetDefaultTempRootPath(),
		CacheTimeoutSettings: []MetadataCacheTimeoutSetting{},

		LogPath: "",

		Profile:            false,
		ProfileServicePort: ProfileServicePortDefault,

		Foreground:   false,
		Debug:        false,
		ChildProcess: false,

		InstanceID: getInstanceID(),
	}
}

// NewConfigFromYAML creates Config from YAML
func NewConfigFromYAML(yamlBytes []byte) (*Config, error) {
	config := Config{
		ServiceEndpoint:      ServiceEndpointDefault,
		DataCacheSizeMax:     DataCacheSizeMaxDefault,
		DataCacheRootPath:    GetDefaultDataCacheRootPath(),
		TempRootPath:         GetDefaultTempRootPath(),
		CacheTimeoutSettings: []MetadataCacheTimeoutSetting{},

		Profile:            false,
		ProfileServicePort: ProfileServicePortDefault,

		Foreground:   false,
		Debug:        false,
		ChildProcess: false,

		InstanceID: getInstanceID(),
	}

	err := yaml.Unmarshal(yamlBytes, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML - %v", err)
	}

	return &config, nil
}

// MakeTempRootDir makes temp root dir
func (config *Config) MakeTempRootDir() error {
	if len(config.TempRootPath) == 0 {
		return nil
	}

	tempDirInfo, err := os.Stat(config.TempRootPath)
	if err != nil {
		if os.IsNotExist(err) {
			// make
			mkdirErr := os.MkdirAll(config.TempRootPath, 0775)
			if mkdirErr != nil {
				return fmt.Errorf("making a temp root dir (%s) error - %v", config.TempRootPath, mkdirErr)
			}

			return nil
		}

		return fmt.Errorf("temp root dir (%s) error - %v", config.TempRootPath, err)
	}

	if !tempDirInfo.IsDir() {
		return fmt.Errorf("temp root dir (%s) exist, but not a directory", config.TempRootPath)
	}

	tempDirPerm := tempDirInfo.Mode().Perm()
	if tempDirPerm&0200 != 0200 {
		return fmt.Errorf("temp root dir (%s) exist, but does not have write permission", config.TempRootPath)
	}

	return nil
}

// RemoveTempRootDir removes temp root dir
func (config *Config) RemoveTempRootDir() error {
	if len(config.TempRootPath) == 0 {
		return nil
	}

	return os.RemoveAll(config.TempRootPath)
}

// Validate validates configuration
func (config *Config) Validate() error {
	scheme, endpoint, err := ParsePoolServiceEndpoint(config.ServiceEndpoint)
	if err != nil {
		return err
	}

	if scheme == "unix" {
		// endpoint is a file
		_, err := os.Stat(endpoint)
		if err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("service unix socket file (%s) error - %v", endpoint, err)
			}
		} else {
			// file exists
			return fmt.Errorf("service unix socket file (%s) already exists", endpoint)
		}

		parentDir := filepath.Dir(endpoint)
		unixSocketDirInfo, err := os.Stat(parentDir)
		if err != nil {
			if os.IsNotExist(err) {
				err2 := os.MkdirAll(parentDir, os.FileMode(0777))
				if err2 != nil {
					return fmt.Errorf("failed to make a directory for unix socket (%s)", parentDir)
				}
			} else {
				return fmt.Errorf("unix socket directory (%s) error - %v", parentDir, err)
			}
		} else {
			unixSocketDirPerm := unixSocketDirInfo.Mode().Perm()
			if unixSocketDirPerm&0200 != 0200 {
				return fmt.Errorf("unix socket directory (%s) must have write permission", parentDir)
			}
		}
	}

	if config.Profile && config.ProfileServicePort <= 0 {
		return fmt.Errorf("profile service port must be given")
	}

	if len(config.TempRootPath) > 0 {
		tempDirInfo, err := os.Stat(config.TempRootPath)
		if err != nil {
			return fmt.Errorf("temp root dir (%s) error - %v", config.TempRootPath, err)
		}

		if !tempDirInfo.IsDir() {
			return fmt.Errorf("temp root dir (%s) must be a directory", config.TempRootPath)
		}

		tempDirPerm := tempDirInfo.Mode().Perm()
		if tempDirPerm&0200 != 0200 {
			return fmt.Errorf("temp root (%s) must have write permission", config.TempRootPath)
		}
	}

	return nil
}

// ParsePoolServiceEndpoint parses endpoint string
func ParsePoolServiceEndpoint(endpoint string) (string, string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", "", fmt.Errorf("could not parse endpoint: %v", err)
	}

	scheme := strings.ToLower(u.Scheme)
	switch scheme {
	case "tcp":
		return "tcp", u.Host, nil
	case "unix":
		path := path.Join("/", u.Path)
		return "unix", path, nil
	case "":
		if len(u.Host) > 0 {
			return "tcp", u.Host, nil
		}
		return "", "", fmt.Errorf("unknown host: %s", u.Host)
	default:
		return "", "", fmt.Errorf("unsupported protocol: %s", scheme)
	}
}
