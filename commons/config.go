package commons

import (
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	yaml "gopkg.in/yaml.v2"

	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
	"github.com/rs/xid"
)

const (
	DataCacheSizeMaxDefault int64 = 1024 * 1024 * 1024 * 20 // 20GB

	ProfileServicePortDefault     int = 12021
	PrometheusExporterPortDefault int = 12022
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

func getLogFilename() string {
	return fmt.Sprintf("%s.log", getInstanceID())
}

func GetDefaultDataRootDirPath() string {
	dirPath, err := os.Getwd()
	if err != nil {
		return "/var/lib/irodsfs_pool"
	}
	return dirPath
}

func GetDefaultServiceEndpoint() string {
	dataRoot := GetDefaultDataRootDirPath()
	return fmt.Sprintf("unix://%s/comm.sock", dataRoot)
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
	DataRootPath         string                        `yaml:"data_root_path,omitempty"`
	CacheTimeoutSettings []MetadataCacheTimeoutSetting `yaml:"cache_timeout_settings,omitempty"`

	LogPath string `yaml:"log_path,omitempty"`

	Profile                bool `yaml:"profile,omitempty"`
	ProfileServicePort     int  `yaml:"profile_service_port,omitempty"`
	PrometheusExporterPort int  `yaml:"prometheus_exporter_port,omitempty"`

	Foreground   bool `yaml:"foreground,omitempty"`
	Debug        bool `yaml:"debug,omitempty"`
	ChildProcess bool `yaml:"childprocess,omitempty"`

	InstanceID string `yaml:"instanceid,omitempty"`
}

// NewDefaultConfig returns a default config
func NewDefaultConfig() *Config {
	return &Config{
		ServiceEndpoint:      GetDefaultServiceEndpoint(),
		DataCacheSizeMax:     DataCacheSizeMaxDefault,
		DataRootPath:         GetDefaultDataRootDirPath(),
		CacheTimeoutSettings: []MetadataCacheTimeoutSetting{},

		LogPath: "", // use default

		Profile:                false,
		ProfileServicePort:     ProfileServicePortDefault,
		PrometheusExporterPort: PrometheusExporterPortDefault,

		Foreground:   false,
		Debug:        false,
		ChildProcess: false,

		InstanceID: getInstanceID(),
	}
}

// NewConfigFromYAML creates Config from YAML
func NewConfigFromYAML(yamlBytes []byte) (*Config, error) {
	config := NewDefaultConfig()

	err := yaml.Unmarshal(yamlBytes, config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML - %v", err)
	}

	return config, nil
}

// GetLogFilePath returns log file path
func (config *Config) GetLogFilePath() string {
	if len(config.LogPath) > 0 {
		return config.LogPath
	}

	// default
	return path.Join(config.DataRootPath, getLogFilename())
}

func (config *Config) GetTempRootDirPath() string {
	dirname := fmt.Sprintf("%s/temp", getInstanceID())
	return path.Join(config.DataRootPath, dirname)
}

func (config *Config) GetDataCacheRootDirPath() string {
	dirname := fmt.Sprintf("%s/cache", getInstanceID())
	return path.Join(config.DataRootPath, dirname)
}

// MakeLogDir makes a log dir required
func (config *Config) MakeLogDir() error {
	logFilePath := config.GetLogFilePath()
	logDirPath := filepath.Dir(logFilePath)
	err := config.makeDir(logDirPath)
	if err != nil {
		return err
	}

	return nil
}

// MakeWorkDirs makes dirs required
func (config *Config) MakeWorkDirs() error {
	tempDirPath := config.GetTempRootDirPath()
	err := config.makeDir(tempDirPath)
	if err != nil {
		return err
	}

	cacheDirPath := config.GetDataCacheRootDirPath()
	err = config.makeDir(cacheDirPath)
	if err != nil {
		return err
	}

	scheme, endpoint, err := ParsePoolServiceEndpoint(config.ServiceEndpoint)
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

// CleanWorkDirs cleans dirs used
func (config *Config) CleanWorkDirs() error {
	tempDirPath := config.GetTempRootDirPath()
	err := config.removeDir(tempDirPath)
	if err != nil {
		return err
	}

	cacheDirPath := config.GetDataCacheRootDirPath()
	err = config.removeDir(cacheDirPath)
	if err != nil {
		return err
	}

	scheme, endpoint, err := ParsePoolServiceEndpoint(config.ServiceEndpoint)
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
		return fmt.Errorf("failed to create a dir with empty path")
	}

	dirInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			// make
			mkdirErr := os.MkdirAll(path, 0775)
			if mkdirErr != nil {
				return fmt.Errorf("making a dir (%s) error - %v", path, mkdirErr)
			}

			return nil
		}

		return fmt.Errorf("stating a dir (%s) error - %v", path, err)
	}

	if !dirInfo.IsDir() {
		return fmt.Errorf("a file (%s) exist, not a directory", path)
	}

	dirPerm := dirInfo.Mode().Perm()
	if dirPerm&0200 != 0200 {
		return fmt.Errorf("a dir (%s) exist, but does not have the write permission", path)
	}

	return nil
}

// removeDir removes a dir
func (config *Config) removeDir(path string) error {
	if len(path) == 0 {
		return fmt.Errorf("failed to remove a dir with empty path")
	}

	return os.RemoveAll(path)
}

// makeUnixSocketDir makes unix socket dir
func (config *Config) makeUnixSocketDir(endpoint string) error {
	// endpoint is a file
	_, err := os.Stat(endpoint)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("service unix socket file (%s) error - %v", endpoint, err)
		}
	} else {
		// file exists
		// remove
		err2 := os.Remove(endpoint)
		if err2 != nil {
			return fmt.Errorf("failed to remove the existing unix socket file (%s) - %v", endpoint, err2)
		}
	}

	parentDir := filepath.Dir(endpoint)
	unixSocketDirInfo, err := os.Stat(parentDir)
	if err != nil {
		if os.IsNotExist(err) {
			err2 := os.MkdirAll(parentDir, os.FileMode(0777))
			if err2 != nil {
				return fmt.Errorf("failed to make a directory for unix socket (%s)", parentDir)
			}
			// ok - fall
		} else {
			return fmt.Errorf("unix socket directory (%s) error - %v", parentDir, err)
		}
	} else {
		unixSocketDirPerm := unixSocketDirInfo.Mode().Perm()
		if unixSocketDirPerm&0200 != 0200 {
			return fmt.Errorf("unix socket directory (%s) must have write permission", parentDir)
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

	return os.Remove(endpoint)
}

// Validate validates configuration
func (config *Config) Validate() error {
	_, _, err := ParsePoolServiceEndpoint(config.ServiceEndpoint)
	if err != nil {
		return err
	}

	if config.Profile && config.ProfileServicePort <= 0 {
		return fmt.Errorf("profile service port must be given")
	}

	if config.PrometheusExporterPort <= 0 {
		return fmt.Errorf("prometheus exporter port must be given")
	}

	if len(config.DataRootPath) == 0 {
		return fmt.Errorf("data root dir must be given")
	}

	if config.DataCacheSizeMax < 0 {
		return fmt.Errorf("data cache size max must be a positive value")
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
