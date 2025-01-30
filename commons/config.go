package commons

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"golang.org/x/xerrors"
	yaml "gopkg.in/yaml.v2"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

// GetDefaultInstanceID returns default instance id
func GetDefaultInstanceID() string {
	return xid.New().String()
}

// GetDefaultDataRootDirPath returns default data root path
func GetDefaultDataRootDirPath() string {
	dirPath, err := os.Getwd()
	if err != nil {
		return DataRootPathDefault
	}
	return dirPath
}

// Config holds the parameters list which can be configured
type Config struct {
	ServiceEndpoint      string                                       `yaml:"service_endpoint" json:"service_endpoint"`
	DataCacheSizeMax     int64                                        `yaml:"data_cache_size_max,omitempty" json:"data_cache_size_max,omitempty"`
	DataRootPath         string                                       `yaml:"data_root_path,omitempty" json:"data_root_path,omitempty"`
	CacheTimeoutSettings []irodsclient_fs.MetadataCacheTimeoutSetting `yaml:"cache_timeout_settings,omitempty" json:"cache_timeout_settings,omitempty"`

	LogPath string `yaml:"log_path,omitempty" json:"log_path,omitempty"`

	Profile                bool `yaml:"profile,omitempty" json:"profile,omitempty"`
	ProfileServicePort     int  `yaml:"profile_service_port,omitempty" json:"profile_service_port,omitempty"`
	PrometheusExporterPort int  `yaml:"prometheus_exporter_port,omitempty" json:"prometheus_exporter_port,omitempty"`

	Foreground   bool `yaml:"foreground,omitempty" json:"foreground,omitempty"`
	Debug        bool `yaml:"debug,omitempty" json:"debug,omitempty"`
	ChildProcess bool `yaml:"childprocess,omitempty" json:"childprocess,omitempty"`

	InstanceID string `yaml:"instanceid,omitempty" json:"instanceid,omitempty"`
}

// NewDefaultConfig returns a default config
func NewDefaultConfig() *Config {
	return &Config{
		ServiceEndpoint:      "",
		DataCacheSizeMax:     DataCacheSizeMaxDefault,
		DataRootPath:         GetDefaultDataRootDirPath(),
		CacheTimeoutSettings: []irodsclient_fs.MetadataCacheTimeoutSetting{},

		LogPath: "", // use default

		Profile:                false,
		ProfileServicePort:     ProfileServicePortDefault,
		PrometheusExporterPort: PrometheusExporterPortDefault,

		Foreground:   false,
		Debug:        false,
		ChildProcess: false,

		InstanceID: GetDefaultInstanceID(),
	}
}

// NewConfigFromYAML creates Config from YAML
func NewConfigFromYAML(yamlBytes []byte) (*Config, error) {
	config := NewDefaultConfig()

	err := yaml.Unmarshal(yamlBytes, config)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal yaml into config: %w", err)
	}

	return config, nil
}

// NewConfigFromJSON creates Config from JSON
func NewConfigFromJSON(jsonBytes []byte) (*Config, error) {
	config := NewDefaultConfig()

	err := json.Unmarshal(jsonBytes, config)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal json into config: %w", err)
	}

	return config, nil
}

// GetLogFilePath returns log file path
func (config *Config) GetLogFilePath() string {
	if len(config.LogPath) > 0 {
		return config.LogPath
	}

	// default
	logFilename := fmt.Sprintf("%s.log", config.InstanceID)
	return path.Join(config.DataRootPath, logFilename)
}

func (config *Config) GetServiceEndpoint() string {
	if len(config.ServiceEndpoint) > 0 {
		return config.ServiceEndpoint
	}

	return fmt.Sprintf("unix://%s/comm.sock", config.DataRootPath)
}

func (config *Config) GetDataCacheRootDirPath() string {
	dirname := fmt.Sprintf("%s/cache", config.InstanceID)
	return path.Join(config.DataRootPath, dirname)
}

func (config *Config) GetInstanceDataRootDirPath() string {
	return path.Join(config.DataRootPath, config.InstanceID)
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

	cacheDirPath := config.GetDataCacheRootDirPath()
	logger.Debugf("making cache dir %q", cacheDirPath)
	err := config.makeDir(cacheDirPath)
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

// CleanWorkDirs cleans dirs used
func (config *Config) CleanWorkDirs() error {
	cacheDirPath := config.GetDataCacheRootDirPath()
	err := config.removeDir(cacheDirPath)
	if err != nil {
		return err
	}

	instanceDataDirPath := config.GetInstanceDataRootDirPath()
	err = config.removeDir(instanceDataDirPath)
	if err != nil {
		return err
	}

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
		return xerrors.Errorf("failed to create a dir with empty path")
	}

	dirInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			// make
			mkdirErr := os.MkdirAll(path, 0775)
			if mkdirErr != nil {
				return xerrors.Errorf("making a dir %q error: %w", path, mkdirErr)
			}

			return nil
		}

		return xerrors.Errorf("stating a dir %q error: %w", path, err)
	}

	if !dirInfo.IsDir() {
		return xerrors.Errorf("a file %q exist, not a directory", path)
	}

	dirPerm := dirInfo.Mode().Perm()
	if dirPerm&0200 != 0200 {
		return xerrors.Errorf("a dir %q exist, but does not have the write permission", path)
	}

	return nil
}

// removeDir removes a dir
func (config *Config) removeDir(path string) error {
	if len(path) == 0 {
		return xerrors.Errorf("failed to remove a dir with empty path")
	}

	return os.RemoveAll(path)
}

// makeUnixSocketDir makes unix socket dir
func (config *Config) makeUnixSocketDir(endpoint string) error {
	// endpoint is a file
	_, err := os.Stat(endpoint)
	if err != nil {
		if !os.IsNotExist(err) {
			return xerrors.Errorf("service unix socket file %q error: %w", endpoint, err)
		}
	} else {
		// file exists
		// remove
		err2 := os.Remove(endpoint)
		if err2 != nil {
			return xerrors.Errorf("failed to remove the existing unix socket file %q: %w", endpoint, err2)
		}
	}

	parentDir := filepath.Dir(endpoint)
	unixSocketDirInfo, err := os.Stat(parentDir)
	if err != nil {
		if os.IsNotExist(err) {
			err2 := os.MkdirAll(parentDir, os.FileMode(0777))
			if err2 != nil {
				return xerrors.Errorf("failed to make a directory for unix socket %q: %w", parentDir, err2)
			}
			// ok - fall
		} else {
			return xerrors.Errorf("unix socket directory %q error: %w", parentDir, err)
		}
	} else {
		unixSocketDirPerm := unixSocketDirInfo.Mode().Perm()
		if unixSocketDirPerm&0200 != 0200 {
			return xerrors.Errorf("unix socket directory %q must have write permission", parentDir)
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
		return xerrors.Errorf("failed to remove unix socket file %q: %w", endpoint, err)
	}
	return nil
}

// Validate validates configuration
func (config *Config) Validate() error {
	_, _, err := ParsePoolServiceEndpoint(config.GetServiceEndpoint())
	if err != nil {
		return err
	}

	if config.Profile && config.ProfileServicePort <= 0 {
		return xerrors.Errorf("profile service port must be given")
	}

	if config.PrometheusExporterPort <= 0 {
		return xerrors.Errorf("prometheus exporter port must be given")
	}

	if len(config.DataRootPath) == 0 {
		return xerrors.Errorf("data root dir must be given")
	}

	if config.DataCacheSizeMax < 0 {
		return xerrors.Errorf("data cache size max must be a positive value")
	}

	return nil
}

func parseRawURL(rawurl string) (string, string, string, error) {
	u, err := url.ParseRequestURI(rawurl)
	if err != nil || u.Host == "" {
		// try adding //
		u, repErr := url.ParseRequestURI("tcp://" + rawurl)
		if repErr != nil {
			return "", "", "", xerrors.Errorf("could not parse raw url: %s, error: %w", rawurl, err)
		}

		return u.Scheme, u.Host, u.Path, nil
	}

	return u.Scheme, u.Host, u.Path, nil
}

// ParsePoolServiceEndpoint parses endpoint string
func ParsePoolServiceEndpoint(endpoint string) (string, string, error) {
	scheme, host, localpath, err := parseRawURL(endpoint)
	if err != nil {
		return "", "", err
	}

	scheme = strings.ToLower(scheme)
	switch scheme {
	case "tcp":
		return "tcp", host, nil
	case "unix":
		localpath = path.Join("/", strings.TrimPrefix(localpath, "/"))
		return "unix", localpath, nil
	case "":
		if len(host) > 0 {
			return "tcp", host, nil
		}
		return "", "", xerrors.Errorf("unknown host: %q", host)
	default:
		return "", "", xerrors.Errorf("unsupported protocol: %q", scheme)
	}
}
