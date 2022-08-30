package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v2"

	"github.com/cyverse/irodsfs-pool/commons"
	log "github.com/sirupsen/logrus"
)

const (
	ChildProcessArgument = "child_process"
)

func processArguments() (*commons.Config, io.WriteCloser, bool, error) {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "processArguments",
	})

	var version bool
	var help bool
	var configFilePath string
	var cacheTimeoutSettingsJSON string

	config := commons.NewDefaultConfig()

	// Parse parameters
	flag.BoolVar(&version, "version", false, "Print client version information")
	flag.BoolVar(&version, "v", false, "Print client version information (shorthand form)")
	flag.BoolVar(&help, "h", false, "Print help")
	flag.StringVar(&configFilePath, "config", "", "Set config YAML File")
	flag.StringVar(&config.ServiceEndpoint, "endpoint", commons.ServiceEndpointDefault, "Set service endpoint (host:port or unix:///file.sock)")
	flag.BoolVar(&config.Foreground, "f", false, "Run in foreground")
	flag.BoolVar(&config.Debug, "d", false, "Run in debug mode")
	flag.BoolVar(&config.ChildProcess, ChildProcessArgument, false, "")
	flag.Int64Var(&config.DataCacheSizeMax, "cache_size_max", commons.DataCacheSizeMaxDefault, "Set file cache max size")
	flag.StringVar(&config.DataCacheRootPath, "cache_root", commons.GetDefaultDataCacheRootPath(), "Set file cache root path")
	flag.StringVar(&config.TempRootPath, "temp_root", commons.GetDefaultTempRootPath(), "Set temp file root path")
	flag.StringVar(&config.LogPath, "log", commons.GetDefaultLogFilePath(), "Set log file path")
	flag.BoolVar(&config.Profile, "profile", false, "Enable profiling")
	flag.IntVar(&config.ProfileServicePort, "profile_port", commons.ProfileServicePortDefault, "Set profile service port")
	flag.IntVar(&config.PrometheusExporterPort, "prometheus_exporter_port", commons.PrometheusExporterPortDefault, "Set prometheus exporter port")
	flag.StringVar(&cacheTimeoutSettingsJSON, "cache_timeout_settings", "", "Set cache timeout settings using JSON")

	flag.Parse()

	if version {
		info, err := commons.GetVersionJSON()
		if err != nil {
			logger.WithError(err).Error("failed to get client version info")
			return nil, nil, true, err
		}

		fmt.Println(info)
		return nil, nil, true, nil
	}

	if help {
		flag.Usage()
		return nil, nil, true, nil
	}

	var logWriter io.WriteCloser
	if config.LogPath == "-" || len(config.LogPath) == 0 {
		log.SetOutput(os.Stderr)
	} else {
		logWriter = getLogWriter(config.LogPath)

		// use multi output - to output to file and stdout
		mw := io.MultiWriter(os.Stderr, logWriter)
		log.SetOutput(mw)
	}

	logger.Infof("Logging to %s", config.LogPath)

	if len(cacheTimeoutSettingsJSON) > 0 {
		metadataCacheTimeoutSettings := []commons.MetadataCacheTimeoutSetting{}
		err := json.Unmarshal([]byte(cacheTimeoutSettingsJSON), &metadataCacheTimeoutSettings)
		if err != nil {
			logger.WithError(err).Errorf("failed to convert JSON object to []MetadataCacheTimeoutSetting - %s", cacheTimeoutSettingsJSON)
			return nil, logWriter, true, err
		}

		config.CacheTimeoutSettings = metadataCacheTimeoutSettings
	}

	if len(configFilePath) > 0 {
		// read config
		configFileAbsPath, err := filepath.Abs(configFilePath)
		if err != nil {
			logger.WithError(err).Errorf("failed to access the local yaml file %s", configFilePath)
			return nil, logWriter, true, err
		}

		fileinfo, err := os.Stat(configFileAbsPath)
		if err != nil {
			logger.WithError(err).Errorf("failed to access the local yaml file %s", configFileAbsPath)
			return nil, logWriter, true, err
		}

		if fileinfo.IsDir() {
			logger.WithError(err).Errorf("local yaml file %s is not a file", configFileAbsPath)
			return nil, logWriter, true, fmt.Errorf("local yaml file %s is not a file", configFileAbsPath)
		}

		yamlBytes, err := ioutil.ReadFile(configFileAbsPath)
		if err != nil {
			logger.WithError(err).Errorf("failed to read the local yaml file %s", configFileAbsPath)
			return nil, logWriter, true, err
		}

		err = yaml.Unmarshal(yamlBytes, config)
		if err != nil {
			return nil, logWriter, true, fmt.Errorf("failed to unmarshal YAML - %v", err)
		}
	}

	return config, logWriter, false, nil
}

func getLogWriter(logPath string) io.WriteCloser {
	return &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    10, // 10MB
		MaxBackups: 1,
		MaxAge:     30, // 30 days
		Compress:   false,
	}
}
