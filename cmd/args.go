package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"

	"github.com/cyverse/irodsfs-pool/commons"
	log "github.com/sirupsen/logrus"
)

const (
	ChildProcessArgument = "child_process"
)

func processArguments() (*commons.Config, *os.File, error, bool) {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "processArguments",
	})

	var version bool
	var help bool
	var configFilePath string

	config := commons.NewDefaultConfig()

	// Parse parameters
	flag.BoolVar(&version, "version", false, "Print client version information")
	flag.BoolVar(&version, "v", false, "Print client version information (shorthand form)")
	flag.BoolVar(&help, "h", false, "Print help")
	flag.StringVar(&configFilePath, "config", "", "Set Config YAML File")
	flag.IntVar(&config.ServicePort, "p", commons.ServicePortDefault, "Service port")
	flag.BoolVar(&config.Foreground, "f", false, "Run in foreground")
	flag.BoolVar(&config.ChildProcess, ChildProcessArgument, false, "")
	flag.Int64Var(&config.BufferSizeMax, "buffer_size_max", commons.BufferSizeMaxDefault, "Set file buffer max size")
	flag.Int64Var(&config.DataCacheSizeMax, "cache_size_max", commons.DataCacheSizeMaxDefault, "Set file cache max size")
	flag.StringVar(&config.DataCacheRootPath, "cache_root", commons.GetDefaultDataCacheRootPath(), "Set file cache root path")
	flag.StringVar(&config.LogPath, "log", commons.GetDefaultLogFilePath(), "Set log file path")

	flag.Parse()

	if version {
		info, err := commons.GetVersionJSON()
		if err != nil {
			logger.WithError(err).Error("failed to get client version info")
			return nil, nil, err, true
		}

		fmt.Println(info)
		return nil, nil, nil, true
	}

	if help {
		flag.Usage()
		return nil, nil, nil, true
	}

	var logFile *os.File
	logFile = nil
	if config.LogPath == "-" || len(config.LogPath) == 0 {
		log.SetOutput(os.Stderr)
	} else {
		logFileHandle, err := os.OpenFile(config.LogPath, os.O_WRONLY|os.O_CREATE, 0755)
		if err != nil {
			logger.WithError(err).Errorf("Could not create log file - %s", config.LogPath)
		} else {
			// use multi output - to output to file and stdout
			mw := io.MultiWriter(os.Stderr, logFileHandle)
			log.SetOutput(mw)
			logFile = logFileHandle
		}
	}

	logger.Infof("Logging to %s", config.LogPath)

	if len(configFilePath) > 0 {
		// read config
		configFileAbsPath, err := filepath.Abs(configFilePath)
		if err != nil {
			logger.WithError(err).Errorf("failed to access the local yaml file %s", configFilePath)
			return nil, logFile, err, true
		}

		fileinfo, err := os.Stat(configFileAbsPath)
		if err != nil {
			logger.WithError(err).Errorf("failed to access the local yaml file %s", configFileAbsPath)
			return nil, logFile, err, true
		}

		if fileinfo.IsDir() {
			logger.WithError(err).Errorf("local yaml file %s is not a file", configFileAbsPath)
			return nil, logFile, fmt.Errorf("local yaml file %s is not a file", configFileAbsPath), true
		}

		yamlBytes, err := ioutil.ReadFile(configFileAbsPath)
		if err != nil {
			logger.WithError(err).Errorf("failed to read the local yaml file %s", configFileAbsPath)
			return nil, logFile, err, true
		}

		err = yaml.Unmarshal(yamlBytes, &config)
		if err != nil {
			return nil, logFile, fmt.Errorf("failed to unmarshal YAML - %v", err), true
		}
	}

	return config, logFile, nil, false
}
