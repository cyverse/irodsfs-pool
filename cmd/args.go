package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"

	"github.com/cyverse/irodsfs-pool/service"
	log "github.com/sirupsen/logrus"
)

const (
	ChildProcessArgument = "child_process"
)

func inputMissingParams(config *service.Config, stdinClosed bool) error {
	/*
		logger := log.WithFields(log.Fields{
			"package":  "main",
			"function": "inputMissingParams",
		})
	*/

	// add code here to input additional params

	return nil
}

func processArguments() (*service.Config, error, bool) {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "processArguments",
	})

	var help bool

	config := service.NewDefaultConfig()

	// Parse parameters
	flag.BoolVar(&help, "h", false, "Print help")
	flag.IntVar(&config.ServicePort, "p", service.ServicePortDefault, "Service port")
	flag.BoolVar(&config.Foreground, "f", false, "Run in foreground")
	flag.BoolVar(&config.ChildProcess, ChildProcessArgument, false, "")
	flag.StringVar(&config.LogPath, "log", "", "Set log file path")

	flag.Parse()

	if help {
		flag.Usage()
		return nil, nil, true
	}

	if len(config.LogPath) > 0 {
		logFile, err := os.OpenFile(config.LogPath, os.O_WRONLY|os.O_CREATE, 0755)
		if err != nil {
			logger.WithError(err).Errorf("Could not create log file - %s", config.LogPath)
		} else {
			log.SetOutput(logFile)
		}
	}

	configFilePath := ""
	if flag.NArg() > 0 {
		configFilePath = flag.Arg(0)
	}

	stdinClosed := false
	if len(configFilePath) == 0 {
		// read from Environmental variables
		envConfig, err := service.NewConfigFromENV()
		if err != nil {
			logger.WithError(err).Error("Could not read Environmental Variables")
			return nil, err, true
		}

		envConfig.Foreground = config.Foreground
		// overwrite
		config = envConfig
	} else if configFilePath == "-" {
		// read from stdin
		stdinReader := bufio.NewReader(os.Stdin)
		yamlBytes, err := ioutil.ReadAll(stdinReader)
		if err != nil {
			logger.WithError(err).Error("Could not read STDIN")
			return nil, err, true
		}

		err = yaml.Unmarshal(yamlBytes, &config)
		if err != nil {
			return nil, fmt.Errorf("YAML Unmarshal Error - %v", err), true
		}

		stdinClosed = true
	} else {
		// read config
		configFileAbsPath, err := filepath.Abs(configFilePath)
		if err != nil {
			logger.WithError(err).Errorf("Could not access the local yaml file %s", configFilePath)
			return nil, err, true
		}

		fileinfo, err := os.Stat(configFileAbsPath)
		if err != nil {
			logger.WithError(err).Errorf("local yaml file (%s) error", configFileAbsPath)
			return nil, err, true
		}

		if fileinfo.IsDir() {
			logger.WithError(err).Errorf("local yaml file (%s) is not a file", configFileAbsPath)
			return nil, fmt.Errorf("local yaml file (%s) is not a file", configFileAbsPath), true
		}

		yamlBytes, err := ioutil.ReadFile(configFileAbsPath)
		if err != nil {
			logger.WithError(err).Errorf("Could not read the local yaml file %s", configFileAbsPath)
			return nil, err, true
		}

		err = yaml.Unmarshal(yamlBytes, &config)
		if err != nil {
			return nil, fmt.Errorf("YAML Unmarshal Error - %v", err), true
		}
	}

	err := inputMissingParams(config, stdinClosed)
	if err != nil {
		logger.WithError(err).Error("Could not input missing parameters")
		return nil, err, true
	}

	return config, nil, false
}
