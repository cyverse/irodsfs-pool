package commons

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"

	"golang.org/x/xerrors"
	"gopkg.in/natefinch/lumberjack.v2"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/irodsfs-pool/commons"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	ChildProcessArgument = "child_process"
)

func SetCommonFlags(command *cobra.Command) {
	command.Flags().BoolP("version", "v", false, "Print version")
	command.Flags().BoolP("help", "h", false, "Print help")
	command.Flags().BoolP("debug", "d", false, "Enable debug mode")
	command.Flags().Bool("profile", false, "Enable profiling")
	command.Flags().BoolP("foreground", "f", false, "Run in foreground")

	command.Flags().StringP("config", "c", "", "Set config file (yaml)")
	command.Flags().String("endpoint", "", "Set service endpoint (host:port or unix:///file.sock)")
	command.Flags().Int64("cache_size_max", -1, "Set file cache max size")
	command.Flags().String("data_root", "", "Set data root dir path")
	command.Flags().String("cache_timeout_settings", "", "Set cache timeout settings in JSON string")

	command.Flags().Int("profile_port", -1, "Set profile service port")
	command.Flags().Int("prometheus_exporter_port", -1, "Set prometheus exporter port")

	command.Flags().Bool(ChildProcessArgument, false, "")
}

func ProcessCommonFlags(command *cobra.Command) (*commons.Config, io.WriteCloser, bool, error) {
	logger := log.WithFields(log.Fields{
		"package":  "commons",
		"function": "ProcessCommonFlags",
	})

	debug := false
	debugFlag := command.Flags().Lookup("debug")
	if debugFlag != nil {
		debug, _ = strconv.ParseBool(debugFlag.Value.String())
	}

	foreground := false
	foregroundFlag := command.Flags().Lookup("foreground")
	if foregroundFlag != nil {
		foreground, _ = strconv.ParseBool(foregroundFlag.Value.String())
	}

	profile := false
	profileFlag := command.Flags().Lookup("profile")
	if profileFlag != nil {
		profile, _ = strconv.ParseBool(profileFlag.Value.String())
	}

	childProcess := false
	childProcessFlag := command.Flags().Lookup(ChildProcessArgument)
	if childProcessFlag != nil {
		childProcess, _ = strconv.ParseBool(childProcessFlag.Value.String())
	}

	if debug {
		log.SetLevel(log.DebugLevel)
	}

	helpFlag := command.Flags().Lookup("help")
	if helpFlag != nil {
		help, _ := strconv.ParseBool(helpFlag.Value.String())
		if help {
			PrintHelp(command)
			return nil, nil, false, nil // stop here
		}
	}

	versionFlag := command.Flags().Lookup("version")
	if versionFlag != nil {
		version, _ := strconv.ParseBool(versionFlag.Value.String())
		if version {
			PrintVersion(command)
			return nil, nil, false, nil // stop here
		}
	}

	readConfig := false
	var config *commons.Config

	configFlag := command.Flags().Lookup("config")
	if configFlag != nil {
		configPath := configFlag.Value.String()
		if len(configPath) > 0 {
			yamlBytes, err := os.ReadFile(configPath)
			if err != nil {
				readErr := xerrors.Errorf("failed to read config file %q: %w", configPath, err)
				logger.Errorf("%+v", readErr)
				return nil, nil, false, readErr // stop here
			}

			serverConfig, err := commons.NewConfigFromYAML(yamlBytes)
			if err != nil {
				logger.Errorf("%+v", err)
				return nil, nil, false, err // stop here
			}

			// overwrite config
			config = serverConfig
			readConfig = true
		}
	}

	// default config
	if !readConfig {
		config = commons.NewDefaultConfig()
	}

	// prioritize command-line flag over config files
	if debug {
		log.SetLevel(log.DebugLevel)
		config.Debug = true
	}

	if foreground {
		config.Foreground = true
	}

	if profile {
		config.Profile = true
	}

	config.ChildProcess = childProcess

	if config.Debug {
		log.SetLevel(log.DebugLevel)
	}

	dataRootFlag := command.Flags().Lookup("data_root")
	if dataRootFlag != nil {
		dataRoot := dataRootFlag.Value.String()
		if len(dataRoot) > 0 {
			config.DataRootPath = dataRoot

			if len(config.LogPath) == 0 {
				config.LogPath = config.GetLogFilePath()
			}

			if len(config.ServiceEndpoint) == 0 {
				config.ServiceEndpoint = config.GetServiceEndpoint()
			}
		}
	}

	err := config.MakeLogDir()
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, nil, false, err // stop here
	}

	var logWriter io.WriteCloser
	logFilePath := config.GetLogFilePath()
	if logFilePath == "-" || len(logFilePath) == 0 {
		log.SetOutput(os.Stderr)
	} else {
		parentLogWriter, parentLogFilePath := getLogWriterForParentProcess(logFilePath)
		logWriter = parentLogWriter

		// use multi output - to output to file and stdout
		mw := io.MultiWriter(os.Stderr, parentLogWriter)
		log.SetOutput(mw)

		logger.Infof("Logging to %q", parentLogFilePath)
	}

	endpointFlag := command.Flags().Lookup("endpoint")
	if endpointFlag != nil {
		endpoint := endpointFlag.Value.String()
		if len(endpoint) > 0 {
			config.ServiceEndpoint = endpoint
		}
	}

	cacheSizeMaxFlag := command.Flags().Lookup("cache_size_max")
	if cacheSizeMaxFlag != nil {
		cacheSizeMax, err := strconv.ParseInt(cacheSizeMaxFlag.Value.String(), 10, 64)
		if err != nil {
			parseErr := xerrors.Errorf("failed to convert input %q to int64: %w", cacheSizeMaxFlag.Value.String(), err)
			logger.Errorf("%+v", parseErr)
			return nil, logWriter, false, parseErr // stop here
		}

		if cacheSizeMax > 0 {
			config.DataCacheSizeMax = cacheSizeMax
		}
	}

	cacheTimeoutSettingsFlag := command.Flags().Lookup("cache_timeout_settings")
	if cacheTimeoutSettingsFlag != nil {
		cacheTimeoutSettingsJson := cacheTimeoutSettingsFlag.Value.String()
		if len(cacheTimeoutSettingsJson) > 0 {
			metadataCacheTimeoutSettings := []irodsclient_fs.MetadataCacheTimeoutSetting{}

			err := json.Unmarshal([]byte(cacheTimeoutSettingsJson), &metadataCacheTimeoutSettings)
			if err != nil {
				jsonErr := xerrors.Errorf("failed to convert JSON object to []MetadataCacheTimeoutSetting: %w", err)
				logger.Errorf("%+v", jsonErr)
				return nil, logWriter, false, jsonErr // stop here
			}

			config.CacheTimeoutSettings = metadataCacheTimeoutSettings
		}
	}

	profilePortFlag := command.Flags().Lookup("profile_port")
	if profilePortFlag != nil {
		profilePort, err := strconv.ParseInt(profilePortFlag.Value.String(), 10, 32)
		if err != nil {
			parseErr := xerrors.Errorf("failed to convert input %q to int64: %w", profilePortFlag.Value.String(), err)
			logger.Errorf("%+v", parseErr)
			return nil, logWriter, false, parseErr // stop here
		}

		if profilePort > 0 {
			config.ProfileServicePort = int(profilePort)
		}
	}

	prometheusExporterPortFlag := command.Flags().Lookup("prometheus_exporter_port")
	if prometheusExporterPortFlag != nil {
		prometheusExporterPort, err := strconv.ParseInt(prometheusExporterPortFlag.Value.String(), 10, 32)
		if err != nil {
			parseErr := xerrors.Errorf("failed to convert input %q to int64: %w", prometheusExporterPortFlag.Value.String(), err)
			logger.Errorf("%+v", parseErr)
			return nil, logWriter, false, parseErr // stop here
		}

		if prometheusExporterPort > 0 {
			config.PrometheusExporterPort = int(prometheusExporterPort)
		}
	}

	err = config.Validate()
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, logWriter, false, err // stop here
	}

	return config, logWriter, true, nil // continue
}

func PrintVersion(command *cobra.Command) error {
	info, err := commons.GetVersionJSON()
	if err != nil {
		return err
	}

	fmt.Println(info)
	return nil
}

func PrintHelp(command *cobra.Command) error {
	return command.Usage()
}

func getLogWriterForParentProcess(logPath string) (io.WriteCloser, string) {
	logFilePath := fmt.Sprintf("%q.parent", logPath)
	return &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    50, // 50MB
		MaxBackups: 5,
		MaxAge:     30, // 30 days
		Compress:   false,
	}, logFilePath
}

func getLogWriterForChildProcess(logPath string) (io.WriteCloser, string) {
	logFilePath := fmt.Sprintf("%q.child", logPath)
	return &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    50, // 50MB
		MaxBackups: 5,
		MaxAge:     30, // 30 days
		Compress:   false,
	}, logFilePath
}
