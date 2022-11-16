package commons

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	"gopkg.in/natefinch/lumberjack.v2"

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
	command.Flags().BoolP("profile", "", false, "Enable profiling")
	command.Flags().BoolP("foreground", "f", false, "Run in foreground")

	command.Flags().StringP("config", "", "", "Set config file (yaml)")
	command.Flags().StringP("endpoint", "", commons.ServiceEndpointDefault, "Set service endpoint (host:port or unix:///file.sock)")
	command.Flags().Int64P("cache_size_max", "", commons.DataCacheSizeMaxDefault, "Set file cache max size")
	command.Flags().StringP("cache_root", "", commons.GetDefaultDataCacheRootPath(), "Set file cache root path")
	command.Flags().StringP("temp_root", "", commons.GetDefaultTempRootPath(), "Set temp file root path")
	command.Flags().StringP("cache_timeout_settings", "", commons.GetDefaultTempRootPath(), "Set cache timeout settings in JSON string")

	command.Flags().IntP("profile_port", "", commons.ProfileServicePortDefault, "Set profile service port")
	command.Flags().IntP("prometheus_exporter_port", "", commons.PrometheusExporterPortDefault, "Set prometheus exporter port")

	command.Flags().BoolP(ChildProcessArgument, "", false, "")
}

func ProcessCommonFlags(command *cobra.Command) (*commons.Config, io.WriteCloser, bool, error) {
	logger := log.WithFields(log.Fields{
		"package":  "commons",
		"function": "ProcessCommonFlags",
	})

	debug := false
	debugFlag := command.Flags().Lookup("debug")
	if debugFlag != nil {
		debugMode, err := strconv.ParseBool(debugFlag.Value.String())
		if err != nil {
			debug = false
		}

		debug = debugMode
	}

	foreground := false
	foregroundFlag := command.Flags().Lookup("foreground")
	if foregroundFlag != nil {
		foregroundMode, err := strconv.ParseBool(foregroundFlag.Value.String())
		if err != nil {
			foreground = false
		}

		foreground = foregroundMode
	}

	profile := false
	profileFlag := command.Flags().Lookup("profile")
	if profileFlag != nil {
		profileMode, err := strconv.ParseBool(profileFlag.Value.String())
		if err != nil {
			profile = false
		}

		profile = profileMode
	}

	childProcess := false
	childProcessFlag := command.Flags().Lookup(ChildProcessArgument)
	if childProcessFlag != nil {
		childProcessMode, err := strconv.ParseBool(childProcessFlag.Value.String())
		if err != nil {
			childProcess = false
		}

		childProcess = childProcessMode
	}

	if debug {
		log.SetLevel(log.DebugLevel)
	}

	helpFlag := command.Flags().Lookup("help")
	if helpFlag != nil {
		help, err := strconv.ParseBool(helpFlag.Value.String())
		if err != nil {
			help = false
		}

		if help {
			PrintHelp(command)
			return nil, nil, false, nil // stop here
		}
	}

	versionFlag := command.Flags().Lookup("version")
	if versionFlag != nil {
		version, err := strconv.ParseBool(versionFlag.Value.String())
		if err != nil {
			version = false
		}

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
			yamlBytes, err := ioutil.ReadFile(configPath)
			if err != nil {
				logger.Error(err)
				return nil, nil, false, err // stop here
			}

			serverConfig, err := commons.NewConfigFromYAML(yamlBytes)
			if err != nil {
				logger.Error(err)
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

	err := config.Validate()
	if err != nil {
		logger.Error(err)
		return nil, nil, false, err // stop here
	}

	if config.Debug {
		log.SetLevel(log.DebugLevel)
	}

	var logWriter io.WriteCloser
	if config.LogPath == "-" || len(config.LogPath) == 0 {
		log.SetOutput(os.Stderr)
	} else {
		parentLogWriter, logFilePath := getLogWriterForParentProcess(config.LogPath)
		logWriter = parentLogWriter

		// use multi output - to output to file and stdout
		mw := io.MultiWriter(os.Stderr, parentLogWriter)
		log.SetOutput(mw)

		logger.Infof("Logging to %s", logFilePath)
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
			logger.WithError(err).Errorf("failed to convert input to int64")
			return nil, logWriter, false, err // stop here
		}

		if cacheSizeMax > 0 {
			config.DataCacheSizeMax = cacheSizeMax
		}
	}

	cacheRootFlag := command.Flags().Lookup("cache_root")
	if cacheRootFlag != nil {
		cacheRoot := cacheRootFlag.Value.String()
		if len(cacheRoot) > 0 {
			config.DataCacheRootPath = cacheRoot
		}
	}

	tempRootFlag := command.Flags().Lookup("temp_root")
	if tempRootFlag != nil {
		tempRoot := tempRootFlag.Value.String()
		if len(tempRoot) > 0 {
			config.TempRootPath = tempRoot
		}
	}

	cacheTimeoutSettingsFlag := command.Flags().Lookup("cache_timeout_settings")
	if cacheTimeoutSettingsFlag != nil {
		cacheTimeoutSettingsJson := cacheTimeoutSettingsFlag.Value.String()
		if len(cacheTimeoutSettingsJson) > 0 {
			metadataCacheTimeoutSettings := []commons.MetadataCacheTimeoutSetting{}

			err := json.Unmarshal([]byte(cacheTimeoutSettingsJson), &metadataCacheTimeoutSettings)
			if err != nil {
				logger.WithError(err).Errorf("failed to convert JSON object to []MetadataCacheTimeoutSetting - %s", cacheTimeoutSettingsJson)
				return nil, logWriter, false, err // stop here
			}

			config.CacheTimeoutSettings = metadataCacheTimeoutSettings
		}
	}

	profilePortFlag := command.Flags().Lookup("profile_port")
	if profilePortFlag != nil {
		profilePort, err := strconv.ParseInt(profilePortFlag.Value.String(), 10, 32)
		if err != nil {
			logger.WithError(err).Errorf("failed to convert input to int")
			return nil, logWriter, false, err // stop here
		}

		if profilePort > 0 {
			config.ProfileServicePort = int(profilePort)
		}
	}

	prometheusExporterPortFlag := command.Flags().Lookup("prometheus_exporter_port")
	if prometheusExporterPortFlag != nil {
		prometheusExporterPort, err := strconv.ParseInt(prometheusExporterPortFlag.Value.String(), 10, 32)
		if err != nil {
			logger.WithError(err).Errorf("failed to convert input to int")
			return nil, logWriter, false, err // stop here
		}

		if prometheusExporterPort > 0 {
			config.PrometheusExporterPort = int(prometheusExporterPort)
		}
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
	logFilePath := fmt.Sprintf("%s.parent", logPath)
	return &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    50, // 50MB
		MaxBackups: 5,
		MaxAge:     30, // 30 days
		Compress:   false,
	}, logFilePath
}

func getLogWriterForChildProcess(logPath string) (io.WriteCloser, string) {
	logFilePath := fmt.Sprintf("%s.child", logPath)
	return &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    50, // 50MB
		MaxBackups: 5,
		MaxAge:     30, // 30 days
		Compress:   false,
	}, logFilePath
}
