package commons

import (
	"fmt"
	"os"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/irodsfs-pool/commons"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func SetCommonFlags(command *cobra.Command) {
	command.Flags().BoolP("version", "v", false, "Print version")
	command.Flags().BoolP("help", "h", false, "Print help")
	command.Flags().BoolP("debug", "d", false, "Enable debug mode")
	command.Flags().BoolP("foreground", "f", false, "Run in foreground")

	command.Flags().StringP("config", "c", "", "Set config file (yaml)")
}

func ProcessCommonFlags(command *cobra.Command) (*commons.Config, bool, error) {
	logger := log.WithFields(log.Fields{})

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

	if debug {
		log.SetLevel(log.DebugLevel)
	}

	helpFlag := command.Flags().Lookup("help")
	if helpFlag != nil {
		help, _ := strconv.ParseBool(helpFlag.Value.String())
		if help {
			PrintHelp(command)
			return nil, false, nil // stop here
		}
	}

	versionFlag := command.Flags().Lookup("version")
	if versionFlag != nil {
		version, _ := strconv.ParseBool(versionFlag.Value.String())
		if version {
			PrintVersion(command)
			return nil, false, nil // stop here
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
				readErr := errors.Errorf("failed to read config file %q: %w", configPath, err)
				logger.Errorf("%+v", readErr)
				return nil, false, readErr // stop here
			}

			serverConfig, err := commons.NewConfigFromYAML(yamlBytes)
			if err != nil {
				logger.Errorf("%+v", err)
				return nil, false, err // stop here
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

	if config.Debug {
		log.SetLevel(log.DebugLevel)
	}

	err := config.Validate()
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, false, err // stop here
	}

	return config, true, nil // continue
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
