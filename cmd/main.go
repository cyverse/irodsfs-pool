package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"

	godaemonizer "github.com/cyverse/go-daemonizer"
	cmd_commons "github.com/cyverse/irodsfs-pool/cmd/commons"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service"
	log "github.com/sirupsen/logrus"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:          "irodsfs-pool [args..]",
	Short:        "Run iRODS FUSE Pool Service",
	Long:         "Run iRODS FUSE Pool Service that handles requests from iRODS FUSE.",
	RunE:         processCommand,
	SilenceUsage: true,
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd:   true,
		DisableNoDescFlag:   true,
		DisableDescriptions: true,
		HiddenDefaultCmd:    true,
	},
	Args: cobra.NoArgs,
}

var daemon *godaemonizer.Daemon

func Execute() error {
	return rootCmd.Execute()
}

func processCommand(command *cobra.Command, args []string) error {
	logger := log.WithFields(log.Fields{})

	// foreground app
	config, cont, err := cmd_commons.ProcessCommonFlags(command)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to process flags: %v\n", err)
		os.Exit(1)
	}

	if !cont {
		os.Exit(0)
	}

	if !config.Foreground {
		fmt.Println("run as daemon")

		if !daemon.IsDaemon() {
			logWriter, err := config.GetLogWriter(true)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to get log writer: %v\n", err)
				os.Exit(1)
			}

			if logWriter != nil {
				defer logWriter.Close()
			}

			log.SetOutput(logWriter)

			err = daemon.Daemonize(context.Background(), config, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to daemonize: %v\n", err)
				logger.WithError(err).Fatal("failed to daemonize")
				os.Exit(1)
			}

			fmt.Println("daemon started successfully")
			logger.Info("daemon started successfully")
			return nil
		}

		// daemon process
		logWriter, err := config.GetLogWriter(false)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get log writer: %v\n", err)
			os.Exit(1)
		}

		if logWriter != nil {
			defer logWriter.Close()
		}

		log.SetOutput(logWriter)

		var config commons.Config
		ready, err := daemon.WaitForParent(&config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to receive params: %v\n", err)
			logger.WithError(err).Fatal("failed to receive params")
			os.Exit(1)
		}

		err, shutdownFn := run(&config)
		if err != nil {
			runErr := errors.Wrapf(err, "failed to run iRODS FUSE Pool Service")
			logger.Errorf("%+v", runErr)

			ready(runErr)
			os.Exit(1)
		} else {
			ready(nil)
		}

		// wait
		waitForCtrlC()

		if shutdownFn != nil {
			shutdownFn()
		}
	} else {
		// run foreground
		fmt.Println("run foreground")

		logWriter, err := config.GetLogWriter(true)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get log writer: %v\n", err)
			os.Exit(1)
		}

		if logWriter != nil {
			defer logWriter.Close()
			log.SetOutput(logWriter)
		}

		err, shutdownFn := run(config)
		if err != nil {
			runErr := errors.Wrapf(err, "failed to run iRODS FUSE Pool Service")
			logger.Errorf("%+v", runErr)
		}

		// wait
		waitForCtrlC()

		if shutdownFn != nil {
			shutdownFn()
		}
	}

	return nil
}

func main() {
	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05.000000",
		FullTimestamp:   true,
	})

	log.SetLevel(log.InfoLevel)

	logger := log.WithFields(log.Fields{})

	// must be called before cobra parses os.Args so --__daemon__ is stripped
	daemon = godaemonizer.New()

	// attach common flags
	cmd_commons.SetCommonFlags(rootCmd)

	err := Execute()
	if err != nil {
		logger.Fatalf("%+v", err)
		os.Exit(1)
	}
}

// run runs iRODS FUSE Pool Service
func run(config *commons.Config) (error, func()) {
	logger := log.WithFields(log.Fields{})

	if config.Debug {
		log.SetLevel(log.DebugLevel)
	}

	versionInfo := commons.GetVersion()
	logger.Infof("iRODS FUSE Pool Service version - %q, commit - %q", versionInfo.ServiceVersion, versionInfo.GitCommit)

	// make work dirs required
	err := config.MakeWorkDirs()
	if err != nil {
		mkdirErr := errors.Wrapf(err, "make work dir error")
		logger.Errorf("%+v", mkdirErr)
		return err, nil
	}

	err = config.Validate()
	if err != nil {
		configErr := errors.Wrapf(err, "invalid configuration")
		logger.Errorf("%+v", configErr)
		return err, nil
	}

	// run a service
	svc, err := service.NewPoolService(config)
	if err != nil {
		serviceErr := errors.Wrapf(err, "failed to create the service")
		logger.Errorf("%+v", serviceErr)
		return err, nil
	}

	err = svc.Start()
	if err != nil {
		serviceErr := errors.Wrapf(err, "failed to start the service")
		logger.Errorf("%+v", serviceErr)
		return err, nil
	}

	shutdown := func() {
		svc.Stop()
		svc.Release()

		// remove socket file if available
		config.CleanSocketFile()

		os.Exit(0)
	}

	return nil, shutdown
}

func waitForCtrlC() {
	var endWaiter sync.WaitGroup

	endWaiter.Add(1)
	signalChannel := make(chan os.Signal, 1)

	signal.Notify(signalChannel, os.Interrupt)

	go func() {
		<-signalChannel
		endWaiter.Done()
	}()

	endWaiter.Wait()
}
