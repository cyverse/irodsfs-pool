package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	godaemonizer "github.com/cyverse/go-daemonizer"
	irodsfs_common_util "github.com/cyverse/irodsfs-common/util"
	cmd_commons "github.com/cyverse/irodsfs-pool/cmd/commons"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const defaultStopWait = 10 * time.Second

func Execute(d *godaemonizer.Daemon) error {
	return newRootCommand(d).Execute()
}

func newRootCommand(d *godaemonizer.Daemon) *cobra.Command {
	root := &cobra.Command{
		Use:           "irodsfs-pool",
		Short:         "Run iRODS FUSE Pool Service",
		Long:          "Run iRODS FUSE Pool Service that handles requests from iRODS FUSE.",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableNoDescFlag:   true,
			DisableDescriptions: true,
			HiddenDefaultCmd:    true,
		},
	}

	cmd_commons.SetCommonFlags(root)
	root.AddCommand(
		newStartCommand(d),
		newRunCommand(),
		newStopCommand(),
		newStatusCommand(),
		newVersionCommand(),
	)
	return root
}

func newStartCommand(d *godaemonizer.Daemon) *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start irodsfs-pool as a background daemon",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if d.IsDaemon() {
				return runDaemonChild(command, d)
			}

			config, err := loadConfig(command)
			if err != nil {
				return fmt.Errorf("process command flags: %w", err)
			}
			logWriter, err := config.GetLogWriter(true)
			if err != nil {
				return fmt.Errorf("get parent log writer: %w", err)
			}
			if logWriter != nil {
				defer logWriter.Close()
				log.SetOutput(logWriter)
			}

			if err := d.Daemonize(context.Background(), config, nil); err != nil {
				return fmt.Errorf("daemonize irodsfs-pool: %w", err)
			}

			fmt.Fprintf(command.OutOrStdout(), "irodsfs-pool started (pid file: %s)\n", config.PIDFile)
			return nil
		},
	}
}

func runDaemonChild(command *cobra.Command, d *godaemonizer.Daemon) error {
	var config commons.Config
	ready, err := d.WaitForParent(&config)
	if err != nil {
		return fmt.Errorf("receive daemon startup parameters: %w", err)
	}

	logWriter, err := config.GetLogWriter(false)
	if err != nil {
		ready(err)
		return fmt.Errorf("get daemon log writer: %w", err)
	}
	if logWriter != nil {
		defer logWriter.Close()
		log.SetOutput(logWriter)
	}

	return runDaemonManaged(&config, ready)
}

func newRunCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Run irodsfs-pool in the foreground",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			config, err := loadConfig(command)
			if err != nil {
				return fmt.Errorf("process command flags: %w", err)
			}
			if err := configureForegroundPaths(config); err != nil {
				return fmt.Errorf("configure foreground paths: %w", err)
			}

			logWriter, err := config.GetLogWriter(true)
			if err != nil {
				return fmt.Errorf("get foreground log writer: %w", err)
			}
			if logWriter != nil {
				defer logWriter.Close()
				log.SetOutput(logWriter)
			}

			return runForeground(config)
		},
	}
}

func loadConfig(command *cobra.Command) (*commons.Config, error) {
	return cmd_commons.ProcessCommonFlags(command)
}

func configureForegroundPaths(config *commons.Config) error {
	workingDirectory, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get current working directory: %w", err)
	}

	oldDataRootPath := config.DataRootPath
	oldDefaultStagingRootPath := filepath.Join(oldDataRootPath, commons.StagingRootPathDefault)

	config.DataRootPath = workingDirectory
	if config.StagingRootPath == "" || filepath.Clean(config.StagingRootPath) == filepath.Clean(oldDefaultStagingRootPath) {
		config.StagingRootPath = filepath.Join(workingDirectory, commons.StagingRootPathDefault)
	}

	return nil
}

func newStopCommand() *cobra.Command {
	var wait time.Duration
	command := &cobra.Command{
		Use:   "stop",
		Short: "Stop the running irodsfs-pool process",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			config, err := cmd_commons.ProcessCommonFlags(command)
			if err != nil {
				return fmt.Errorf("process command flags: %w", err)
			}

			pid, err := cmd_commons.ReadPID(config.PIDFile)
			if err != nil {
				return err
			}
			if !cmd_commons.ProcessRunning(pid) {
				return fmt.Errorf("irodsfs-pool is not running (stale pid %d)", pid)
			}
			if err := cmd_commons.SignalPID(pid, syscall.SIGTERM); err != nil {
				return err
			}

			deadline := time.Now().Add(wait)
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for cmd_commons.ProcessRunning(pid) {
				if time.Now().After(deadline) {
					return fmt.Errorf("timed out waiting for irodsfs-pool process %d to stop", pid)
				}
				select {
				case <-command.Context().Done():
					return command.Context().Err()
				case <-ticker.C:
				}
			}

			fmt.Fprintf(command.OutOrStdout(), "irodsfs-pool stopped (pid %d)\n", pid)
			return nil
		},
	}
	command.Flags().DurationVar(&wait, "wait", defaultStopWait, "Maximum time to wait for shutdown")
	return command
}

func newStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Report whether irodsfs-pool is running",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			config, err := cmd_commons.ProcessCommonFlags(command)
			if err != nil {
				return fmt.Errorf("process command flags: %w", err)
			}

			pid, err := cmd_commons.ReadPID(config.PIDFile)
			if err != nil {
				return err
			}
			if !cmd_commons.ProcessRunning(pid) {
				return fmt.Errorf("irodsfs-pool is not running (stale pid %d)", pid)
			}

			fmt.Fprintf(command.OutOrStdout(), "irodsfs-pool is running (pid %d)\n", pid)
			return nil
		},
	}
}

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return cmd_commons.PrintVersion(command)
		},
	}
}

func main() {
	myFormatter := &irodsfs_common_util.StacktraceTextFormatter{
		TextFormatter: log.TextFormatter{
			TimestampFormat: "2006-01-02 15:04:05.000000",
			FullTimestamp:   true,
		},
	}

	log.SetFormatter(myFormatter)
	log.SetLevel(log.InfoLevel)
	log.SetReportCaller(true)

	logger := log.WithFields(log.Fields{})

	// go-daemonizer relaunches os.Args[0]. Use an absolute path so daemon
	// startup does not depend on the configured working directory.
	executable, err := os.Executable()
	if err != nil {
		logger.WithError(err).Fatal("failed to resolve executable path")
	}
	os.Args[0] = executable

	// must be called before Cobra parses os.Args so --__daemon__ is stripped.
	daemon := godaemonizer.New()
	if err := Execute(daemon); err != nil {
		fmt.Fprintf(os.Stderr, "irodsfs-pool: %v\n", err)
		os.Exit(1)
	}
}

func runDaemonManaged(config *commons.Config, ready func(error)) error {
	pidFile, err := cmd_commons.AcquirePIDFile(config.PIDFile)
	if err != nil {
		reportReady(ready, err)
		return err
	}
	defer pidFile.Close()

	return runUntilShutdown(config, ready)
}

func runForeground(config *commons.Config) error {
	return runUntilShutdown(config, nil)
}

func runUntilShutdown(config *commons.Config, ready func(error)) error {
	runErr, shutdownFn := run(config)
	if runErr != nil {
		reportReady(ready, runErr)
		return runErr
	}

	reportReady(ready, nil)
	waitForShutdown()

	if shutdownFn != nil {
		shutdownFn()
	}
	return nil
}

func reportReady(ready func(error), err error) {
	if ready != nil {
		ready(err)
	}
}

// run runs iRODS FUSE Pool Service.
func run(config *commons.Config) (error, func()) {
	logger := log.WithFields(log.Fields{})

	if config.Debug {
		log.SetLevel(log.DebugLevel)
	}

	versionInfo := commons.GetVersion()
	logger.Infof("iRODS FUSE Pool Service version - %q, commit - %q", versionInfo.ServiceVersion, versionInfo.GitCommit)

	if err := config.MakeWorkDirs(); err != nil {
		mkdirErr := errors.Wrapf(err, "make work dir error")
		logger.Error(mkdirErr)
		return err, nil
	}

	if err := config.Validate(); err != nil {
		configErr := errors.Wrapf(err, "invalid configuration")
		logger.Error(configErr)
		return err, nil
	}

	svc, err := service.NewPoolService(config)
	if err != nil {
		serviceErr := errors.Wrapf(err, "failed to create the service")
		logger.Error(serviceErr)
		return serviceErr, nil
	}

	if err := svc.Start(); err != nil {
		serviceErr := errors.Wrapf(err, "failed to start the service")
		logger.Error(serviceErr)
		svc.Release()
		return serviceErr, nil
	}

	shutdown := func() {
		svc.Stop()
		svc.Release()
	}
	return nil, shutdown
}

func waitForShutdown() {
	signalChannel := make(chan os.Signal, 1)

	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signalChannel)
	<-signalChannel
}
