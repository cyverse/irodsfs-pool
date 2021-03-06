package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"

	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service"
	"github.com/pkg/profile"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

const (
	// InterProcessCommunicationFinishSuccess is the message that parent process receives when child process is executed successfully
	InterProcessCommunicationFinishSuccess string = "<<COMMUNICATION_CLOSE_SUCCESS>>"
	// InterProcessCommunicationFinishError is the message that parent process receives when child process fails to run
	InterProcessCommunicationFinishError string = "<<COMMUNICATION_CLOSE_ERROR>>"
)

// NilWriter drains output
type NilWriter struct{}

// Write does nothing
func (w *NilWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func main() {
	log.SetLevel(log.InfoLevel)
	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05.000000",
	})

	// check if this is subprocess running in the background
	isChildProc := false

	childProcessArgument := fmt.Sprintf("-%s", ChildProcessArgument)
	for _, arg := range os.Args[1:] {
		if arg == childProcessArgument {
			// background
			isChildProc = true
			break
		}
	}

	if isChildProc {
		// child process
		childMain()
	} else {
		// parent process
		parentMain()
	}
}

// RunFSDaemon runs irodsfs-pool as a daemon
func RunFSDaemon(irodsfsPoolExec string, config *commons.Config) error {
	return parentRun(irodsfsPoolExec, config)
}

// parentRun executes irodsfs-pool with the config given
func parentRun(irodsfsPoolExec string, config *commons.Config) error {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "parentRun",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	// make temp dir if required
	err := config.MakeTempRootDir()
	if err != nil {
		logger.WithError(err).Error("invalid configuration")
		return err
	}

	err = config.Validate()
	if err != nil {
		logger.WithError(err).Error("invalid configuration")
		return err
	}

	if !config.Foreground {
		// run child process in background and pass parameters via stdin PIPE
		// receives result from the child process
		logger.Info("Running the process in the background mode")
		childProcessArgument := fmt.Sprintf("-%s", ChildProcessArgument)
		cmd := exec.Command(irodsfsPoolExec, childProcessArgument)
		subStdin, err := cmd.StdinPipe()
		if err != nil {
			logger.WithError(err).Error("failed to communicate to background process")
			return err
		}

		subStdout, err := cmd.StdoutPipe()
		if err != nil {
			logger.WithError(err).Error("failed to communicate to background process")
			return err
		}

		cmd.Stderr = cmd.Stdout

		err = cmd.Start()
		if err != nil {
			logger.WithError(err).Errorf("failed to start a child process")
			return err
		}

		logger.Infof("Process id = %d", cmd.Process.Pid)

		logger.Info("Sending configuration data")
		configBytes, err := yaml.Marshal(config)
		if err != nil {
			logger.WithError(err).Error("failed to serialize configuration")
			return err
		}

		// send it to child
		_, err = io.WriteString(subStdin, string(configBytes))
		if err != nil {
			logger.WithError(err).Error("failed to communicate to background process")
			return err
		}
		subStdin.Close()
		logger.Info("Successfully sent configuration data to background process")

		childProcessFailed := false

		// receive output from child
		subOutputScanner := bufio.NewScanner(subStdout)
		for {
			if subOutputScanner.Scan() {
				errMsg := strings.TrimSpace(subOutputScanner.Text())
				if errMsg == InterProcessCommunicationFinishSuccess {
					logger.Info("Successfully started background process")
					break
				} else if errMsg == InterProcessCommunicationFinishError {
					logger.Error("failed to start background process")
					childProcessFailed = true
					break
				} else {
					logger.Info(errMsg)
				}
			} else {
				// check err
				if subOutputScanner.Err() != nil {
					logger.Error(subOutputScanner.Err().Error())
					childProcessFailed = true
					break
				}
			}
		}

		subStdout.Close()

		if childProcessFailed {
			return fmt.Errorf("failed to start background process")
		}
	} else {
		// foreground
		err = run(config, false)
		if err != nil {
			logger.WithError(err).Error("failed to run iRODS FUSE Lite Pool Service")
			return err
		}
	}

	return nil
}

// parentMain handles command-line parameters and run parent process
func parentMain() {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "parentMain",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	// parse argument
	config, logWriter, exit, err := processArguments()
	if err != nil {
		logger.WithError(err).Error("failed to process arguments")
		if exit {
			logger.Fatal(err)
		}
	}
	if exit {
		os.Exit(0)
	}

	// run
	err = parentRun(os.Args[0], config)
	if err != nil {
		logger.WithError(err).Error("failed to run the foreground process")
		logger.Fatal(err)
	}

	// clean up
	if logWriter != nil {
		logWriter.Close()
	}

	os.Exit(0)
}

// childMain runs child process
func childMain() {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "childMain",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Info("Start background process")

	logger.Info("Check STDIN to communicate to parent process")
	// read from stdin
	_, err := os.Stdin.Stat()
	if err != nil {
		logger.WithError(err).Error("failed to communicate to foreground process")
		fmt.Fprintln(os.Stderr, InterProcessCommunicationFinishError)
		os.Exit(1)
	}

	logger.Info("Reading configuration from STDIN")
	configBytes, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		logger.WithError(err).Error("failed to read configuration")
		fmt.Fprintln(os.Stderr, InterProcessCommunicationFinishError)
		os.Exit(1)
	}
	logger.Info("Successfully read configuration from STDIN")

	config, err := commons.NewConfigFromYAML(configBytes)
	if err != nil {
		logger.WithError(err).Error("failed to read configuration")
		fmt.Fprintln(os.Stderr, InterProcessCommunicationFinishError)
		os.Exit(1)
	}

	// output to log file
	var logWriter io.WriteCloser
	if len(config.LogPath) > 0 {
		logWriter = getLogWriter(config.LogPath)
		log.SetOutput(logWriter)
	}

	// make temp dir if required
	err = config.MakeTempRootDir()
	if err != nil {
		logger.WithError(err).Error("invalid configuration")
		fmt.Fprintln(os.Stderr, InterProcessCommunicationFinishError)
		os.Exit(1)
	}

	err = config.Validate()
	if err != nil {
		logger.WithError(err).Error("invalid configuration")
		fmt.Fprintln(os.Stderr, InterProcessCommunicationFinishError)
		os.Exit(1)
	}

	logger.Info("Run background process")

	// background
	err = run(config, true)
	if err != nil {
		logger.WithError(err).Error("failed to run iRODS FUSE Lite Pool Service")
		os.Exit(1)
	}

	if logWriter != nil {
		logWriter.Close()
	}
}

// run runs irodsfs-pool
func run(config *commons.Config, isChildProcess bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "run",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	if config.Debug {
		log.SetLevel(log.DebugLevel)
	}

	// profile
	if config.Profile {
		go func() {
			profileServiceAddr := fmt.Sprintf(":%d", config.ProfileServicePort)
			http.ListenAndServe(profileServiceAddr, nil)
		}()

		prof := profile.Start(profile.MemProfile)
		defer prof.Stop()
	}

	versionInfo := commons.GetVersion()
	logger.Infof("iRODS FUSE Lite Pool Service version - %s, commit - %s", versionInfo.ServiceVersion, versionInfo.GitCommit)

	// run a service
	svc, err := service.NewPoolService(config)
	if err != nil {
		logger.WithError(err).Error("failed to create the service")
		if isChildProcess {
			fmt.Fprintln(os.Stderr, InterProcessCommunicationFinishError)
		}
		return err
	}

	err = svc.Init()
	if err != nil {
		logger.WithError(err).Error("failed to init the service")
		if isChildProcess {
			fmt.Fprintln(os.Stderr, InterProcessCommunicationFinishError)
		}
		return err
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		receivedSignal := <-signalChan

		logger.Infof("received signal (%s), terminating iRODS FUSE Lite Pool Service", receivedSignal.String())
		if isChildProcess {
			fmt.Fprintln(os.Stderr, InterProcessCommunicationFinishError)
		}

		svc.Destroy()

		// remote temp dir
		config.RemoveTempRootDir()

		os.Exit(0)
	}()

	if isChildProcess {
		fmt.Fprintln(os.Stdout, InterProcessCommunicationFinishSuccess)
		if len(config.LogPath) == 0 {
			// stderr is not a local file, so is closed by parent
			var nilWriter NilWriter
			log.SetOutput(&nilWriter)
		}
	}

	err = svc.Start()
	if err != nil {
		logger.WithError(err).Error("failed to start the service, terminating iRODS FUSE Lite Pool Service")
		svc.Destroy()
		return err
	}

	// returns if fails, or stopped.
	logger.Info("Service stopped, terminating iRODS FUSE Lite Pool Service")
	svc.Destroy()

	// remote temp dir
	config.RemoveTempRootDir()

	return nil
}
