package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/cyverse/go-irodsclient/config"
	"github.com/cyverse/irodsfs-pool/client"

	log "github.com/sirupsen/logrus"
)

func main() {
	logger := log.WithFields(log.Fields{})

	// Parse cli parameters
	flag.Parse()
	args := flag.Args()

	if len(args) != 2 {
		fmt.Fprintf(os.Stderr, "Give a local path and an iRODS path!\n")
		os.Exit(1)
	}

	inputPath := args[0]
	outputPath := args[1]

	// Read account configuration from YAML file
	cfg, err := config.NewConfigFromYAMLFile(config.GetDefaultConfig(), "account.yml")
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	account := cfg.ToIRODSAccount()
	logger.Debugf("Account : %v", account.GetRedacted())

	poolClient := client.NewPoolServiceClient(":12020", time.Minute*5, "test_client_1", logger)
	err = poolClient.Connect()
	if err != nil {
		logger.Errorf("%+v", err)
		panic(err)
	}

	defer poolClient.Disconnect()

	appName := "upload_file"
	poolSession, err := poolClient.NewSession(account, appName)
	if err != nil {
		logger.Errorf("%+v", err)
		panic(err)
	}
	defer poolSession.Release()

	trackerCB := func(task string, processed int64, total int64) {
		logger.Infof("%s] %d / %d", task, processed, total)
	}

	err = poolSession.UploadFile(inputPath, outputPath, trackerCB)
	if err != nil {
		logger.Errorf("%+v", err)
		panic(err)
	}

	fmt.Printf("Uploaded %q to %q\n", inputPath, outputPath)
}
