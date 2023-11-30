package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-pool/client"

	log "github.com/sirupsen/logrus"
)

func main() {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "main",
	})

	// Parse cli parameters
	flag.Parse()
	args := flag.Args()

	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Give an iRODS path!\n")
		os.Exit(1)
	}

	inputPath := args[0]

	// Read account configuration from YAML file
	yaml, err := os.ReadFile("account.yml")
	if err != nil {
		logger.Errorf("%+v", err)
		panic(err)
	}

	account, err := types.CreateIRODSAccountFromYAML(yaml)
	if err != nil {
		logger.Errorf("%+v", err)
		panic(err)
	}

	logger.Debugf("Account : %v", account.MaskSensitiveData())

	poolClient := client.NewPoolServiceClient(":12020", time.Minute*5, "test_client_1")
	err = poolClient.Connect()
	if err != nil {
		logger.Errorf("%+v", err)
		panic(err)
	}

	defer poolClient.Disconnect()

	appName := "list_dir"
	poolSession, err := poolClient.NewSession(account, appName)
	if err != nil {
		logger.Errorf("%+v", err)
		panic(err)
	}
	defer poolSession.Release()

	entries, err := poolSession.List(inputPath)
	if err != nil {
		logger.Errorf("%+v", err)
		panic(err)
	}

	if len(entries) == 0 {
		fmt.Printf("Found no entries in the directory - %q\n", inputPath)
	} else {
		fmt.Printf("DIR: %s\n", inputPath)
		for _, entry := range entries {
			if entry.Type == fs.FileEntry {
				fmt.Printf("> FILE:\t%s\t%d\n", entry.Path, entry.Size)
			} else {
				// dir
				fmt.Printf("> DIRECTORY:\t%s\n", entry.Path)
			}
		}
	}
}
