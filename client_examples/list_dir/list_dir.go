package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"github.com/cyverse/irodsfs-pool/client"
)

func main() {
	util.SetLogLevel(9)

	// Parse cli parameters
	flag.Parse()
	args := flag.Args()

	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Give an iRODS path!\n")
		os.Exit(1)
	}

	inputPath := args[0]

	// Read account configuration from YAML file
	yaml, err := ioutil.ReadFile("account.yml")
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	account, err := types.CreateIRODSAccountFromYAML(yaml)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	util.LogDebugf("Account : %v", account.MaskSensitiveData())

	poolClient := client.NewPoolServiceClient(":12020")
	err = poolClient.Connect()
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	defer poolClient.Disconnect()

	appName := "list_dir"
	conn, err := poolClient.Login(account, appName)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	entries, err := poolClient.List(conn, inputPath)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	if len(entries) == 0 {
		fmt.Printf("Found no entries in the directory - %s\n", inputPath)
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

	err = poolClient.Logout(conn)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}
}