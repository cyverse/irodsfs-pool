package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	godaemonizer "github.com/cyverse/go-daemonizer"
	"github.com/cyverse/irodsfs-pool/commons"
)

func TestConfigureForegroundPaths(t *testing.T) {
	workingDirectory := t.TempDir()
	t.Chdir(workingDirectory)

	config := commons.NewDefaultConfig()
	config.DataRootPath = "/unwritable/data"
	config.StagingRootPath = "/unwritable/data/staging"

	if err := configureForegroundPaths(config); err != nil {
		t.Fatalf("configureForegroundPaths: %v", err)
	}

	if config.DataRootPath != workingDirectory {
		t.Fatalf("DataRootPath = %q, want %q", config.DataRootPath, workingDirectory)
	}
	expectedStagingRootPath := filepath.Join(workingDirectory, commons.StagingRootPathDefault)
	if config.StagingRootPath != expectedStagingRootPath {
		t.Fatalf("StagingRootPath = %q, want %q", config.StagingRootPath, expectedStagingRootPath)
	}
	if config.GetLogRootPath() != workingDirectory {
		t.Fatalf("default log root = %q, want %q", config.GetLogRootPath(), workingDirectory)
	}
}

func TestConfigureForegroundPathsPreservesExplicitStagingRoot(t *testing.T) {
	workingDirectory := t.TempDir()
	t.Chdir(workingDirectory)

	config := commons.NewDefaultConfig()
	config.StagingRootPath = "/explicit/staging"

	if err := configureForegroundPaths(config); err != nil {
		t.Fatalf("configureForegroundPaths: %v", err)
	}

	if config.StagingRootPath != "/explicit/staging" {
		t.Fatalf("StagingRootPath = %q, want explicit path", config.StagingRootPath)
	}
}

func TestRootCommandContainsLifecycleCommands(t *testing.T) {
	root := newRootCommand(godaemonizer.New())
	for _, name := range []string{"start", "run", "stop", "status", "version"} {
		command, _, err := root.Find([]string{name})
		if err != nil {
			t.Fatalf("find command %q: %v", name, err)
		}
		if command.Name() != name {
			t.Fatalf("command name = %q, want %q", command.Name(), name)
		}
	}
}

func TestLoadConfig(t *testing.T) {
	configPath := writeTestConfig(t, os.Getpid())
	root := newRootCommand(godaemonizer.New())
	if err := root.PersistentFlags().Set("config", configPath); err != nil {
		t.Fatalf("set config flag: %v", err)
	}

	config, err := loadConfig(root)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if config.PIDFile == "" {
		t.Fatal("PIDFile is empty")
	}
}

func TestStatusCommandRecognizesRunningProcess(t *testing.T) {
	configPath := writeTestConfig(t, os.Getpid())
	out := &bytes.Buffer{}
	root := newRootCommand(godaemonizer.New())
	root.SetOut(out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"status", "--config", configPath})

	if err := root.Execute(); err != nil {
		t.Fatalf("execute status: %v", err)
	}
	if !strings.Contains(out.String(), strconv.Itoa(os.Getpid())) {
		t.Fatalf("status output = %q", out.String())
	}
}

func TestLifecycleCommandRejectsArguments(t *testing.T) {
	root := newRootCommand(godaemonizer.New())
	root.SetArgs([]string{"status", "unexpected"})
	if err := root.Execute(); err == nil {
		t.Fatal("status unexpectedly accepted a positional argument")
	}
}

func writeTestConfig(t *testing.T, pid int) string {
	t.Helper()
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "irodsfs-pool.pid")
	if err := os.WriteFile(pidPath, []byte(fmt.Sprintf("%d\n", pid)), 0o644); err != nil {
		t.Fatalf("write pid file: %v", err)
	}

	configPath := filepath.Join(dir, "config.yaml")
	config := fmt.Sprintf("service_endpoint: tcp://127.0.0.1:12020\ndata_root_path: %s\npid_file: %s\n", dir, pidPath)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return configPath
}
