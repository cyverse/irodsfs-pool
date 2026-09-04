package commons

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/cockroachdb/errors"
)

// PIDFile holds an exclusive lock on a process ID file.
type PIDFile struct {
	file *os.File
	path string
	pid  int
}

// AcquirePIDFile creates and exclusively locks a PID file for this process.
func AcquirePIDFile(path string) (*PIDFile, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("pid file path cannot be empty")
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, errors.Wrapf(err, "create pid directory for %q", path)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, errors.Wrapf(err, "open pid file %q", path)
	}

	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errors.Newf("irodsfs-pool is already running (pid file: %s)", path)
		}
		return nil, errors.Wrapf(err, "lock pid file %q", path)
	}

	pid := os.Getpid()
	if err := file.Truncate(0); err != nil {
		_ = releaseFile(file)
		return nil, errors.Wrapf(err, "truncate pid file %q", path)
	}
	if _, err := file.Seek(0, 0); err != nil {
		_ = releaseFile(file)
		return nil, errors.Wrapf(err, "seek pid file %q", path)
	}
	if _, err := fmt.Fprintf(file, "%d\n", pid); err != nil {
		_ = releaseFile(file)
		return nil, errors.Wrapf(err, "write pid file %q", path)
	}
	if err := file.Sync(); err != nil {
		_ = releaseFile(file)
		return nil, errors.Wrapf(err, "sync pid file %q", path)
	}

	return &PIDFile{
		file: file,
		path: path,
		pid:  pid,
	}, nil
}

// Close removes the owned PID file and releases its lock.
func (p *PIDFile) Close() error {
	if p == nil || p.file == nil {
		return nil
	}

	// Do not remove a replacement PID file created by another process.
	data, readErr := os.ReadFile(p.path)
	if readErr == nil {
		pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
		if parseErr == nil && pid == p.pid {
			_ = os.Remove(p.path)
		}
	}

	err := releaseFile(p.file)
	p.file = nil
	return err
}

func releaseFile(file *os.File) error {
	unlockErr := syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	closeErr := file.Close()
	return errors.Join(unlockErr, closeErr)
}

// ReadPID reads and validates a PID from a PID file.
func ReadPID(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, errors.Newf("irodsfs-pool is not running: pid file %q does not exist", path)
		}
		return 0, errors.Wrapf(err, "read pid file %q", path)
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 1 {
		return 0, errors.Newf("pid file %q contains an invalid pid", path)
	}
	return pid, nil
}

// ProcessRunning reports whether a process exists or cannot be inspected due
// to insufficient permission.
func ProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	err = process.Signal(syscall.Signal(0))
	return err == nil || errors.Is(err, syscall.EPERM)
}

// SignalPID sends a signal to a process and reports stale PID files clearly.
func SignalPID(pid int, signal syscall.Signal) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return errors.Wrapf(err, "find process %d", pid)
	}

	if err := process.Signal(signal); err != nil {
		if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
			return errors.Newf("irodsfs-pool is not running (stale pid %d)", pid)
		}
		return errors.Wrapf(err, "signal process %d", pid)
	}
	return nil
}
