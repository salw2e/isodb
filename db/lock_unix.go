//go:build unix

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockDir keeps a second server off the same data directory; the lock goes when the process does.
func lockDir(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("data directory %s is in use by another isodb", dir)
	}
	return f, nil
}
