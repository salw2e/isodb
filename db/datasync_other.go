//go:build !linux

package db

import "os"

func datasync(f *os.File) error { return f.Sync() }
