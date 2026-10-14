//go:build !unix

package db

import "os"

func lockDir(dir string) (*os.File, error) { return nil, nil }
