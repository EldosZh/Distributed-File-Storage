package storage

import (
	"golang.org/x/sys/unix"
	"os"
)

func diskFree(path string) (int64, error) {
	var s unix.Statfs_t
	e := unix.Statfs(path, &s)
	return int64(s.Bavail) * s.Bsize, e
}
func syncDirectory(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
