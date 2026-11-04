package storage

import "golang.org/x/sys/windows"

func diskFree(path string) (int64, error) {
	p, e := windows.UTF16PtrFromString(path)
	if e != nil {
		return 0, e
	}
	var free, total, unused uint64
	e = windows.GetDiskFreeSpaceEx(p, &free, &total, &unused)
	return int64(min(free, uint64(1<<63-1))), e
}

// Directory fsync is not exposed consistently on Windows. Atomic rename and file
// FlushFileBuffers are used; power-loss durability of the directory is not claimed.
func syncDirectory(string) error { return nil }
