//go:build !windows && !linux

package storage

func diskFree(string) (int64, error) { return 1<<63 - 1, nil }
func syncDirectory(string) error     { return nil }
