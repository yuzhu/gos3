//go:build !linux

package downloader

import "os"

// platformPreallocate falls back to ftruncate on non-Linux platforms.
// macOS has fcntl(F_PREALLOCATE) but it requires more complex handling
// and ftruncate is sufficient for correctness.
func platformPreallocate(f *os.File, size int64) error {
	return f.Truncate(size)
}
