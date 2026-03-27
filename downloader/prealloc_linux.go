package downloader

import (
	"os"
	"syscall"
)

// platformPreallocate uses fallocate(2) on Linux for true zero-cost
// space reservation. This avoids writing zeros and creates a single
// extent, reducing fragmentation and metadata overhead.
func platformPreallocate(f *os.File, size int64) error {
	// Try fallocate first — it's the optimal path:
	// - No zeroing of blocks
	// - Allocates contiguous extents when possible
	// - Returns ENOTSUP on filesystems that don't support it
	err := syscall.Fallocate(int(f.Fd()), 0, 0, size)
	if err == nil {
		return nil
	}

	// Fallback to ftruncate for filesystems that don't support fallocate
	// (e.g., NFS, some FUSE mounts)
	return f.Truncate(size)
}
