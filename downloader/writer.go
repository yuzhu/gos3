package downloader

import (
	"fmt"
	"os"
	"path/filepath"
)

// SectionWriter implements io.Writer that writes to a specific offset in a file.
// It advances the offset as data is written, allowing parallel chunk writes
// to different regions of the same file without seeking conflicts.
type SectionWriter struct {
	file    *os.File
	offset  int64
	initOff int64
}

// NewSectionWriter creates a writer that writes to file starting at the given offset.
func NewSectionWriter(f *os.File, offset int64) *SectionWriter {
	return &SectionWriter{file: f, offset: offset, initOff: offset}
}

// Write writes p to the underlying file at the current offset.
// It uses Pwrite (pwrite syscall) which is atomic and does not affect
// the file's seek position, making it safe for concurrent use by
// multiple SectionWriters on the same file descriptor.
func (sw *SectionWriter) Write(p []byte) (int, error) {
	n, err := sw.file.WriteAt(p, sw.offset)
	sw.offset += int64(n)
	return n, err
}

// Written returns the number of bytes written since creation.
func (sw *SectionWriter) Written() int64 {
	return sw.offset - sw.initOff
}

// preallocateFile creates or truncates the file and preallocates the given size.
// On Linux, this uses fallocate(2) for true zero-cost space reservation
// (no zeroing, no metadata overhead for extents). On other platforms,
// it falls back to ftruncate which may write zeros.
func preallocateFile(path string, size int64) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("open file for preallocation: %w", err)
	}

	if err := platformPreallocate(f, size); err != nil {
		f.Close()
		return nil, fmt.Errorf("preallocate file: %w", err)
	}

	return f, nil
}

// prepareTempPath returns a temporary file path for atomic writes.
// The file is written to path + ".tmp" and renamed on completion.
func prepareTempPath(outputPath string) string {
	dir := filepath.Dir(outputPath)
	base := filepath.Base(outputPath)
	return filepath.Join(dir, "."+base+".tmp")
}

// atomicRename renames src to dst. On Unix this is atomic if src and dst
// are on the same filesystem.
func atomicRename(src, dst string) error {
	return os.Rename(src, dst)
}
