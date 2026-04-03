package downloader

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MultiConfig extends Config with multi-file download settings.
type MultiConfig struct {
	*Config

	// S3 prefix to list and download (e.g., "models/v2/")
	Prefix string

	// Local directory to write files into, preserving path structure under prefix
	OutputDir string

	// Number of files to download simultaneously (default: 4)
	FileConcurrency int

	// If true, stop all downloads on first error. Otherwise continue and report.
	FailFast bool
}

// DefaultMultiConfig returns a MultiConfig with sensible defaults.
func DefaultMultiConfig() *MultiConfig {
	return &MultiConfig{
		Config:          DefaultConfig(),
		FileConcurrency: 4,
	}
}

// FileResult contains stats for a single file download within a multi-file operation.
type FileResult struct {
	Key        string
	LocalPath  string
	Size       int64
	Duration   time.Duration
	Error      error
	Skipped    bool // true if file already exists and matches size
}

// MultiResult contains aggregate stats for a multi-file download.
type MultiResult struct {
	TotalFiles      int
	TotalBytes      int64
	SucceededFiles  int
	FailedFiles     int
	SkippedFiles    int
	Duration        time.Duration
	FileResults     []FileResult
	Errors          []error
}

// DownloadPrefix lists all objects under a prefix and downloads them in parallel.
// Files are downloaded with two-level parallelism:
//   - FileConcurrency controls how many files download simultaneously
//   - Config.Concurrency controls chunk parallelism within each file
//
// Progress is reported via cfg.OnProgress (aggregate bytes across all files)
// and cfg.OnTotalSize (aggregate total size).
func DownloadPrefix(ctx context.Context, mcfg *MultiConfig) (*MultiResult, error) {
	if mcfg.Prefix == "" {
		return nil, fmt.Errorf("prefix is required for multi-file download")
	}
	if mcfg.OutputDir == "" {
		return nil, fmt.Errorf("output directory is required for multi-file download")
	}
	if mcfg.FileConcurrency < 1 {
		mcfg.FileConcurrency = 4
	}

	// Ensure output directory exists
	if err := os.MkdirAll(mcfg.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("create output directory %q: %w", mcfg.OutputDir, err)
	}

	client := newHTTPClient(mcfg.Config)
	start := time.Now()

	// Phase 1: List objects (pipelined — downloads begin as objects are listed)
	objectCh, listErrCh := ListObjects(ctx, client, mcfg.Config, mcfg.Prefix)

	// Collect all objects first so we can report total size for progress display.
	// For very large prefixes this adds latency, but total size is needed for
	// accurate progress percentage. The listing is fast compared to downloads.
	var objects []S3Object
	for obj := range objectCh {
		objects = append(objects, obj)
	}

	// Check for listing errors
	if err := <-listErrCh; err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}

	if len(objects) == 0 {
		return nil, fmt.Errorf("no objects found under prefix %q", mcfg.Prefix)
	}

	// Calculate and report total size
	var totalSize int64
	for _, obj := range objects {
		totalSize += obj.Size
	}

	log.Printf("[info] found %d objects under prefix %q (total: %s)",
		len(objects), mcfg.Prefix, formatBytesHuman(totalSize))

	if mcfg.Config.OnTotalSize != nil {
		mcfg.Config.OnTotalSize(totalSize)
	}

	// Phase 2: Download files in parallel
	var (
		mu             sync.Mutex
		fileResults    []FileResult
		succeededFiles int64
		failedFiles    int64
		skippedFiles   int64
		totalDownloaded int64
		firstErr       error
	)

	// Shared progress counter across ALL files for aggregate display
	var globalBytesCounter int64

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, mcfg.FileConcurrency)
	var wg sync.WaitGroup

	for _, obj := range objects {
		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		sem <- struct{}{} // Acquire file slot

		go func(obj S3Object) {
			defer wg.Done()
			defer func() { <-sem }() // Release file slot

			// Map S3 key to local path (strip prefix)
			relativePath := strings.TrimPrefix(obj.Key, mcfg.Prefix)
			if relativePath == "" {
				relativePath = filepath.Base(obj.Key)
			}
			localPath := filepath.Join(mcfg.OutputDir, relativePath)

			// Skip if file exists and matches size
			if info, err := os.Stat(localPath); err == nil && info.Size() == obj.Size {
				if mcfg.Config.Verbose {
					log.Printf("[skip] %s (already exists, %d bytes)", obj.Key, obj.Size)
				}
				atomic.AddInt64(&skippedFiles, 1)
				// Count skipped bytes toward progress
				atomic.AddInt64(&globalBytesCounter, obj.Size)
				if mcfg.Config.OnProgress != nil {
					mcfg.Config.OnProgress(atomic.LoadInt64(&globalBytesCounter))
				}
				mu.Lock()
				fileResults = append(fileResults, FileResult{
					Key:       obj.Key,
					LocalPath: localPath,
					Size:      obj.Size,
					Skipped:   true,
				})
				mu.Unlock()
				return
			}

			// Create per-file config (clone to avoid races on Key/OutputPath)
			fileCfg := *mcfg.Config
			fileCfg.Key = obj.Key
			fileCfg.OutputPath = localPath

			// Wire per-file progress into the global counter
			fileCfg.OnProgress = func(perFileBytesTotal int64) {
				// We can't simply forward — we need to track the delta
				// that this file contributes. However, the counter inside
				// Download() is per-file. We'll use the global counter directly.
			}
			fileCfg.OnTotalSize = nil // Already aggregated above

			// Redirect the per-file byte counter into the global one
			fileStart := time.Now()
			var preDownloadGlobal int64 = atomic.LoadInt64(&globalBytesCounter)
			_ = preDownloadGlobal

			// Use a custom OnProgress that adds deltas to the global counter
			var lastReported int64
			fileCfg.OnProgress = func(perFileBytes int64) {
				delta := perFileBytes - lastReported
				if delta > 0 {
					lastReported = perFileBytes
					newGlobal := atomic.AddInt64(&globalBytesCounter, delta)
					if mcfg.Config.OnProgress != nil {
						mcfg.Config.OnProgress(newGlobal)
					}
				}
			}

			result, err := Download(ctx, &fileCfg)
			elapsed := time.Since(fileStart)

			fr := FileResult{
				Key:       obj.Key,
				LocalPath: localPath,
				Duration:  elapsed,
			}

			if err != nil {
				fr.Error = err
				atomic.AddInt64(&failedFiles, 1)

				if mcfg.Config.Verbose {
					log.Printf("[error] %s: %v", obj.Key, err)
				}

				if mcfg.FailFast {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("%s: %w", obj.Key, err)
						cancel()
					}
					mu.Unlock()
				}
			} else {
				fr.Size = result.TotalBytes
				atomic.AddInt64(&succeededFiles, 1)
				atomic.AddInt64(&totalDownloaded, result.TotalBytes)

				if mcfg.Config.Verbose {
					speed := float64(result.TotalBytes) / (1024 * 1024) / elapsed.Seconds()
					log.Printf("[done] %s (%s, %.1f MB/s)",
						obj.Key, formatBytesHuman(result.TotalBytes), speed)
				}
			}

			mu.Lock()
			fileResults = append(fileResults, fr)
			mu.Unlock()
		}(obj)
	}

	wg.Wait()

	elapsed := time.Since(start)

	// Collect errors
	var errors []error
	for _, fr := range fileResults {
		if fr.Error != nil {
			errors = append(errors, fmt.Errorf("%s: %w", fr.Key, fr.Error))
		}
	}

	multiResult := &MultiResult{
		TotalFiles:     len(objects),
		TotalBytes:     atomic.LoadInt64(&totalDownloaded),
		SucceededFiles: int(atomic.LoadInt64(&succeededFiles)),
		FailedFiles:    int(atomic.LoadInt64(&failedFiles)),
		SkippedFiles:   int(atomic.LoadInt64(&skippedFiles)),
		Duration:       elapsed,
		FileResults:    fileResults,
		Errors:         errors,
	}

	if mcfg.FailFast && firstErr != nil {
		return multiResult, firstErr
	}

	if len(errors) > 0 {
		return multiResult, fmt.Errorf("%d of %d files failed", len(errors), len(objects))
	}

	return multiResult, nil
}

func formatBytesHuman(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
