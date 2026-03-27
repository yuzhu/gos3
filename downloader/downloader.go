package downloader

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// bufPool reuses 256KB buffers across chunk downloads to reduce GC pressure.
// Each parallel goroutine needs one buffer, so pooling avoids N allocations.
var bufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 256*1024)
		return &b
	},
}

// ProgressFunc is called periodically with the total number of bytes
// downloaded so far. It is called from multiple goroutines and must be safe
// for concurrent use.
type ProgressFunc func(bytesDownloaded int64)

// Config holds all configuration for a download operation.
type Config struct {
	// S3 endpoint URL (e.g., "http://alluxio-proxy:29998")
	Endpoint string
	// S3 bucket name
	Bucket string
	// S3 object key (path within bucket)
	Key string
	// Local file path to write the downloaded object to
	OutputPath string

	// AWS credentials (can be empty for anonymous access)
	AccessKey string
	SecretKey string
	// AWS region (default: "us-east-1")
	Region string

	// Number of parallel chunk downloads (default: 16)
	Concurrency int
	// Size of each download chunk in bytes (default: 64MB)
	ChunkSize int64
	// Per-chunk download timeout (default: 5 minutes)
	ChunkTimeout time.Duration

	// Maximum retries per chunk on transient errors (default: 3)
	MaxRetries int
	// Base delay for exponential backoff between retries (default: 500ms)
	RetryBaseDelay time.Duration

	// Skip TLS certificate verification
	InsecureTLS bool
	// Enable verbose logging (redirects, chunk progress)
	Verbose bool
	// Skip ETag/MD5 checksum verification after download
	SkipChecksumVerify bool

	// ProgressFunc is called with cumulative bytes downloaded.
	// Called from multiple goroutines — must be goroutine-safe.
	OnProgress ProgressFunc

	// OnTotalSize is called once with the total file size after the HEAD request.
	OnTotalSize func(totalBytes int64)
}

// DefaultConfig returns a Config with sensible defaults filled in.
func DefaultConfig() *Config {
	return &Config{
		Region:         "us-east-1",
		Concurrency:    16,
		ChunkSize:      64 * 1024 * 1024, // 64MB
		ChunkTimeout:   5 * time.Minute,
		MaxRetries:     3,
		RetryBaseDelay: 500 * time.Millisecond,
	}
}

// Result contains statistics from a completed download.
type Result struct {
	// Total bytes downloaded
	TotalBytes int64
	// Wall-clock duration
	Duration time.Duration
	// Throughput in bytes/sec
	Throughput float64
	// Number of chunks used
	NumChunks int
	// Total retries across all chunks
	TotalRetries int64
	// ETag from server (if available)
	ETag string
	// Whether checksum was verified
	ChecksumVerified bool
}

// countingReader wraps an io.Reader and atomically increments a shared counter
// as bytes are read. This drives the live progress display.
type countingReader struct {
	reader  io.Reader
	counter *int64        // shared atomic counter
	onRead  ProgressFunc  // optional callback
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.reader.Read(p)
	if n > 0 {
		newTotal := atomic.AddInt64(cr.counter, int64(n))
		if cr.onRead != nil {
			cr.onRead(newTotal)
		}
	}
	return n, err
}

// Download downloads an S3 object to the local filesystem.
// It uses parallel range requests for large files and direct single-request
// download for small files. Returns download statistics.
func Download(ctx context.Context, cfg *Config) (*Result, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	client := newHTTPClient(cfg)
	start := time.Now()

	// Shared atomic counters for live progress across all goroutines
	var bytesCounter int64
	var totalRetries int64

	// Step 1: HEAD request to get object size and ETag
	contentLength, etag, err := headObject(ctx, client, cfg)
	if err != nil {
		return nil, fmt.Errorf("HEAD object: %w", err)
	}

	if cfg.OnTotalSize != nil {
		cfg.OnTotalSize(contentLength)
	}

	if cfg.Verbose {
		log.Printf("[info] object size: %d bytes (%.2f MB), etag=%s",
			contentLength, float64(contentLength)/(1024*1024), etag)
	}

	// Step 2: Prepare output file with atomic write pattern
	tmpPath := prepareTempPath(cfg.OutputPath)

	// Ensure parent directory exists
	if err := os.MkdirAll(getDir(cfg.OutputPath), 0755); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}

	// Step 3: Download
	var totalBytes int64
	var numChunks int

	if contentLength <= cfg.ChunkSize {
		// Small file: single GET request (with retry)
		totalBytes, err = downloadSingleWithRetry(ctx, client, cfg, tmpPath, contentLength, &bytesCounter, &totalRetries)
		numChunks = 1
	} else {
		// Large file: parallel range downloads
		totalBytes, numChunks, err = downloadParallel(ctx, client, cfg, tmpPath, contentLength, &bytesCounter, &totalRetries)
	}

	if err != nil {
		os.Remove(tmpPath) // Clean up on failure
		return nil, err
	}

	// Verify we got all the bytes
	if totalBytes != contentLength {
		os.Remove(tmpPath)
		return nil, fmt.Errorf("size mismatch: expected %d bytes, got %d", contentLength, totalBytes)
	}

	// Step 4: Verify ETag/MD5 checksum if available
	checksumOK := false
	if !cfg.SkipChecksumVerify && isSimpleETag(etag) {
		if err := verifyMD5(tmpPath, etag); err != nil {
			os.Remove(tmpPath)
			return nil, fmt.Errorf("checksum verification failed: %w", err)
		}
		checksumOK = true
		if cfg.Verbose {
			log.Printf("[info] checksum verified (ETag=%s)", etag)
		}
	}

	// Step 5: Atomic rename to final path
	if err := atomicRename(tmpPath, cfg.OutputPath); err != nil {
		os.Remove(tmpPath)
		return nil, fmt.Errorf("rename to final path: %w", err)
	}

	elapsed := time.Since(start)
	return &Result{
		TotalBytes:       totalBytes,
		Duration:         elapsed,
		Throughput:       float64(totalBytes) / elapsed.Seconds(),
		NumChunks:        numChunks,
		TotalRetries:     atomic.LoadInt64(&totalRetries),
		ETag:             etag,
		ChecksumVerified: checksumOK,
	}, nil
}

// headObject performs a HEAD request to get the object's Content-Length and ETag.
// The ETag is used for post-download MD5 verification (for non-multipart uploads).
// HEAD may also be 307-redirected by Alluxio, which is handled by the client's
// CheckRedirect function.
func headObject(ctx context.Context, client *http.Client, cfg *Config) (int64, string, error) {
	reqURL := buildObjectURL(cfg)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, reqURL, nil)
	if err != nil {
		return 0, "", fmt.Errorf("create HEAD request: %w", err)
	}

	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		signRequest(req, cfg.AccessKey, cfg.SecretKey, cfg.Region, "s3")
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("execute HEAD request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Drain body to allow connection reuse
		io.Copy(io.Discard, resp.Body)
		return 0, "", fmt.Errorf("HEAD request returned %d %s", resp.StatusCode, resp.Status)
	}

	if resp.ContentLength < 0 {
		return 0, "", fmt.Errorf("server did not return Content-Length")
	}

	// Capture ETag for checksum verification
	etag := resp.Header.Get("ETag")

	return resp.ContentLength, etag, nil
}

// downloadSingle performs a single GET request for small files.
// Uses io.Copy which Go optimizes to sendfile/splice on Linux.
func downloadSingle(ctx context.Context, client *http.Client, cfg *Config, outPath string, size int64, counter *int64) (int64, error) {
	if cfg.Verbose {
		log.Printf("[info] single-request download (size=%d)", size)
	}

	reqURL := buildObjectURL(cfg)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return 0, fmt.Errorf("create GET request: %w", err)
	}

	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		signRequest(req, cfg.AccessKey, cfg.SecretKey, cfg.Region, "s3")
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("execute GET request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return 0, fmt.Errorf("GET request returned %d %s", resp.StatusCode, resp.Status)
	}

	f, err := preallocateFile(outPath, size)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// Wrap resp.Body with countingReader for live progress
	reader := &countingReader{reader: resp.Body, counter: counter, onRead: cfg.OnProgress}

	bufp := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufp)
	n, err := io.CopyBuffer(f, reader, *bufp)
	if err != nil {
		return n, fmt.Errorf("write response body: %w", err)
	}

	if err := f.Sync(); err != nil {
		return n, fmt.Errorf("fsync: %w", err)
	}

	return n, nil
}

// downloadSingleWithRetry wraps downloadSingle with exponential backoff retry.
func downloadSingleWithRetry(ctx context.Context, client *http.Client, cfg *Config, outPath string, size int64, counter *int64, retryCounter *int64) (int64, error) {
	var lastErr error

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := cfg.RetryBaseDelay * time.Duration(1<<uint(attempt-1))
			jitter := time.Duration(rand.Int63n(int64(delay / 4)))
			delay += jitter

			if cfg.Verbose {
				log.Printf("[retry] single download attempt %d/%d after %v (err: %v)",
					attempt+1, cfg.MaxRetries+1, delay, lastErr)
			}

			atomic.AddInt64(retryCounter, 1)

			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(delay):
			}

			// Clean up temp file from previous failed attempt
			os.Remove(outPath)
		}

		preAttemptCounter := atomic.LoadInt64(counter)

		n, err := downloadSingle(ctx, client, cfg, outPath, size, counter)
		if err == nil {
			return n, nil
		}

		lastErr = err

		// Roll back counter
		currentCounter := atomic.LoadInt64(counter)
		if partial := currentCounter - preAttemptCounter; partial > 0 {
			atomic.AddInt64(counter, -partial)
		}

		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
	}

	return 0, fmt.Errorf("all %d attempts failed: %w", cfg.MaxRetries+1, lastErr)
}

// chunk represents a byte range to download.
type chunk struct {
	index int
	start int64
	end   int64 // inclusive
}

// downloadParallel downloads a large file using parallel range requests.
// The file is split into chunks, each downloaded by a separate goroutine.
// All goroutines write to different offsets of the same preallocated file.
func downloadParallel(ctx context.Context, client *http.Client, cfg *Config, outPath string, totalSize int64, counter *int64, retryCounter *int64) (int64, int, error) {
	// Calculate chunks
	chunks := splitIntoChunks(totalSize, cfg.ChunkSize)
	numChunks := len(chunks)

	if cfg.Verbose {
		log.Printf("[info] parallel download: %d chunks, %d concurrent, chunk_size=%d",
			numChunks, cfg.Concurrency, cfg.ChunkSize)
	}

	// Preallocate the output file
	f, err := preallocateFile(outPath, totalSize)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	// Download chunks in parallel with bounded concurrency
	var (
		totalWritten int64
		mu           sync.Mutex
		firstErr     error
	)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, cfg.Concurrency)
	var wg sync.WaitGroup

	var completedChunks atomic.Int64

	for _, c := range chunks {
		// Check if context is already cancelled (or first error triggered cancel)
		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		sem <- struct{}{} // Acquire semaphore slot

		go func(c chunk) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore slot

			n, err := downloadChunkWithRetry(ctx, client, cfg, f, c, counter, retryCounter)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("chunk %d [%d-%d]: %w", c.index, c.start, c.end, err)
					cancel() // Cancel remaining chunks on first error
				}
				mu.Unlock()
				return
			}

			atomic.AddInt64(&totalWritten, n)
			completed := completedChunks.Add(1)

			if cfg.Verbose {
				log.Printf("[progress] chunk %d/%d complete (%d bytes)",
					completed, numChunks, n)
			}
		}(c)
	}

	wg.Wait()

	if firstErr != nil {
		return atomic.LoadInt64(&totalWritten), numChunks, firstErr
	}

	// Sync to disk
	if err := f.Sync(); err != nil {
		return atomic.LoadInt64(&totalWritten), numChunks, fmt.Errorf("fsync: %w", err)
	}

	return atomic.LoadInt64(&totalWritten), numChunks, nil
}

// downloadChunkWithRetry wraps downloadChunk with exponential backoff retry.
// On failure, the progress counter is rolled back by the number of bytes
// partially read so the next attempt re-counts from the chunk start.
func downloadChunkWithRetry(ctx context.Context, client *http.Client, cfg *Config, f *os.File, c chunk, counter *int64, retryCounter *int64) (int64, error) {
	var lastErr error

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff with jitter: base * 2^(attempt-1) + random jitter
			delay := cfg.RetryBaseDelay * time.Duration(1<<uint(attempt-1))
			jitter := time.Duration(rand.Int63n(int64(delay / 4)))
			delay += jitter

			if cfg.Verbose {
				log.Printf("[retry] chunk %d attempt %d/%d after %v (err: %v)",
					c.index, attempt+1, cfg.MaxRetries+1, delay, lastErr)
			}

			atomic.AddInt64(retryCounter, 1)

			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(delay):
			}
		}

		// Record counter before attempt so we can roll back on failure
		preAttemptCounter := atomic.LoadInt64(counter)

		n, err := downloadChunk(ctx, client, cfg, f, c, counter)
		if err == nil {
			return n, nil
		}

		lastErr = err

		// Roll back the counter for bytes partially read in this failed attempt
		currentCounter := atomic.LoadInt64(counter)
		partialBytes := currentCounter - preAttemptCounter
		if partialBytes > 0 {
			atomic.AddInt64(counter, -partialBytes)
		}

		// Don't retry on context cancellation
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
	}

	return 0, fmt.Errorf("all %d attempts failed: %w", cfg.MaxRetries+1, lastErr)
}

// downloadChunk downloads a single byte range and writes it to the file
// at the correct offset using SectionWriter + io.Copy (zero-copy path).
func downloadChunk(ctx context.Context, client *http.Client, cfg *Config, f *os.File, c chunk, counter *int64) (int64, error) {
	chunkCtx, chunkCancel := context.WithTimeout(ctx, cfg.ChunkTimeout)
	defer chunkCancel()

	reqURL := buildObjectURL(cfg)
	req, err := http.NewRequestWithContext(chunkCtx, http.MethodGet, reqURL, nil)
	if err != nil {
		return 0, fmt.Errorf("create range GET request: %w", err)
	}

	// Set Range header: bytes=start-end (inclusive)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", c.start, c.end))

	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		signRequest(req, cfg.AccessKey, cfg.SecretKey, cfg.Region, "s3")
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("execute range GET: %w", err)
	}
	defer resp.Body.Close()

	// Accept both 200 (full content) and 206 (partial content)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		io.Copy(io.Discard, resp.Body)
		return 0, fmt.Errorf("range GET returned %d %s", resp.StatusCode, resp.Status)
	}

	expectedSize := c.end - c.start + 1
	sw := NewSectionWriter(f, c.start)

	// Wrap resp.Body with countingReader for live progress
	reader := &countingReader{reader: resp.Body, counter: counter, onRead: cfg.OnProgress}

	bufp := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufp)
	n, err := io.CopyBuffer(sw, reader, *bufp)
	if err != nil {
		return n, fmt.Errorf("copy response body: %w", err)
	}

	if n != expectedSize {
		return n, fmt.Errorf("size mismatch: expected %d, got %d", expectedSize, n)
	}

	return n, nil
}

// splitIntoChunks divides totalSize into chunks of chunkSize bytes.
// The last chunk may be smaller.
func splitIntoChunks(totalSize, chunkSize int64) []chunk {
	var chunks []chunk
	var idx int
	for offset := int64(0); offset < totalSize; offset += chunkSize {
		end := offset + chunkSize - 1
		if end >= totalSize {
			end = totalSize - 1
		}
		chunks = append(chunks, chunk{index: idx, start: offset, end: end})
		idx++
	}
	return chunks
}

// buildObjectURL constructs the S3 path-style URL for the object.
// Uses path-style (required by Alluxio): http://endpoint/bucket/key
// Each segment of the key is individually percent-encoded to handle
// keys containing special characters (spaces, unicode, etc.).
func buildObjectURL(cfg *Config) string {
	// Encode each path segment of the key individually
	parts := strings.Split(cfg.Key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	encodedKey := strings.Join(parts, "/")
	return fmt.Sprintf("%s/%s/%s", cfg.Endpoint, url.PathEscape(cfg.Bucket), encodedKey)
}

func validateConfig(cfg *Config) error {
	if cfg.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}
	if cfg.Bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	if cfg.Key == "" {
		return fmt.Errorf("key is required")
	}
	if cfg.OutputPath == "" {
		return fmt.Errorf("output path is required")
	}
	if cfg.Concurrency < 1 {
		return fmt.Errorf("concurrency must be >= 1")
	}
	if cfg.ChunkSize < 1 {
		return fmt.Errorf("chunk size must be >= 1")
	}
	if cfg.MaxRetries < 0 {
		return fmt.Errorf("max retries must be >= 0")
	}
	return nil
}

// isSimpleETag returns true if the ETag is a simple MD5 hex string
// (not a multipart upload ETag which contains a "-" suffix).
// Only simple ETags can be verified against a local MD5 hash.
func isSimpleETag(etag string) bool {
	if etag == "" {
		return false
	}
	// Strip quotes
	etag = strings.Trim(etag, `"`)
	// Multipart ETags look like: "d41d8cd98f00b204e9800998ecf8427e-2"
	if strings.Contains(etag, "-") {
		return false
	}
	// Should be 32 hex chars (MD5)
	if len(etag) != 32 {
		return false
	}
	for _, c := range etag {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// verifyMD5 computes the MD5 hash of the file and compares it against
// the expected ETag (quotes stripped). Returns nil if they match.
func verifyMD5(path string, etag string) error {
	expected := strings.ToLower(strings.Trim(etag, `"`))

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open file for checksum: %w", err)
	}
	defer f.Close()

	h := md5.New()
	// Use a large buffer for efficient hashing of big files
	buf := make([]byte, 1024*1024) // 1MB
	if _, err := io.CopyBuffer(h, f, buf); err != nil {
		return fmt.Errorf("compute MD5: %w", err)
	}

	actual := hex.EncodeToString(h.Sum(nil))
	if actual != expected {
		return fmt.Errorf("MD5 mismatch: expected %s, got %s", expected, actual)
	}

	return nil
}

func getDir(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}
