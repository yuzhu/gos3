package downloader

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestSingleChunkDownload verifies that a file smaller than the chunk size
// is downloaded in a single GET request.
func TestSingleChunkDownload(t *testing.T) {
	// Generate random test data
	testData := make([]byte, 1024) // 1KB
	if _, err := rand.Read(testData); err != nil {
		t.Fatal(err)
	}

	var headCalls, getCalls atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls.Add(1)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			getCalls.Add(1)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
			w.Write(testData)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.bin")

	cfg := DefaultConfig()
	cfg.Endpoint = server.URL
	cfg.Bucket = "test-bucket"
	cfg.Key = "test-key"
	cfg.OutputPath = outputPath
	cfg.ChunkSize = 4096 // Larger than test data → single request
	cfg.Concurrency = 1

	result, err := Download(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	if result.TotalBytes != int64(len(testData)) {
		t.Errorf("expected %d bytes, got %d", len(testData), result.TotalBytes)
	}

	if result.NumChunks != 1 {
		t.Errorf("expected 1 chunk, got %d", result.NumChunks)
	}

	if headCalls.Load() != 1 {
		t.Errorf("expected 1 HEAD call, got %d", headCalls.Load())
	}

	if getCalls.Load() != 1 {
		t.Errorf("expected 1 GET call, got %d", getCalls.Load())
	}

	// Verify content
	downloaded, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if len(downloaded) != len(testData) {
		t.Fatalf("output size mismatch: %d vs %d", len(downloaded), len(testData))
	}
	for i := range testData {
		if downloaded[i] != testData[i] {
			t.Fatalf("content mismatch at byte %d", i)
		}
	}
}

// TestParallelDownload verifies that a file larger than the chunk size
// is split into chunks and reassembled correctly.
func TestParallelDownload(t *testing.T) {
	// 256KB test data, 64KB chunks → 4 chunks
	testData := make([]byte, 256*1024)
	if _, err := rand.Read(testData); err != nil {
		t.Fatal(err)
	}

	var rangeCalls atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			rangeHeader := r.Header.Get("Range")
			if rangeHeader == "" {
				// No range — serve full file
				w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
				w.WriteHeader(http.StatusOK)
				w.Write(testData)
				return
			}

			rangeCalls.Add(1)

			// Parse Range header: bytes=start-end
			var start, end int64
			fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)

			if start < 0 || end >= int64(len(testData)) || start > end {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}

			data := testData[start : end+1]
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(data)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.bin")

	cfg := DefaultConfig()
	cfg.Endpoint = server.URL
	cfg.Bucket = "test-bucket"
	cfg.Key = "large-file"
	cfg.OutputPath = outputPath
	cfg.ChunkSize = 64 * 1024 // 64KB chunks
	cfg.Concurrency = 4

	result, err := Download(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	if result.TotalBytes != int64(len(testData)) {
		t.Errorf("expected %d bytes, got %d", len(testData), result.TotalBytes)
	}

	if result.NumChunks != 4 {
		t.Errorf("expected 4 chunks, got %d", result.NumChunks)
	}

	if rangeCalls.Load() != 4 {
		t.Errorf("expected 4 range GET calls, got %d", rangeCalls.Load())
	}

	// Verify content byte-by-byte
	downloaded, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if len(downloaded) != len(testData) {
		t.Fatalf("output size mismatch: %d vs %d", len(downloaded), len(testData))
	}
	for i := range testData {
		if downloaded[i] != testData[i] {
			t.Fatalf("content mismatch at byte %d: expected 0x%02x, got 0x%02x",
				i, testData[i], downloaded[i])
		}
	}
}

// TestRedirect307 verifies that 307 redirects are followed correctly,
// simulating Alluxio's proxy→worker redirect pattern.
func TestRedirect307(t *testing.T) {
	testData := []byte("hello from the worker")

	// Worker server: serves the actual data
	var workerHits atomic.Int64
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		workerHits.Add(1)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
		w.WriteHeader(http.StatusOK)
		w.Write(testData)
	}))
	defer worker.Close()

	// Proxy server: returns 307 redirect to worker
	var proxyHits atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyHits.Add(1)
		switch r.Method {
		case http.MethodHead:
			// HEAD doesn't redirect — returns size directly
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			// 307 redirect to worker
			w.Header().Set("Location", worker.URL+r.URL.Path)
			w.WriteHeader(http.StatusTemporaryRedirect)
		}
	}))
	defer proxy.Close()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.bin")

	cfg := DefaultConfig()
	cfg.Endpoint = proxy.URL
	cfg.Bucket = "test-bucket"
	cfg.Key = "redirected-file"
	cfg.OutputPath = outputPath
	cfg.Verbose = true

	result, err := Download(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	if result.TotalBytes != int64(len(testData)) {
		t.Errorf("expected %d bytes, got %d", len(testData), result.TotalBytes)
	}

	// Proxy should have been hit for HEAD + GET
	if proxyHits.Load() != 2 {
		t.Errorf("expected 2 proxy hits (HEAD+GET), got %d", proxyHits.Load())
	}

	// Worker should have been hit once (from redirect)
	if workerHits.Load() != 1 {
		t.Errorf("expected 1 worker hit, got %d", workerHits.Load())
	}

	// Verify content
	downloaded, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if string(downloaded) != string(testData) {
		t.Errorf("content mismatch: %q vs %q", string(downloaded), string(testData))
	}
}

// TestRedirect307WithRangeRequests verifies that range headers are preserved
// through 307 redirects (critical for parallel chunk downloads via Alluxio).
func TestRedirect307WithRangeRequests(t *testing.T) {
	testData := make([]byte, 128*1024) // 128KB
	if _, err := rand.Read(testData); err != nil {
		t.Fatal(err)
	}

	// Worker: serves range requests
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
			w.Write(testData)
			return
		}

		var start, end int64
		fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)
		data := testData[start : end+1]
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data)
	}))
	defer worker.Close()

	// Proxy: 307 redirects all GETs to worker
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			w.Header().Set("Location", worker.URL+r.URL.Path)
			w.WriteHeader(http.StatusTemporaryRedirect)
		}
	}))
	defer proxy.Close()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.bin")

	cfg := DefaultConfig()
	cfg.Endpoint = proxy.URL
	cfg.Bucket = "test-bucket"
	cfg.Key = "large-redirected"
	cfg.OutputPath = outputPath
	cfg.ChunkSize = 32 * 1024 // 32KB chunks → 4 chunks
	cfg.Concurrency = 4
	cfg.Verbose = true

	result, err := Download(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	if result.TotalBytes != int64(len(testData)) {
		t.Errorf("expected %d bytes, got %d", len(testData), result.TotalBytes)
	}

	if result.NumChunks != 4 {
		t.Errorf("expected 4 chunks, got %d", result.NumChunks)
	}

	// Verify full content integrity
	downloaded, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	for i := range testData {
		if downloaded[i] != testData[i] {
			t.Fatalf("content mismatch at byte %d", i)
		}
	}
}

// TestSectionWriter verifies that SectionWriter writes to correct offsets.
func TestSectionWriter(t *testing.T) {
	tmpFile, err := os.CreateTemp(t.TempDir(), "section-test")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpFile.Close()

	// Preallocate 100 bytes
	if err := tmpFile.Truncate(100); err != nil {
		t.Fatal(err)
	}

	// Write "hello" at offset 10
	sw1 := NewSectionWriter(tmpFile, 10)
	n, err := sw1.Write([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Errorf("expected 5 bytes written, got %d", n)
	}

	// Write "world" at offset 50
	sw2 := NewSectionWriter(tmpFile, 50)
	n, err = sw2.Write([]byte("world"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Errorf("expected 5 bytes written, got %d", n)
	}

	// Read back and verify
	data := make([]byte, 100)
	if _, err := tmpFile.ReadAt(data, 0); err != nil {
		t.Fatal(err)
	}

	if string(data[10:15]) != "hello" {
		t.Errorf("expected 'hello' at offset 10, got %q", string(data[10:15]))
	}
	if string(data[50:55]) != "world" {
		t.Errorf("expected 'world' at offset 50, got %q", string(data[50:55]))
	}
}

// TestSplitIntoChunks verifies chunk splitting logic.
func TestSplitIntoChunks(t *testing.T) {
	tests := []struct {
		name       string
		totalSize  int64
		chunkSize  int64
		wantChunks int
		lastEnd    int64
	}{
		{"exact_division", 100, 25, 4, 99},
		{"remainder", 100, 30, 4, 99},
		{"single_chunk", 50, 100, 1, 49},
		{"one_byte", 1, 100, 1, 0},
		{"large_file", 1024 * 1024 * 1024, 64 * 1024 * 1024, 16, 1024*1024*1024 - 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks := splitIntoChunks(tt.totalSize, tt.chunkSize)

			if len(chunks) != tt.wantChunks {
				t.Errorf("expected %d chunks, got %d", tt.wantChunks, len(chunks))
			}

			// Verify first chunk starts at 0
			if chunks[0].start != 0 {
				t.Errorf("first chunk should start at 0, got %d", chunks[0].start)
			}

			// Verify last chunk ends at totalSize-1
			lastChunk := chunks[len(chunks)-1]
			if lastChunk.end != tt.lastEnd {
				t.Errorf("last chunk should end at %d, got %d", tt.lastEnd, lastChunk.end)
			}

			// Verify no gaps or overlaps
			for i := 1; i < len(chunks); i++ {
				if chunks[i].start != chunks[i-1].end+1 {
					t.Errorf("gap/overlap between chunk %d (end=%d) and %d (start=%d)",
						i-1, chunks[i-1].end, i, chunks[i].start)
				}
			}

			// Verify total coverage
			var totalCovered int64
			for _, c := range chunks {
				totalCovered += c.end - c.start + 1
			}
			if totalCovered != tt.totalSize {
				t.Errorf("chunks cover %d bytes, expected %d", totalCovered, tt.totalSize)
			}
		})
	}
}

// TestContextCancellation verifies that downloads respect context cancellation.
func TestContextCancellation(t *testing.T) {
	// Server that slowly streams data
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "1048576") // 1MB
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			w.Header().Set("Content-Length", "1048576")
			w.WriteHeader(http.StatusOK)
			// Write slowly — 1 byte at a time to simulate slow transfer
			for i := 0; i < 1048576; i++ {
				if _, err := w.Write([]byte{0x42}); err != nil {
					return
				}
				w.(http.Flusher).Flush()
			}
		}
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.bin")

	cfg := DefaultConfig()
	cfg.Endpoint = server.URL
	cfg.Bucket = "test"
	cfg.Key = "slow-file"
	cfg.OutputPath = outputPath

	// Cancel immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Download(ctx, cfg)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

// TestAtomicRename verifies that partial downloads don't leave corrupt files.
func TestAtomicRename(t *testing.T) {
	// Server that returns wrong content length to trigger error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "1000")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			// Return less data than promised
			w.Header().Set("Content-Length", "1000")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("short"))
		}
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.bin")

	cfg := DefaultConfig()
	cfg.Endpoint = server.URL
	cfg.Bucket = "test"
	cfg.Key = "bad-file"
	cfg.OutputPath = outputPath

	_, err := Download(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error from size mismatch")
	}

	// Output file should NOT exist (atomic — only renamed on success)
	if _, statErr := os.Stat(outputPath); !os.IsNotExist(statErr) {
		t.Error("output file should not exist after failed download")
	}
}

// TestSigV4Signing verifies that the signing process produces valid auth headers.
func TestSigV4Signing(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "https://s3.us-east-1.amazonaws.com/test-bucket/test-key", nil)
	req.Host = "s3.us-east-1.amazonaws.com"

	signRequest(req, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1", "s3")

	// Verify required headers are set
	if req.Header.Get("Authorization") == "" {
		t.Error("Authorization header is empty")
	}
	if req.Header.Get("X-Amz-Date") == "" {
		t.Error("X-Amz-Date header is empty")
	}
	if req.Header.Get("X-Amz-Content-Sha256") != unsignedPayload {
		t.Errorf("X-Amz-Content-Sha256 should be %s, got %s",
			unsignedPayload, req.Header.Get("X-Amz-Content-Sha256"))
	}

	// Verify auth header format
	auth := req.Header.Get("Authorization")
	if len(auth) < 50 {
		t.Errorf("Authorization header seems too short: %s", auth)
	}
	if auth[:16] != "AWS4-HMAC-SHA256" {
		t.Errorf("Authorization should start with AWS4-HMAC-SHA256, got: %s", auth[:16])
	}
}

// Benchmark parallel vs single download
func BenchmarkSectionWriter(b *testing.B) {
	tmpFile, err := os.CreateTemp(b.TempDir(), "bench")
	if err != nil {
		b.Fatal(err)
	}
	defer tmpFile.Close()

	data := make([]byte, 4096)
	if _, err := rand.Read(data); err != nil {
		b.Fatal(err)
	}

	if err := tmpFile.Truncate(int64(b.N * len(data))); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sw := NewSectionWriter(tmpFile, int64(i*len(data)))
		if _, err := io.Copy(sw, io.LimitReader(rand.Reader, int64(len(data)))); err != nil {
			b.Fatal(err)
		}
	}
}

// TestRetryOnTransientError verifies that a chunk download retries after
// transient server errors and eventually succeeds.
func TestRetryOnTransientError(t *testing.T) {
	testData := make([]byte, 4096)
	if _, err := rand.Read(testData); err != nil {
		t.Fatal(err)
	}

	var getAttempts atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			attempt := getAttempts.Add(1)
			// Fail first 2 attempts, succeed on 3rd
			if attempt <= 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
			w.Write(testData)
		}
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.bin")

	cfg := DefaultConfig()
	cfg.Endpoint = server.URL
	cfg.Bucket = "test"
	cfg.Key = "flaky-file"
	cfg.OutputPath = outputPath
	cfg.MaxRetries = 3
	cfg.RetryBaseDelay = 10 * time.Millisecond // Fast retries for testing
	cfg.Verbose = true

	result, err := Download(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Download should succeed after retries, got: %v", err)
	}

	if result.TotalBytes != int64(len(testData)) {
		t.Errorf("expected %d bytes, got %d", len(testData), result.TotalBytes)
	}

	// Should have tried 3 times total (2 failures + 1 success)
	if getAttempts.Load() != 3 {
		t.Errorf("expected 3 GET attempts, got %d", getAttempts.Load())
	}

	// Verify content
	downloaded, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	for i := range testData {
		if downloaded[i] != testData[i] {
			t.Fatalf("content mismatch at byte %d", i)
		}
	}
}

// TestRetryExhausted verifies that downloads fail after exhausting all retries.
func TestRetryExhausted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "4096")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			// Always fail
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.bin")

	cfg := DefaultConfig()
	cfg.Endpoint = server.URL
	cfg.Bucket = "test"
	cfg.Key = "always-fails"
	cfg.OutputPath = outputPath
	cfg.MaxRetries = 2
	cfg.RetryBaseDelay = 10 * time.Millisecond

	_, err := Download(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error after retry exhaustion")
	}

	if !strings.Contains(err.Error(), "all 3 attempts failed") {
		t.Errorf("expected 'all 3 attempts failed' in error, got: %v", err)
	}
}

// TestETagChecksumVerification verifies that ETag/MD5 checksums are checked.
func TestETagChecksumVerification(t *testing.T) {
	testData := []byte("hello checksum world")
	// MD5 of "hello checksum world"
	h := md5.Sum(testData)
	expectedMD5 := hex.EncodeToString(h[:])

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.Header().Set("ETag", `"`+expectedMD5+`"`)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
			w.Write(testData)
		}
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.bin")

	cfg := DefaultConfig()
	cfg.Endpoint = server.URL
	cfg.Bucket = "test"
	cfg.Key = "checksum-file"
	cfg.OutputPath = outputPath

	result, err := Download(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	if !result.ChecksumVerified {
		t.Error("expected checksum to be verified")
	}
	if result.ETag != `"`+expectedMD5+`"` {
		t.Errorf("expected ETag %q, got %q", expectedMD5, result.ETag)
	}
}

// TestETagChecksumMismatch verifies that checksum mismatch is detected.
func TestETagChecksumMismatch(t *testing.T) {
	testData := []byte("hello checksum world")
	wrongMD5 := "0000000000000000ffffffffffffffff" // wrong

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.Header().Set("ETag", `"`+wrongMD5+`"`)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
			w.Write(testData)
		}
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.bin")

	cfg := DefaultConfig()
	cfg.Endpoint = server.URL
	cfg.Bucket = "test"
	cfg.Key = "bad-checksum"
	cfg.OutputPath = outputPath

	_, err := Download(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected checksum mismatch error")
	}
	if !strings.Contains(err.Error(), "MD5 mismatch") {
		t.Errorf("expected 'MD5 mismatch' in error, got: %v", err)
	}

	// File should not exist (removed on checksum failure)
	if _, statErr := os.Stat(outputPath); !os.IsNotExist(statErr) {
		t.Error("output file should not exist after checksum failure")
	}
}

// TestIsSimpleETag tests the ETag format detection.
func TestIsSimpleETag(t *testing.T) {
	tests := []struct {
		etag     string
		expected bool
	}{
		{`"d41d8cd98f00b204e9800998ecf8427e"`, true},
		{"d41d8cd98f00b204e9800998ecf8427e", true},
		{`"d41d8cd98f00b204e9800998ecf8427e-2"`, false}, // multipart
		{"", false},
		{`"too-short"`, false},
		{`"ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"`, false}, // non-hex
		{`"D41D8CD98F00B204E9800998ECF8427E"`, true},   // uppercase hex ok
	}

	for _, tt := range tests {
		t.Run(tt.etag, func(t *testing.T) {
			if got := isSimpleETag(tt.etag); got != tt.expected {
				t.Errorf("isSimpleETag(%q) = %v, want %v", tt.etag, got, tt.expected)
			}
		})
	}
}

