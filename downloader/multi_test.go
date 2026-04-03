package downloader

import (
	"context"
	"crypto/rand"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

// TestListObjectsV2Parsing verifies that the ListObjectsV2 XML response is parsed correctly.
func TestListObjectsV2Parsing(t *testing.T) {
	xmlResponse := `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name>
  <Prefix>data/</Prefix>
  <KeyCount>3</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>data/file1.bin</Key>
    <Size>1024</Size>
    <ETag>"aaa"</ETag>
  </Contents>
  <Contents>
    <Key>data/</Key>
    <Size>0</Size>
    <ETag>""</ETag>
  </Contents>
  <Contents>
    <Key>data/file2.bin</Key>
    <Size>2048</Size>
    <ETag>"bbb"</ETag>
  </Contents>
</ListBucketResult>`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify it's a list request
		if r.URL.Query().Get("list-type") != "2" {
			t.Errorf("expected list-type=2, got %s", r.URL.Query().Get("list-type"))
		}
		if r.URL.Query().Get("prefix") != "data/" {
			t.Errorf("expected prefix=data/, got %s", r.URL.Query().Get("prefix"))
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(xmlResponse))
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.Endpoint = server.URL
	cfg.Bucket = "test-bucket"

	client := newHTTPClient(cfg)
	objectCh, errCh := ListObjects(context.Background(), client, cfg, "data/")

	var objects []S3Object
	for obj := range objectCh {
		objects = append(objects, obj)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("listing error: %v", err)
	}

	// Should have 2 objects (directory marker filtered out)
	if len(objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(objects))
	}

	if objects[0].Key != "data/file1.bin" || objects[0].Size != 1024 {
		t.Errorf("object 0: got %+v", objects[0])
	}
	if objects[1].Key != "data/file2.bin" || objects[1].Size != 2048 {
		t.Errorf("object 1: got %+v", objects[1])
	}
}

// TestListObjectsPagination verifies that continuation tokens are followed.
func TestListObjectsPagination(t *testing.T) {
	var mu sync.Mutex
	callCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		page := callCount
		mu.Unlock()

		w.Header().Set("Content-Type", "application/xml")

		var result listObjectsV2Result
		switch page {
		case 1:
			result = listObjectsV2Result{
				Contents: []listObjectEntry{
					{Key: "p/file1.bin", Size: 100},
					{Key: "p/file2.bin", Size: 200},
				},
				IsTruncated:           true,
				NextContinuationToken: "token-page-2",
				KeyCount:              2,
			}
		case 2:
			// Verify continuation token was passed
			if r.URL.Query().Get("continuation-token") != "token-page-2" {
				t.Errorf("expected continuation-token=token-page-2, got %q", r.URL.Query().Get("continuation-token"))
			}
			result = listObjectsV2Result{
				Contents: []listObjectEntry{
					{Key: "p/file3.bin", Size: 300},
				},
				IsTruncated: false,
				KeyCount:    1,
			}
		default:
			t.Errorf("unexpected page %d", page)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		data, _ := xml.Marshal(result)
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.Endpoint = server.URL
	cfg.Bucket = "test-bucket"

	client := newHTTPClient(cfg)
	objectCh, errCh := ListObjects(context.Background(), client, cfg, "p/")

	var objects []S3Object
	for obj := range objectCh {
		objects = append(objects, obj)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("listing error: %v", err)
	}

	if len(objects) != 3 {
		t.Fatalf("expected 3 objects across 2 pages, got %d", len(objects))
	}

	mu.Lock()
	if callCount != 2 {
		t.Errorf("expected 2 list API calls, got %d", callCount)
	}
	mu.Unlock()
}

// TestDownloadPrefix verifies the full multi-file download pipeline.
func TestDownloadPrefix(t *testing.T) {
	// Generate test files
	testFiles := map[string][]byte{
		"prefix/file1.txt":    make([]byte, 512),
		"prefix/sub/file2.txt": make([]byte, 1024),
		"prefix/sub/file3.bin": make([]byte, 256),
	}
	for _, data := range testFiles {
		rand.Read(data)
	}

	// Build ListObjectsV2 XML response
	var entries []listObjectEntry
	for key, data := range testFiles {
		entries = append(entries, listObjectEntry{Key: key, Size: int64(len(data))})
	}
	// Sort for deterministic order
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

	listResult := listObjectsV2Result{
		Contents:    entries,
		IsTruncated: false,
		KeyCount:    len(entries),
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// List request
		if r.URL.Query().Get("list-type") == "2" {
			data, _ := xml.Marshal(listResult)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			w.Write(data)
			return
		}

		// Extract key from path: /bucket/key
		path := strings.TrimPrefix(r.URL.Path, "/test-bucket/")

		fileData, ok := testFiles[path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(fileData)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(fileData)))
			w.WriteHeader(http.StatusOK)
			w.Write(fileData)
		}
	}))
	defer server.Close()

	tmpDir := t.TempDir()

	mcfg := DefaultMultiConfig()
	mcfg.Config.Endpoint = server.URL
	mcfg.Config.Bucket = "test-bucket"
	mcfg.Prefix = "prefix/"
	mcfg.OutputDir = tmpDir
	mcfg.FileConcurrency = 2

	result, err := DownloadPrefix(context.Background(), mcfg)
	if err != nil {
		t.Fatalf("DownloadPrefix failed: %v", err)
	}

	if result.TotalFiles != 3 {
		t.Errorf("expected 3 total files, got %d", result.TotalFiles)
	}
	if result.SucceededFiles != 3 {
		t.Errorf("expected 3 succeeded files, got %d", result.SucceededFiles)
	}
	if result.FailedFiles != 0 {
		t.Errorf("expected 0 failed files, got %d", result.FailedFiles)
	}

	// Verify each file content
	for key, expectedData := range testFiles {
		relativePath := strings.TrimPrefix(key, "prefix/")
		localPath := filepath.Join(tmpDir, relativePath)

		downloaded, err := os.ReadFile(localPath)
		if err != nil {
			t.Fatalf("read %s: %v", localPath, err)
		}

		if len(downloaded) != len(expectedData) {
			t.Errorf("%s: size mismatch: expected %d, got %d", key, len(expectedData), len(downloaded))
			continue
		}
		for i := range expectedData {
			if downloaded[i] != expectedData[i] {
				t.Errorf("%s: content mismatch at byte %d", key, i)
				break
			}
		}
	}
}

// TestDownloadPrefixSkipExisting verifies that existing files are skipped.
func TestDownloadPrefixSkipExisting(t *testing.T) {
	testData := make([]byte, 256)
	rand.Read(testData)

	listResult := listObjectsV2Result{
		Contents: []listObjectEntry{
			{Key: "p/existing.bin", Size: int64(len(testData))},
		},
		IsTruncated: false,
		KeyCount:    1,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") == "2" {
			data, _ := xml.Marshal(listResult)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			w.Write(data)
			return
		}

		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			t.Error("GET should not be called for existing file")
			w.WriteHeader(http.StatusOK)
			w.Write(testData)
		}
	}))
	defer server.Close()

	tmpDir := t.TempDir()

	// Pre-create existing file with matching size
	existingPath := filepath.Join(tmpDir, "existing.bin")
	if err := os.WriteFile(existingPath, testData, 0644); err != nil {
		t.Fatal(err)
	}

	mcfg := DefaultMultiConfig()
	mcfg.Config.Endpoint = server.URL
	mcfg.Config.Bucket = "test-bucket"
	mcfg.Config.Verbose = true
	mcfg.Prefix = "p/"
	mcfg.OutputDir = tmpDir

	result, err := DownloadPrefix(context.Background(), mcfg)
	if err != nil {
		t.Fatalf("DownloadPrefix failed: %v", err)
	}

	if result.SkippedFiles != 1 {
		t.Errorf("expected 1 skipped file, got %d", result.SkippedFiles)
	}
	if result.SucceededFiles != 0 {
		t.Errorf("expected 0 succeeded files (all skipped), got %d", result.SucceededFiles)
	}
}

// TestBuildListURL verifies list URL construction.
func TestBuildListURL(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Endpoint = "https://s3.amazonaws.com"
	cfg.Bucket = "my-bucket"

	url := buildListURL(cfg, "data/models/", "")
	if !strings.Contains(url, "list-type=2") {
		t.Errorf("URL should contain list-type=2: %s", url)
	}
	if !strings.Contains(url, "prefix=data%2Fmodels%2F") {
		t.Errorf("URL should contain encoded prefix: %s", url)
	}

	urlWithToken := buildListURL(cfg, "data/", "abc123")
	if !strings.Contains(urlWithToken, "continuation-token=abc123") {
		t.Errorf("URL should contain continuation-token: %s", urlWithToken)
	}
}
