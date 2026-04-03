package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/david/gos3/downloader"
)

func main() {
	// Set GOMAXPROCS based on container CPU quota if available.
	setGOMAXPROCS()

	cfg := downloader.DefaultConfig()

	// Required flags
	flag.StringVar(&cfg.Endpoint, "endpoint", "", "S3-compatible endpoint URL (e.g., http://alluxio-proxy:29998)")
	flag.StringVar(&cfg.Bucket, "bucket", "", "S3 bucket name")
	flag.StringVar(&cfg.Key, "key", "", "S3 object key (single file) or prefix ending with / (multi-file)")
	flag.StringVar(&cfg.OutputPath, "output", "", "Local file path (single) or directory (prefix mode)")

	// Auth flags
	var profile string
	flag.StringVar(&profile, "profile", "", "AWS credentials profile name (from ~/.aws/credentials, default: 'default')")
	flag.StringVar(&cfg.AccessKey, "access-key", "", "AWS access key ID (overrides profile/env)")
	flag.StringVar(&cfg.SecretKey, "secret-key", "", "AWS secret access key (overrides profile/env)")
	flag.StringVar(&cfg.Region, "region", "", "AWS region (overrides profile/env, default: us-east-1)")

	// Performance flags
	flag.IntVar(&cfg.Concurrency, "concurrency", cfg.Concurrency, "Number of parallel chunk downloads per file")
	chunkSizeMB := flag.Int64("chunk-size-mb", cfg.ChunkSize/(1024*1024), "Chunk size in MB for parallel downloads")
	flag.DurationVar(&cfg.ChunkTimeout, "chunk-timeout", cfg.ChunkTimeout, "Timeout per chunk download")

	// Multi-file flags
	var prefix string
	var fileConcurrency int
	var failFast bool
	flag.StringVar(&prefix, "prefix", "", "S3 key prefix to download all objects under (alternative to --key with trailing /)")
	flag.IntVar(&fileConcurrency, "file-concurrency", 4, "Number of files to download simultaneously in prefix mode")
	flag.BoolVar(&failFast, "fail-fast", false, "In prefix mode, stop all downloads on first error")

	// Optional flags
	flag.BoolVar(&cfg.InsecureTLS, "insecure", false, "Skip TLS certificate verification")
	flag.BoolVar(&cfg.Verbose, "verbose", false, "Enable verbose logging")
	flag.IntVar(&cfg.MaxRetries, "max-retries", cfg.MaxRetries, "Maximum retries per chunk on transient errors")
	flag.BoolVar(&cfg.SkipChecksumVerify, "skip-checksum", false, "Skip ETag/MD5 checksum verification")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "gos3 - High-performance S3 downloader for Alluxio\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  gos3 --endpoint URL --bucket BUCKET --key KEY --output PATH\n")
		fmt.Fprintf(os.Stderr, "  gos3 --endpoint URL --bucket BUCKET --prefix PREFIX --output DIR\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  # Download a single file\n")
		fmt.Fprintf(os.Stderr, "  gos3 --endpoint http://alluxio-lb:29998 --bucket mydata --key models/large.bin --output /tmp/large.bin\n\n")
		fmt.Fprintf(os.Stderr, "  # Download all files under a prefix\n")
		fmt.Fprintf(os.Stderr, "  gos3 --endpoint https://s3.amazonaws.com --bucket mydata --prefix models/v2/ --output /tmp/models/ --file-concurrency 8\n\n")
		fmt.Fprintf(os.Stderr, "  # Prefix mode via --key with trailing slash\n")
		fmt.Fprintf(os.Stderr, "  gos3 --endpoint https://s3.amazonaws.com --bucket mydata --key models/v2/ --output /tmp/models/\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
	}

	flag.Parse()
	cfg.ChunkSize = *chunkSizeMB * 1024 * 1024

	// Resolve AWS credentials with clear priority:
	//   1. Explicit CLI flags (--access-key, --secret-key)
	//   2. Explicit --profile from ~/.aws/credentials
	//   3. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
	//   4. Default profile from ~/.aws/credentials
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		var creds *downloader.AWSCredentials
		var err error

		if profile != "" {
			creds, err = downloader.LoadAWSCredentialsFromProfile(profile)
			if err != nil {
				log.Fatalf("could not load credentials for profile %q: %v", profile, err)
			}
		} else if envCreds := downloader.LoadAWSCredentialsFromEnv(); envCreds != nil {
			creds = envCreds
		} else {
			creds, err = downloader.LoadAWSCredentialsFromProfile("default")
			if err != nil {
				log.Printf("[warn] no credentials found (no --profile, no env vars, no default profile): %v", err)
			}
		}

		if creds != nil {
			if cfg.AccessKey == "" {
				cfg.AccessKey = creds.AccessKeyID
			}
			if cfg.SecretKey == "" {
				cfg.SecretKey = creds.SecretAccessKey
			}
		}
	}

	// Resolve region
	if cfg.Region == "" {
		cfg.Region = downloader.LoadAWSRegion(profile, "us-east-1")
	}

	// Determine mode: prefix (multi-file) vs single file
	isPrefixMode := prefix != ""
	if !isPrefixMode && cfg.Key != "" && strings.HasSuffix(cfg.Key, "/") {
		isPrefixMode = true
		prefix = cfg.Key
		cfg.Key = "" // clear so validation doesn't complain
	}

	// Strip trailing slash from endpoint
	cfg.Endpoint = strings.TrimRight(cfg.Endpoint, "/")

	// Validate required flags
	var missing []string
	if cfg.Endpoint == "" {
		missing = append(missing, "--endpoint")
	}
	if cfg.Bucket == "" {
		missing = append(missing, "--bucket")
	}
	if !isPrefixMode && cfg.Key == "" {
		missing = append(missing, "--key or --prefix")
	}
	if cfg.OutputPath == "" {
		missing = append(missing, "--output")
	}
	if len(missing) > 0 {
		fmt.Fprintf(os.Stderr, "Error: missing required flags: %s\n\n", strings.Join(missing, ", "))
		flag.Usage()
		os.Exit(1)
	}

	// Setup context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("\nReceived %v, cancelling download...", sig)
		cancel()
	}()

	if isPrefixMode {
		runPrefixDownload(ctx, cfg, prefix, fileConcurrency, failFast)
	} else {
		runSingleDownload(ctx, cfg)
	}
}

// runSingleDownload handles downloading a single file (existing behavior).
func runSingleDownload(ctx context.Context, cfg *downloader.Config) {
	log.Printf("Downloading s3://%s/%s → %s", cfg.Bucket, cfg.Key, cfg.OutputPath)
	log.Printf("  endpoint=%s concurrency=%d chunk_size=%dMB",
		cfg.Endpoint, cfg.Concurrency, cfg.ChunkSize/(1024*1024))

	var liveBytes int64
	var totalSize int64

	cfg.OnProgress = func(bytesDownloaded int64) {
		atomic.StoreInt64(&liveBytes, bytesDownloaded)
	}
	cfg.OnTotalSize = func(size int64) {
		atomic.StoreInt64(&totalSize, size)
	}

	startTime := time.Now()
	stopTicker := make(chan struct{})
	tickerDone := make(chan struct{})
	go progressTicker(startTime, &liveBytes, &totalSize, stopTicker, tickerDone)

	result, err := downloader.Download(ctx, cfg)

	close(stopTicker)
	<-tickerDone

	if err != nil {
		fmt.Fprintf(os.Stderr, "\nError: %v\n", err)
		os.Remove(cfg.OutputPath)
		os.Exit(1)
	}

	elapsed := time.Since(startTime)
	avgMBs := float64(result.TotalBytes) / (1024 * 1024) / elapsed.Seconds()
	avgGbps := float64(result.TotalBytes) * 8 / (1024 * 1024 * 1024) / elapsed.Seconds()

	fmt.Fprintf(os.Stderr, "\n")
	log.Printf("✓ Download complete!")
	log.Printf("  File:         %s (%d bytes)", formatBytes(result.TotalBytes), result.TotalBytes)
	log.Printf("  Duration:     %s", elapsed.Round(time.Millisecond))
	log.Printf("  Avg Speed:    %.2f MB/s (%.2f Gbps)", avgMBs, avgGbps)
	log.Printf("  Chunks:       %d", result.NumChunks)
	if result.TotalRetries > 0 {
		log.Printf("  Retries:      %d", result.TotalRetries)
	}
	if result.ChecksumVerified {
		log.Printf("  Checksum:     ✓ verified (MD5=%s)", strings.Trim(result.ETag, `"`))
	} else if result.ETag != "" {
		log.Printf("  Checksum:     skipped (multipart ETag)")
	}
	log.Printf("  Output:       %s", cfg.OutputPath)
}

// runPrefixDownload handles downloading all objects under a prefix.
func runPrefixDownload(ctx context.Context, cfg *downloader.Config, prefix string, fileConcurrency int, failFast bool) {
	log.Printf("Downloading s3://%s/%s* → %s", cfg.Bucket, prefix, cfg.OutputPath)
	log.Printf("  endpoint=%s file_concurrency=%d chunk_concurrency=%d chunk_size=%dMB",
		cfg.Endpoint, fileConcurrency, cfg.Concurrency, cfg.ChunkSize/(1024*1024))

	mcfg := &downloader.MultiConfig{
		Config:          cfg,
		Prefix:          prefix,
		OutputDir:       cfg.OutputPath,
		FileConcurrency: fileConcurrency,
		FailFast:        failFast,
	}

	var liveBytes int64
	var totalSize int64

	cfg.OnProgress = func(bytesDownloaded int64) {
		atomic.StoreInt64(&liveBytes, bytesDownloaded)
	}
	cfg.OnTotalSize = func(size int64) {
		atomic.StoreInt64(&totalSize, size)
	}

	startTime := time.Now()
	stopTicker := make(chan struct{})
	tickerDone := make(chan struct{})
	go progressTicker(startTime, &liveBytes, &totalSize, stopTicker, tickerDone)

	result, err := downloader.DownloadPrefix(ctx, mcfg)

	close(stopTicker)
	<-tickerDone

	fmt.Fprintf(os.Stderr, "\n")

	if result != nil {
		elapsed := time.Since(startTime)
		totalBytes := result.TotalBytes
		// Include skipped file bytes in total for speed calculation
		for _, fr := range result.FileResults {
			if fr.Skipped {
				totalBytes += fr.Size
			}
		}

		avgMBs := float64(totalBytes) / (1024 * 1024) / elapsed.Seconds()
		avgGbps := float64(totalBytes) * 8 / (1024 * 1024 * 1024) / elapsed.Seconds()

		log.Printf("✓ Prefix download complete!")
		log.Printf("  Total Files:  %d (%d succeeded, %d skipped, %d failed)",
			result.TotalFiles, result.SucceededFiles, result.SkippedFiles, result.FailedFiles)
		log.Printf("  Total Size:   %s", formatBytes(totalBytes))
		log.Printf("  Duration:     %s", elapsed.Round(time.Millisecond))
		log.Printf("  Avg Speed:    %.2f MB/s (%.2f Gbps)", avgMBs, avgGbps)
		log.Printf("  Output:       %s", cfg.OutputPath)

		if result.FailedFiles > 0 {
			log.Printf("\n  Failed files:")
			for _, fr := range result.FileResults {
				if fr.Error != nil {
					log.Printf("    ✗ %s: %v", fr.Key, fr.Error)
				}
			}
		}
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "\nError: %v\n", err)
		os.Exit(1)
	}
}

// progressTicker prints a live progress line every 500ms.
func progressTicker(startTime time.Time, liveBytes, totalSize *int64, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var prevBytes int64
	prevTime := startTime

	for {
		select {
		case <-stop:
			fmt.Fprintf(os.Stderr, "\r\033[K")
			return
		case now := <-ticker.C:
			currentBytes := atomic.LoadInt64(liveBytes)
			elapsed := now.Sub(startTime)
			dt := now.Sub(prevTime)

			var instantMBs float64
			if dt.Seconds() > 0 {
				instantMBs = float64(currentBytes-prevBytes) / (1024 * 1024) / dt.Seconds()
			}

			var avgMBs float64
			if elapsed.Seconds() > 0 {
				avgMBs = float64(currentBytes) / (1024 * 1024) / elapsed.Seconds()
			}

			ts := atomic.LoadInt64(totalSize)
			var pct float64
			if ts > 0 {
				pct = float64(currentBytes) / float64(ts) * 100
			}

			fmt.Fprintf(os.Stderr, "\r  %s / %s (%.1f%%)  speed: %.2f MB/s  avg: %.2f MB/s   ",
				formatBytes(currentBytes), formatBytes(ts), pct, instantMBs, avgMBs)

			prevBytes = currentBytes
			prevTime = now
		}
	}
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func formatBytes(b int64) string {
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

// setGOMAXPROCS reads the cgroup CPU quota (v1 or v2) to determine how many
// CPUs are available inside a container.
func setGOMAXPROCS() {
	if os.Getenv("GOMAXPROCS") != "" {
		return
	}

	quota := detectCgroupCPUQuota()
	if quota > 0 && quota < runtime.NumCPU() {
		runtime.GOMAXPROCS(quota)
		log.Printf("[info] GOMAXPROCS set to %d (container CPU quota)", quota)
	}
}

func detectCgroupCPUQuota() int {
	// Try cgroup v2 first
	if data, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil {
		parts := strings.Fields(string(data))
		if len(parts) >= 2 && parts[0] != "max" {
			quota, e1 := strconv.Atoi(parts[0])
			period, e2 := strconv.Atoi(parts[1])
			if e1 == nil && e2 == nil && period > 0 {
				cpus := quota / period
				if cpus < 1 {
					cpus = 1
				}
				return cpus
			}
		}
	}

	// Try cgroup v1
	quotaData, err1 := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
	periodData, err2 := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
	if err1 == nil && err2 == nil {
		quota, e1 := strconv.Atoi(strings.TrimSpace(string(quotaData)))
		period, e2 := strconv.Atoi(strings.TrimSpace(string(periodData)))
		if e1 == nil && e2 == nil && quota > 0 && period > 0 {
			cpus := quota / period
			if cpus < 1 {
				cpus = 1
			}
			return cpus
		}
	}

	return 0
}
