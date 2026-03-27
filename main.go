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
	// In Kubernetes pods with CPU limits, the Go runtime defaults to
	// the host's NumCPU which can be far higher than the cgroup quota,
	// causing excessive context switching and throttling.
	setGOMAXPROCS()

	cfg := downloader.DefaultConfig()

	// Required flags
	flag.StringVar(&cfg.Endpoint, "endpoint", "", "S3-compatible endpoint URL (e.g., http://alluxio-proxy:29998)")
	flag.StringVar(&cfg.Bucket, "bucket", "", "S3 bucket name")
	flag.StringVar(&cfg.Key, "key", "", "S3 object key (path within bucket)")
	flag.StringVar(&cfg.OutputPath, "output", "", "Local file path to write to")

	// Auth flags
	var profile string
	flag.StringVar(&profile, "profile", "", "AWS credentials profile name (from ~/.aws/credentials, default: 'default')")
	flag.StringVar(&cfg.AccessKey, "access-key", "", "AWS access key ID (overrides profile/env)")
	flag.StringVar(&cfg.SecretKey, "secret-key", "", "AWS secret access key (overrides profile/env)")
	flag.StringVar(&cfg.Region, "region", "", "AWS region (overrides profile/env, default: us-east-1)")

	// Performance flags
	flag.IntVar(&cfg.Concurrency, "concurrency", cfg.Concurrency, "Number of parallel chunk downloads")
	chunkSizeMB := flag.Int64("chunk-size-mb", cfg.ChunkSize/(1024*1024), "Chunk size in MB for parallel downloads")
	flag.DurationVar(&cfg.ChunkTimeout, "chunk-timeout", cfg.ChunkTimeout, "Timeout per chunk download")

	// Optional flags
	flag.BoolVar(&cfg.InsecureTLS, "insecure", false, "Skip TLS certificate verification")
	flag.BoolVar(&cfg.Verbose, "verbose", false, "Enable verbose logging")
	flag.IntVar(&cfg.MaxRetries, "max-retries", cfg.MaxRetries, "Maximum retries per chunk on transient errors")
	flag.BoolVar(&cfg.SkipChecksumVerify, "skip-checksum", false, "Skip ETag/MD5 checksum verification")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "gos3 - High-performance S3 downloader for Alluxio\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  gos3 --endpoint URL --bucket BUCKET --key KEY --output PATH\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  # Download from Alluxio S3 proxy\n")
		fmt.Fprintf(os.Stderr, "  gos3 --endpoint http://alluxio-lb:29998 --bucket mydata --key models/large.bin --output /tmp/large.bin\n\n")
		fmt.Fprintf(os.Stderr, "  # With auth and tuned parallelism\n")
		fmt.Fprintf(os.Stderr, "  gos3 --endpoint http://alluxio-lb:29998 --bucket mydata --key models/large.bin --output /tmp/large.bin \\\n")
		fmt.Fprintf(os.Stderr, "    --access-key AKIA... --secret-key ... --concurrency 32 --chunk-size-mb 128\n\n")
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
			// User explicitly requested a profile — use it
			creds, err = downloader.LoadAWSCredentialsFromProfile(profile)
			if err != nil {
				log.Fatalf("could not load credentials for profile %q: %v", profile, err)
			}
		} else if envCreds := downloader.LoadAWSCredentialsFromEnv(); envCreds != nil {
			// Fall back to environment variables
			creds = envCreds
		} else {
			// Fall back to default profile
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

	// Resolve region: CLI flag > env vars > config file > default
	if cfg.Region == "" {
		cfg.Region = downloader.LoadAWSRegion(profile, "us-east-1")
	}

	// Validate required flags
	var missing []string
	if cfg.Endpoint == "" {
		missing = append(missing, "--endpoint")
	}
	if cfg.Bucket == "" {
		missing = append(missing, "--bucket")
	}
	if cfg.Key == "" {
		missing = append(missing, "--key")
	}
	if cfg.OutputPath == "" {
		missing = append(missing, "--output")
	}
	if len(missing) > 0 {
		fmt.Fprintf(os.Stderr, "Error: missing required flags: %s\n\n", strings.Join(missing, ", "))
		flag.Usage()
		os.Exit(1)
	}

	// Strip trailing slash from endpoint
	cfg.Endpoint = strings.TrimRight(cfg.Endpoint, "/")

	// Setup context with signal handling for graceful cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("\nReceived %v, cancelling download...", sig)
		cancel()
	}()

	// Execute download
	log.Printf("Downloading s3://%s/%s → %s", cfg.Bucket, cfg.Key, cfg.OutputPath)
	log.Printf("  endpoint=%s concurrency=%d chunk_size=%dMB",
		cfg.Endpoint, cfg.Concurrency, cfg.ChunkSize/(1024*1024))

	// --- Live progress display ---
	// Track bytes for the speed ticker goroutine
	var liveBytes int64
	var totalSize int64

	cfg.OnProgress = func(bytesDownloaded int64) {
		atomic.StoreInt64(&liveBytes, bytesDownloaded)
	}

	cfg.OnTotalSize = func(size int64) {
		atomic.StoreInt64(&totalSize, size)
	}

	startTime := time.Now()

	// Start live speed ticker — prints current speed every 500ms
	stopTicker := make(chan struct{})
	tickerDone := make(chan struct{})
	go func() {
		defer close(tickerDone)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		var prevBytes int64
		prevTime := startTime

		for {
			select {
			case <-stopTicker:
				// Clear the progress line
				fmt.Fprintf(os.Stderr, "\r\033[K")
				return
			case now := <-ticker.C:
				currentBytes := atomic.LoadInt64(&liveBytes)
				elapsed := now.Sub(startTime)
				dt := now.Sub(prevTime)

				// Instantaneous speed (over last tick interval)
				var instantMBs float64
				if dt.Seconds() > 0 {
					instantMBs = float64(currentBytes-prevBytes) / (1024 * 1024) / dt.Seconds()
				}

				// Average speed (since start)
				var avgMBs float64
				if elapsed.Seconds() > 0 {
					avgMBs = float64(currentBytes) / (1024 * 1024) / elapsed.Seconds()
				}

				// Progress percentage
				ts := atomic.LoadInt64(&totalSize)
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
	}()

	result, err := downloader.Download(ctx, cfg)

	// Stop the ticker before printing final output
	close(stopTicker)
	<-tickerDone

	if err != nil {
		fmt.Fprintf(os.Stderr, "\nError: %v\n", err)
		os.Remove(cfg.OutputPath)
		os.Exit(1)
	}

	// Store totalSize so ticker can show it (already stopped, but for completeness)
	atomic.StoreInt64(&totalSize, result.TotalBytes)

	// Print final results
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
// CPUs are available inside a container. If the quota is lower than
// runtime.NumCPU(), it sets GOMAXPROCS accordingly. This prevents the Go
// scheduler from spinning up more OS threads than the cgroup allows,
// which causes throttling in Kubernetes pods.
func setGOMAXPROCS() {
	// If user explicitly set GOMAXPROCS env, respect it
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

	return 0 // not in a container or no quota
}
