package downloader

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"
)

const (
	maxRedirects = 10
)

// newHTTPClient creates an HTTP client optimized for high-throughput downloads
// with proper 307 redirect handling for Alluxio S3 proxy.
func newHTTPClient(cfg *Config) *http.Client {
	transport := &http.Transport{
		// Connection pooling tuned for parallel chunk downloads.
		// Alluxio redirects to workers, so we need pools for both
		// the proxy and the worker hosts. 4x gives headroom.
		MaxIdleConns:        cfg.Concurrency * 4,
		MaxIdleConnsPerHost: cfg.Concurrency * 2,
		MaxConnsPerHost:     cfg.Concurrency * 2,

		// Aggressive timeouts for fast failure detection
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,

		// TLS settings
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: cfg.InsecureTLS,
		},
		TLSHandshakeTimeout: 10 * time.Second,

		// Disable compression — data from S3 is typically already compressed
		// or incompressible, and decompression adds CPU overhead + prevents
		// proper Content-Length based range requests.
		DisableCompression: true,

		// Tuned buffer sizes for large transfers.
		// Larger read buffer reduces syscall count on high-throughput links.
		WriteBufferSize: 64 * 1024,  // 64KB write buffer
		ReadBufferSize:  128 * 1024, // 128KB read buffer

		// Allow HTTP for dev/test Alluxio endpoints
		ForceAttemptHTTP2: false,

		// Response header timeout
		ResponseHeaderTimeout: 30 * time.Second,

		// Idle connection timeout
		IdleConnTimeout: 90 * time.Second,
	}

	return &http.Client{
		Transport: transport,
		// No overall timeout — individual requests manage their own via context.
		// Setting a global timeout would kill long-running large chunk downloads.
		Timeout: 0,

		// Custom redirect policy for Alluxio 307 handling.
		// Alluxio S3 proxy returns 307 to redirect the client directly to the
		// worker holding the data. We must:
		// 1. Follow the redirect (default Go behavior)
		// 2. Preserve the original HTTP method (307 requires this per RFC)
		// 3. Re-sign the request for the new host
		// 4. Limit redirect depth to prevent loops
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				return fmt.Errorf("stopped after %d redirects", maxRedirects)
			}

			if cfg.Verbose {
				log.Printf("[redirect] %d %s → %s",
					len(via), via[len(via)-1].URL.String(), req.URL.String())
			}

			// Copy essential headers from the original request.
			// The redirect may go to a different host (Alluxio worker),
			// so we need to re-apply our custom headers.
			// Use the immediately-preceding request to handle multi-hop chains.
			prev := via[len(via)-1]
			for _, header := range []string{"Range"} {
				if v := prev.Header.Get(header); v != "" {
					req.Header.Set(header, v)
				}
			}

			// Re-sign the request for the new endpoint.
			// The Host changed due to redirect, so the original signature is invalid.
			if cfg.AccessKey != "" && cfg.SecretKey != "" {
				// Clear old auth headers before re-signing
				req.Header.Del("Authorization")
				req.Header.Del("X-Amz-Date")
				req.Header.Del("X-Amz-Content-Sha256")
				req.Header.Del("Host")
				signRequest(req, cfg.AccessKey, cfg.SecretKey, cfg.Region, "s3")
			}

			return nil
		},
	}
}
