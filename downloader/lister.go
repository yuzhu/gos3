package downloader

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
)

// S3Object represents a single object returned by ListObjectsV2.
type S3Object struct {
	Key  string
	Size int64
	ETag string
}

// listObjectsV2Result maps the XML response from S3 ListObjectsV2 API.
type listObjectsV2Result struct {
	XMLName               xml.Name          `xml:"ListBucketResult"`
	Contents              []listObjectEntry `xml:"Contents"`
	IsTruncated           bool              `xml:"IsTruncated"`
	NextContinuationToken string            `xml:"NextContinuationToken"`
	KeyCount              int               `xml:"KeyCount"`
}

type listObjectEntry struct {
	Key  string `xml:"Key"`
	Size int64  `xml:"Size"`
	ETag string `xml:"ETag"`
}

// ListObjects lists all objects under the given prefix, streaming results
// into the returned channel. Listing is paginated (1000 keys per request)
// and runs concurrently so downloads can begin before listing completes.
// The channel is closed when listing finishes or context is cancelled.
// Any listing error is sent to the errCh channel.
func ListObjects(ctx context.Context, client *http.Client, cfg *Config, prefix string) (<-chan S3Object, <-chan error) {
	objectCh := make(chan S3Object, 1000) // Buffer a full page
	errCh := make(chan error, 1)

	go func() {
		defer close(objectCh)
		defer close(errCh)

		continuationToken := ""
		pageNum := 0

		for {
			if ctx.Err() != nil {
				errCh <- ctx.Err()
				return
			}

			objects, nextToken, truncated, err := listPage(ctx, client, cfg, prefix, continuationToken)
			if err != nil {
				errCh <- fmt.Errorf("list page %d: %w", pageNum, err)
				return
			}

			pageNum++
			for _, obj := range objects {
				// Skip directory markers (keys ending with "/" and size 0)
				if obj.Size == 0 && strings.HasSuffix(obj.Key, "/") {
					continue
				}

				select {
				case objectCh <- obj:
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				}
			}

			if cfg.Verbose {
				log.Printf("[list] page %d: %d objects (truncated=%v)", pageNum, len(objects), truncated)
			}

			if !truncated {
				return
			}
			continuationToken = nextToken
		}
	}()

	return objectCh, errCh
}

// listPage performs a single ListObjectsV2 API call and returns parsed results.
func listPage(ctx context.Context, client *http.Client, cfg *Config, prefix, continuationToken string) ([]S3Object, string, bool, error) {
	// Build the list URL with query parameters
	listURL := buildListURL(cfg, prefix, continuationToken)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, listURL, nil)
	if err != nil {
		return nil, "", false, fmt.Errorf("create list request: %w", err)
	}

	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		signRequest(req, cfg.AccessKey, cfg.SecretKey, cfg.Region, "s3")
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", false, fmt.Errorf("execute list request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, "", false, fmt.Errorf("list request returned %d %s: %s", resp.StatusCode, resp.Status, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", false, fmt.Errorf("read list response: %w", err)
	}

	var result listObjectsV2Result
	if err := xml.Unmarshal(body, &result); err != nil {
		return nil, "", false, fmt.Errorf("parse list response XML: %w", err)
	}

	objects := make([]S3Object, 0, len(result.Contents))
	for _, entry := range result.Contents {
		objects = append(objects, S3Object{
			Key:  entry.Key,
			Size: entry.Size,
			ETag: entry.ETag,
		})
	}

	return objects, result.NextContinuationToken, result.IsTruncated, nil
}

// buildListURL constructs the S3 ListObjectsV2 URL with query parameters.
// Uses path-style: http://endpoint/bucket?list-type=2&prefix=xxx
//
// Note: We cannot use url.Values.Encode() for the prefix because it encodes
// "/" to "%2F". S3 treats "%2F" as a literal character, not a path separator,
// so a prefix of "a%2Fb%2F" matches nothing while "a/b/" matches correctly.
func buildListURL(cfg *Config, prefix, continuationToken string) string {
	params := url.Values{}
	params.Set("list-type", "2")
	params.Set("max-keys", "1000")
	if continuationToken != "" {
		params.Set("continuation-token", continuationToken)
	}

	query := params.Encode()
	if prefix != "" {
		// Encode prefix carefully: encode each segment but preserve slashes
		parts := strings.Split(prefix, "/")
		for i, p := range parts {
			parts[i] = url.QueryEscape(p)
		}
		query += "&prefix=" + strings.Join(parts, "/")
	}

	return fmt.Sprintf("%s/%s?%s", cfg.Endpoint, url.PathEscape(cfg.Bucket), query)
}

