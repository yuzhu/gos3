package downloader

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	unsignedPayload = "UNSIGNED-PAYLOAD"
	awsDateFormat   = "20060102T150405Z"
	awsShortDate    = "20060102"
	sigV4Algorithm  = "AWS4-HMAC-SHA256"
)

// signingKeyCache caches the derived signing key for a given date+region+service
// combination. The signing key only changes daily, so caching avoids repeated
// HMAC derivations (4 HMAC-SHA256 per key derivation) across all requests.
type signingKeyCache struct {
	mu        sync.RWMutex
	cacheKey  string // "dateStamp/region/service"
	signingKey []byte
}

var sigKeyCache = &signingKeyCache{}

func (c *signingKeyCache) get(secretKey, dateStamp, region, service string) []byte {
	key := dateStamp + "/" + region + "/" + service

	c.mu.RLock()
	if c.cacheKey == key && c.signingKey != nil {
		defer c.mu.RUnlock()
		return c.signingKey
	}
	c.mu.RUnlock()

	// Cache miss — derive and store
	signingKey := deriveSigningKey(secretKey, dateStamp, region, service)

	c.mu.Lock()
	c.cacheKey = key
	c.signingKey = signingKey
	c.mu.Unlock()

	return signingKey
}

// signRequest signs an HTTP request with AWS Signature V4.
// Uses UNSIGNED-PAYLOAD for GET/HEAD since no request body is sent.
// The signing key is cached per-day to avoid redundant HMAC derivations.
func signRequest(req *http.Request, accessKey, secretKey, region, service string) {
	now := time.Now().UTC()
	dateStamp := now.Format(awsShortDate)
	amzDate := now.Format(awsDateFormat)

	// Set required headers before signing
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", unsignedPayload)

	// Ensure Host header is set (Go may use req.Host)
	if req.Header.Get("Host") == "" {
		req.Header.Set("Host", req.Host)
	}

	// Build canonical request
	canonicalURI := getCanonicalURI(req.URL)
	canonicalQueryString := getCanonicalQueryString(req.URL)
	signedHeaders, canonicalHeaders := getCanonicalHeaders(req)

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeaders,
		unsignedPayload,
	}, "\n")

	// Build string to sign
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)
	canonicalRequestHash := sha256Hex([]byte(canonicalRequest))

	stringToSign := strings.Join([]string{
		sigV4Algorithm,
		amzDate,
		credentialScope,
		canonicalRequestHash,
	}, "\n")

	// Get signing key from cache (avoids 4 HMAC-SHA256 calls per request)
	signingKey := sigKeyCache.get(secretKey, dateStamp, region, service)

	// Calculate signature
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	// Set Authorization header
	authHeader := fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		sigV4Algorithm, accessKey, credentialScope, signedHeaders, signature)
	req.Header.Set("Authorization", authHeader)
}

func getCanonicalURI(u *url.URL) string {
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	return path
}

func getCanonicalQueryString(u *url.URL) string {
	query := u.Query()
	if len(query) == 0 {
		return ""
	}

	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, k := range keys {
		values := query[k]
		sort.Strings(values)
		for _, v := range values {
			parts = append(parts, url.QueryEscape(k)+"="+url.QueryEscape(v))
		}
	}
	return strings.Join(parts, "&")
}

func getCanonicalHeaders(req *http.Request) (signedHeaders, canonicalHeaders string) {
	// Headers to sign: host, x-amz-*
	headerMap := make(map[string]string)
	var headerKeys []string

	// Always include host
	host := req.Header.Get("Host")
	if host == "" {
		host = req.Host
	}
	headerMap["host"] = host
	headerKeys = append(headerKeys, "host")

	// Include all x-amz- headers
	for k, v := range req.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-") {
			headerMap[lk] = strings.TrimSpace(v[0])
			headerKeys = append(headerKeys, lk)
		}
	}

	sort.Strings(headerKeys)

	var canonParts []string
	for _, k := range headerKeys {
		canonParts = append(canonParts, k+":"+headerMap[k]+"\n")
	}

	canonicalHeaders = strings.Join(canonParts, "")
	signedHeaders = strings.Join(headerKeys, ";")
	return
}

func deriveSigningKey(secretKey, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
