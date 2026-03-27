package downloader

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// AWSCredentials holds parsed credentials from the AWS credentials file.
type AWSCredentials struct {
	AccessKeyID     string
	SecretAccessKey  string
	SessionToken    string // Optional, for temporary credentials
}

// LoadAWSCredentialsFromEnv reads credentials from environment variables.
// Returns nil if no credentials are found in the environment.
func LoadAWSCredentialsFromEnv() *AWSCredentials {
	ak := os.Getenv("AWS_ACCESS_KEY_ID")
	sk := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if ak == "" || sk == "" {
		return nil
	}
	return &AWSCredentials{
		AccessKeyID:    ak,
		SecretAccessKey: sk,
		SessionToken:   os.Getenv("AWS_SESSION_TOKEN"),
	}
}

// LoadAWSCredentialsFromProfile reads credentials from ~/.aws/credentials for the given profile.
func LoadAWSCredentialsFromProfile(profile string) (*AWSCredentials, error) {
	if profile == "" {
		profile = "default"
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("cannot determine home directory: %w", err)
	}

	credPath := filepath.Join(home, ".aws", "credentials")
	creds, err := parseCredentialsFile(credPath, profile)
	if err != nil {
		return nil, fmt.Errorf("load credentials for profile %q: %w", profile, err)
	}

	if creds.AccessKeyID == "" {
		return nil, fmt.Errorf("no aws_access_key_id found in profile %q", profile)
	}

	return creds, nil
}

// LoadAWSRegion reads the region from ~/.aws/config for the given profile.
// Falls back to AWS_REGION env var, then to defaultRegion.
func LoadAWSRegion(profile, defaultRegion string) string {
	if r := os.Getenv("AWS_REGION"); r != "" {
		return r
	}
	if r := os.Getenv("AWS_DEFAULT_REGION"); r != "" {
		return r
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return defaultRegion
	}

	configPath := filepath.Join(home, ".aws", "config")
	// In config file, non-default profiles are prefixed with "profile "
	configProfile := profile
	if configProfile != "" && configProfile != "default" {
		configProfile = "profile " + configProfile
	}
	if configProfile == "" {
		configProfile = "default"
	}

	region := parseConfigValue(configPath, configProfile, "region")
	if region != "" {
		return region
	}

	return defaultRegion
}

// parseCredentialsFile parses an INI-style credentials file.
func parseCredentialsFile(path, profile string) (*AWSCredentials, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	creds := &AWSCredentials{}
	inProfile := false

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		// Section header
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section := strings.TrimSpace(line[1 : len(line)-1])
			inProfile = (section == profile)
			continue
		}

		if !inProfile {
			continue
		}

		// Parse key=value
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "aws_access_key_id":
			creds.AccessKeyID = value
		case "aws_secret_access_key":
			creds.SecretAccessKey = value
		case "aws_session_token":
			creds.SessionToken = value
		}
	}

	return creds, scanner.Err()
}

// parseConfigValue extracts a single value from an INI-style config file.
func parseConfigValue(path, section, key string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()

	inSection := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			s := strings.TrimSpace(line[1 : len(line)-1])
			inSection = (s == section)
			continue
		}

		if !inSection {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 && strings.TrimSpace(parts[0]) == key {
			return strings.TrimSpace(parts[1])
		}
	}

	return ""
}
