# gos3 - High-Performance S3 Downloader

`gos3` is a fast, production-grade S3-compatible downloader written in Go. It is optimized for speed using parallel range-based chunking and zero-copy I/O. It was specifically designed to handle redirects (307) for protocols like Alluxio's S3 proxy-to-worker pattern.

## Features

- **Parallel Chunked Downloads**: Downloads files in multiple parallel ranges for maximum throughput.
- **Zero-Copy I/O**: Efficiently handles data transfer to minimize memory consumption and GC pressure.
- **AWS SigV4 Signing**: Built-in support for SigV4 with credential and region auto-detection.
- **Automated Redirect Handling**: Correctly follows 307 temporary redirects while preserving range headers and re-signing requests.
- **Credential File Support**: Loads credentials from `~/.aws/credentials` and `~/.aws/config` with profile support.
- **Reliability & Integrity**:
  - **Exponential Backoff**: Automatic retries with jitter for transient errors.
  - **ETag Checksum Verification**: Post-download MD5 validation for object integrity.
  - **Atomic Writes**: Downloads to a temporary file and renames it only upon successful completion.
- **Container Optimized**: Automatically detects cgroup CPU quotas and adjusts `GOMAXPROCS`.

## Installation

```bash
go build -o gos3 .
```

## Usage

### Explicit Access Keys
```bash
./gos3 \
  --endpoint https://s3.amazonaws.com \
  --bucket my-bucket \
  --key my-object.tar \
  --output local-file.tar \
  --access-key AKIAIOSFODNN7EXAMPLE \
  --secret-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### With AWS Profile (from `~/.aws/credentials`)
```bash
./gos3 --profile my-profile --bucket my-bucket --key my-object.tar --output local-file.tar
```

### With Environment Variables
```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
./gos3 --bucket my-bucket --key my-object.tar --output local-file.tar
```

### Tuning for Performance
```bash
./gos3 --concurrency 32 --chunk-size-mb 128 --bucket my-bucket --key my-object.tar --output local-file.tar
```

## Authentication Priority

Credentials are resolved in this order (highest to lowest):

1. `--access-key` / `--secret-key` CLI flags
2. `--profile` from `~/.aws/credentials`
3. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables
4. `default` profile from `~/.aws/credentials`

## Flags Reference

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--endpoint` | *(required)* | S3-compatible endpoint URL |
| `--bucket` | *(required)* | S3 bucket name |
| `--key` | *(required)* | S3 object key |
| `--output` | *(required)* | Local output path |
| `--access-key` | | AWS access key ID |
| `--secret-key` | | AWS secret access key |
| `--profile` | | AWS credentials profile (`~/.aws/credentials`) |
| `--region` | `us-east-1` | AWS region |
| `--concurrency` | `16` | Number of parallel chunk downloads |
| `--chunk-size-mb` | `64` | Chunk size in MB |
| `--max-retries` | `3` | Max retries per chunk on transient errors |
| `--skip-checksum` | `false` | Skip ETag/MD5 checksum verification |
| `--chunk-timeout` | `5m` | Per-chunk download timeout |
| `--insecure` | `false` | Skip TLS certificate verification |
| `--verbose` | `false` | Enable verbose logging |

## Benchmarks

Benchmark results for a ~829 MiB file in `us-east-1`:

| Downloader | Configuration | Duration | Avg Speed |
| :--- | :--- | :--- | :--- |
| **AWS CLI (`s3 cp`)** | Defaults | 16.15s | 48.9 MB/s |
| **gos3 (Tuned)** | 32 Concurrency, 128MB Chunks | 18.42s | 42.9 MB/s |
| **gos3 (Default)** | 16 Concurrency, 64MB Chunks | 20.64s | 38.3 MB/s |

## License

BSD 3-Clause License - see the [LICENSE](LICENSE) file for details.
