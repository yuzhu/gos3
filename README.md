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

### Simple Download
```bash
./gos3 --endpoint https://s3.amazonaws.com --bucket my-bucket --key my-object.tar --output local-file.tar
```

### With AWS Profile
```bash
./gos3 --profile my-profile --bucket my-bucket --key my-object.tar --output local-file.tar
```

### Tuning for Performance
```bash
./gos3 --concurrency 32 --chunk-size-mb 128 --bucket my-bucket --key my-object.tar --output local-file.tar
```

## Benchmarks

Benchmark results for a ~829 MiB file in `us-east-1`:

| Downloader | Configuration | Duration | Avg Speed |
| :--- | :--- | :--- | :--- |
| **AWS CLI (`s3 cp`)** | Defaults | 16.15s | 48.9 MB/s |
| **gos3 (Tuned)** | 32 Concurrency, 128MB Chunks | 18.42s | 42.9 MB/s |
| **gos3 (Default)** | 16 Concurrency, 64MB Chunks | 20.64s | 38.3 MB/s |

## License

BSD 3-Clause License - see the [LICENSE](LICENSE) file for details.
