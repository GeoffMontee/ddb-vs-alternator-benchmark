# DynamoDB vs ScyllaDB Alternator Benchmark Tool

A Go-based benchmarking tool for comparing performance between AWS DynamoDB and ScyllaDB's Alternator (DynamoDB-compatible API).

## Features

- Supports both AWS DynamoDB and ScyllaDB Alternator
- Configurable number of concurrent worker threads
- Multiple DynamoDB operations: GetItem, PutItem, UpdateItem, BatchGetItem, BatchWriteItem, ListTables
- **Percentile latency tracking (P50/P95/P99)** per operation type
- **Configurable warmup period** (metrics discarded during warmup)
- **Configurable read/write ratio** (`--read-pct`)
- **Data seeding** before benchmark (`--seed-items`)
- **Table management** (`--create-table`, `--drop-table`)
- Variable batch sizes for batch operations
- Exponential backoff retry logic (5 retries with increasing delays: 1s, 2s, 4s, 8s, 16s)
- Real-time metrics reporting every 10 seconds
- Graceful shutdown on SIGINT/SIGTERM

## ScyllaDB Alternator Client Library

This benchmark uses `github.com/scylladb/alternator-client-golang/sdkv2` v1.0.5 (the AWS SDK v2 subpackage) with the following options:
- `helper.WithPort(port)` - Set the Alternator port
- `helper.WithCredentials(accessKey, secretKey)` - Set credentials (ignored when auth is disabled)
- `helper.WithOptimizeHeaders(true)` - Optimize HTTP headers (reduces traffic up to 56%)
- `helper.WithIgnoreServerCertificateError(true)` - Skip TLS cert validation
- `helper.WithScheme("http")` - Use HTTP or "https" for TLS

The library provides automatic load balancing across all Alternator nodes.

## Prerequisites

- **Go 1.21 or later** - [Download Go](https://go.dev/dl/)
- For DynamoDB: Valid AWS credentials configured
- For ScyllaDB: A running ScyllaDB cluster with Alternator enabled

## Installation

### 1. Install Go (if not already installed)

**On Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install golang-go
```

**On macOS (using Homebrew):**
```bash
brew install go
```

**On Windows:**
Download and run the installer from [https://go.dev/dl/](https://go.dev/dl/)

### 2. Clone or download this project

```bash
cd ddb-benchmark
```

### 3. Download dependencies

```bash
# This downloads all required Go modules and generates go.sum
go mod tidy
```

This command will download:
- AWS SDK v2 for Go
- ScyllaDB alternator-client-golang/sdkv2 v1.0.5
- All transitive dependencies

## Troubleshooting go.sum / Module Issues

If you see errors about missing `go.sum` entries after running `go mod tidy`:

```bash
# Method 1: Run go mod download first
go mod download
go mod tidy

# Method 2: Clean the module cache and retry
go clean -modcache
go mod tidy

# Method 3: If specific packages are missing, download them explicitly
go get github.com/aws/aws-sdk-go-v2@v1.30.3
go get github.com/aws/aws-sdk-go-v2/config@v1.27.27
go get github.com/aws/aws-sdk-go-v2/credentials@v1.17.27
go get github.com/aws/aws-sdk-go-v2/service/dynamodb@v1.34.4
go get github.com/scylladb/alternator-client-golang/sdkv2@v1.0.5
```

**Note:** The alternator-client-golang library has separate subpackages for AWS SDK v1 and v2:
- `github.com/scylladb/alternator-client-golang/sdkv1` - for AWS SDK v1
- `github.com/scylladb/alternator-client-golang/sdkv2` - for AWS SDK v2 (used in this benchmark)

## Usage

### Command-Line Parameters

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| `--target` | Target database: `ddb` or `alternator` | `ddb` | No |
| `--threads` | Number of concurrent worker threads | `16` | No |
| `--duration` | Benchmark duration in seconds | `300` | No |
| `--warmup` | Warmup period in seconds (metrics discarded) | `10` | No |
| `--table-name` | Name of the benchmark table | `benchmark_table` | No |
| `--region` | AWS region (DynamoDB only) | `us-east-1` | No |
| `--scylla-contact-points` | Comma-separated ScyllaDB nodes | - | Yes (for alternator) |
| `--scylla-port` | ScyllaDB Alternator port | `8000` | No |
| `--max-conns` | Max HTTP connections per host (0 = 2x threads) | `0` | No |
| `--direct` | Bypass alternator-client-golang library | `false` | No |

#### Table Management

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--create-table` | Create the table if it does not exist | `true` |
| `--drop-table` | Drop the table after the benchmarks | `false` |
| `--perform-benchmark` | Perform the benchmark | `true` |

#### Data Seeding

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--seed-items` | Number of items to seed before benchmark (0 = no seeding) | `0` |
| `--seed-batch-size` | Number of items per BatchWriteItem during seeding (max 25) | `25` |

#### Key Partitioning (for parallel loaders)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--loader-id` | Loader ID for key partitioning (0 = no prefix). Use different IDs for parallel loaders. | `0` |
| `--key-range` | Number of unique sort keys per partition key | `1000` |

#### Operation Mix

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--read-pct` | Percentage of read operations (0-100) | `50` |

Reads = GetItem + BatchGetItem  
Writes = PutItem + UpdateItem + BatchWriteItem  
ListTables is always ~5% of operations.

### Running against AWS DynamoDB

Make sure your AWS credentials are configured (via `~/.aws/credentials`, environment variables, or IAM role).

```bash
# Basic run with defaults
go run main.go --target ddb

# Custom configuration
go run main.go --target ddb \
    --region us-west-2 \
    --threads 32 \
    --duration 600 \
    --table-name my_benchmark
```

### Running against ScyllaDB Alternator

```bash
# Single node
go run main.go --target alternator \
    --scylla-contact-points 192.168.1.100 \
    --scylla-port 8000

# Multiple nodes with full options
go run main.go --target alternator \
    --scylla-contact-points "192.168.1.100,192.168.1.101,192.168.1.102" \
    --scylla-port 8000 \
    --threads 32 \
    --duration 600 \
    --warmup 10 \
    --read-pct 80 \
    --max-conns 64
```

### Seeding Data Before Benchmark

```bash
# Seed 10000 items, then run benchmark
go run main.go --target alternator \
    --scylla-contact-points "192.168.1.100" \
    --seed-items 10000 \
    --seed-batch-size 25 \
    --threads 32 \
    --duration 60

# Seed data only (no benchmark)
go run main.go --target alternator \
    --scylla-contact-points "192.168.1.100" \
    --seed-items 10000 \
    --perform-benchmark=false \
    --drop-table=false
```

### Reusing an Existing Table

```bash
# Skip table creation, run benchmark, keep table after
go run main.go --target alternator \
    --scylla-contact-points "192.168.1.100" \
    --create-table=false \
    --drop-table=false \
    --threads 32 \
    --duration 60
```

### Running Parallel Loaders

Use `--loader-id` to run multiple loader instances with non-overlapping keys:

```bash
# Terminal 1: Loader 1
go run main.go --target alternator \
    --scylla-contact-points "192.168.1.100" \
    --loader-id 1 \
    --seed-items 100000 \
    --threads 32 \
    --duration 60

# Terminal 2: Loader 2
go run main.go --target alternator \
    --scylla-contact-points "192.168.1.100" \
    --loader-id 2 \
    --seed-items 100000 \
    --threads 32 \
    --duration 60

# Terminal 3: Loader 3
go run main.go --target alternator \
    --scylla-contact-points "192.168.1.100" \
    --loader-id 3 \
    --seed-items 100000 \
    --threads 32 \
    --duration 60
```

Each loader will use keys prefixed with `L1_`, `L2_`, `L3_`, etc., ensuring no key collisions.

### Building a Binary

```bash
# Build for current platform
go build -o ddb-benchmark main.go

# Run the binary
./ddb-benchmark --target ddb --threads 16 --duration 300

# Cross-compile for Linux (from macOS/Windows)
GOOS=linux GOARCH=amd64 go build -o ddb-benchmark-linux main.go
```

## Output

The tool provides:

1. **Real-time metrics** every 10 seconds showing operation counts and throughput
2. **Final summary** with per-operation latency percentiles (P50/P95/P99)

Example output:
```
2024/01/15 10:00:00 Starting benchmark against alternator with 32 threads for 60 seconds (warmup: 10s)
2024/01/15 10:00:00 Table name: benchmark_table
2024/01/15 10:00:00 Read percentage: 80% (reads=GetItem+BatchGetItem, writes=PutItem+UpdateItem+BatchWriteItem)
2024/01/15 10:00:00 Warmup period: 10 seconds...
2024/01/15 10:00:10 Warmup complete, starting measurement
2024/01/15 10:00:20 [METRICS] Elapsed: 10s | Ops: 5142 (514.2/s) | Get: 851 | Put: 861 | Update: 843 | BatchGet: 819 | BatchWrite: 937 | List: 831 | Err: 0

========== FINAL BENCHMARK RESULTS ==========
Target:              alternator
Threads:             32
Duration (seconds):  60
Warmup (seconds):    10
Read Percentage:     80%
Table Name:          benchmark_table
----------------------------------------------

GetItem:
  Count:    5100
  Errors:   0
  Avg (ms): 62.34
  P50 (ms): 61.00
  P95 (ms): 85.00
  P99 (ms): 102.00

PutItem:
  Count:    1275
  Errors:   0
  Avg (ms): 63.12
  P50 (ms): 62.00
  P95 (ms): 86.00
  P99 (ms): 105.00

... (other operations)

----------------------------------------------
Total Operations:    30852
Operations/Second:   514.20
Total Errors:        0
Error Rate:          0.00%
==============================================
```

## Operations Performed

| Operation | Description | Batch Size |
|-----------|-------------|------------|
| GetItem | Read a single item by key | 1 |
| PutItem | Write a single item | 1 |
| UpdateItem | Update attributes of an item | 1 |
| BatchGetItem | Read multiple items | 5-100 items |
| BatchWriteItem | Write multiple items | 5-25 items |
| ListTables | List tables in the database | N/A |

## Retry Logic

The tool implements exponential backoff retry to avoid request storms:

- **Max retries**: 5
- **Backoff schedule**:
  - 1st retry: ~1 second
  - 2nd retry: ~2 seconds
  - 3rd retry: ~4 seconds
  - 4th retry: ~8 seconds
  - 5th retry: ~16 seconds
- **Jitter**: Random jitter (up to 25% of backoff) is added to prevent thundering herd

## Table Schema

The benchmark creates a table with the following schema:

| Attribute | Type | Key Type |
|-----------|------|----------|
| `pk` | String | Partition Key (HASH) |
| `sk` | String | Sort Key (RANGE) |
| `data` | String | - |
| `timestamp` | Number | - |

- **DynamoDB**: Uses on-demand (PAY_PER_REQUEST) billing mode
- **ScyllaDB Alternator**: Uses provisioned throughput (100 RCU/WCU)

## ScyllaDB Alternator Setup

To enable Alternator on ScyllaDB 2025.3.4:

1. Edit `/etc/scylla/scylla.yaml`:
```yaml
alternator_port: 8000
alternator_write_isolation: always_use_lwt
# Disable auth for testing (not recommended for production)
alternator_enforce_authorization: false
```

2. Restart ScyllaDB:
```bash
sudo systemctl restart scylla-server
```

## License

MIT License - feel free to use and modify for your benchmarking needs.
