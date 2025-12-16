# DynamoDB vs ScyllaDB Alternator Benchmark Tool

A Go-based benchmarking tool for comparing performance between AWS DynamoDB and ScyllaDB's Alternator (DynamoDB-compatible API).

## Features

- Supports both AWS DynamoDB and ScyllaDB Alternator
- Configurable number of concurrent worker threads
- Multiple DynamoDB operations: GetItem, PutItem, UpdateItem, BatchGetItem, BatchWriteItem, ListTables
- Variable batch sizes for batch operations
- Exponential backoff retry logic (5 retries with increasing delays: 1s, 2s, 4s, 8s, 16s)
- Real-time metrics reporting every 10 seconds
- Graceful shutdown on SIGINT/SIGTERM
- Automatic table creation and cleanup

## ScyllaDB Library Options

This benchmark provides **two implementations** for connecting to ScyllaDB Alternator:

### Option 1: alternator-client-golang (Default - main.go)
Uses `github.com/scylladb/alternator-client-golang` v1.0.5 with the `helper` package:
- `helper.WithIgnoreServerCertificateError(true)` - Skip TLS cert validation
- `helper.WithOptimizeHeaders(true)` - Optimize HTTP headers for Alternator
- `helper.WithPort(port)` - Set the Alternator port
- `helper.WithScheme("http")` - Use HTTP or HTTPS

### Option 2: alternator-load-balancing (Alternative - main_alternative.go)
Uses `github.com/scylladb/alternator-load-balancing/go/v2`:
- Well-documented library with stable API
- Built-in node discovery via `/localnodes`
- Simple configuration

To use the alternative implementation:
```bash
# Rename files
mv main.go main_acg.go
mv main_alternative.go main.go

# Update go.mod to use alternator-load-balancing instead
```

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
# If using git
git clone <repository-url>
cd ddb-benchmark

# Or simply navigate to the directory containing the source files
cd ddb-benchmark
```

### 3. Download dependencies

```bash
# This downloads all required Go modules
go mod tidy
```

This command will download:
- AWS SDK v2 for Go
- ScyllaDB alternator-client-golang v1.0.5 (or alternator-load-balancing for alternative)
- All transitive dependencies

## Usage

### Command-Line Parameters

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| `--target` | Target database: `ddb` or `alternator` | `ddb` | No |
| `--threads` | Number of concurrent worker threads | `16` | No |
| `--duration` | Benchmark duration in seconds | `300` | No |
| `--table-name` | Name of the benchmark table | `benchmark_table` | No |
| `--region` | AWS region (DynamoDB only) | `us-east-1` | No |
| `--scylla-contact-points` | Comma-separated ScyllaDB nodes | - | Yes (for alternator) |
| `--scylla-port` | ScyllaDB Alternator port | `8000` | No |

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

# Multiple nodes
go run main.go --target alternator \
    --scylla-contact-points "192.168.1.100,192.168.1.101,192.168.1.102" \
    --scylla-port 8000 \
    --threads 32 \
    --duration 600

# Using hostnames
go run main.go --target alternator \
    --scylla-contact-points "scylla-node1.example.com,scylla-node2.example.com" \
    --scylla-port 8000
```

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
2. **Final summary** at the end of the benchmark

Example output:
```
2024/01/15 10:00:00 Starting benchmark against alternator with 16 threads for 300 seconds
2024/01/15 10:00:00 Table name: benchmark_table
2024/01/15 10:00:10 [METRICS] Elapsed: 10s | Total Ops: 1523 (152.3/s) | GetItem: 245 | PutItem: 267 | UpdateItem: 251 | BatchGet: 253 | BatchWrite: 248 | ListTables: 259 | Errors: 0

========== FINAL BENCHMARK RESULTS ==========
Target:              alternator
Threads:             16
Duration (seconds):  300
Table Name:          benchmark_table
----------------------------------------------
Total Operations:    45690
Operations/Second:   152.30
----------------------------------------------
GetItem:             7450
PutItem:             7612
UpdateItem:          7589
BatchGetItem:        7520
BatchWriteItem:      7498
ListTables:          8021
----------------------------------------------
Total Errors:        12
Error Rate:          0.03%
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

## Troubleshooting

### AWS Credentials Not Found
```bash
# Set environment variables
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_REGION=us-east-1

# Or configure AWS CLI
aws configure
```

### ScyllaDB Connection Refused
1. Ensure Alternator is enabled in ScyllaDB configuration
2. Check the port is correct (default: 8000)
3. Verify network connectivity to ScyllaDB nodes

### Go Module Errors
```bash
# Clear module cache and re-download
go clean -modcache
go mod tidy
```

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

## Contributing

Contributions are welcome! Please submit issues and pull requests.
