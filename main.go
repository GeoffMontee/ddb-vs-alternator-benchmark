package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	// Import the alternator-client-golang sdkv2 subpackage
	helper "github.com/scylladb/alternator-client-golang/sdkv2"
)

// originalStderr holds the real stderr before we redirect it
var originalStderr *os.File

// filteringWriter filters out SDK warning messages
type filteringWriter struct {
	writer io.Writer
}

func (fw *filteringWriter) Write(p []byte) (n int, err error) {
	s := string(p)
	// Filter out SDK warnings
	if strings.Contains(s, "failed to close HTTP response body") {
		return len(p), nil
	}
	if strings.Contains(s, "SDK") && strings.Contains(s, "WARN") {
		return len(p), nil
	}
	return fw.writer.Write(p)
}

func init() {
	// Save original stderr
	originalStderr = os.Stderr

	// Create a pipe
	r, w, err := os.Pipe()
	if err != nil {
		return // Fall back to normal stderr
	}

	// Replace stderr with the write end of the pipe
	os.Stderr = w

	// Also redirect the log package to our filtered output
	log.SetOutput(&filteringWriter{writer: originalStderr})

	// Start a goroutine to read from the pipe and filter
	go func() {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Text()
			// Filter out SDK warnings
			if strings.Contains(line, "failed to close HTTP response body") {
				continue
			}
			if strings.HasPrefix(line, "SDK") && strings.Contains(line, "WARN") {
				continue
			}
			fmt.Fprintln(originalStderr, line)
		}
	}()
}

// Command-line flags
var (
	target              = flag.String("target", "ddb", "Target database: 'ddb' for DynamoDB or 'alternator' for ScyllaDB Alternator")
	threads             = flag.Int("threads", 16, "Number of concurrent worker threads")
	duration            = flag.Int("duration", 300, "Duration of the benchmark in seconds")
	warmup              = flag.Int("warmup", 10, "Warmup period in seconds (metrics discarded)")
	scyllaContactPoints = flag.String("scylla-contact-points", "", "Comma-separated list of ScyllaDB contact points (IP addresses or hostnames)")
	scyllaPort          = flag.Int("scylla-port", 8000, "ScyllaDB Alternator port")
	region              = flag.String("region", "us-east-1", "AWS region for DynamoDB")
	tableName           = flag.String("table-name", "benchmark_table", "Name of the table to use for benchmarking")
	maxConns            = flag.Int("max-conns", 0, "Max HTTP connections per host (0 = 2x threads)")
	directMode          = flag.Bool("direct", false, "Bypass alternator-client-golang library and connect directly to first node (for debugging)")

	// Table management flags
	createTableFlag  = flag.Bool("create-table", true, "Create the table if it does not exist")
	dropTable        = flag.Bool("drop-table", false, "Drop the table after the benchmarks")
	performBenchmark = flag.Bool("perform-benchmark", true, "Perform the benchmark")

	// Data seeding flags
	seedItems     = flag.Int("seed-items", 0, "Number of items to seed before benchmark (0 = no seeding)")
	seedBatchSize = flag.Int("seed-batch-size", 25, "Number of items per BatchWriteItem during seeding (max 25)")

	// Operation mix flags
	readPct = flag.Int("read-pct", 50, "Percentage of read operations (0-100). Reads = GetItem + BatchGetItem. Writes = PutItem + UpdateItem + BatchWriteItem. ListTables is separate.")
)

// Operation types for latency tracking
const (
	OpGetItem = iota
	OpPutItem
	OpUpdateItem
	OpBatchGetItem
	OpBatchWriteItem
	OpListTables
	OpCount
)

var opNames = []string{"GetItem", "PutItem", "UpdateItem", "BatchGetItem", "BatchWriteItem", "ListTables"}

// LatencyTracker tracks latencies for a single operation type
type LatencyTracker struct {
	mu        sync.Mutex
	latencies []int64 // in microseconds
	count     int64
	errors    int64
}

func (lt *LatencyTracker) Record(latencyUs int64) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.latencies = append(lt.latencies, latencyUs)
	lt.count++
}

func (lt *LatencyTracker) RecordError() {
	atomic.AddInt64(&lt.errors, 1)
}

func (lt *LatencyTracker) GetStats() (count, errors int64, p50, p95, p99, avg float64) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	count = lt.count
	errors = atomic.LoadInt64(&lt.errors)

	if len(lt.latencies) == 0 {
		return count, errors, 0, 0, 0, 0
	}

	// Sort for percentiles
	sorted := make([]int64, len(lt.latencies))
	copy(sorted, lt.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	n := len(sorted)
	p50 = float64(sorted[n*50/100]) / 1000.0 // Convert to ms
	p95 = float64(sorted[n*95/100]) / 1000.0
	p99 = float64(sorted[n*99/100]) / 1000.0

	var sum int64
	for _, v := range sorted {
		sum += v
	}
	avg = float64(sum) / float64(n) / 1000.0

	return
}

func (lt *LatencyTracker) Reset() {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.latencies = lt.latencies[:0]
	lt.count = 0
	atomic.StoreInt64(&lt.errors, 0)
}

// Metrics holds all operation trackers
type Metrics struct {
	trackers [OpCount]*LatencyTracker
}

func NewMetrics() *Metrics {
	m := &Metrics{}
	for i := 0; i < OpCount; i++ {
		m.trackers[i] = &LatencyTracker{
			latencies: make([]int64, 0, 10000),
		}
	}
	return m
}

func (m *Metrics) Record(op int, latencyUs int64) {
	m.trackers[op].Record(latencyUs)
}

func (m *Metrics) RecordError(op int) {
	m.trackers[op].RecordError()
}

func (m *Metrics) Reset() {
	for i := 0; i < OpCount; i++ {
		m.trackers[i].Reset()
	}
}

func (m *Metrics) GetTotalOps() int64 {
	var total int64
	for i := 0; i < OpCount; i++ {
		total += atomic.LoadInt64(&m.trackers[i].count)
	}
	return total
}

func (m *Metrics) GetTotalErrors() int64 {
	var total int64
	for i := 0; i < OpCount; i++ {
		total += atomic.LoadInt64(&m.trackers[i].errors)
	}
	return total
}

// Global metrics
var metrics = NewMetrics()

// Warmup state
var warmupComplete int32 // 0 = warming up, 1 = complete

// Retry configuration
const (
	maxRetries       = 5
	initialBackoffMs = 1000 // 1 second initial backoff
)

func main() {
	flag.Parse()

	// Validate flags
	if err := validateFlags(); err != nil {
		log.Fatalf("Invalid flags: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Create DynamoDB client based on target
	client, err := createClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Create the benchmark table if requested
	if *createTableFlag {
		if err := createBenchmarkTable(ctx, client); err != nil {
			log.Fatalf("Failed to create benchmark table: %v", err)
		}
	} else {
		log.Printf("Skipping table creation (--create-table=false)")
	}

	// Seed data if requested
	if *seedItems > 0 {
		if err := seedData(ctx, client); err != nil {
			log.Fatalf("Failed to seed data: %v", err)
		}
	}

	// Run benchmark if requested
	if *performBenchmark {
		runBenchmark(ctx, client, sigChan, cancel)
	} else {
		log.Printf("Skipping benchmark (--perform-benchmark=false)")
	}

	// Cleanup - delete the table if requested
	if *dropTable {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		if err := deleteBenchmarkTable(cleanupCtx, client); err != nil {
			log.Printf("Warning: Failed to delete benchmark table: %v", err)
		}
	} else {
		log.Printf("Table %s retained (use --drop-table=true to delete)", *tableName)
	}

	log.Println("Completed successfully")
}

func runBenchmark(ctx context.Context, client *dynamodb.Client, sigChan chan os.Signal, cancel context.CancelFunc) {
	log.Printf("Starting benchmark against %s with %d threads for %d seconds (warmup: %ds)", *target, *threads, *duration, *warmup)
	log.Printf("Table name: %s", *tableName)
	log.Printf("Read percentage: %d%% (reads=GetItem+BatchGetItem, writes=PutItem+UpdateItem+BatchWriteItem)", *readPct)

	// Start workers
	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	for i := 0; i < *threads; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(ctx, client, workerID, stopChan)
		}(i)
	}

	// Start metrics reporter
	go reportMetrics(stopChan)

	// Handle warmup period
	if *warmup > 0 {
		log.Printf("Warmup period: %d seconds...", *warmup)
		select {
		case <-time.After(time.Duration(*warmup) * time.Second):
			metrics.Reset()
			atomic.StoreInt32(&warmupComplete, 1)
			log.Printf("Warmup complete, starting measurement")
		case sig := <-sigChan:
			log.Printf("Received signal %v during warmup, shutting down...", sig)
			cancel()
			close(stopChan)
			wg.Wait()
			return
		}
	} else {
		atomic.StoreInt32(&warmupComplete, 1)
	}

	// Wait for duration or signal
	select {
	case <-time.After(time.Duration(*duration) * time.Second):
		log.Println("Benchmark duration completed")
	case sig := <-sigChan:
		log.Printf("Received signal %v, shutting down immediately...", sig)
		cancel()
	}

	// Signal workers to stop
	close(stopChan)

	// Wait for all workers to finish
	wg.Wait()

	// Print final metrics
	printFinalMetrics()
}

func seedData(ctx context.Context, client *dynamodb.Client) error {
	log.Printf("Seeding %d items with batch size %d...", *seedItems, *seedBatchSize)

	batchSize := *seedBatchSize
	if batchSize > 25 {
		batchSize = 25 // DynamoDB limit
	}

	totalBatches := (*seedItems + batchSize - 1) / batchSize
	itemsWritten := 0

	startTime := time.Now()

	for batch := 0; batch < totalBatches; batch++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		itemsInBatch := batchSize
		remaining := *seedItems - itemsWritten
		if remaining < itemsInBatch {
			itemsInBatch = remaining
		}

		writeRequests := make([]types.WriteRequest, 0, itemsInBatch)
		for i := 0; i < itemsInBatch; i++ {
			itemNum := itemsWritten + i
			pk := fmt.Sprintf("worker_%d", itemNum%(*threads))
			sk := fmt.Sprintf("item_%d", itemNum)

			writeRequests = append(writeRequests, types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"pk":        &types.AttributeValueMemberS{Value: pk},
						"sk":        &types.AttributeValueMemberS{Value: sk},
						"data":      &types.AttributeValueMemberS{Value: fmt.Sprintf("seed_data_%d", itemNum)},
						"timestamp": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().UnixNano())},
					},
				},
			})
		}

		_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				*tableName: writeRequests,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to seed batch %d: %w", batch, err)
		}

		itemsWritten += itemsInBatch

		// Progress update every 10%
		if (batch+1)%((totalBatches/10)+1) == 0 || batch == totalBatches-1 {
			elapsed := time.Since(startTime).Seconds()
			rate := float64(itemsWritten) / elapsed
			log.Printf("Seeding progress: %d/%d items (%.0f items/s)", itemsWritten, *seedItems, rate)
		}
	}

	elapsed := time.Since(startTime).Seconds()
	log.Printf("Seeding complete: %d items in %.1fs (%.0f items/s)", itemsWritten, elapsed, float64(itemsWritten)/elapsed)
	return nil
}

func validateFlags() error {
	if *target != "ddb" && *target != "alternator" {
		return fmt.Errorf("target must be 'ddb' or 'alternator', got '%s'", *target)
	}

	if *target == "alternator" && *scyllaContactPoints == "" {
		return fmt.Errorf("scylla-contact-points is required when target is 'alternator'")
	}

	if *threads < 1 {
		return fmt.Errorf("threads must be at least 1")
	}

	if *duration < 1 {
		return fmt.Errorf("duration must be at least 1 second")
	}

	if *warmup < 0 {
		return fmt.Errorf("warmup must be non-negative")
	}

	if *readPct < 0 || *readPct > 100 {
		return fmt.Errorf("read-pct must be between 0 and 100")
	}

	if *seedBatchSize < 1 || *seedBatchSize > 25 {
		return fmt.Errorf("seed-batch-size must be between 1 and 25")
	}

	return nil
}

// createHighConcurrencyHTTPClient creates an HTTP client optimized for high-concurrency workloads
func createHighConcurrencyHTTPClient() *http.Client {
	// Determine connection pool size
	poolSize := *maxConns
	if poolSize <= 0 {
		poolSize = *threads * 2 // Default: 2x the number of threads
	}

	log.Printf("HTTP connection pool: MaxIdleConnsPerHost=%d, MaxConnsPerHost=%d", poolSize, poolSize)

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          poolSize * 2, // Total idle connections across all hosts
		MaxIdleConnsPerHost:   poolSize,     // Idle connections per host
		MaxConnsPerHost:       poolSize,     // Max connections per host
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second,
	}
}

// createAlternatorHTTPClient creates an HTTP client for Alternator
func createAlternatorHTTPClient() *http.Client {
	// Determine connection pool size
	poolSize := *maxConns
	if poolSize <= 0 {
		poolSize = *threads * 2 // Default: 2x the number of threads
	}

	log.Printf("Alternator HTTP connection pool: MaxIdleConnsPerHost=%d, MaxConnsPerHost=%d", poolSize, poolSize)

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     false, // Alternator uses HTTP/1.1
		MaxIdleConns:          poolSize * 2,
		MaxIdleConnsPerHost:   poolSize,
		MaxConnsPerHost:       poolSize,
		IdleConnTimeout:       90 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		DisableCompression:    false,
		WriteBufferSize:       64 * 1024,
		ReadBufferSize:        64 * 1024,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second,
	}
}

func createClient(ctx context.Context) (*dynamodb.Client, error) {
	switch *target {
	case "ddb":
		return createDynamoDBClient(ctx)
	case "alternator":
		return createAlternatorClient(ctx)
	default:
		return nil, fmt.Errorf("unknown target: %s", *target)
	}
}

func createDynamoDBClient(ctx context.Context) (*dynamodb.Client, error) {
	httpClient := createHighConcurrencyHTTPClient()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(*region),
		config.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	return client, nil
}

func createAlternatorClient(ctx context.Context) (*dynamodb.Client, error) {
	contactPoints := strings.Split(*scyllaContactPoints, ",")
	for i, cp := range contactPoints {
		contactPoints[i] = strings.TrimSpace(cp)
	}

	// Direct mode: bypass alternator-client-golang library entirely
	// This helps isolate whether the library is the bottleneck
	if *directMode {
		endpoint := fmt.Sprintf("http://%s:%d", contactPoints[0], *scyllaPort)
		log.Printf("DIRECT MODE: Connecting directly to %s (bypassing alternator-client-golang)", endpoint)

		// Create HTTP client with high concurrency settings
		httpClient := createAlternatorHTTPClient()

		// Use config.LoadDefaultConfig with custom endpoint resolver
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion("us-east-1"), // Alternator ignores this but SDK requires it
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				"alternator",  // Access key
				"secret_pass", // Secret key
				"",            // Session token
			)),
			config.WithHTTPClient(httpClient),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to load config: %w", err)
		}

		// Create DynamoDB client with custom endpoint
		client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})

		return client, nil
	}

	// Use the ScyllaDB alternator-client-golang sdkv2 library v1.0.5
	log.Printf("Using alternator-client-golang library with %d contact points", len(contactPoints))

	// Build helper options
	helperOpts := []helper.Option{
		helper.WithPort(*scyllaPort),
		helper.WithCredentials("alternator", "secret_pass"),
		helper.WithOptimizeHeaders(true),
		helper.WithIgnoreServerCertificateError(true),
		helper.WithScheme("http"),
	}

	// Only add custom HTTP client if max-conns is explicitly set
	// (the library may have issues with custom HTTP clients)
	if *maxConns > 0 {
		httpClient := createAlternatorHTTPClient()
		helperOpts = append(helperOpts, helper.WithAWSConfigOptions(func(cfg *aws.Config) {
			cfg.HTTPClient = httpClient
		}))
		log.Printf("Using custom HTTP client with MaxConnsPerHost=%d", *maxConns)
	}

	h, err := helper.NewHelper(contactPoints, helperOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create alternator helper: %w", err)
	}

	// Create the DynamoDB client from the helper
	client, err := h.NewDynamoDB()
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamodb client from helper: %w", err)
	}

	return client, nil
}

func createBenchmarkTable(ctx context.Context, client *dynamodb.Client) error {
	log.Printf("Creating benchmark table: %s", *tableName)

	// Check if table already exists
	_, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(*tableName),
	})
	if err == nil {
		log.Printf("Table %s already exists", *tableName)
		return nil
	}

	// Create table with partition key (pk) and sort key (sk)
	createInput := &dynamodb.CreateTableInput{
		TableName: aws.String(*tableName),
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("pk"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("sk"),
				KeyType:       types.KeyTypeRange,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("pk"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("sk"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	}

	_, err = client.CreateTable(ctx, createInput)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Wait for table to become active
	log.Printf("Waiting for table %s to become active...", *tableName)
	waiter := dynamodb.NewTableExistsWaiter(client)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(*tableName),
	}, 5*time.Minute)
	if err != nil {
		return fmt.Errorf("failed waiting for table to become active: %w", err)
	}

	log.Printf("Table %s is ready", *tableName)
	return nil
}

func deleteBenchmarkTable(ctx context.Context, client *dynamodb.Client) error {
	log.Printf("Deleting benchmark table: %s", *tableName)

	_, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(*tableName),
	})
	if err != nil {
		return fmt.Errorf("failed to delete table: %w", err)
	}

	log.Printf("Table %s deleted", *tableName)
	return nil
}

func runWorker(ctx context.Context, client *dynamodb.Client, workerID int, stopChan <-chan struct{}) {
	log.Printf("Worker %d started", workerID)

	// Create a per-worker random source to avoid global rand mutex contention
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

	for {
		// Check for shutdown signals first
		select {
		case <-stopChan:
			log.Printf("Worker %d stopping (stopChan)", workerID)
			return
		case <-ctx.Done():
			log.Printf("Worker %d stopping (context cancelled)", workerID)
			return
		default:
			// Continue with work
		}

		// Select operation based on read-pct
		// Reads: GetItem, BatchGetItem
		// Writes: PutItem, UpdateItem, BatchWriteItem
		// ListTables: always 5% chance
		roll := rng.Intn(100)

		if roll < 5 {
			// 5% ListTables
			performListTables(ctx, client, workerID, rng)
		} else if roll < 5+(*readPct*95/100) {
			// Read operation (scaled to remaining 95%)
			if rng.Intn(2) == 0 {
				performGetItem(ctx, client, workerID, rng)
			} else {
				performBatchGetItem(ctx, client, workerID, rng)
			}
		} else {
			// Write operation
			writeType := rng.Intn(3)
			switch writeType {
			case 0:
				performPutItem(ctx, client, workerID, rng)
			case 1:
				performUpdateItem(ctx, client, workerID, rng)
			case 2:
				performBatchWriteItem(ctx, client, workerID, rng)
			}
		}

		// Check again after operation completes
		select {
		case <-stopChan:
			log.Printf("Worker %d stopping (stopChan)", workerID)
			return
		case <-ctx.Done():
			log.Printf("Worker %d stopping (context cancelled)", workerID)
			return
		default:
			// Continue to next operation
		}
	}
}

func withRetry(ctx context.Context, operationName string, rng *rand.Rand, operation func() error) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Check if context is cancelled before attempting
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := operation()
		if err == nil {
			return nil
		}

		// Check if context is cancelled - don't retry if shutting down
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		lastErr = err

		// Don't log or retry on last attempt
		if attempt == maxRetries-1 {
			break
		}

		// Calculate exponential backoff with jitter
		backoffMs := initialBackoffMs * (1 << attempt) // 1s, 2s, 4s, 8s, 16s
		jitter := rng.Intn(backoffMs / 4)              // Add some jitter
		sleepDuration := time.Duration(backoffMs+jitter) * time.Millisecond

		log.Printf("[%s] Attempt %d failed: %v. Retrying in %v...", operationName, attempt+1, lastErr, sleepDuration)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepDuration):
			// Continue to next attempt
		}
	}

	return fmt.Errorf("all %d retries failed for %s: %w", maxRetries, operationName, lastErr)
}

// isShutdownError returns true if the error is due to context cancellation (Ctrl+C)
func isShutdownError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func performPutItem(ctx context.Context, client *dynamodb.Client, workerID int, rng *rand.Rand) {
	pk := fmt.Sprintf("worker_%d", workerID)
	sk := fmt.Sprintf("item_%d", rng.Intn(1000))
	data := fmt.Sprintf("data_%d_%d", workerID, time.Now().UnixNano())

	start := time.Now()
	err := withRetry(ctx, "PutItem", rng, func() error {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(*tableName),
			Item: map[string]types.AttributeValue{
				"pk":        &types.AttributeValueMemberS{Value: pk},
				"sk":        &types.AttributeValueMemberS{Value: sk},
				"data":      &types.AttributeValueMemberS{Value: data},
				"timestamp": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().UnixNano())},
			},
		})
		return err
	})
	latencyUs := time.Since(start).Microseconds()

	if err != nil && !isShutdownError(err) {
		metrics.RecordError(OpPutItem)
	} else if err == nil {
		metrics.Record(OpPutItem, latencyUs)
	}
}

func performGetItem(ctx context.Context, client *dynamodb.Client, workerID int, rng *rand.Rand) {
	pk := fmt.Sprintf("worker_%d", rng.Intn(*threads))
	sk := fmt.Sprintf("item_%d", rng.Intn(1000))

	start := time.Now()
	err := withRetry(ctx, "GetItem", rng, func() error {
		_, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(*tableName),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: pk},
				"sk": &types.AttributeValueMemberS{Value: sk},
			},
		})
		return err
	})
	latencyUs := time.Since(start).Microseconds()

	if err != nil && !isShutdownError(err) {
		metrics.RecordError(OpGetItem)
	} else if err == nil {
		metrics.Record(OpGetItem, latencyUs)
	}
}

func performUpdateItem(ctx context.Context, client *dynamodb.Client, workerID int, rng *rand.Rand) {
	pk := fmt.Sprintf("worker_%d", workerID)
	sk := fmt.Sprintf("item_%d", rng.Intn(1000))

	start := time.Now()
	err := withRetry(ctx, "UpdateItem", rng, func() error {
		_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(*tableName),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: pk},
				"sk": &types.AttributeValueMemberS{Value: sk},
			},
			UpdateExpression: aws.String("SET #data = :data, #ts = :timestamp"),
			ExpressionAttributeNames: map[string]string{
				"#data": "data",
				"#ts":   "timestamp",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":data":      &types.AttributeValueMemberS{Value: fmt.Sprintf("updated_%d_%d", workerID, time.Now().UnixNano())},
				":timestamp": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().UnixNano())},
			},
		})
		return err
	})
	latencyUs := time.Since(start).Microseconds()

	if err != nil && !isShutdownError(err) {
		metrics.RecordError(OpUpdateItem)
	} else if err == nil {
		metrics.Record(OpUpdateItem, latencyUs)
	}
}

func performBatchWriteItem(ctx context.Context, client *dynamodb.Client, workerID int, rng *rand.Rand) {
	// Batch size between 5 and 25 items
	batchSize := 5 + rng.Intn(21)
	pk := fmt.Sprintf("batch_worker_%d", workerID)

	// Use a map to track used SKs and ensure uniqueness
	usedSKs := make(map[string]bool)
	writeRequests := make([]types.WriteRequest, 0, batchSize)

	for len(writeRequests) < batchSize {
		// Generate unique SK using timestamp + counter
		sk := fmt.Sprintf("batch_item_%d_%d_%d", workerID, time.Now().UnixNano(), len(writeRequests))
		if usedSKs[sk] {
			continue
		}
		usedSKs[sk] = true

		writeRequests = append(writeRequests, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"pk":        &types.AttributeValueMemberS{Value: pk},
					"sk":        &types.AttributeValueMemberS{Value: sk},
					"data":      &types.AttributeValueMemberS{Value: fmt.Sprintf("batch_data_%d", rng.Int())},
					"timestamp": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().UnixNano())},
				},
			},
		})
	}

	start := time.Now()
	err := withRetry(ctx, "BatchWriteItem", rng, func() error {
		_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				*tableName: writeRequests,
			},
		})
		return err
	})
	latencyUs := time.Since(start).Microseconds()

	if err != nil && !isShutdownError(err) {
		metrics.RecordError(OpBatchWriteItem)
	} else if err == nil {
		metrics.Record(OpBatchWriteItem, latencyUs)
	}
}

func performBatchGetItem(ctx context.Context, client *dynamodb.Client, workerID int, rng *rand.Rand) {
	// Batch size between 5 and 100 items
	batchSize := 5 + rng.Intn(96)

	// Use a map to ensure unique key combinations
	usedKeys := make(map[string]bool)
	keys := make([]map[string]types.AttributeValue, 0, batchSize)

	for len(keys) < batchSize {
		pk := fmt.Sprintf("worker_%d", rng.Intn(*threads))
		sk := fmt.Sprintf("item_%d", rng.Intn(1000))
		keyStr := pk + "|" + sk

		if usedKeys[keyStr] {
			continue
		}
		usedKeys[keyStr] = true

		keys = append(keys, map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
			"sk": &types.AttributeValueMemberS{Value: sk},
		})
	}

	start := time.Now()
	err := withRetry(ctx, "BatchGetItem", rng, func() error {
		_, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
			RequestItems: map[string]types.KeysAndAttributes{
				*tableName: {
					Keys: keys,
				},
			},
		})
		return err
	})
	latencyUs := time.Since(start).Microseconds()

	if err != nil && !isShutdownError(err) {
		metrics.RecordError(OpBatchGetItem)
	} else if err == nil {
		metrics.Record(OpBatchGetItem, latencyUs)
	}
}

func performListTables(ctx context.Context, client *dynamodb.Client, workerID int, rng *rand.Rand) {
	start := time.Now()
	err := withRetry(ctx, "ListTables", rng, func() error {
		_, err := client.ListTables(ctx, &dynamodb.ListTablesInput{
			Limit: aws.Int32(10),
		})
		return err
	})
	latencyUs := time.Since(start).Microseconds()

	if err != nil && !isShutdownError(err) {
		metrics.RecordError(OpListTables)
	} else if err == nil {
		metrics.Record(OpListTables, latencyUs)
	}
}

func reportMetrics(stopChan <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			if atomic.LoadInt32(&warmupComplete) == 0 {
				log.Printf("[WARMUP] Still warming up...")
				continue
			}

			elapsed := time.Since(startTime).Seconds()
			totalOps := metrics.GetTotalOps()
			totalErrs := metrics.GetTotalErrors()

			// Get per-operation counts
			var counts [OpCount]int64
			for i := 0; i < OpCount; i++ {
				counts[i] = atomic.LoadInt64(&metrics.trackers[i].count)
			}

			log.Printf("[METRICS] Elapsed: %.0fs | Ops: %d (%.1f/s) | Get: %d | Put: %d | Update: %d | BatchGet: %d | BatchWrite: %d | List: %d | Err: %d",
				elapsed, totalOps, float64(totalOps)/elapsed,
				counts[OpGetItem], counts[OpPutItem], counts[OpUpdateItem],
				counts[OpBatchGetItem], counts[OpBatchWriteItem], counts[OpListTables], totalErrs)
		}
	}
}

func printFinalMetrics() {
	fmt.Println("\n========== FINAL BENCHMARK RESULTS ==========")
	fmt.Printf("Target:              %s\n", *target)
	fmt.Printf("Threads:             %d\n", *threads)
	fmt.Printf("Duration (seconds):  %d\n", *duration)
	fmt.Printf("Warmup (seconds):    %d\n", *warmup)
	fmt.Printf("Read Percentage:     %d%%\n", *readPct)
	fmt.Printf("Table Name:          %s\n", *tableName)
	fmt.Println("----------------------------------------------")

	var totalOps, totalErrs int64
	for i := 0; i < OpCount; i++ {
		count, errs, p50, p95, p99, avg := metrics.trackers[i].GetStats()
		totalOps += count
		totalErrs += errs

		if count > 0 {
			fmt.Printf("\n%s:\n", opNames[i])
			fmt.Printf("  Count:    %d\n", count)
			fmt.Printf("  Errors:   %d\n", errs)
			fmt.Printf("  Avg (ms): %.2f\n", avg)
			fmt.Printf("  P50 (ms): %.2f\n", p50)
			fmt.Printf("  P95 (ms): %.2f\n", p95)
			fmt.Printf("  P99 (ms): %.2f\n", p99)
		}
	}

	fmt.Println("\n----------------------------------------------")
	fmt.Printf("Total Operations:    %d\n", totalOps)
	fmt.Printf("Operations/Second:   %.2f\n", float64(totalOps)/float64(*duration))
	fmt.Printf("Total Errors:        %d\n", totalErrs)
	if totalOps+totalErrs > 0 {
		fmt.Printf("Error Rate:          %.2f%%\n", float64(totalErrs)/float64(totalOps+totalErrs)*100)
	}
	fmt.Println("==============================================")
}
