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
	scyllaContactPoints = flag.String("scylla-contact-points", "", "Comma-separated list of ScyllaDB contact points (IP addresses or hostnames)")
	scyllaPort          = flag.Int("scylla-port", 8000, "ScyllaDB Alternator port")
	region              = flag.String("region", "us-east-1", "AWS region for DynamoDB")
	tableName           = flag.String("table-name", "benchmark_table", "Name of the table to use for benchmarking")
	maxConns            = flag.Int("max-conns", 0, "Max HTTP connections per host (0 = 2x threads)")
	directMode          = flag.Bool("direct", false, "Bypass alternator-client-golang library and connect directly to first node (for debugging)")
)

// Metrics counters
type Metrics struct {
	getItemOps        int64
	putItemOps        int64
	updateItemOps     int64
	batchGetItemOps   int64
	batchWriteItemOps int64
	listTablesOps     int64
	errors            int64
	
	// Latency tracking (in microseconds)
	totalLatencyUs    int64
	opCount           int64
}

// Global metrics
var metrics = &Metrics{}

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

	// Create the benchmark table
	if err := createBenchmarkTable(ctx, client); err != nil {
		log.Fatalf("Failed to create benchmark table: %v", err)
	}

	log.Printf("Starting benchmark against %s with %d threads for %d seconds", *target, *threads, *duration)
	log.Printf("Table name: %s", *tableName)

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

	// Wait for duration or signal
	select {
	case <-time.After(time.Duration(*duration) * time.Second):
		log.Println("Benchmark duration completed")
	case sig := <-sigChan:
		log.Printf("Received signal %v, shutting down immediately...", sig)
		cancel() // Cancel context to stop all operations immediately
	}

	// Signal workers to stop
	close(stopChan)

	// Wait for all workers to finish
	wg.Wait()

	// Print final metrics
	printFinalMetrics()

	// Cleanup - delete the table (use background context since main context may be cancelled)
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cleanupCancel()
	if err := deleteBenchmarkTable(cleanupCtx, client); err != nil {
		log.Printf("Warning: Failed to delete benchmark table: %v", err)
	}

	log.Println("Benchmark completed successfully")
}

func validateFlags() error {
	if *target != "ddb" && *target != "alternator" {
		return fmt.Errorf("target must be 'ddb' or 'alternator', got '%s'", *target)
	}

	if *target == "alternator" && *scyllaContactPoints == "" {
		return fmt.Errorf("scylla-contact-points is required when target is 'alternator'")
	}

	if *threads <= 0 {
		return fmt.Errorf("threads must be positive, got %d", *threads)
	}

	if *duration <= 0 {
		return fmt.Errorf("duration must be positive, got %d", *duration)
	}

	if *tableName == "" {
		return fmt.Errorf("table-name cannot be empty")
	}

	return nil
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
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     false, // Alternator uses HTTP/1.1
		MaxIdleConns:          poolSize * 2,
		MaxIdleConnsPerHost:   poolSize,
		MaxConnsPerHost:       poolSize,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		// Don't set TLSClientConfig for HTTP connections - it can cause issues
	}

	return &http.Client{
		Transport: transport,
		// Don't set client-level timeout - let the SDK handle timeouts
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
		log.Printf("Table %s already exists, deleting it first...", *tableName)
		if err := deleteBenchmarkTable(ctx, client); err != nil {
			return err
		}
		// Wait a bit for the table to be fully deleted
		time.Sleep(5 * time.Second)
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
	}

	// Set billing mode based on target
	if *target == "ddb" {
		// DynamoDB: use on-demand billing
		createInput.BillingMode = types.BillingModePayPerRequest
	} else {
		// ScyllaDB Alternator: use provisioned throughput (supported in Scylla 2025.3.4)
		createInput.BillingMode = types.BillingModeProvisioned
		createInput.ProvisionedThroughput = &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(100),
			WriteCapacityUnits: aws.Int64(100),
		}
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
	}, 2*time.Minute)
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

	// Wait for table to be deleted
	waiter := dynamodb.NewTableNotExistsWaiter(client)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(*tableName),
	}, 2*time.Minute)
	if err != nil {
		return fmt.Errorf("failed waiting for table deletion: %w", err)
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

		// Randomly select an operation to perform
		operation := rng.Intn(6)
		switch operation {
		case 0:
			performPutItem(ctx, client, workerID, rng)
		case 1:
			performGetItem(ctx, client, workerID, rng)
		case 2:
			performUpdateItem(ctx, client, workerID, rng)
		case 3:
			performBatchWriteItem(ctx, client, workerID, rng)
		case 4:
			performBatchGetItem(ctx, client, workerID, rng)
		case 5:
			performListTables(ctx, client, workerID, rng)
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

		start := time.Now()
		err := operation()
		latencyUs := time.Since(start).Microseconds()
		
		// Track latency for successful ops
		if err == nil {
			atomic.AddInt64(&metrics.totalLatencyUs, latencyUs)
			atomic.AddInt64(&metrics.opCount, 1)
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

		log.Printf("[%s] Attempt %d failed (latency=%dms): %v. Retrying in %v...", operationName, attempt+1, latencyUs/1000, err, sleepDuration)

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

	err := withRetry(ctx, "PutItem", rng, func() error {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(*tableName),
			Item: map[string]types.AttributeValue{
				"pk":        &types.AttributeValueMemberS{Value: pk},
				"sk":        &types.AttributeValueMemberS{Value: sk},
				"data":      &types.AttributeValueMemberS{Value: fmt.Sprintf("data_%d_%d", workerID, time.Now().UnixNano())},
				"timestamp": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().UnixNano())},
			},
		})
		return err
	})

	if err != nil && !isShutdownError(err) {
		atomic.AddInt64(&metrics.errors, 1)
	} else if err == nil {
		atomic.AddInt64(&metrics.putItemOps, 1)
	}
}

func performGetItem(ctx context.Context, client *dynamodb.Client, workerID int, rng *rand.Rand) {
	pk := fmt.Sprintf("worker_%d", rng.Intn(*threads))
	sk := fmt.Sprintf("item_%d", rng.Intn(1000))

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

	if err != nil && !isShutdownError(err) {
		atomic.AddInt64(&metrics.errors, 1)
	} else if err == nil {
		atomic.AddInt64(&metrics.getItemOps, 1)
	}
}

func performUpdateItem(ctx context.Context, client *dynamodb.Client, workerID int, rng *rand.Rand) {
	pk := fmt.Sprintf("worker_%d", workerID)
	sk := fmt.Sprintf("item_%d", rng.Intn(1000))

	err := withRetry(ctx, "UpdateItem", rng, func() error {
		_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(*tableName),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: pk},
				"sk": &types.AttributeValueMemberS{Value: sk},
			},
			UpdateExpression: aws.String("SET #d = :data, #ts = :timestamp"),
			ExpressionAttributeNames: map[string]string{
				"#d":  "data",
				"#ts": "timestamp",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":data":      &types.AttributeValueMemberS{Value: fmt.Sprintf("updated_%d", time.Now().UnixNano())},
				":timestamp": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().UnixNano())},
			},
		})
		return err
	})

	if err != nil && !isShutdownError(err) {
		atomic.AddInt64(&metrics.errors, 1)
	} else if err == nil {
		atomic.AddInt64(&metrics.updateItemOps, 1)
	}
}

func performBatchWriteItem(ctx context.Context, client *dynamodb.Client, workerID int, rng *rand.Rand) {
	// Batch size between 5 and 25 items
	batchSize := 5 + rng.Intn(21)
	pk := fmt.Sprintf("batch_worker_%d", workerID)

	// Use a map to ensure unique sort keys
	usedSKs := make(map[string]bool)
	writeRequests := make([]types.WriteRequest, 0, batchSize)

	for len(writeRequests) < batchSize {
		// Use timestamp + counter to guarantee uniqueness
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
					"data":      &types.AttributeValueMemberS{Value: fmt.Sprintf("batch_data_%d", time.Now().UnixNano())},
					"timestamp": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().UnixNano())},
				},
			},
		})
	}

	err := withRetry(ctx, "BatchWriteItem", rng, func() error {
		_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				*tableName: writeRequests,
			},
		})
		return err
	})

	if err != nil && !isShutdownError(err) {
		atomic.AddInt64(&metrics.errors, 1)
	} else if err == nil {
		atomic.AddInt64(&metrics.batchWriteItemOps, 1)
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

	if err != nil && !isShutdownError(err) {
		atomic.AddInt64(&metrics.errors, 1)
	} else if err == nil {
		atomic.AddInt64(&metrics.batchGetItemOps, 1)
	}
}

func performListTables(ctx context.Context, client *dynamodb.Client, workerID int, rng *rand.Rand) {
	err := withRetry(ctx, "ListTables", rng, func() error {
		_, err := client.ListTables(ctx, &dynamodb.ListTablesInput{
			Limit: aws.Int32(10),
		})
		return err
	})

	if err != nil && !isShutdownError(err) {
		atomic.AddInt64(&metrics.errors, 1)
	} else if err == nil {
		atomic.AddInt64(&metrics.listTablesOps, 1)
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
			elapsed := time.Since(startTime).Seconds()

			getOps := atomic.LoadInt64(&metrics.getItemOps)
			putOps := atomic.LoadInt64(&metrics.putItemOps)
			updateOps := atomic.LoadInt64(&metrics.updateItemOps)
			batchGetOps := atomic.LoadInt64(&metrics.batchGetItemOps)
			batchWriteOps := atomic.LoadInt64(&metrics.batchWriteItemOps)
			listOps := atomic.LoadInt64(&metrics.listTablesOps)
			errs := atomic.LoadInt64(&metrics.errors)
			totalOps := getOps + putOps + updateOps + batchGetOps + batchWriteOps + listOps
			
			// Calculate average latency
			totalLatencyUs := atomic.LoadInt64(&metrics.totalLatencyUs)
			opCount := atomic.LoadInt64(&metrics.opCount)
			avgLatencyMs := float64(0)
			if opCount > 0 {
				avgLatencyMs = float64(totalLatencyUs) / float64(opCount) / 1000.0
			}

			log.Printf("[METRICS] Elapsed: %.0fs | Ops: %d (%.1f/s) | AvgLatency: %.0fms | Get: %d | Put: %d | Update: %d | BatchGet: %d | BatchWrite: %d | List: %d | Err: %d",
				elapsed, totalOps, float64(totalOps)/elapsed, avgLatencyMs,
				getOps, putOps, updateOps, batchGetOps, batchWriteOps, listOps, errs)
		}
	}
}

func printFinalMetrics() {
	getOps := atomic.LoadInt64(&metrics.getItemOps)
	putOps := atomic.LoadInt64(&metrics.putItemOps)
	updateOps := atomic.LoadInt64(&metrics.updateItemOps)
	batchGetOps := atomic.LoadInt64(&metrics.batchGetItemOps)
	batchWriteOps := atomic.LoadInt64(&metrics.batchWriteItemOps)
	listOps := atomic.LoadInt64(&metrics.listTablesOps)
	errs := atomic.LoadInt64(&metrics.errors)
	totalOps := getOps + putOps + updateOps + batchGetOps + batchWriteOps + listOps

	fmt.Println("\n========== FINAL BENCHMARK RESULTS ==========")
	fmt.Printf("Target:              %s\n", *target)
	fmt.Printf("Threads:             %d\n", *threads)
	fmt.Printf("Duration (seconds):  %d\n", *duration)
	fmt.Printf("Table Name:          %s\n", *tableName)
	fmt.Println("----------------------------------------------")
	fmt.Printf("Total Operations:    %d\n", totalOps)
	fmt.Printf("Operations/Second:   %.2f\n", float64(totalOps)/float64(*duration))
	fmt.Println("----------------------------------------------")
	fmt.Printf("GetItem:             %d\n", getOps)
	fmt.Printf("PutItem:             %d\n", putOps)
	fmt.Printf("UpdateItem:          %d\n", updateOps)
	fmt.Printf("BatchGetItem:        %d\n", batchGetOps)
	fmt.Printf("BatchWriteItem:      %d\n", batchWriteOps)
	fmt.Printf("ListTables:          %d\n", listOps)
	fmt.Println("----------------------------------------------")
	fmt.Printf("Total Errors:        %d\n", errs)
	fmt.Printf("Error Rate:          %.2f%%\n", float64(errs)/float64(totalOps+errs)*100)
	fmt.Println("==============================================")
}
