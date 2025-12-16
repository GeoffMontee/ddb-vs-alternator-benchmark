package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
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
	"github.com/scylladb/alternator-client-golang/helper"
)

// Command-line flags
var (
	target             = flag.String("target", "ddb", "Target database: 'ddb' for DynamoDB or 'alternator' for ScyllaDB Alternator")
	threads            = flag.Int("threads", 16, "Number of concurrent worker threads")
	duration           = flag.Int("duration", 300, "Duration of the benchmark in seconds")
	scyllaContactPoints = flag.String("scylla-contact-points", "", "Comma-separated list of ScyllaDB contact points (IP addresses or hostnames)")
	scyllaPort         = flag.Int("scylla-port", 8000, "ScyllaDB Alternator port")
	region             = flag.String("region", "us-east-1", "AWS region for DynamoDB")
	tableName          = flag.String("table-name", "benchmark_table", "Name of the table to use for benchmarking")
)

// Metrics counters
type Metrics struct {
	getItemOps       int64
	putItemOps       int64
	updateItemOps    int64
	batchGetItemOps  int64
	batchWriteItemOps int64
	listTablesOps    int64
	errors           int64
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
		log.Printf("Received signal %v, shutting down...", sig)
	}

	// Signal workers to stop
	close(stopChan)

	// Wait for all workers to finish
	wg.Wait()

	// Print final metrics
	printFinalMetrics()

	// Cleanup - delete the table
	if err := deleteBenchmarkTable(ctx, client); err != nil {
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

func createDynamoDBClient(ctx context.Context) (*dynamodb.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(*region))
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

	// Use the ScyllaDB alternator-client-golang helper with special options
	// This library provides load balancing and optimized headers for Alternator
	//
	// The helper.NewHelper function creates a client helper that:
	// - Balances requests across multiple ScyllaDB nodes
	// - Optimizes HTTP headers for better Alternator performance
	// - Handles TLS certificate validation
	h, err := helper.NewHelper(
		contactPoints,
		helper.WithIgnoreServerCertificateError(true), // Skip TLS cert validation (useful for self-signed certs)
		helper.WithOptimizeHeaders(true),              // Optimize HTTP headers for Alternator
		helper.WithPort(*scyllaPort),                  // Set the Alternator port
		helper.WithScheme("http"),                     // Use HTTP (change to "https" for TLS)
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create alternator helper: %w", err)
	}

	// Create the AWS config using the helper
	// The helper provides the necessary components for the AWS SDK v2
	cfg := aws.Config{
		Region: "us-east-1", // Alternator ignores region but SDK requires it
		Credentials: credentials.NewStaticCredentialsProvider(
			"alternator",  // Access key (ignored when auth is disabled)
			"secret_pass", // Secret key (ignored when auth is disabled)
			"",            // Session token
		),
		EndpointResolverWithOptions: h.EndpointResolver(),
		HTTPClient:                  h.HTTPClient(),
	}

	client := dynamodb.NewFromConfig(cfg)
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
		// Note: Scylla Alternator doesn't fully support on-demand, so we use provisioned
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

	for {
		select {
		case <-stopChan:
			log.Printf("Worker %d stopping", workerID)
			return
		case <-ctx.Done():
			log.Printf("Worker %d context cancelled", workerID)
			return
		default:
			// Randomly select an operation to perform
			operation := rand.Intn(6)
			switch operation {
			case 0:
				performPutItem(ctx, client, workerID)
			case 1:
				performGetItem(ctx, client, workerID)
			case 2:
				performUpdateItem(ctx, client, workerID)
			case 3:
				performBatchWriteItem(ctx, client, workerID)
			case 4:
				performBatchGetItem(ctx, client, workerID)
			case 5:
				performListTables(ctx, client, workerID)
			}
		}
	}
}

func withRetry(ctx context.Context, operationName string, operation func() error) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := operation()
		if err == nil {
			return nil
		}

		lastErr = err

		// Calculate exponential backoff
		backoffMs := initialBackoffMs * (1 << attempt) // 1s, 2s, 4s, 8s, 16s
		jitter := rand.Intn(backoffMs / 4)             // Add some jitter
		sleepDuration := time.Duration(backoffMs+jitter) * time.Millisecond

		log.Printf("[%s] Attempt %d failed: %v. Retrying in %v...", operationName, attempt+1, err, sleepDuration)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepDuration):
			// Continue to next attempt
		}
	}

	return fmt.Errorf("all %d retries failed for %s: %w", maxRetries, operationName, lastErr)
}

func performPutItem(ctx context.Context, client *dynamodb.Client, workerID int) {
	pk := fmt.Sprintf("worker_%d", workerID)
	sk := fmt.Sprintf("item_%d", rand.Intn(1000))

	err := withRetry(ctx, "PutItem", func() error {
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

	if err != nil {
		atomic.AddInt64(&metrics.errors, 1)
	} else {
		atomic.AddInt64(&metrics.putItemOps, 1)
	}
}

func performGetItem(ctx context.Context, client *dynamodb.Client, workerID int) {
	pk := fmt.Sprintf("worker_%d", rand.Intn(*threads))
	sk := fmt.Sprintf("item_%d", rand.Intn(1000))

	err := withRetry(ctx, "GetItem", func() error {
		_, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(*tableName),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: pk},
				"sk": &types.AttributeValueMemberS{Value: sk},
			},
		})
		return err
	})

	if err != nil {
		atomic.AddInt64(&metrics.errors, 1)
	} else {
		atomic.AddInt64(&metrics.getItemOps, 1)
	}
}

func performUpdateItem(ctx context.Context, client *dynamodb.Client, workerID int) {
	pk := fmt.Sprintf("worker_%d", workerID)
	sk := fmt.Sprintf("item_%d", rand.Intn(1000))

	err := withRetry(ctx, "UpdateItem", func() error {
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

	if err != nil {
		atomic.AddInt64(&metrics.errors, 1)
	} else {
		atomic.AddInt64(&metrics.updateItemOps, 1)
	}
}

func performBatchWriteItem(ctx context.Context, client *dynamodb.Client, workerID int) {
	// Batch size between 5 and 25 items
	batchSize := 5 + rand.Intn(21)
	pk := fmt.Sprintf("batch_worker_%d", workerID)

	writeRequests := make([]types.WriteRequest, batchSize)
	for i := 0; i < batchSize; i++ {
		sk := fmt.Sprintf("batch_item_%d_%d", workerID, rand.Intn(10000))
		writeRequests[i] = types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"pk":        &types.AttributeValueMemberS{Value: pk},
					"sk":        &types.AttributeValueMemberS{Value: sk},
					"data":      &types.AttributeValueMemberS{Value: fmt.Sprintf("batch_data_%d", time.Now().UnixNano())},
					"timestamp": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().UnixNano())},
				},
			},
		}
	}

	err := withRetry(ctx, "BatchWriteItem", func() error {
		_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				*tableName: writeRequests,
			},
		})
		return err
	})

	if err != nil {
		atomic.AddInt64(&metrics.errors, 1)
	} else {
		atomic.AddInt64(&metrics.batchWriteItemOps, 1)
	}
}

func performBatchGetItem(ctx context.Context, client *dynamodb.Client, workerID int) {
	// Batch size between 5 and 100 items
	batchSize := 5 + rand.Intn(96)
	
	keys := make([]map[string]types.AttributeValue, batchSize)
	for i := 0; i < batchSize; i++ {
		pk := fmt.Sprintf("worker_%d", rand.Intn(*threads))
		sk := fmt.Sprintf("item_%d", rand.Intn(1000))
		keys[i] = map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
			"sk": &types.AttributeValueMemberS{Value: sk},
		}
	}

	err := withRetry(ctx, "BatchGetItem", func() error {
		_, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
			RequestItems: map[string]types.KeysAndAttributes{
				*tableName: {
					Keys: keys,
				},
			},
		})
		return err
	})

	if err != nil {
		atomic.AddInt64(&metrics.errors, 1)
	} else {
		atomic.AddInt64(&metrics.batchGetItemOps, 1)
	}
}

func performListTables(ctx context.Context, client *dynamodb.Client, workerID int) {
	err := withRetry(ctx, "ListTables", func() error {
		_, err := client.ListTables(ctx, &dynamodb.ListTablesInput{
			Limit: aws.Int32(10),
		})
		return err
	})

	if err != nil {
		atomic.AddInt64(&metrics.errors, 1)
	} else {
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

			log.Printf("[METRICS] Elapsed: %.0fs | Total Ops: %d (%.1f/s) | GetItem: %d | PutItem: %d | UpdateItem: %d | BatchGet: %d | BatchWrite: %d | ListTables: %d | Errors: %d",
				elapsed, totalOps, float64(totalOps)/elapsed,
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
