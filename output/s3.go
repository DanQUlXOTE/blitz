package output

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/observiq/blitz/internal/workermanager"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

const (
	// DefaultS3ChannelSize is the default size of the data channel
	DefaultS3ChannelSize = 100

	// DefaultS3Workers is the default number of worker goroutines
	DefaultS3Workers = 1

	// DefaultS3BatchSize is the default number of logs per batch
	DefaultS3BatchSize = 10000

	// DefaultS3BatchTimeout is the default timeout for batching log records
	DefaultS3BatchTimeout = 30 * time.Second

	// DefaultS3StopTimeout is the default timeout for graceful shutdown
	DefaultS3StopTimeout = 30 * time.Second

	// DefaultS3Region is the default AWS region
	DefaultS3Region = "us-east-1"

	// Reusable keys
	attrKeyComponent = "component"
	attrKeyErrorType = "error_type"
	logFieldWorkerID = "worker_id"

	// Reusable values
	attrValOutputS3 = "output_s3"
)

// S3 implements the Output interface for S3 uploads
type S3 struct {
	logger        *zap.Logger
	bucket        string
	region        string
	keyPrefix     string
	workers       int
	dataChan      chan string
	ctx           context.Context
	cancel        context.CancelFunc
	workerManager *workermanager.WorkerManager
	meter         metric.Meter
	s3Client      *s3.Client

	// Metrics
	s3LogsReceived     metric.Int64Counter
	s3ActiveWorkers    metric.Int64Gauge
	s3LogRate          metric.Float64Counter
	s3RequestSizeBytes metric.Int64Histogram
	s3UploadLatency    metric.Float64Histogram
	s3UploadErrors     metric.Int64Counter
	s3ObjectsUploaded  metric.Int64Counter

	// Configuration
	batchTimeout time.Duration
	maxBatchSize int
}

// S3Option is a functional option for configuring S3 output
type S3Option func(*S3Config) error

// S3Config holds configuration for S3 output
type S3Config struct {
	region          string
	keyPrefix       string
	workers         int
	batchSize       int
	batchTimeout    time.Duration
	accessKeyID     string
	secretAccessKey string
}

// WithRegion sets the AWS region
func WithS3Region(region string) S3Option {
	return func(cfg *S3Config) error {
		cfg.region = region
		return nil
	}
}

// WithKeyPrefix sets the S3 key prefix for uploaded objects
func WithS3KeyPrefix(prefix string) S3Option {
	return func(cfg *S3Config) error {
		cfg.keyPrefix = prefix
		return nil
	}
}

// WithWorkers sets the number of worker goroutines
func WithS3Workers(workers int) S3Option {
	return func(cfg *S3Config) error {
		cfg.workers = workers
		return nil
	}
}

// WithBatchSize sets the number of logs per batch
func WithS3BatchSize(size int) S3Option {
	return func(cfg *S3Config) error {
		cfg.batchSize = size
		return nil
	}
}

// WithBatchTimeout sets the timeout for batching log records
func WithS3BatchTimeout(timeout time.Duration) S3Option {
	return func(cfg *S3Config) error {
		cfg.batchTimeout = timeout
		return nil
	}
}

// WithCredentials sets static AWS credentials (optional). When unset, default chain is used.
func WithS3Credentials(accessKeyID, secretAccessKey string) S3Option {
	return func(cfg *S3Config) error {
		cfg.accessKeyID = accessKeyID
		cfg.secretAccessKey = secretAccessKey
		return nil
	}
}

// New creates a new S3 output instance using functional options. Logger and bucket are required.
func NewS3(logger *zap.Logger, bucket string, opts ...S3Option) (*S3, error) {
	var err error

	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}
	if bucket == "" {
		return nil, fmt.Errorf("bucket cannot be empty")
	}

	// Initialize config with defaults
	cfg := &S3Config{
		region:       DefaultS3Region,
		workers:      DefaultS3Workers,
		batchSize:    DefaultS3BatchSize,
		batchTimeout: DefaultS3BatchTimeout,
	}

	// Apply options
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, fmt.Errorf("apply option: %w", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	meter := otel.Meter("blitz-s3-output")

	// Initialize metrics
	s3LogsReceived, err := meter.Int64Counter(
		"blitz.s3.logs.received",
		metric.WithDescription("Number of logs received from the write channel"),
	)
	if err != nil {
		return nil, fmt.Errorf("create logs received counter: %w", err)
	}

	s3ActiveWorkers, err := meter.Int64Gauge(
		"blitz.s3.workers.active",
		metric.WithDescription("Number of active worker goroutines"),
	)
	if err != nil {
		return nil, fmt.Errorf("create active workers gauge: %w", err)
	}

	s3LogRate, err := meter.Float64Counter(
		"blitz.s3.log.rate",
		metric.WithDescription("Rate at which logs are successfully uploaded to S3"),
	)
	if err != nil {
		return nil, fmt.Errorf("create log rate counter: %w", err)
	}

	s3RequestSizeBytes, err := meter.Int64Histogram(
		"blitz.s3.request.size.bytes",
		metric.WithDescription("Size of uploaded objects in bytes"),
	)
	if err != nil {
		return nil, fmt.Errorf("create request size histogram: %w", err)
	}

	s3UploadLatency, err := meter.Float64Histogram(
		"blitz.s3.upload.latency",
		metric.WithDescription("Upload latency in seconds"),
	)
	if err != nil {
		return nil, fmt.Errorf("create upload latency histogram: %w", err)
	}

	s3UploadErrors, err := meter.Int64Counter(
		"blitz.s3.upload.errors",
		metric.WithDescription("Total number of upload errors"),
	)
	if err != nil {
		return nil, fmt.Errorf("create upload errors counter: %w", err)
	}

	s3ObjectsUploaded, err := meter.Int64Counter(
		"blitz.s3.objects.uploaded",
		metric.WithDescription("Total number of S3 objects uploaded"),
	)
	if err != nil {
		return nil, fmt.Errorf("create objects uploaded counter: %w", err)
	}

	// Create AWS config
	cfgOpts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.region),
	}

	// Configure credentials if provided
	if cfg.accessKeyID != "" && cfg.secretAccessKey != "" {
		cfgOpts = append(cfgOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.accessKeyID, cfg.secretAccessKey, ""),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg)

	s3Output := &S3{
		logger:             logger.Named("output-s3"),
		bucket:             bucket,
		region:             cfg.region,
		keyPrefix:          cfg.keyPrefix,
		workers:            cfg.workers,
		dataChan:           make(chan string, DefaultS3ChannelSize),
		ctx:                ctx,
		cancel:             cancel,
		meter:              meter,
		s3Client:           s3Client,
		s3LogsReceived:     s3LogsReceived,
		s3ActiveWorkers:    s3ActiveWorkers,
		s3LogRate:          s3LogRate,
		s3RequestSizeBytes: s3RequestSizeBytes,
		s3UploadLatency:    s3UploadLatency,
		s3UploadErrors:     s3UploadErrors,
		s3ObjectsUploaded:  s3ObjectsUploaded,
		batchTimeout:       cfg.batchTimeout,
		maxBatchSize:       cfg.batchSize,
	}

	s3Output.logger.Info("Starting S3 output",
		zap.String("bucket", s3Output.bucket),
		zap.String("region", s3Output.region),
		zap.String("key_prefix", s3Output.keyPrefix),
		zap.Int("workers", s3Output.workers),
		zap.Int("channel_size", DefaultS3ChannelSize),
		zap.Int("batch_size", s3Output.maxBatchSize),
		zap.Duration("batch_timeout", s3Output.batchTimeout),
	)

	// Create channel size gauge
	_, err = meter.Int64ObservableGauge(
		"blitz.s3.channel.size",
		metric.WithDescription("Current size of the data channel"),
		metric.WithInt64Callback(func(_ context.Context, io metric.Int64Observer) error {
			io.Observe(int64(len(s3Output.dataChan)))
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create channel size gauge: %w", err)
	}

	// Create worker manager
	s3Output.workerManager = workermanager.NewWorkerManager(s3Output.logger, s3Output.workers, s3Output.s3Worker)

	// Record initial active workers count
	s3Output.s3ActiveWorkers.Record(context.Background(), int64(s3Output.workers),
		metric.WithAttributeSet(
			attribute.NewSet(
				attribute.String(attrKeyComponent, attrValOutputS3),
			),
		),
	)

	// Start the workers
	s3Output.workerManager.Start()

	return s3Output, nil
}

// Write sends data to the S3 output channel for processing by workers.
// Write shall not be called after Stop is called.
// If the provided context is done, Write will return immediately
// even if the data is not written to the channel.
func (s *S3) Write(ctx context.Context, data LogRecord) error {
	select {
	case s.dataChan <- data.Message:
		// Record logs received
		s.s3LogsReceived.Add(ctx, 1,
			metric.WithAttributeSet(
				attribute.NewSet(
					attribute.String(attrKeyComponent, attrValOutputS3),
				),
			),
		)
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while waiting to write data: %w", ctx.Err())
	case <-s.ctx.Done():
		return fmt.Errorf("S3 output is shutting down")
	}
}

// Stop gracefully shuts down all workers and closes S3 connections
// Stop shall not be called more than once.
// If the provided context is done, Stop will return immediately
// even if workers are still shutting down.
func (s *S3) Stop(ctx context.Context) error {
	s.logger.Info("Stopping S3 output")

	// Record zero active workers
	s.s3ActiveWorkers.Record(ctx, 0,
		metric.WithAttributeSet(
			attribute.NewSet(
				attribute.String("component", "output_s3"),
			),
		),
	)

	// Close the channel to ensure workers do not
	// process new data.
	close(s.dataChan)

	// Signal the workers to stop.
	s.cancel()

	// Stop the worker manager
	s.workerManager.Stop()

	s.logger.Info("S3 output stopped successfully")
	return nil
}

// s3Worker processes S3 data from the channel and uploads batches to S3.
// This function is designed to work with the worker manager, which handles automatic restart
// with exponential backoff when the worker exits due to upload failures or errors.
// The worker should return immediately on any failure - the worker manager will handle
// reconnection attempts with appropriate backoff delays.
func (s *S3) s3Worker(id int) {
	s.logger.Info("Starting S3 worker", zap.Int(logFieldWorkerID, id))

	batch := newStringBatch(s.maxBatchSize, s.batchTimeout)

	for {
		select {
		case data, ok := <-s.dataChan:
			if !ok {
				s.logger.Info("S3 worker exiting - channel closed", zap.Int(logFieldWorkerID, id))
				// Flush remaining logs
				if err := s.flushBatch(batch); err != nil {
					s.logger.Error("Failed to flush final batch", zap.Int(logFieldWorkerID, id), zap.Error(err))
				}
				return
			}

			// Add to batch
			batch.add(data)

			// Upload batch if it's full
			if batch.isFull() {
				if !batch.timer.Stop() {
					select {
					case <-batch.timer.C:
					default:
					}
				}
				if err := s.uploadBatch(batch); err != nil {
					s.logger.Error("Failed to upload S3 batch",
						zap.Int(logFieldWorkerID, id),
						zap.Error(err))
					return
				}
				batch = newStringBatch(s.maxBatchSize, s.batchTimeout)
			}

		case <-batch.timer.C:
			// Batch timeout reached, upload batch
			if !batch.isEmpty() {
				if err := s.uploadBatch(batch); err != nil {
					s.logger.Error("Failed to upload S3 batch",
						zap.Int(logFieldWorkerID, id),
						zap.Error(err))
					return
				}
			}
			// Create new batch with new timer
			batch = newStringBatch(s.maxBatchSize, s.batchTimeout)

		case <-s.ctx.Done():
			s.logger.Info("S3 worker exiting - context cancelled", zap.Int(logFieldWorkerID, id))
			// Flush remaining logs
			if err := s.flushBatch(batch); err != nil {
				s.logger.Error("Failed to flush final batch", zap.Int(logFieldWorkerID, id), zap.Error(err))
			}
			return
		}
	}
}

// stringBatch holds a batch of log strings to be uploaded
type stringBatch struct {
	logs    []string
	maxSize int
	timer   *time.Timer
	mu      sync.Mutex
}

// newStringBatch creates a new string batch
func newStringBatch(maxSize int, timeout time.Duration) *stringBatch {
	return &stringBatch{
		logs:    make([]string, 0, maxSize),
		maxSize: maxSize,
		timer:   time.NewTimer(timeout),
	}
}

// add adds a log to the batch
func (b *stringBatch) add(data string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.logs = append(b.logs, data)
}

// isFull returns true if the batch is full
func (b *stringBatch) isFull() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.logs) >= b.maxSize
}

// isEmpty returns true if the batch is empty
func (b *stringBatch) isEmpty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.logs) == 0
}

// getAndClear returns all logs and clears the batch
func (b *stringBatch) getAndClear() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	logs := b.logs
	b.logs = make([]string, 0, b.maxSize)
	return logs
}

// uploadBatch uploads a batch of logs to S3
func (s *S3) uploadBatch(batch *stringBatch) error {
	logs := batch.getAndClear()
	if len(logs) == 0 {
		return nil
	}

	// Build the object content with logs separated by newlines
	var buf bytes.Buffer
	for _, log := range logs {
		buf.WriteString(log)
		buf.WriteString("\n")
	}

	// Generate S3 key with timestamp
	key := s.generateS3Key()

	// Upload to S3
	ctx, cancel := context.WithTimeout(context.Background(), s.batchTimeout)
	defer cancel()

	startTime := time.Now()
	_, err := s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: aws.String("application/octet-stream"),
	})
	if err != nil {
		s.recordUploadError("upload_error", err)
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	// Record successful upload metrics
	latency := time.Since(startTime).Seconds()
	objectSize := int64(buf.Len())
	s.s3LogRate.Add(context.Background(), float64(len(logs)),
		metric.WithAttributeSet(
			attribute.NewSet(
				attribute.String(attrKeyComponent, attrValOutputS3),
			),
		),
	)
	s.s3RequestSizeBytes.Record(context.Background(), objectSize,
		metric.WithAttributeSet(
			attribute.NewSet(
				attribute.String(attrKeyComponent, attrValOutputS3),
			),
		),
	)
	s.s3UploadLatency.Record(context.Background(), latency,
		metric.WithAttributeSet(
			attribute.NewSet(
				attribute.String(attrKeyComponent, attrValOutputS3),
			),
		),
	)
	s.s3ObjectsUploaded.Add(context.Background(), 1,
		metric.WithAttributeSet(
			attribute.NewSet(
				attribute.String(attrKeyComponent, attrValOutputS3),
			),
		),
	)

	s.logger.Debug("Successfully uploaded batch to S3",
		zap.String("bucket", s.bucket),
		zap.String("key", key),
		zap.Int("log_count", len(logs)),
		zap.Int64("size_bytes", objectSize),
	)

	return nil
}

// flushBatch flushes any remaining logs in the batch
func (s *S3) flushBatch(batch *stringBatch) error {
	if !batch.timer.Stop() {
		select {
		case <-batch.timer.C:
		default:
		}
	}
	if batch.isEmpty() {
		return nil
	}
	return s.uploadBatch(batch)
}

// generateS3Key generates a unique S3 key for the upload
func (s *S3) generateS3Key() string {
	timestamp := time.Now().UnixNano()
	key := fmt.Sprintf("%d.ndjson", timestamp)

	if s.keyPrefix != "" {
		key = fmt.Sprintf("%s/%s", s.keyPrefix, key)
	}

	return key
}

// recordUploadError records metrics for upload errors
func (s *S3) recordUploadError(errorType string, err error) {
	ctx := context.Background()

	s.s3UploadErrors.Add(ctx, 1,
		metric.WithAttributeSet(
			attribute.NewSet(
				attribute.String(attrKeyComponent, attrValOutputS3),
				attribute.String(attrKeyErrorType, errorType),
			),
		),
	)
}
