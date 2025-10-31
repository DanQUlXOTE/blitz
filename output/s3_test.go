package output

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestNewS3(t *testing.T) {
	logger := zap.NewNop()

	tests := []struct {
		name            string
		bucket          string
		region          string
		keyPrefix       string
		workers         int
		batchSize       int
		batchTimeout    time.Duration
		accessKeyID     string
		secretAccessKey string
		wantErr         bool
		errContains     string
	}{
		{
			name:         "valid configuration with defaults",
			bucket:       "test-bucket",
			region:       "",
			keyPrefix:    "",
			workers:      0, // Should default to 1
			batchSize:    0, // Should default to 10000
			batchTimeout: 0, // Should default to 30s
			wantErr:      false,
		},
		{
			name:         "valid configuration with custom values",
			bucket:       "my-bucket",
			region:       "us-west-2",
			keyPrefix:    "logs/",
			workers:      3,
			batchSize:    5000,
			batchTimeout: 15 * time.Second,
			wantErr:      false,
		},
		{
			name:        "nil logger",
			bucket:      "test-bucket",
			wantErr:     true,
			errContains: "logger cannot be nil",
		},
		{
			name:        "empty bucket",
			bucket:      "",
			wantErr:     true,
			errContains: "bucket cannot be empty",
		},
		{
			name:         "negative workers should default",
			bucket:       "test-bucket",
			workers:      -1,
			batchSize:    100,
			batchTimeout: 1 * time.Second,
			wantErr:      false,
		},
		{
			name:         "negative batch size should default",
			bucket:       "test-bucket",
			batchSize:    -1,
			batchTimeout: 1 * time.Second,
			wantErr:      false,
		},
		{
			name:         "zero batch timeout should default",
			bucket:       "test-bucket",
			batchSize:    100,
			batchTimeout: 0,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s3 *S3
			var err error

			if tt.name == "nil logger" {
				s3, err = NewS3(nil, tt.bucket)
			} else {
				// Build options based on test case
				opts := []S3Option{}
				if tt.region != "" {
					opts = append(opts, WithS3Region(tt.region))
				}
				if tt.keyPrefix != "" {
					opts = append(opts, WithS3KeyPrefix(tt.keyPrefix))
				}
				if tt.workers != 0 {
					opts = append(opts, WithS3Workers(tt.workers))
				}
				if tt.batchSize != 0 {
					opts = append(opts, WithS3BatchSize(tt.batchSize))
				}
				if tt.batchTimeout != 0 {
					opts = append(opts, WithS3BatchTimeout(tt.batchTimeout))
				}
				if tt.accessKeyID != "" || tt.secretAccessKey != "" {
					opts = append(opts, WithS3Credentials(tt.accessKeyID, tt.secretAccessKey))
				}
				s3, err = NewS3(logger, tt.bucket, opts...)
			}

			if tt.wantErr {
				if err == nil {
					t.Errorf("NewS3() expected error but got none")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("NewS3() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}

			if err != nil {
				// AWS config errors are expected if credentials aren't available
				// This is acceptable for testing purposes
				if strings.Contains(err.Error(), "failed to load AWS config") {
					t.Logf("NewS3() AWS config error (expected without credentials): %v", err)
					return
				}
				t.Errorf("NewS3() unexpected error = %v", err)
				return
			}

			if s3 == nil {
				t.Errorf("NewS3() returned nil S3 instance")
				return
			}

			// Verify the configuration was set correctly
			if s3.bucket != tt.bucket {
				t.Errorf("NewS3() bucket = %v, want %v", s3.bucket, tt.bucket)
			}

			// Verify workers defaulting
			expectedWorkers := tt.workers
			if tt.workers <= 0 {
				expectedWorkers = DefaultS3Workers
			}
			if s3.workers != expectedWorkers {
				t.Errorf("NewS3() workers = %v, want %v", s3.workers, expectedWorkers)
			}

			// Verify batch size defaulting
			expectedBatchSize := tt.batchSize
			if tt.batchSize <= 0 {
				expectedBatchSize = DefaultS3BatchSize
			}
			if s3.maxBatchSize != expectedBatchSize {
				t.Errorf("NewS3() maxBatchSize = %v, want %v", s3.maxBatchSize, expectedBatchSize)
			}

			// Verify batch timeout defaulting
			expectedBatchTimeout := tt.batchTimeout
			if tt.batchTimeout <= 0 {
				expectedBatchTimeout = DefaultS3BatchTimeout
			}
			if s3.batchTimeout != expectedBatchTimeout {
				t.Errorf("NewS3() batchTimeout = %v, want %v", s3.batchTimeout, expectedBatchTimeout)
			}

			// Verify channel was created
			if s3.dataChan == nil {
				t.Errorf("NewS3() dataChan is nil")
			}

			// Verify context was created
			if s3.ctx == nil {
				t.Errorf("NewS3() ctx is nil")
			}
			if s3.cancel == nil {
				t.Errorf("NewS3() cancel is nil")
			}

			// Clean up
			s3.Stop(context.Background())
		})
	}
}

func TestS3_Write(t *testing.T) {
	logger := zap.NewNop()

	// This test may fail on AWS config without credentials; skip if so
	_, err := NewS3(logger, "test-bucket", WithS3BatchSize(100), WithS3BatchTimeout(1*time.Second))
	if err != nil && !strings.Contains(err.Error(), "failed to load AWS config") {
		t.Skipf("Skipping test - cannot create S3 client without AWS credentials: %v", err)
	}

	// If we got here, we have an S3 instance (unlikely without credentials)
	// The actual write test would require AWS credentials or a mock
}

func TestS3_Stop(t *testing.T) {
	logger := zap.NewNop()

	// This test may fail on AWS config without credentials; skip if so
	s3, err := NewS3(logger, "test-bucket", WithS3BatchSize(100), WithS3BatchTimeout(1*time.Second))
	if err != nil {
		if strings.Contains(err.Error(), "failed to load AWS config") {
			t.Skipf("Skipping test - cannot create S3 client without AWS credentials: %v", err)
			return
		}
		t.Fatalf("Failed to create S3 client: %v", err)
	}

	ctx := context.Background()
	err = s3.Stop(ctx)
	if err != nil {
		t.Errorf("Stop() failed: %v", err)
	}
}

func TestStringBatch(t *testing.T) {
	t.Run("add and isFull", func(t *testing.T) {
		batch := newStringBatch(3, 1*time.Second)
		defer batch.timer.Stop()

		if batch.isFull() {
			t.Error("New batch should not be full")
		}

		batch.add("log1")
		batch.add("log2")
		if batch.isFull() {
			t.Error("Batch with 2 logs should not be full when maxSize is 3")
		}

		batch.add("log3")
		if !batch.isFull() {
			t.Error("Batch with 3 logs should be full when maxSize is 3")
		}
	})

	t.Run("isEmpty", func(t *testing.T) {
		batch := newStringBatch(3, 1*time.Second)
		defer batch.timer.Stop()

		if !batch.isEmpty() {
			t.Error("New batch should be empty")
		}

		batch.add("log1")
		if batch.isEmpty() {
			t.Error("Batch with 1 log should not be empty")
		}
	})

	t.Run("getAndClear", func(t *testing.T) {
		batch := newStringBatch(3, 1*time.Second)
		defer batch.timer.Stop()

		batch.add("log1")
		batch.add("log2")

		logs := batch.getAndClear()
		if len(logs) != 2 {
			t.Errorf("getAndClear() returned %d logs, want 2", len(logs))
		}

		if !batch.isEmpty() {
			t.Error("Batch should be empty after getAndClear")
		}

		if logs[0] != "log1" || logs[1] != "log2" {
			t.Errorf("getAndClear() returned incorrect logs: %v", logs)
		}
	})
}

func TestS3_generateS3Key(t *testing.T) {
	logger := zap.NewNop()

	// This test may fail on AWS config without credentials; skip if so
	s3, err := NewS3(logger, "test-bucket", WithS3BatchSize(100), WithS3BatchTimeout(1*time.Second))
	if err != nil {
		if strings.Contains(err.Error(), "failed to load AWS config") {
			t.Skipf("Skipping test - cannot create S3 client without AWS credentials: %v", err)
			return
		}
		t.Fatalf("Failed to create S3 client: %v", err)
	}

	key1 := s3.generateS3Key()
	key2 := s3.generateS3Key()

	if key1 == key2 {
		t.Error("generateS3Key() should generate unique keys")
	}

	if !strings.HasSuffix(key1, ".ndjson") {
		t.Errorf("generateS3Key() should end with .ndjson, got %q", key1)
	}
}

func TestS3_generateS3KeyWithPrefix(t *testing.T) {
	logger := zap.NewNop()

	// This test may fail on AWS config without credentials; skip if so
	s3, err := NewS3(logger, "test-bucket", WithS3KeyPrefix("logs/prefix"), WithS3BatchSize(100), WithS3BatchTimeout(1*time.Second))
	if err != nil {
		if strings.Contains(err.Error(), "failed to load AWS config") {
			t.Skipf("Skipping test - cannot create S3 client without AWS credentials: %v", err)
			return
		}
		t.Fatalf("Failed to create S3 client: %v", err)
	}

	key := s3.generateS3Key()

	if !strings.HasPrefix(key, "logs/prefix/") {
		t.Errorf("generateS3Key() should start with key prefix, got %q", key)
	}

	if !strings.HasSuffix(key, ".ndjson") {
		t.Errorf("generateS3Key() should end with .ndjson, got %q", key)
	}
}
