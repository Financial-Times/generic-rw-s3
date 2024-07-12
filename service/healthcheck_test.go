package service

import (
	"errors"
	"testing"

	"net/http/httptest"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/stretchr/testify/assert"
)

type mockConsumerInstance struct {
	isConnectionHealthy bool
	isNotLagging        bool
}

func (c *mockConsumerInstance) MonitorCheck() error {
	if c.isNotLagging {
		return nil
	}
	return errors.New("error kafka client is lagging")
}

func (c *mockConsumerInstance) ConnectivityCheck() error {
	if c.isConnectionHealthy {
		return nil
	}
	return errors.New("error connecting to the queue")
}

func initHealthCheck(isConsumerConnectionHealthy bool, isConsumerNotLagging bool, accessS3Bucket bool) *HealthCheck {
	log := logger.NewUPPLogger("test-healthcheck", "INFO")
	s := &mockS3Client{log: log}
	if !accessS3Bucket {
		s.s3error = errors.New("S3 bucket error")
	}
	return &HealthCheck{
		s3API: s,
		consumer: &mockConsumerInstance{
			isConnectionHealthy: isConsumerConnectionHealthy,
			isNotLagging:        isConsumerNotLagging,
		},
		appName:       "generic-rw-s3",
		appSystemCode: "generic-rw-s3",
		bucketName:    "bucketName",
		log:           log,
	}
}

func TestNewHealthCheck(t *testing.T) {
	log := logger.NewUPPLogger("handlers_test", "Debug")
	s := &mockS3Client{log: log}

	healthcheck := NewHealthCheck(
		&mockConsumerInstance{
			isConnectionHealthy: true,
			isNotLagging:        true,
		},
		s, "generic-rw-s3", "generic-rw-s3", "bucketName", log,
	)
	assert.NotNil(t, healthcheck.consumer)
	assert.NotNil(t, healthcheck.s3API)
	assert.NotNil(t, healthcheck.log)
}

func TestHappyHealthCheck(t *testing.T) {
	hc := initHealthCheck(true, true, true)

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Kafka Connectivity to MSK","ok":true`, "Read Message queue healthcheck should be happy")
	assert.Contains(t, w.Body.String(), `"name":"Kafka consumer lagging","ok":true`, "Read Message queue is not lagging healthcheck should be happy")
	assert.Contains(t, w.Body.String(), `"name":"S3 Bucket check","ok":true`, "S3 bucket check should be happy")
}

func TestHealthCheckWithUnhappyConsumer(t *testing.T) {
	hc := initHealthCheck(false, false, true)

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Kafka Connectivity to MSK","ok":false`, "Read Message queue healthcheck should be happy")
	assert.Contains(t, w.Body.String(), `"name":"Kafka consumer lagging","ok":false`, "Read Message queue is not lagging healthcheck should be happy")
	assert.Contains(t, w.Body.String(), `"name":"S3 Bucket check","ok":true`, "S3 bucket check should be happy")
}

func TestUnhappyHealthcheck(t *testing.T) {
	hc := initHealthCheck(false, false, false)

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Kafka Connectivity to MSK","ok":false`, "Read Message queue healthcheck should be happy")
	assert.Contains(t, w.Body.String(), `"name":"Kafka consumer lagging","ok":false`, "Read Message queue is not lagging healthcheck should be happy")
	assert.Contains(t, w.Body.String(), `"name":"S3 Bucket check","ok":false`, "S3 bucket check should be happy")
}

func TestGTGHappyFlow(t *testing.T) {
	hc := initHealthCheck(true, true, true)

	status := hc.GTG()
	assert.True(t, status.GoodToGo)
	assert.Empty(t, status.Message)
}

func TestGTGBrokenConsumer(t *testing.T) {
	hc := initHealthCheck(false, true, true)

	status := hc.GTG()
	assert.False(t, status.GoodToGo)
	assert.Equal(t, "error connecting to the queue", status.Message)
}

func TestGTGBrokenS3(t *testing.T) {
	hc := initHealthCheck(true, true, false)

	status := hc.GTG()
	assert.False(t, status.GoodToGo)
	assert.Equal(t, "Head request to S3 failed", status.Message)
}

func TestNilConsumer(t *testing.T) {
	log := logger.NewUPPLogger("test-healthcheck", "INFO")
	s := &mockS3Client{log: log}
	c := &mockConsumerInstance{}
	hc := &HealthCheck{
		s3API:         s,
		consumer:      c,
		appName:       "generic-rw-s3",
		appSystemCode: "generic-rw-s3",
		bucketName:    "bucketName",
		log:           log,
	}

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"S3 Bucket check","ok":true`, "S3 bucket check should be happy")
}
