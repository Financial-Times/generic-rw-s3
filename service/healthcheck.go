package service

import (
	"net/http"
	"reflect"
	"time"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/service-status-go/gtg"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type HealthCheck struct {
	s3API         s3iface.S3API
	consumer      messageConsumerHealthcheck
	appName       string
	appSystemCode string
	bucketName    string
	log           *logger.UPPLogger
}

type messageConsumerHealthcheck interface {
	ConnectivityCheck() error
	MonitorCheck() error
}

func NewHealthCheck(c messageConsumerHealthcheck, s3API s3iface.S3API, appName string, appSystemCode string, bucketName string, log *logger.UPPLogger) *HealthCheck {
	return &HealthCheck{
		s3API:         s3API,
		consumer:      c,
		appName:       appName,
		appSystemCode: appSystemCode,
		bucketName:    bucketName,
		log:           log,
	}
}

func (h *HealthCheck) Health() func(w http.ResponseWriter, r *http.Request) {
	checks := []fthealth.Check{h.accessS3bucketCheck()}
	if !reflect.ValueOf(h.consumer).IsNil() {
		checks = append(checks, h.consumerHealthCheck(), h.consumerLagCheck())
	}
	hc := fthealth.TimedHealthCheck{
		HealthCheck: fthealth.HealthCheck{
			SystemCode:  h.appSystemCode,
			Name:        h.appName + " Healthchecks",
			Description: "Runs a HEAD check on bucket",
			Checks:      checks,
		},
		Timeout: 10 * time.Second,
	}
	return fthealth.Handler(hc)
}

func (h *HealthCheck) accessS3bucketCheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Unable to access S3 bucket",
		Name:             "S3 Bucket check",
		PanicGuide:       "https://runbooks.ftops.tech/" + h.appSystemCode,
		Severity:         3,
		TechnicalSummary: `Can not access S3 bucket.`,
		Checker:          h.s3HealthCheck,
	}
}

func (h *HealthCheck) s3HealthCheck() (string, error) {
	params := &s3.HeadBucketInput{
		Bucket: aws.String(h.bucketName), // Required
	}
	_, err := h.s3API.HeadBucket(params)
	if err != nil {
		h.log.WithError(err).Error("Got error running S3 health check")
		return "Can not perform check on S3 bucket", err
	}
	return "Access to S3 bucket ok", err
}

func (h *HealthCheck) consumerHealthCheck() fthealth.Check {
	return fthealth.Check{
		ID:               "kafka-connectivity",
		Name:             "Kafka Connectivity to MSK",
		Severity:         2,
		BusinessImpact:   "Cannot read content and store to S3",
		TechnicalSummary: "Kafka consumer is not reachable/healthy",
		PanicGuide:       "https://runbooks.ftops.tech/" + h.appSystemCode,
		Checker:          h.consumerConnectivityChecker,
	}
}

func (h *HealthCheck) consumerConnectivityChecker() (string, error) {
	if err := h.consumer.ConnectivityCheck(); err != nil {
		return "", err
	}
	return "OK", nil
}

func (h *HealthCheck) consumerLagCheck() fthealth.Check {
	return fthealth.Check{
		ID:               "kafka-consumer-lagcheck",
		Name:             "Kafka consumer lagging",
		Severity:         3,
		BusinessImpact:   "Reading messages is delayed",
		TechnicalSummary: "Messages awaiting handling exceed the configured lag tolerance. Check if Kafka consumer is stuck.",
		PanicGuide:       "https://runbooks.ftops.tech/" + h.appSystemCode,
		Checker:          h.consumerMonitorChecker,
	}
}

func (h *HealthCheck) consumerMonitorChecker() (string, error) {
	if err := h.consumer.MonitorCheck(); err != nil {
		return "", err
	}
	return "OK", nil
}

func (h *HealthCheck) GTG() gtg.Status {
	s3Check := func() gtg.Status {
		if _, err := h.s3HealthCheck(); err != nil {
			h.log.Info("Healthcheck failed, gtg is bad.")
			return gtg.Status{GoodToGo: false, Message: "Head request to S3 failed"}
		}
		return gtg.Status{GoodToGo: true, Message: "OK"}
	}

	sc := []gtg.StatusChecker{
		s3Check,
	}

	if !reflect.ValueOf(h.consumer).IsNil() {
		consumerCheck := func() gtg.Status {
			return gtgCheck(h.consumerConnectivityChecker)
		}
		sc = append(sc, consumerCheck)
	}

	return gtg.FailFastParallelCheck(sc)()
}

func gtgCheck(handler func() (string, error)) gtg.Status {
	if _, err := handler(); err != nil {
		return gtg.Status{GoodToGo: false, Message: err.Error()}
	}
	return gtg.Status{GoodToGo: true}
}
