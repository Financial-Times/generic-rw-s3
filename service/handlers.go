package service

import (
	"github.com/Financial-Times/go-fthealth/v1a"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
	"net/http"
	"time"
)

func AddAdminHandlers(servicesRouter *mux.Router, svc s3iface.S3API, bucketName string, writer Writer, reader Reader) {
	c := checker{svc, bucketName, writer, reader}
	var monitoringRouter http.Handler = servicesRouter
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)
	http.HandleFunc(status.PingPath, status.PingHandler)
	http.HandleFunc(status.PingPathDW, status.PingHandler)
	http.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)
	http.HandleFunc(status.BuildInfoPathDW, status.BuildInfoHandler)
	http.HandleFunc("/__health", v1a.Handler("GenericReadWriteS3 Healthchecks",
		"Runs a HEAD check on bucket", v1a.Check{
			BusinessImpact:   "Unable to access S3 bucket",
			Name:             "S3 Bucket check",
			PanicGuide:       "http://ft.com",
			Severity:         1,
			TechnicalSummary: `Can not access S3 bucket.`,
			Checker:          c.healthCheck,
		}))

	http.HandleFunc("/__gtg", c.gtgCheckHandler)
	http.Handle("/", monitoringRouter)
}

type checker struct {
	s3iface.S3API
	bucketName string
	w          Writer
	r          Reader
}

func (c *checker) healthCheck() (string, error) {
	params := &s3.HeadBucketInput{
		Bucket: aws.String(c.bucketName), // Required
	}
	_, err := c.HeadBucket(params)
	if err != nil {
		log.Errorf("Got error running S3 health check, %v", err.Error())
		return "Can not perform check on S3 bucket", err
	}
	return "Access to S3 bucket ok", err
}

func (c *checker) gtgCheckHandler(rw http.ResponseWriter, r *http.Request) {
	pl := []byte("{}")
	gtg := "__gtg_" + time.Now().String()
	var err error
	err = c.w.Write(gtg, &pl, "application/json")
	if err != nil {
		log.Errorf("Could not write %v", err.Error())
		rw.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	_, _, err = c.r.Get(gtg)
	if err != nil {
		log.Errorf("Could not read %v", err.Error())
		rw.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	rw.WriteHeader(http.StatusOK)
}

func Handlers(servicesRouter *mux.Router, wh WriterHandler, rh ReaderHandler) {
	mh := handlers.MethodHandler{
		"PUT": http.HandlerFunc(wh.HandleWrite),
		"GET": http.HandlerFunc(rh.HandleGet),
	}

	servicesRouter.Handle("/{uuid:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[34][0-9a-fA-F]{3}-[89ab][0-9a-fA-F]{3}-[0-9a-fA-F]{12}}", mh)
}
