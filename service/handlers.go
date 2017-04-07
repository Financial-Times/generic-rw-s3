package service

import (
	"fmt"
	"net/http"
	"time"

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
	"github.com/Financial-Times/service-status-go/gtg"
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

	gtgHandler := status.NewGoodToGoHandler(c.gtgCheckHandler)
	http.HandleFunc(status.GTGPath, gtgHandler)
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

func (c *checker) gtgCheckHandler() gtg.Status {
	pl := []byte("{}")
	key := "__gtg_" + time.Now().Format(time.RFC3339)
	var err error
	err = c.w.Write(key, &pl, "application/json", "tid_gtg")
	if err != nil {
		msg := fmt.Sprintf("Could not write key=%v, %v", key, err.Error())
		log.Error(msg)
		return gtg.Status{GoodToGo: false, Message: msg}
	}
	_, _, _, err = c.r.Get(key)
	if err != nil {
		msg := fmt.Sprintf("Could not read key=%v, %v", key, err.Error())
		log.Error(msg)
		return gtg.Status{GoodToGo: false, Message: msg}
	}

	if err := c.w.Delete(key); err != nil {
		msg := fmt.Sprintf("Could not delete key=%v, %v", key, err.Error())
		log.Error(msg)
		return gtg.Status{GoodToGo: false, Message: msg}
	}

	return gtg.Status{GoodToGo: true}
}

func Handlers(servicesRouter *mux.Router, wh WriterHandler, rh ReaderHandler, resourcePath string) {
	mh := handlers.MethodHandler{
		"PUT":    http.HandlerFunc(wh.HandleWrite),
		"GET":    http.HandlerFunc(rh.HandleGet),
		"DELETE": http.HandlerFunc(wh.HandleDelete),
	}

	ch := handlers.MethodHandler{
		"GET": http.HandlerFunc(rh.HandleCount),
	}

	ih := handlers.MethodHandler{
		"GET": http.HandlerFunc(rh.HandleIds),
	}

	ah := handlers.MethodHandler{
		"GET": http.HandlerFunc(rh.HandleGetAll),
	}

	if resourcePath != "" {
		resourcePath = fmt.Sprintf("/%s", resourcePath)
	}
	servicesRouter.Handle(fmt.Sprintf("%s%s", resourcePath, "/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}"), mh)
	servicesRouter.Handle(fmt.Sprintf("%s%s", resourcePath, "/__count"), ch)
	servicesRouter.Handle(fmt.Sprintf("%s%s", resourcePath, "/__ids"), ih)
	servicesRouter.Handle(fmt.Sprintf("%s%s", resourcePath, "/"), ah)
}
