package service

import (
	"fmt"
	"net/http"

	"time"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	"github.com/Financial-Times/service-status-go/gtg"
	httpStatus "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
)

func AddAdminHandlers(servicesRouter *mux.Router, svc s3iface.S3API, bucketName string, appName string, appSystemCode string, requestLoggingEnabled bool, log *logger.UPPLogger) {
	c := checker{svc, bucketName, log}
	var monitoringRouter http.Handler = servicesRouter
	if requestLoggingEnabled {
		monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.Logger, monitoringRouter)
		monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)
	}

	http.HandleFunc(httpStatus.PingPath, httpStatus.PingHandler)
	http.HandleFunc(httpStatus.PingPathDW, httpStatus.PingHandler)
	http.HandleFunc(httpStatus.BuildInfoPath, httpStatus.BuildInfoHandler)
	http.HandleFunc(httpStatus.BuildInfoPathDW, httpStatus.BuildInfoHandler)
	http.HandleFunc("/__health", fthealth.Handler(
		fthealth.TimedHealthCheck{
			HealthCheck: fthealth.HealthCheck{
				SystemCode:  appSystemCode,
				Name:        appName + " Healthchecks",
				Description: "Runs a HEAD check on bucket",
				Checks: []fthealth.Check{
					{
						BusinessImpact:   "Unable to access S3 bucket",
						Name:             "S3 Bucket check",
						PanicGuide:       "https://runbooks.ftops.tech/" + appSystemCode,
						Severity:         3,
						TechnicalSummary: `Can not access S3 bucket.`,
						Checker:          c.healthCheck,
					},
				},
			},
			Timeout: 10 * time.Second,
		}))

	gtgHandler := httpStatus.NewGoodToGoHandler(c.gtgCheckHandler)
	http.HandleFunc(httpStatus.GTGPath, gtgHandler)
	http.Handle("/", monitoringRouter)
}

type checker struct {
	s3iface.S3API
	bucketName string
	log        *logger.UPPLogger
}

func (c *checker) healthCheck() (string, error) {
	params := &s3.HeadBucketInput{
		Bucket: aws.String(c.bucketName), // Required
	}
	_, err := c.HeadBucket(params)
	if err != nil {
		c.log.Errorf("Got error running S3 health check, %v", err.Error())
		return "Can not perform check on S3 bucket", err
	}
	return "Access to S3 bucket ok", err
}

func (c *checker) gtgCheckHandler() gtg.Status {
	if _, err := c.healthCheck(); err != nil {
		c.log.Info("Healthcheck failed, gtg is bad.")
		return gtg.Status{GoodToGo: false, Message: "Head request to S3 failed"}
	}
	return gtg.Status{GoodToGo: true, Message: "OK"}
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
