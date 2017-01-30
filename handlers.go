package main
import (
	"github.com/Financial-Times/go-fthealth/v1a"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
	"net/http"
)

func AddAdminHandlers(servicesRouter *mux.Router) {

	var monitoringRouter http.Handler = servicesRouter
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)
	http.HandleFunc(status.PingPath, status.PingHandler)
	http.HandleFunc(status.PingPathDW, status.PingHandler)
	http.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)
	http.HandleFunc(status.BuildInfoPathDW, status.BuildInfoHandler)
	http.HandleFunc("/__health", v1a.Handler("Organisations RW Gazetteer Healthchecks",
		"NOOP healthcheck", v1a.Check{
			BusinessImpact:   "Unable to respond to requests",
			Name:             "NOOP health check",
			PanicGuide:       "http://lmgtfy.com/?q=NOOP",
			Severity:         1,
			TechnicalSummary: `NOOP.`,
			Checker: func() (string, error) {
				return "NOOP", nil
			},
		}))

	http.Handle("/", monitoringRouter)
}
