package service

import (
	"fmt"
	"net/http"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	httpStatus "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
)

func AddAdminHandlers(servicesRouter *mux.Router, requestLoggingEnabled bool, log *logger.UPPLogger, hc *HealthCheck) {
	var monitoringRouter http.Handler = servicesRouter
	if requestLoggingEnabled {
		monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.Logger, monitoringRouter)
		monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)
	}

	http.HandleFunc(httpStatus.PingPath, httpStatus.PingHandler)
	http.HandleFunc(httpStatus.PingPathDW, httpStatus.PingHandler)
	http.HandleFunc(httpStatus.BuildInfoPath, httpStatus.BuildInfoHandler)
	http.HandleFunc(httpStatus.BuildInfoPathDW, httpStatus.BuildInfoHandler)
	http.HandleFunc("/__health", hc.Health())

	gtgHandler := httpStatus.NewGoodToGoHandler(hc.GTG)
	http.HandleFunc(httpStatus.GTGPath, gtgHandler)
	http.Handle("/", monitoringRouter)
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
