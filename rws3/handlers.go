package rws3

import (
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"net/http"
)

func AddAdminHandlers(monitoringHandler *http.Handler) {
	http.HandleFunc(status.PingPath, status.PingHandler)
	http.HandleFunc(status.PingPathDW, status.PingHandler)
	http.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)
	http.HandleFunc(status.BuildInfoPathDW, status.BuildInfoHandler)
	http.Handle("/", *monitoringHandler)
}
