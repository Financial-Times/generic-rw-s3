package rws3

import (
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAddAdminHandlers(t *testing.T) {

	servicesRouter := mux.NewRouter()
	AddAdminHandlers(servicesRouter)
	assertRequestAndResponse(t, status.PingPath, 200, "pong")
	assertRequestAndResponse(t, status.PingPath, 200, "pong")

	assertRequestAndResponse(t, status.BuildInfoPath, 200, "")
	assertRequestAndResponse(t, status.BuildInfoPath, 200, "")

	assertRequestAndResponse(t, "/__health", 200, "")
}

func assertRequestAndResponse(t testing.TB, url string, expectedStatus int, expectedBody string) {

	rec := httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(rec, newRequest("GET", url))
	assert.Equal(t, expectedStatus, rec.Code)
	if expectedBody != "" {
		assert.Equal(t, expectedBody, rec.Body.String())
	}
}

func newRequest(method, url string) *http.Request {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		panic(err)
	}
	return req
}
