package service

import (
	"bytes"
	"errors"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	ExpectedContentType = "application/json"
)

func TestAddAdminHandlers(t *testing.T) {
	s := &mockS3Client{}
	mw := &mockWriter{}
	mr := &mockReader{payload: "Soemthing found"}
	r := mux.NewRouter()
	AddAdminHandlers(r, s, "bucketName", mw, mr)

	t.Run(status.PingPath, func(t *testing.T) {
		assertRequestAndResponse(t, status.PingPath, 200, "pong")
	})

	t.Run(status.PingPathDW, func(t *testing.T) {
		assertRequestAndResponse(t, status.PingPathDW, 200, "pong")
	})

	t.Run(status.BuildInfoPath, func(t *testing.T) {
		assertRequestAndResponse(t, status.BuildInfoPath, 200, "")
	})

	t.Run(status.BuildInfoPathDW, func(t *testing.T) {
		assertRequestAndResponse(t, status.BuildInfoPathDW, 200, "")
	})

	t.Run("/__health good", func(t *testing.T) {
		rec := assertRequestAndResponse(t, "/__health", 200, "")
		assert.Equal(t, "bucketName", *s.headBucketInput.Bucket)
		body := rec.Body.String()
		log.Infof("Body was %v", body)
		assert.Contains(t, body, "\"S3 Bucket check\",\"ok\":true")
	})

	t.Run("/__gtg good", func(t *testing.T) {
		assertRequestAndResponse(t, "/__gtg", 200, "")
	})

	t.Run("/__health bad", func(t *testing.T) {
		s.s3error = errors.New("S3 error")
		rec := assertRequestAndResponse(t, "/__health", 200, "")
		assert.Equal(t, "bucketName", *s.headBucketInput.Bucket)
		body := rec.Body.String()
		log.Infof("Body was %v", body)
		assert.Contains(t, body, "\"S3 Bucket check\",\"ok\":false")
	})

	t.Run("/_gtg bad can't write", func(t *testing.T) {
		mw.returnError = errors.New("S3 write error")
		mr.returnError = nil
		assertRequestAndResponse(t, "/__gtg", 503, "")
	})

	t.Run("/_gtg bad can't read", func(t *testing.T) {
		mw.returnError = nil
		mr.returnError = errors.New("S3 read error")
		assertRequestAndResponse(t, "/__gtg", 503, "")
	})
}

func TestWriteHandler(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{}
	Handlers(r, NewWriterHandler(mw), ReaderHandler{})

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("PUT", "/22f53313-85c6-46b2-94e7-cfde9322f26c", "PAYLOAD"))

	assert.Equal(t, 201, rec.Code)
	assert.Equal(t, "PAYLOAD", mw.payload)
	assert.Equal(t, "22f53313-85c6-46b2-94e7-cfde9322f26c", mw.uuid)
	assert.Equal(t, ExpectedContentType, mw.ct)
}

func TestWriterHandlerFailReadingBody(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{}
	Handlers(r, NewWriterHandler(mw), ReaderHandler{})

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequestBodyFail("PUT", "/22f53313-85c6-46b2-94e7-cfde9322f26c"))
	assert.Equal(t, 500, rec.Code)
}

func TestWriterHandlerFailWrite(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{returnError: errors.New("error writing")}
	Handlers(r, WriterHandler{writer: mw}, ReaderHandler{})

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("PUT", "/22f53313-85c6-46b2-94e7-cfde9322f26c", "PAYLOAD"))
	assert.Equal(t, 500, rec.Code)
}

func TestReadHandlerForUUID(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{payload: "Some content"}
	Handlers(r, WriterHandler{}, NewReaderHandler(mr))
	assertRequestAndResponseFromRouter(t, r, "/22f53313-85c6-46b2-94e7-cfde9322f26c", 200, "Some content")
}

func TestReadHandlerForUUIDNotFound(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{}
	Handlers(r, WriterHandler{}, NewReaderHandler(mr))
	assertRequestAndResponseFromRouter(t, r, "/22f53313-85c6-46b2-94e7-cfde9322f26c", 404, "{\"msg\":\"item not found\"}")
}

func TestReadHandlerForErrorFromReader(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{payload: "something came back but", returnError: errors.New("Some error from reader though")}
	Handlers(r, WriterHandler{}, NewReaderHandler(mr))
	assertRequestAndResponseFromRouter(t, r, "/22f53313-85c6-46b2-94e7-cfde9322f26c", 500, "{\"msg\":\"Internal Server Error\"}")
}

func TestReadHandlerForErrorReadingBody(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{rc: &mockReaderCloser{err: errors.New("Some error")}}
	Handlers(r, WriterHandler{}, NewReaderHandler(mr))

	assertRequestAndResponseFromRouter(t, r, "/22f53313-85c6-46b2-94e7-cfde9322f26c", 502, "{\"msg\":\"Status Bad Gateway\"}")
}

func assertRequestAndResponseFromRouter(t testing.TB, r *mux.Router, url string, expectedStatus int, expectedBody string) *httptest.ResponseRecorder {

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("GET", url, ""))
	assert.Equal(t, expectedStatus, rec.Code)
	if expectedBody != "" {
		assert.Equal(t, expectedBody, rec.Body.String())
	}

	return rec
}

func assertRequestAndResponse(t testing.TB, url string, expectedStatus int, expectedBody string) *httptest.ResponseRecorder {

	rec := httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(rec, newRequest("GET", url, ""))
	assert.Equal(t, expectedStatus, rec.Code)
	if expectedBody != "" {
		assert.Equal(t, expectedBody, rec.Body.String())
	}

	return rec
}

type mockReaderCloser struct {
	err error
	n   int
}

func (mr *mockReaderCloser) Read(p []byte) (int, error) {
	return mr.n, mr.err
}

func (mr *mockReaderCloser) Close() error {
	return mr.err
}

func newRequestBodyFail(method, url string) *http.Request {
	mr := &mockReaderCloser{err: errors.New("Badbody")}
	r := io.Reader(mr)
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		panic(err)
	}
	return req
}

func newRequest(method, url string, body string) *http.Request {
	var payload io.Reader
	if body != "" {
		payload = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequest(method, url, payload)
	req.Header = map[string][]string{
		"Content-Type": {ExpectedContentType},
	}
	if err != nil {
		panic(err)
	}
	return req
}

type mockReader struct {
	uuid        string
	payload     string
	rc          io.ReadCloser
	returnError error
}

func (r *mockReader) Get(uuid string) (bool, io.ReadCloser, error) {
	log.Infof("Got request for uuid: %v", uuid)
	r.uuid = uuid
	var body io.ReadCloser

	if r.payload != "" {
		body = ioutil.NopCloser(strings.NewReader(r.payload))
	}

	if r.rc != nil {
		body = r.rc
	}

	return r.payload != "" || r.rc != nil, body, r.returnError
}

type mockWriter struct {
	uuid        string
	payload     string
	returnError error
	ct          string
}

func (mw *mockWriter) Write(uuid string, b *[]byte, ct string) error {
	mw.uuid = uuid
	mw.payload = string((*b)[:])
	mw.ct = ct
	return mw.returnError
}
