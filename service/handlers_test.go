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
	"sync"
	"testing"
)

const (
	ExpectedContentType = "application/json"
)

func TestAddAdminHandlers(t *testing.T) {
	s := &mockS3Client{}
	mw := &mockWriter{}
	mr := &mockReader{payload: "Something found"}
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

func TestWriteHandlerNewContentReturnsCreated(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{}
	mr := &mockReader{}
	Handlers(r, NewWriterHandler(mw, mr), ReaderHandler{})

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("PUT", "/22f53313-85c6-46b2-94e7-cfde9322f26c", "PAYLOAD"))

	assert.Equal(t, 201, rec.Code)
	assert.Equal(t, "PAYLOAD", mw.payload)
	assert.Equal(t, "22f53313-85c6-46b2-94e7-cfde9322f26c", mw.uuid)
	assert.Equal(t, ExpectedContentType, mw.ct)
	assert.Equal(t, "{\"message\":\"CREATED\"}", rec.Body.String())
}

func TestWriteHandlerUpdateContentReturnsOK(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{}
	mr := &mockReader{found: true}
	Handlers(r, NewWriterHandler(mw, mr), ReaderHandler{})

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("PUT", "/22f53313-85c6-46b2-94e7-cfde9322f26c", "PAYLOAD"))

	assert.Equal(t, 200, rec.Code)
	assert.Equal(t, "PAYLOAD", mw.payload)
	assert.Equal(t, "22f53313-85c6-46b2-94e7-cfde9322f26c", mw.uuid)
	assert.Equal(t, "22f53313-85c6-46b2-94e7-cfde9322f26c", mr.headUuid)
	assert.Equal(t, ExpectedContentType, mw.ct)
	assert.Equal(t, "{\"message\":\"UPDATED\"}", rec.Body.String())
}

func TestWriterHandlerFailReadingBody(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{}
	mr := &mockReader{}
	Handlers(r, NewWriterHandler(mw, mr), ReaderHandler{})

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequestBodyFail("PUT", "/22f53313-85c6-46b2-94e7-cfde9322f26c"))
	assert.Equal(t, 500, rec.Code)
	assert.Equal(t, "{\"message\":\"Unknown internal error\"}", rec.Body.String())
}

func TestWriterHandlerFailWrite(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{returnError: errors.New("error writing")}
	mr := &mockReader{}
	Handlers(r, NewWriterHandler(mw, mr), ReaderHandler{})

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("PUT", "/22f53313-85c6-46b2-94e7-cfde9322f26c", "PAYLOAD"))
	assert.Equal(t, 500, rec.Code)
	assert.Equal(t, "{\"message\":\"Unknown internal error\"}", rec.Body.String())
}

func TestWriterHandlerDeleteReturnsOK(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{}
	mr := &mockReader{}
	Handlers(r, NewWriterHandler(mw, mr), ReaderHandler{})

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("DELETE", "/22f53313-85c6-46b2-94e7-cfde9322f26c", ""))
	assert.Equal(t, "22f53313-85c6-46b2-94e7-cfde9322f26c", mw.uuid)
	assert.Equal(t, 204, rec.Code)
	assert.Empty(t, rec.Body.String())
}

func TestWriterHandlerDeleteFailsReturns500(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{returnError: errors.New("Some error from writer")}
	mr := &mockReader{}
	Handlers(r, NewWriterHandler(mw, mr), ReaderHandler{})

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("DELETE", "/22f53313-85c6-46b2-94e7-cfde9322f26c", ""))
	assert.Equal(t, 500, rec.Code)
	assert.Equal(t, "{\"message\":\"Unknown internal error\"}", rec.Body.String())
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
	assertRequestAndResponseFromRouter(t, r, "/22f53313-85c6-46b2-94e7-cfde9322f26c", 404, "{\"message\":\"Item not found\"}")
}

func TestReadHandlerForErrorFromReader(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{payload: "something came back but", returnError: errors.New("Some error from reader though")}
	Handlers(r, WriterHandler{}, NewReaderHandler(mr))
	assertRequestAndResponseFromRouter(t, r, "/22f53313-85c6-46b2-94e7-cfde9322f26c", 500, "{\"message\":\"Unknown internal error\"}")
}

func TestReadHandlerForErrorReadingBody(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{rc: &mockReaderCloser{err: errors.New("Some error")}}
	Handlers(r, WriterHandler{}, NewReaderHandler(mr))

	assertRequestAndResponseFromRouter(t, r, "/22f53313-85c6-46b2-94e7-cfde9322f26c", 502, "{\"message\":\"Error while communicating to other service\"}")
}

func TestReadHandlerCountOK(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{count: 1337}
	Handlers(r, WriterHandler{}, NewReaderHandler(mr))
	assertRequestAndResponseFromRouter(t, r, "/__count", 200, "1337")
}

func TestReadHandlerCountFailsReturnsInternalServerError(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{returnError: errors.New("Some error from reader though")}
	Handlers(r, WriterHandler{}, NewReaderHandler(mr))
	assertRequestAndResponseFromRouter(t, r, "/__count", 500, "{\"message\":\"Unknown internal error\"}")
}

func TestReaderHandlerIdsOK(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{payload: "PAYLOAD"}
	Handlers(r, WriterHandler{}, NewReaderHandler(mr))
	assertRequestAndResponseFromRouter(t, r, "/__ids", 200, "PAYLOAD")
}

func TestReaderHandlerIdsFailsReturnsInternalServerError(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{returnError: errors.New("Some error from reader though")}
	Handlers(r, WriterHandler{}, NewReaderHandler(mr))
	assertRequestAndResponseFromRouter(t, r, "/__ids", 500, "{\"message\":\"Unknown internal error\"}")
}

func TestHandleGetAllOK(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{payload: "PAYLOAD"}
	Handlers(r, WriterHandler{}, NewReaderHandler(mr))
	assertRequestAndResponseFromRouter(t, r, "/", 200, "PAYLOAD")
}

func TestHandleGetAllFailsReturnsInternalServerError(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{returnError: errors.New("Some error from reader though")}
	Handlers(r, WriterHandler{}, NewReaderHandler(mr))
	assertRequestAndResponseFromRouter(t, r, "/", 500, "{\"message\":\"Unknown internal error\"}")
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
	sync.Mutex
	found       bool
	uuid        string
	headUuid    string
	payload     string
	rc          io.ReadCloser
	returnError error
	count       int64
}

func (r *mockReader) Get(uuid string) (bool, io.ReadCloser, error) {
	r.Lock()
	defer r.Unlock()
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

func (r *mockReader) Head(uuid string) (bool, error) {
	r.Lock()
	defer r.Unlock()
	r.headUuid = uuid
	return r.found, r.returnError
}

func (r *mockReader) Count() (int64, error) {
	r.Lock()
	defer r.Unlock()
	return r.count, r.returnError
}

func (r *mockReader) processPipe() (io.PipeReader, error) {
	pv, pw := io.Pipe()
	go func(p *io.PipeWriter) {
		if r.payload != "" {
			p.Write([]byte(r.payload))
		}
		p.Close()
	}(pw)
	return *pv, r.returnError
}

func (r *mockReader) GetAll() (io.PipeReader, error) {
	return r.processPipe()
}

func (r *mockReader) Ids() (io.PipeReader, error) {
	return r.processPipe()
}

type mockWriter struct {
	sync.Mutex
	uuid        string
	payload     string
	returnError error
	ct          string
}

func (mw *mockWriter) Delete(uuid string) error {
	mw.Lock()
	defer mw.Unlock()
	mw.uuid = uuid
	return mw.returnError
}

func (mw *mockWriter) Write(uuid string, b *[]byte, ct string) error {
	mw.Lock()
	defer mw.Unlock()
	mw.uuid = uuid
	mw.payload = string((*b)[:])
	mw.ct = ct
	return mw.returnError
}