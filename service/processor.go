package service

import (
	"bytes"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

type Reader interface {
	Get(uuid string) (bool, io.ReadCloser, error)
}

func NewS3Reader(svc s3iface.S3API, bucketName string, bucketPrefix string) Reader {
	return &S3Reader{
		svc:          svc,
		bucketName:   bucketName,
		bucketPrefix: bucketPrefix,
	}
}

type S3Reader struct {
	svc          s3iface.S3API
	bucketName   string
	bucketPrefix string
}

func (r *S3Reader) Get(uuid string) (bool, io.ReadCloser, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(r.bucketName),
		Key:    aws.String(r.bucketPrefix + "/" + uuid),
	}
	resp, err := r.svc.GetObject(params)

	if err != nil {
		e, ok := err.(awserr.Error)
		if ok && e.Code() == "NoSuchKey" {
			return false, nil, nil
		}
	}

	return true, resp.Body, err
}

type Writer interface {
	Write(uuid string, b *[]byte, contentType string) error
}

type S3Writer struct {
	svc          s3iface.S3API
	bucketName   string
	bucketPrefix string
}

func NewS3Writer(svc s3iface.S3API, bucketName string, bucketPrefix string) Writer {
	return &S3Writer{
		svc:          svc,
		bucketName:   bucketName,
		bucketPrefix: bucketPrefix,
	}
}

func (w *S3Writer) Write(uuid string, b *[]byte, ct string) error {

	params := &s3.PutObjectInput{
		Bucket:      aws.String(w.bucketName),
		Key:         aws.String(w.bucketPrefix + "/" + uuid),
		Body:        bytes.NewReader(*b),
		ContentType: aws.String(ct),
	}

	resp, err := w.svc.PutObject(params)

	if err != nil {
		log.Infof("Error found, Resp was : %v", resp)
		return err
	}

	return nil
}

type WriterHandler struct {
	writer Writer
}

func NewWriterHandler(writer Writer) WriterHandler {
	return WriterHandler{writer: writer}
}

func (w *WriterHandler) HandleWrite(rw http.ResponseWriter, r *http.Request) {
	var err error
	bs, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorf("Error reading request body: %v", err.Error())
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	ct := r.Header.Get("Content-Type")
	uuid := strings.Split(r.URL.Path, "/")[1]
	err = w.writer.Write(uuid, &bs, ct)
	if err != nil {
		log.Errorf("Error writing '%v': %v", uuid, err.Error())
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte("{\"msg\":\"Internal Server Error\"}"))
		return
	}
	log.Infof("Wrote '%v' succesfully", uuid)
	rw.WriteHeader(http.StatusCreated)
}

func NewReaderHandler(reader Reader) ReaderHandler {
	return ReaderHandler{reader: reader}
}

type ReaderHandler struct {
	reader Reader
}

func (rh *ReaderHandler) HandleGet(rw http.ResponseWriter, r *http.Request) {

	f, i, err := rh.reader.Get(strings.Split(r.URL.Path, "/")[1])

	if !f {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusNotFound)
		rw.Write([]byte("{\"msg\":\"item not found\"}"))
		return
	}

	if err != nil {
		log.Errorf("Error from reader: %v", err.Error())
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte("{\"msg\":\"Internal Server Error\"}"))
		return
	}

	b, err := ioutil.ReadAll(i)

	if err != nil {
		log.Errorf("Error reading body: %v", err.Error())
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusBadGateway)
		rw.Write([]byte("{\"msg\":\"Status Bad Gateway\"}"))
		return
	}
	rw.WriteHeader(http.StatusOK)
	rw.Write(b)
}
