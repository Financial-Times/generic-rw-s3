package service

import (
	"bytes"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"io/ioutil"
	"net/http"
	"strings"
)

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
		log.Errorf("Got the following error %v", err.Error())
		return err
	}

	log.Infof("Resp was : %v", resp)
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
	}
	ct := r.Header.Get("Content-Type")
	err = w.writer.Write(strings.Split(r.URL.Path, "/")[1], &bs, ct)
	if err != nil {
		log.Errorf("Error writing body: %v", err.Error())
		rw.WriteHeader(http.StatusInternalServerError)
	}
	rw.WriteHeader(http.StatusCreated)
}
