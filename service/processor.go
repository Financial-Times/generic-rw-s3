package service

import (
	"bytes"
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

type Reader interface {
	Get(uuid string) (bool, io.ReadCloser, error)
	Count() (int64, error)
	Ids() (io.PipeReader, error)
	GetAll() (io.PipeReader, error)
}

func NewS3Reader(svc s3iface.S3API, bucketName string, bucketPrefix string, workers int16) Reader {
	return &S3Reader{
		svc:          svc,
		bucketName:   bucketName,
		bucketPrefix: bucketPrefix,
		workers:      workers,
	}
}

type S3Reader struct {
	svc          s3iface.S3API
	bucketName   string
	bucketPrefix string
	workers      int16
}

func (r *S3Reader) Get(uuid string) (bool, io.ReadCloser, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(r.bucketName),
		Key:    aws.String(getKey(r.bucketPrefix, uuid)),
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

func (r *S3Reader) Count() (int64, error) {
	c := int64(0)
	err := r.svc.ListObjectsV2Pages(r.getListObjectsV2Input(),
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			c = c + *page.KeyCount
			return !lastPage
		})
	return c, err
}

func (r *S3Reader) getListObjectsV2Input() *s3.ListObjectsV2Input {
	if r.bucketPrefix == "" {
		return &s3.ListObjectsV2Input{
			Bucket: aws.String(r.bucketName),
		}
	}
	return &s3.ListObjectsV2Input{
		Bucket: aws.String(r.bucketName),
		Prefix: aws.String(r.bucketPrefix + "/"),
	}
}

func (r *S3Reader) GetAll() (io.PipeReader, error) {
	err := r.checkListOk()
	pv, pw := io.Pipe()
	if err != nil {
		pv.Close()
		return *pv, err
	}

	itemSize := float32(r.workers) * 1.5
	items := make(chan *io.ReadCloser, int(itemSize))
	keys := make(chan *string, 3000) //  Three times the default Page size
	go r.processItems(items, pw)
	var wg sync.WaitGroup
	tw := int(r.workers)
	for w := 0; w < tw; w++ {
		wg.Add(1)
		go r.getItemWorker(w, &wg, keys, items)
	}

	go r.listObjects(keys)

	go func(w *sync.WaitGroup, i chan *io.ReadCloser) {
		w.Wait()
		close(i)
	}(&wg, items)

	return *pv, err
}

func (r *S3Reader) getItemWorker(w int, wg *sync.WaitGroup, keys <-chan *string, items chan<- *io.ReadCloser) {
	defer wg.Done()
	for uuid := range keys {
		log.Infof("worker %v, getting uuid : %v", w, *uuid)
		if found, i, _ := r.Get(*uuid); found {
			items <- &i
		}
	}
}

func (r *S3Reader) processItems(items <-chan *io.ReadCloser, pw *io.PipeWriter) {
	for item := range items {
		if _, err := io.Copy(pw, *item); err != nil {
			log.Errorf("Error reading from S3: %v", err.Error())
		} else {
			io.WriteString(pw, "\n")
		}
	}
	pw.Close()
}

func (r *S3Reader) Ids() (io.PipeReader, error) {

	err := r.checkListOk()
	pv, pw := io.Pipe()
	if err != nil {
		pv.Close()
		return *pv, err
	}

	go func(p *io.PipeWriter) {

		keys := make(chan *string, 3000) //  Three times the default Page size
		go func(c <-chan *string, out *io.PipeWriter) {
			encoder := json.NewEncoder(out)
			for key := range c {
				pl := obj{UUID: *key}
				if err := encoder.Encode(pl); err != nil {
					log.Errorf("Got error encoding key : %v", err.Error())
					break
				}
			}
			out.Close()
		}(keys, p)

		err := r.listObjects(keys)
		if err != nil {
			log.Errorf("Got an error reading content of bucket : %v", err.Error())
		}
	}(pw)
	return *pv, err
}

func (r *S3Reader) checkListOk() (err error) {
	p := r.getListObjectsV2Input()
	p.MaxKeys = aws.Int64(1)
	_, err = r.svc.ListObjectsV2(p)
	return err
}

func (r *S3Reader) listObjects(keys chan<- *string) error {
	return r.svc.ListObjectsV2Pages(r.getListObjectsV2Input(),
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, o := range page.Contents {

				if !strings.HasSuffix(*o.Key, "/") && (*o.Key != ".") {
					var key string
					if r.bucketPrefix == "" {
						key = *o.Key
					} else {
						k := strings.SplitAfter(*o.Key, r.bucketPrefix+"/")
						log.Infof("k: %v", k)
						key = k[1]
					}
					uuid := strings.Replace(key, "/", "-", -1)
					keys <- &uuid
				}
			}

			if lastPage {
				close(keys)
			}

			return !lastPage
		})
}

type Writer interface {
	Write(uuid string, b *[]byte, contentType string) error
	Delete(uuid string) error
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

func getKey(bucketPrefix string, uuid string) string {
	return bucketPrefix + "/" + strings.Replace(uuid, "-", "/", -1)
}

func (w *S3Writer) Delete(uuid string) error {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(w.bucketName),                 // Required
		Key:    aws.String(getKey(w.bucketPrefix, uuid)), // Required
	}

	if resp, err := w.svc.DeleteObject(params); err != nil {
		log.Infof("Error found, Resp was : %v", resp)
		return err
	}

	return nil
}

func (w *S3Writer) Write(uuid string, b *[]byte, ct string) error {

	params := &s3.PutObjectInput{
		Bucket:      aws.String(w.bucketName),
		Key:         aws.String(getKey(w.bucketPrefix, uuid)),
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

func (w *WriterHandler) HandleDelete(rw http.ResponseWriter, r *http.Request) {
	uuid := strings.Split(r.URL.Path, "/")[1]
	if err := w.writer.Delete(uuid); err != nil {
		log.Errorf("Error reading request body: %v", err.Error())
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte("{\"msg\":\"Internal Server Error\"}"))
		return
	}

	log.Infof("Deleted '%v' succesfully", uuid)
	rw.WriteHeader(http.StatusNoContent)
}

func NewReaderHandler(reader Reader) ReaderHandler {
	return ReaderHandler{reader: reader}
}

type ReaderHandler struct {
	reader Reader
}

func (rh *ReaderHandler) HandleIds(rw http.ResponseWriter, r *http.Request) {
	pv, err := rh.reader.Ids()
	defer pv.Close()
	if err != nil {
		log.Errorf("Error from reader: %v", err.Error())
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte("{\"msg\":\"Internal Server Error\"}"))
		return
	}

	rw.WriteHeader(http.StatusOK)
	io.Copy(rw, &pv)
}

func (rh *ReaderHandler) HandleCount(rw http.ResponseWriter, r *http.Request) {
	i, err := rh.reader.Count()
	rw.Header().Set("Content-Type", "application/json")
	if err != nil {
		log.Errorf("Error from reader: %v", err.Error())
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte("{\"msg\":\"Internal Server Error\"}"))
		return
	}
	log.Infof("Got a count back of '%v'", i)
	rw.WriteHeader(http.StatusOK)
	b := []byte{}
	b = strconv.AppendInt(b, i, 10)
	rw.Write(b)
}

func (rh *ReaderHandler) HandleGetAll(rw http.ResponseWriter, r *http.Request) {
	pv, err := rh.reader.GetAll()

	if err != nil {
		log.Errorf("Error from reader: %v", err.Error())
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte("{\"msg\":\"Internal Server Error\"}"))
		return
	}

	rw.WriteHeader(http.StatusOK)
	io.Copy(rw, &pv)
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
