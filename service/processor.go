package service

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/kafka-client-go/v4"
	transactionid "github.com/Financial-Times/transactionid-utils-go"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/mitchellh/hashstructure"
)

type QProcessor interface {
	ProcessMsg(m kafka.FTMessage)
}

func NewQProcessor(w Writer, log *logger.UPPLogger) QProcessor {
	return &S3QProcessor{w, log}
}

type S3QProcessor struct {
	Writer
	log *logger.UPPLogger
}
type KafkaMsg struct {
	Id string `json:"uuid"`
}

type Status int

const (
	UNCHANGED Status = iota
	CREATED
	UPDATED
	INTERNAL_ERROR
	SERVICE_UNAVAILABLE
)

func (r *S3QProcessor) ProcessMsg(m kafka.FTMessage) {
	var uuid string
	var ct string
	var ok bool
	tid := m.Headers[transactionid.TransactionIDHeader]
	if tid == "" {
		tid = transactionid.NewTransactionID()
	}
	if ct, ok = m.Headers["Content-Type"]; !ok {
		ct = ""
	}

	var km KafkaMsg
	b := []byte(m.Body)
	if err := json.Unmarshal(b, &km); err != nil {
		r.log.WithError(err).WithTransactionID(tid).WithField("message_id", m.Headers["Message-Id"]).Errorf("Could not unmarshal message: %v", b)
		return
	}

	if uuid = km.Id; uuid == "" {
		uuid = m.Headers["Message-Id"]
	}

	writeStatus, err := r.Write(uuid, "", &b, ct, tid, false)
	if err != nil {
		r.log.WithError(err).WithTransactionID(tid).WithUUID(uuid).Error("Failed to write")
		return
	}

	switch writeStatus {
	case UNCHANGED:
		return
	case UPDATED:
		r.log.WithTransactionID(tid).WithUUID(uuid).Info("Updated concept record in s3 successfully")
		return
	case CREATED:
		r.log.WithTransactionID(tid).WithUUID(uuid).Info("Created concept record in s3 successfully")
		return
	default:
		r.log.WithTransactionID(tid).WithUUID(uuid).Error("Unhandled error occurred!")
		return
	}
}

type Reader interface {
	Get(uuid string, path string) (bool, io.ReadCloser, *string, error)
	Count() (int64, error)
	Ids() (*io.PipeReader, error)
	GetAll(path string) (*io.PipeReader, error)
}

func NewS3Reader(svc s3iface.S3API, bucketName string, bucketPrefix string, workers int16, log *logger.UPPLogger) Reader {
	return &S3Reader{
		svc:          svc,
		bucketName:   bucketName,
		bucketPrefix: bucketPrefix,
		workers:      workers,
		log:          log,
	}
}

type S3Reader struct {
	svc          s3iface.S3API
	bucketName   string
	bucketPrefix string
	workers      int16
	log          *logger.UPPLogger
}

func (r *S3Reader) Get(uuid string, path string) (bool, io.ReadCloser, *string, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(r.bucketName),                       // Required
		Key:    aws.String(getKey(r.bucketPrefix, path, uuid)), // Required
	}
	resp, err := r.svc.GetObject(params)

	if err != nil {
		e, ok := err.(awserr.Error)
		if ok && e.Code() == "NoSuchKey" {
			return false, nil, nil, nil
		}
		return false, nil, nil, err
	}

	return true, resp.Body, resp.ContentType, err
}

func (r *S3Reader) Count() (int64, error) {
	cc := make(chan *s3.ListObjectsV2Output, 10)
	rc := make(chan int64, 1)

	go func() {
		t := int64(0)
		for i := range cc {
			for _, o := range i.Contents {
				if (!strings.HasSuffix(*o.Key, "/") && !strings.HasPrefix(*o.Key, "__")) && (*o.Key != ".") {
					t++
				}
			}
		}
		rc <- t
	}()

	err := r.svc.ListObjectsV2Pages(r.getListObjectsV2Input(),
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			cc <- page

			if lastPage {
				close(cc)
			}
			return !lastPage
		})

	var c int64
	if err == nil {
		c = <-rc
	} else {
		close(rc)
	}
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

func (r *S3Reader) GetAll(path string) (*io.PipeReader, error) {
	err := r.checkListOk()
	pv, pw := io.Pipe()
	if err != nil {
		pv.Close()
		return pv, err
	}

	itemSize := float32(r.workers) * 1.5
	items := make(chan *io.ReadCloser, int(itemSize))
	keys := make(chan *string, 3000) //  Three times the default Page size
	go r.processItems(items, pw)
	var wg sync.WaitGroup
	tw := int(r.workers)
	for w := 0; w < tw; w++ {
		wg.Add(1)
		go r.getItemWorker(path, &wg, keys, items)
	}

	go r.listObjects(keys)

	go func(w *sync.WaitGroup, i chan *io.ReadCloser) {
		w.Wait()
		close(i)
	}(&wg, items)

	return pv, err
}

func (r *S3Reader) getItemWorker(path string, wg *sync.WaitGroup, keys <-chan *string, items chan<- *io.ReadCloser) {
	defer wg.Done()
	for uuid := range keys {
		if found, i, _, _ := r.Get(*uuid, path); found {
			items <- &i
		}
	}
}

func (r *S3Reader) processItems(items <-chan *io.ReadCloser, pw *io.PipeWriter) {
	for item := range items {
		if _, err := io.Copy(pw, *item); err != nil {
			r.log.WithError(err).Error("Error reading from S3")
		} else {
			io.WriteString(pw, "\n")
		}
	}
	pw.Close()
}

func (r *S3Reader) Ids() (*io.PipeReader, error) {

	err := r.checkListOk()
	pv, pw := io.Pipe()
	if err != nil {
		pv.Close()
		return pv, err
	}

	go func(p *io.PipeWriter) {

		keys := make(chan *string, 3000) //  Three times the default Page size
		go func(c <-chan *string, out *io.PipeWriter) {
			encoder := json.NewEncoder(out)
			for key := range c {
				pl := obj{UUID: *key}
				if err := encoder.Encode(pl); err != nil {
					r.log.WithError(err).Error("Got error encoding key")
					break
				}
			}
			out.Close()
		}(keys, p)

		err := r.listObjects(keys)
		if err != nil {
			r.log.WithError(err).Error("Got an error reading content of bucket")
		}
	}(pw)
	return pv, err
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
				if (!strings.HasSuffix(*o.Key, "/") && !strings.HasPrefix(*o.Key, "__")) && (*o.Key != ".") {
					var key string
					if r.bucketPrefix == "" {
						key = *o.Key
					} else {
						k := strings.SplitAfter(*o.Key, r.bucketPrefix+"/")
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
	Write(uuid string, path string, b *[]byte, contentType string, transactionID string, ignoreHash bool) (Status, error)
	Delete(uuid string, path string, transactionID string) error
}

type S3Writer struct {
	svc                s3iface.S3API
	bucketName         string
	bucketPrefix       string
	onlyUpdatesEnabled bool
	log                *logger.UPPLogger
}

func NewS3Writer(svc s3iface.S3API, bucketName string, bucketPrefix string, onlyUpdatesEnabled bool, log *logger.UPPLogger) Writer {
	return &S3Writer{
		svc:                svc,
		bucketName:         bucketName,
		bucketPrefix:       bucketPrefix,
		onlyUpdatesEnabled: onlyUpdatesEnabled,
		log:                log,
	}
}

func getKey(bucketPrefix string, path string, uuid string) string {
	if bucketPrefix == "" && path != "" {
		return path + "/" + strings.Replace(uuid, "-", "/", -1)
	}

	return bucketPrefix + "/" + strings.Replace(uuid, "-", "/", -1)
}

func (w *S3Writer) Delete(uuid string, path string, tid string) error {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(w.bucketName),                       // Required
		Key:    aws.String(getKey(w.bucketPrefix, path, uuid)), // Required
	}

	if resp, err := w.svc.DeleteObject(params); err != nil {
		w.log.WithError(err).WithTransactionID(tid).WithUUID(uuid).Errorf("Error found, Resp was : %v", resp)
		return err
	}
	return nil
}

func (w *S3Writer) Write(uuid string, path string, b *[]byte, ct string, tid string, ignoreHash bool) (Status, error) {
	params := &s3.PutObjectInput{
		Bucket: aws.String(w.bucketName),
		Key:    aws.String(getKey(w.bucketPrefix, path, uuid)),
		Body:   bytes.NewReader(*b),
	}

	if ct != "" {
		params.ContentType = aws.String(ct)
	}

	if params.Metadata == nil {
		params.Metadata = make(map[string]*string)
	}
	params.Metadata[transactionid.TransactionIDKey] = &tid

	status, newHash, err := w.compareObjectToStore(uuid, path, b, tid)
	if err != nil {
		return status, err
	} else if w.onlyUpdatesEnabled && !ignoreHash && status == UNCHANGED {
		w.log.WithTransactionID(tid).WithUUID(uuid).Debug("Concept has not been updated since last upload, record was skipped")
		return status, nil
	}

	if status == UNCHANGED {
		status = UPDATED
	}

	hashAsString := strconv.FormatUint(newHash, 10)
	params.Metadata["Current-Object-Hash"] = &hashAsString

	resp, err := w.svc.PutObject(params)
	if err != nil {
		w.log.WithError(err).WithTransactionID(tid).WithUUID(uuid).Errorf("Error writing payload to s3, response was %v", resp)
		return SERVICE_UNAVAILABLE, err
	}
	return status, nil
}

func (w *S3Writer) compareObjectToStore(uuid string, path string, b *[]byte, tid string) (Status, uint64, error) {
	objectHash, err := hashstructure.Hash(&b, nil)
	if err != nil {
		w.log.WithError(err).WithTransactionID(tid).WithUUID(uuid).Errorf("Error whilst hashing payload: %v", &b)
		return INTERNAL_ERROR, 0, err
	}

	hoi := &s3.HeadObjectInput{
		Bucket: aws.String(w.bucketName),
		Key:    aws.String(getKey(w.bucketPrefix, path, uuid)),
	}
	hoo, err := w.svc.HeadObject(hoi)
	if err != nil {
		e, ok := err.(awserr.Error)
		if ok && e.Code() == "NotFound" {
			return CREATED, objectHash, nil
		}
		w.log.WithError(err).WithTransactionID(tid).WithUUID(uuid).Error("Error retrieving object metadata")
		return SERVICE_UNAVAILABLE, 0, err
	}

	metadataMap := hoo.Metadata
	var currentHashString string

	if hash, ok := metadataMap["Current-Object-Hash"]; ok {
		currentHashString = *hash
	} else {
		currentHashString = "0"
	}

	currentHash, err := strconv.ParseUint(currentHashString, 10, 64)
	if err != nil {
		w.log.WithError(err).WithTransactionID(tid).WithUUID(uuid).Error("Error whilst parsing current hash")
		return INTERNAL_ERROR, 0, err
	}
	w.log.WithTransactionID(tid).WithUUID(uuid).Debugf("Concept payload has hash of: %v", objectHash)
	w.log.WithTransactionID(tid).WithUUID(uuid).Debugf("Stored concept has hash of: %v", currentHash)
	if objectHash != currentHash {
		w.log.WithTransactionID(tid).WithUUID(uuid).Debug("Concept is different to the stored record")
		return UPDATED, objectHash, nil
	}
	return UNCHANGED, 0, nil
}

type WriterHandler struct {
	writer Writer
	reader Reader
	log    *logger.UPPLogger
}

func NewWriterHandler(writer Writer, reader Reader, log *logger.UPPLogger) WriterHandler {
	return WriterHandler{
		writer: writer,
		reader: reader,
		log:    log,
	}
}

func (w *WriterHandler) HandleWrite(rw http.ResponseWriter, r *http.Request) {
	tid := transactionid.GetTransactionIDFromRequest(r)
	path := r.URL.Query().Get("path")
	uuid := uuid(r.URL.Path)
	rw.Header().Set("Content-Type", "application/json")
	var err error
	bs, err := io.ReadAll(r.Body)
	if err != nil {
		writerStatusInternalServerError(uuid, err, rw, tid, w.log)
		return
	}

	ignoreHash, _ := strconv.ParseBool(r.Header.Get("X-Ignore-Hash"))
	ct := r.Header.Get("Content-Type")
	writeStatus, _ := w.writer.Write(uuid, path, &bs, ct, tid, ignoreHash)

	switch writeStatus {
	case INTERNAL_ERROR:
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte("{\"message\":\"An error occurred whilst processing request\"}"))
		return
	case SERVICE_UNAVAILABLE:
		rw.WriteHeader(http.StatusServiceUnavailable)
		rw.Write([]byte("{\"message\":\"Downstream service responded with error\"}"))
		return
	case UNCHANGED:
		rw.WriteHeader(http.StatusNotModified)
		return
	case UPDATED:
		rw.WriteHeader(http.StatusOK)
		w.log.WithTransactionID(tid).WithUUID(uuid).Info("Concept updated in s3")
		rw.Write([]byte("{\"message\":\"Updated concept record in store\"}"))
		return
	case CREATED:
		rw.WriteHeader(http.StatusCreated)
		w.log.WithTransactionID(tid).WithUUID(uuid).Info("Concept created in s3")
		rw.Write([]byte("{\"message\":\"Created concept record in store\"}"))
		return
	default:
		rw.WriteHeader(http.StatusServiceUnavailable)
		rw.Write([]byte("{\"message\":\"Unhandled error occurred\"}"))
		return
	}
}

func writerStatusInternalServerError(uuid string, err error, rw http.ResponseWriter, tid string, log *logger.UPPLogger) {
	log.WithError(err).WithTransactionID(tid).WithUUID(uuid).Error("Error writing object")
	rw.WriteHeader(http.StatusInternalServerError)
	rw.Write([]byte("{\"message\":\"Unknown internal error\"}"))
}

func (w *WriterHandler) HandleDelete(rw http.ResponseWriter, r *http.Request) {
	tid := transactionid.GetTransactionIDFromRequest(r)
	path := r.URL.Query().Get("path")
	uuid := uuid(r.URL.Path)
	if err := w.writer.Delete(uuid, path, tid); err != nil {
		rw.Header().Set("Content-Type", "application/json")
		writerServiceUnavailable(uuid, err, rw, tid, w.log)
		return
	}

	w.log.WithTransactionID(tid).WithUUID(uuid).Info("Delete successful")
	rw.WriteHeader(http.StatusNoContent)
}

func NewReaderHandler(reader Reader, log *logger.UPPLogger) ReaderHandler {
	return ReaderHandler{reader: reader, log: log}
}

type ReaderHandler struct {
	reader Reader
	log    *logger.UPPLogger
}

func (rh *ReaderHandler) HandleIds(rw http.ResponseWriter, r *http.Request) {
	tid := transactionid.GetTransactionIDFromRequest(r)
	pv, err := rh.reader.Ids()
	defer pv.Close()
	if err != nil {
		readerServiceUnavailable(r.URL.RequestURI(), err, rw, tid, rh.log)
		return
	}

	rw.Header().Set("Content-Type", "application/octet-stream")
	rw.WriteHeader(http.StatusOK)
	io.Copy(rw, pv)
}

func (rh *ReaderHandler) HandleCount(rw http.ResponseWriter, r *http.Request) {
	tid := transactionid.GetTransactionIDFromRequest(r)
	i, err := rh.reader.Count()
	if err != nil {
		readerServiceUnavailable("", err, rw, tid, rh.log)
		return
	}
	rh.log.WithTransactionID(tid).Infof("Got a count back of '%v'", i)
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)

	b := []byte{}
	b = strconv.AppendInt(b, i, 10)
	rw.Write(b)
}

func (rh *ReaderHandler) HandleGetAll(rw http.ResponseWriter, r *http.Request) {
	tid := transactionid.GetTransactionIDFromRequest(r)
	path := r.URL.Query().Get("path")
	pv, err := rh.reader.GetAll(path)

	if err != nil {
		readerServiceUnavailable(r.URL.RequestURI(), err, rw, tid, rh.log)
		return
	}

	rw.Header().Set("Content-Type", "application/octet-stream")
	rw.WriteHeader(http.StatusOK)
	io.Copy(rw, pv)
}

func (rh *ReaderHandler) HandleGet(rw http.ResponseWriter, r *http.Request) {
	tid := transactionid.GetTransactionIDFromRequest(r)
	path := r.URL.Query().Get("path")
	uuid := uuid(r.URL.Path)
	f, i, ct, err := rh.reader.Get(uuid, path)
	if err != nil {
		readerServiceUnavailable(r.URL.RequestURI(), err, rw, tid, rh.log)
		return
	}
	if !f {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusNotFound)
		rw.Write([]byte("{\"message\":\"Item not found\"}"))
		return
	}

	b, err := io.ReadAll(i)
	if err != nil {
		rh.log.WithError(err).WithTransactionID(tid).WithUUID(uuid).Error("Error reading body")
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusBadGateway)
		rw.Write([]byte("{\"message\":\"Error while communicating to other service\"}"))
		return
	}

	if ct != nil || *ct != "" {
		rw.Header().Set("Content-Type", *ct)
	}

	rw.WriteHeader(http.StatusOK)
	rw.Write(b)
}

func uuid(path string) string {
	parts := strings.Split(path, "/")
	return parts[len(parts)-1]
}

func respondServiceUnavailable(err error, rw http.ResponseWriter, tid string, log *logger.UPPLogger) {
	e, ok := err.(awserr.Error)
	if ok {
		errorCode := e.Code()
		log.WithTransactionID(tid).Errorf("Response from S3. %s. More info %s ",
			errorCode, "https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html")
	}
	rw.WriteHeader(http.StatusServiceUnavailable)
	rw.Write([]byte("{\"message\":\"Service currently unavailable\"}"))
}

func writerServiceUnavailable(uuid string, err error, rw http.ResponseWriter, tid string, log *logger.UPPLogger) {
	log.WithError(err).WithTransactionID(tid).WithUUID(uuid).Error("Error writing object")
	respondServiceUnavailable(err, rw, tid, log)
}

func readerServiceUnavailable(requestURI string, err error, rw http.ResponseWriter, tid string, log *logger.UPPLogger) {
	log.WithError(err).WithTransactionID(tid).WithField("requestURI", requestURI).Error("Error from reader")
	rw.Header().Set("Content-Type", "application/json")
	respondServiceUnavailable(err, rw, tid, log)
}
