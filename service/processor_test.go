package service

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

type mockS3Client struct {
	s3iface.S3API
	s3error         error
	putParams       *s3.PutObjectInput
	headBucketInput *s3.HeadBucketInput
}

func (m *mockS3Client) PutObject(poi *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	log.Infof("Put params: %v", poi)
	m.putParams = poi
	return nil, m.s3error
}

func (m *mockS3Client) HeadBucket(hbi *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	log.Infof("Head params: %v", hbi)
	m.headBucketInput = hbi
	return nil, m.s3error
}

func TestWritingToS3(t *testing.T) {
	w, s := getWriter()
	p := []byte("PAYLOAD")
	ct := "content/type"
	var err error
	err = w.Write("uuid", &p, ct)
	assert.NoError(t, err)
	assert.NotEmpty(t, s.putParams)
	assert.Equal(t, "test/prefix/uuid", *s.putParams.Key)
	assert.Equal(t, "testBucket", *s.putParams.Bucket)
	assert.Equal(t, "content/type", *s.putParams.ContentType)

	rs := s.putParams.Body
	assert.NotNil(t, rs)
	ba, err := ioutil.ReadAll(rs)
	assert.NoError(t, err)
	body := string(ba[:])
	assert.Equal(t, "PAYLOAD", body)
}

func TestFailingToWriteToS3(t *testing.T) {
	w, s := getWriter()
	p := []byte("PAYLOAD")
	ct := "content/type"
	s.s3error = errors.New("S3 error")
	err := w.Write("uuid", &p, ct)
	assert.Error(t, err)
}

func getWriter() (Writer, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Writer(s, "testBucket", "test/prefix"), s
}
