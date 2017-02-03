package service

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"strings"
	"testing"
)

type mockS3Client struct {
	s3iface.S3API
	s3error         error
	putObjectInput  *s3.PutObjectInput
	headBucketInput *s3.HeadBucketInput
	getObjectInput  *s3.GetObjectInput
	getObjectOutput *s3.GetObjectOutput
}

func (m *mockS3Client) PutObject(poi *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	log.Infof("Put params: %v", poi)
	m.putObjectInput = poi
	return nil, m.s3error
}

func (m *mockS3Client) HeadBucket(hbi *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	log.Infof("Head params: %v", hbi)
	m.headBucketInput = hbi
	return nil, m.s3error
}

func (m *mockS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	log.Infof("Get params: %v", input)
	m.getObjectInput = input
	return m.getObjectOutput, m.s3error
}

func TestWritingToS3(t *testing.T) {
	w, s := getWriter()
	p := []byte("PAYLOAD")
	ct := "content/type"
	var err error
	err = w.Write("uuid", &p, ct)
	assert.NoError(t, err)
	assert.NotEmpty(t, s.putObjectInput)
	assert.Equal(t, "test/prefix/uuid", *s.putObjectInput.Key)
	assert.Equal(t, "testBucket", *s.putObjectInput.Bucket)
	assert.Equal(t, "content/type", *s.putObjectInput.ContentType)

	rs := s.putObjectInput.Body
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

func TestGetFromS3(t *testing.T) {
	r, s := getReader()
	s.getObjectOutput = &s3.GetObjectOutput{
		Body: ioutil.NopCloser(strings.NewReader("PAYLOAD")),
	}
	b, i, err := r.Get("uuid")
	assert.NoError(t, err)
	assert.NotEmpty(t, s.getObjectInput)
	assert.Equal(t, "test/prefix/uuid", *s.getObjectInput.Key)
	assert.Equal(t, "testBucket", *s.getObjectInput.Bucket)
	assert.True(t, b)
	p, _ := ioutil.ReadAll(i)
	assert.Equal(t, "PAYLOAD", string(p[:]))
}

func TestGetFromS3WhenNoSuchKey(t *testing.T) {
	r, s := getReader()
	s.s3error = awserr.New("NoSuchKey", "message", errors.New("Some error"))
	s.getObjectOutput = &s3.GetObjectOutput{
		Body: ioutil.NopCloser(strings.NewReader("PAYLOAD")),
	}
	b, i, err := r.Get("uuid")
	assert.NoError(t, err)
	assert.False(t, b)
	assert.Nil(t, i)
}

func TestGetFromS3WithUnknownError(t *testing.T) {
	r, s := getReader()
	s.s3error = awserr.New("I don't know", "message", errors.New("Some error"))
	s.getObjectOutput = &s3.GetObjectOutput{
		Body: ioutil.NopCloser(strings.NewReader("ERROR PAYLOAD")),
	}
	b, i, err := r.Get("uuid")
	assert.Error(t, err)
	assert.Equal(t, s.s3error, err)
	assert.True(t, b)
	p, _ := ioutil.ReadAll(i)
	assert.Equal(t, "ERROR PAYLOAD", string(p[:]))
}

func TestGetFromS3WithNoneAWSError(t *testing.T) {
	r, s := getReader()
	s.s3error = errors.New("Some error")
	s.getObjectOutput = &s3.GetObjectOutput{
		Body: ioutil.NopCloser(strings.NewReader("ERROR PAYLOAD")),
	}
	b, i, err := r.Get("uuid")
	assert.Error(t, err)
	assert.Equal(t, s.s3error, err)
	assert.True(t, b)
	p, _ := ioutil.ReadAll(i)
	assert.Equal(t, "ERROR PAYLOAD", string(p[:]))
}

func getReader() (Reader, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Reader(s, "testBucket", "test/prefix"), s
}

func getWriter() (Writer, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Writer(s, "testBucket", "test/prefix"), s
}
