package service

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
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
	s3error              error
	putObjectInput       *s3.PutObjectInput
	headBucketInput      *s3.HeadBucketInput
	getObjectInput       *s3.GetObjectInput
	getObjectOutput      *s3.GetObjectOutput
	deleteObjectInput    *s3.DeleteObjectInput
	deleteObjectOutput   *s3.DeleteObjectOutput
	listObjectsV2Outputs []*s3.ListObjectsV2Output
	listObjectsV2Input   []*s3.ListObjectsV2Input
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

func (m *mockS3Client) DeleteObject(doi *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	log.Infof("Delete params: %v", doi)
	m.deleteObjectInput = doi
	return m.deleteObjectOutput, m.s3error
}

func (m *mockS3Client) GetObject(goi *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	log.Infof("Get params: %v", goi)
	m.getObjectInput = goi
	return m.getObjectOutput, m.s3error
}

func (m *mockS3Client) ListObjectsV2(loi *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	log.Infof("Get ListObjectsV2: %v", loi)
	m.listObjectsV2Input = append(m.listObjectsV2Input, loi)
	return nil, m.s3error
}
func (m *mockS3Client) ListObjectsV2Pages(loi *s3.ListObjectsV2Input, fn func(p *s3.ListObjectsV2Output, lastPage bool) (shouldContinue bool)) error {
	log.Infof("Get ListObjectsV2Pages: %v", loi)
	m.listObjectsV2Input = append(m.listObjectsV2Input, loi)

	for i := 0; i < len(m.listObjectsV2Outputs); i++ {
		fn(m.listObjectsV2Outputs[i], i != (len(m.listObjectsV2Outputs)-1))
	}

	return m.s3error
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

func TestGetCountFromS3(t *testing.T) {
	r, s := getReader()
	s.listObjectsV2Outputs = []*s3.ListObjectsV2Output{
		{KeyCount: aws.Int64(100)},
		{KeyCount: aws.Int64(1)},
	}
	i, err := r.Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(101), i)
}

func TestGetCountFromS3WithoutPrefix(t *testing.T) {
	r, s := getReaderNoPrefix()

	s.listObjectsV2Outputs = []*s3.ListObjectsV2Output{
		{KeyCount: aws.Int64(100)},
		{KeyCount: aws.Int64(1)},
	}
	i, err := r.Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(101), i)
	assert.NotEmpty(t, s.listObjectsV2Input)
	assert.Nil(t, s.listObjectsV2Input[0].Prefix)
}

func TestGetCountFromS3Errors(t *testing.T) {
	r, s := getReader()
	s.s3error = errors.New("Some error")
	_, err := r.Count()
	assert.Error(t, err)
	assert.Equal(t, s.s3error, err)
}

func TestGetIdsFromS3(t *testing.T) {
	r, s := getReader()
	s.listObjectsV2Outputs = []*s3.ListObjectsV2Output{
		{KeyCount: aws.Int64(1)},
		{
			KeyCount: aws.Int64(5),
			Contents: []*s3.Object{
				{Key: aws.String("UUID-1")},
				{Key: aws.String("UUID-2")},
				{Key: aws.String("UUID-3")},
				{Key: aws.String("UUID-4")},
				{Key: aws.String("UUID-5")},
			},
		},
		{
			KeyCount: aws.Int64(5),
			Contents: []*s3.Object{
				{Key: aws.String("UUID-6")},
				{Key: aws.String("UUID-7")},
				{Key: aws.String("UUID-8")},
				{Key: aws.String("UUID-9")},
				{Key: aws.String("UUID-10")},
			},
		},
	}
	p, err := r.Ids()
	assert.NoError(t, err)
	payload, err := ioutil.ReadAll(&p)
	assert.NoError(t, err)
	assert.Equal(t, string(payload[:]), `{"ID":"UUID-1"}
{"ID":"UUID-2"}
{"ID":"UUID-3"}
{"ID":"UUID-4"}
{"ID":"UUID-5"}
{"ID":"UUID-6"}
{"ID":"UUID-7"}
{"ID":"UUID-8"}
{"ID":"UUID-9"}
{"ID":"UUID-10"}
`)
}

func TestGetIdsFromS3Fails(t *testing.T) {
	r, s := getReader()
	s.s3error = errors.New("Some error")
	_, err := r.Ids()
	assert.Error(t, err)
	assert.Equal(t, s.s3error, err)
}

func TestDelete(t *testing.T) {
	var w Writer
	var s *mockS3Client

	t.Run("With prefix", func(t *testing.T) {
		w, s = getWriter()
		err := w.Delete("UUID")
		assert.NoError(t, err)
		assert.Equal(t, "test/prefix/UUID", *s.deleteObjectInput.Key)
		assert.Equal(t, "testBucket", *s.deleteObjectInput.Bucket)
	})

	t.Run("Without prefix", func(t *testing.T) {
		w, s = getWriterNoPrefix()
		err := w.Delete("UUID")
		assert.NoError(t, err)
		assert.Equal(t, "/UUID", *s.deleteObjectInput.Key)
		assert.Equal(t, "testBucket", *s.deleteObjectInput.Bucket)
	})

	t.Run("Fails", func(t *testing.T) {
		w, s = getWriter()
		s.s3error = errors.New("Some S3 error")
		err := w.Delete("UUID")
		assert.Error(t, err)
		assert.Equal(t, s.s3error, err)
	})
}

func getReader() (Reader, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Reader(s, "testBucket", "test/prefix"), s
}

func getReaderNoPrefix() (Reader, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Reader(s, "testBucket", ""), s
}

func getWriter() (Writer, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Writer(s, "testBucket", "test/prefix"), s
}

func getWriterNoPrefix() (Writer, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Writer(s, "testBucket", ""), s
}
