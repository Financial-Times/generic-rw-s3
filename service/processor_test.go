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
	"strconv"
	"strings"
	"sync"
	"testing"
)

const (
	expectedUUID = "123e4567-e89b-12d3-a456-426655440000"
)

type mockS3Client struct {
	s3iface.S3API
	sync.Mutex
	s3error              error
	putObjectInput       *s3.PutObjectInput
	headBucketInput      *s3.HeadBucketInput
	headObjectInput      *s3.HeadObjectInput
	getObjectInput       *s3.GetObjectInput
	deleteObjectInput    *s3.DeleteObjectInput
	deleteObjectOutput   *s3.DeleteObjectOutput
	listObjectsV2Outputs []*s3.ListObjectsV2Output
	listObjectsV2Input   []*s3.ListObjectsV2Input
	count                int
	getObjectCount       int
	payload              string
	ct                   string
}

func (m *mockS3Client) PutObject(poi *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	m.Lock()
	defer m.Unlock()
	log.Infof("Put params: %v", poi)
	m.putObjectInput = poi
	return nil, m.s3error
}

func (m *mockS3Client) HeadBucket(hbi *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	m.Lock()
	defer m.Unlock()
	log.Infof("Head params: %v", hbi)
	m.headBucketInput = hbi
	return nil, m.s3error
}
func (m *mockS3Client) HeadObject(hoi *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	m.Lock()
	defer m.Unlock()
	log.Infof("Head params: %v", hoi)
	m.headObjectInput = hoi
	return nil, m.s3error

}
func (m *mockS3Client) DeleteObject(doi *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	m.Lock()
	defer m.Unlock()
	log.Infof("Delete params: %v", doi)
	m.deleteObjectInput = doi
	return m.deleteObjectOutput, m.s3error
}

func (m *mockS3Client) GetObject(goi *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	m.Lock()
	defer m.Unlock()
	log.Infof("Get params: %v", goi)
	m.getObjectInput = goi
	payload := m.payload + strconv.Itoa(m.getObjectCount)
	m.getObjectCount++
	return &s3.GetObjectOutput{
		Body:        ioutil.NopCloser(strings.NewReader(payload)),
		ContentType: aws.String(m.ct),
	}, m.s3error
}

func (m *mockS3Client) ListObjectsV2(loi *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	m.Lock()
	defer m.Unlock()
	log.Infof("Get ListObjectsV2: %v", loi)
	m.listObjectsV2Input = append(m.listObjectsV2Input, loi)
	var loo *s3.ListObjectsV2Output
	if len(m.listObjectsV2Outputs) > 0 {
		loo = m.listObjectsV2Outputs[m.count]
	}
	m.count++
	return loo, m.s3error
}
func (m *mockS3Client) ListObjectsV2Pages(loi *s3.ListObjectsV2Input, fn func(p *s3.ListObjectsV2Output, lastPage bool) (shouldContinue bool)) error {
	m.Lock()
	defer m.Unlock()
	log.Infof("Get ListObjectsV2Pages: %v", loi)
	m.listObjectsV2Input = append(m.listObjectsV2Input, loi)

	for i := m.count; i < len(m.listObjectsV2Outputs); i++ {
		lastPage := i == (len(m.listObjectsV2Outputs) - 1)
		fn(m.listObjectsV2Outputs[i], lastPage)
	}

	return m.s3error
}

func TestWritingToS3(t *testing.T) {
	w, s := getWriter()
	p := []byte("PAYLOAD")
	ct := "content/type"
	var err error
	err = w.Write(expectedUUID, &p, ct)
	assert.NoError(t, err)
	assert.NotEmpty(t, s.putObjectInput)
	assert.Equal(t, "test/prefix/123e4567/e89b/12d3/a456/426655440000", *s.putObjectInput.Key)
	assert.Equal(t, "testBucket", *s.putObjectInput.Bucket)
	assert.Equal(t, "content/type", *s.putObjectInput.ContentType)

	rs := s.putObjectInput.Body
	assert.NotNil(t, rs)
	ba, err := ioutil.ReadAll(rs)
	assert.NoError(t, err)
	body := string(ba[:])
	assert.Equal(t, "PAYLOAD", body)
}

func TestWritingToS3WithNoContentType(t *testing.T) {
	w, s := getWriter()
	p := []byte("PAYLOAD")
	var err error
	err = w.Write(expectedUUID, &p, "")
	assert.NoError(t, err)
	assert.NotEmpty(t, s.putObjectInput)
	assert.Equal(t, "test/prefix/123e4567/e89b/12d3/a456/426655440000", *s.putObjectInput.Key)
	assert.Equal(t, "testBucket", *s.putObjectInput.Bucket)
	assert.Nil(t, s.putObjectInput.ContentType)

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
	err := w.Write(expectedUUID, &p, ct)
	assert.Error(t, err)
}

func TestGetFromS3(t *testing.T) {
	r, s := getReader()
	s.payload = "PAYLOAD"
	s.ct = "content/type"
	b, i, ct, err := r.Get(expectedUUID)
	assert.NoError(t, err)
	assert.NotEmpty(t, s.getObjectInput)
	assert.Equal(t, "test/prefix/123e4567/e89b/12d3/a456/426655440000", *s.getObjectInput.Key)
	assert.Equal(t, "testBucket", *s.getObjectInput.Bucket)
	assert.Equal(t, "content/type", *ct)
	assert.True(t, b)
	p, _ := ioutil.ReadAll(i)
	assert.Equal(t, "PAYLOAD0", string(p[:]))
}
func TestGetFromS3NoPrefix(t *testing.T) {
	r, s := getReaderNoPrefix()
	s.payload = "PAYLOAD"
	s.ct = "content/type"
	b, i, ct, err := r.Get(expectedUUID)
	assert.NoError(t, err)
	assert.NotEmpty(t, s.getObjectInput)
	assert.Equal(t, "/123e4567/e89b/12d3/a456/426655440000", *s.getObjectInput.Key)
	assert.Equal(t, "testBucket", *s.getObjectInput.Bucket)
	assert.Equal(t, "content/type", *ct)
	assert.True(t, b)
	p, _ := ioutil.ReadAll(i)
	assert.Equal(t, "PAYLOAD0", string(p[:]))
}

func TestGetFromS3WhenNoSuchKey(t *testing.T) {
	r, s := getReader()
	s.s3error = awserr.New("NoSuchKey", "message", errors.New("Some error"))
	s.payload = "PAYLOAD"
	b, i, ct, err := r.Get(expectedUUID)
	assert.NoError(t, err)
	assert.False(t, b)
	assert.Nil(t, i)
	assert.Nil(t, ct)
}

func TestGetFromS3WithUnknownError(t *testing.T) {
	r, s := getReader()
	s.s3error = awserr.New("I don't know", "message", errors.New("Some error"))
	s.payload = "ERROR PAYLOAD"
	b, i, ct, err := r.Get(expectedUUID)
	assert.Error(t, err)
	assert.Equal(t, s.s3error, err)
	assert.False(t, b)
	assert.Nil(t, i)
	assert.Nil(t, ct)
}

func TestGetFromS3WithNoneAWSError(t *testing.T) {
	r, s := getReader()
	s.s3error = errors.New("Some error")
	s.payload = "ERROR PAYLOAD"
	b, i, ct, err := r.Get(expectedUUID)
	assert.Error(t, err)
	assert.Equal(t, s.s3error, err)
	assert.False(t, b)
	assert.Nil(t, i)
	assert.Nil(t, ct)
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
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440001")},
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440002")},
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440003")},
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440004")},
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440005")},
			},
		},
		{
			KeyCount: aws.Int64(5),
			Contents: []*s3.Object{
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440006")},
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440007")},
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440008")},
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440009")},
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440010")},
			},
		},
	}
	p, err := r.Ids()
	assert.NoError(t, err)
	payload, err := ioutil.ReadAll(&p)
	assert.NoError(t, err)
	assert.Equal(t, `{"ID":"123e4567-e89b-12d3-a456-426655440001"}
{"ID":"123e4567-e89b-12d3-a456-426655440002"}
{"ID":"123e4567-e89b-12d3-a456-426655440003"}
{"ID":"123e4567-e89b-12d3-a456-426655440004"}
{"ID":"123e4567-e89b-12d3-a456-426655440005"}
{"ID":"123e4567-e89b-12d3-a456-426655440006"}
{"ID":"123e4567-e89b-12d3-a456-426655440007"}
{"ID":"123e4567-e89b-12d3-a456-426655440008"}
{"ID":"123e4567-e89b-12d3-a456-426655440009"}
{"ID":"123e4567-e89b-12d3-a456-426655440010"}
`, string(payload[:]))
}

func TestGetIdsFromS3NoPrefix(t *testing.T) {
	r, s := getReaderNoPrefix()
	s.listObjectsV2Outputs = []*s3.ListObjectsV2Output{
		{KeyCount: aws.Int64(1)},
		{
			KeyCount: aws.Int64(5),
			Contents: []*s3.Object{
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440001")},
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440002")},
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440003")},
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440004")},
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440005")},
			},
		},
		{
			KeyCount: aws.Int64(5),
			Contents: []*s3.Object{
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440006")},
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440007")},
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440008")},
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440009")},
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440010")},
			},
		},
	}
	p, err := r.Ids()
	assert.NoError(t, err)
	payload, err := ioutil.ReadAll(&p)
	assert.NoError(t, err)
	assert.Equal(t, `{"ID":"123e4567-e89b-12d3-a456-426655440001"}
{"ID":"123e4567-e89b-12d3-a456-426655440002"}
{"ID":"123e4567-e89b-12d3-a456-426655440003"}
{"ID":"123e4567-e89b-12d3-a456-426655440004"}
{"ID":"123e4567-e89b-12d3-a456-426655440005"}
{"ID":"123e4567-e89b-12d3-a456-426655440006"}
{"ID":"123e4567-e89b-12d3-a456-426655440007"}
{"ID":"123e4567-e89b-12d3-a456-426655440008"}
{"ID":"123e4567-e89b-12d3-a456-426655440009"}
{"ID":"123e4567-e89b-12d3-a456-426655440010"}
`, string(payload[:]))
}

func TestGetIdsFromS3Fails(t *testing.T) {
	r, s := getReader()
	s.s3error = errors.New("Some error")
	_, err := r.Ids()
	assert.Error(t, err)
	assert.Equal(t, s.s3error, err)
}

func TestReaderHandler_HandleGetAllOK(t *testing.T) {
	r, s := getReader()
	s.payload = "PAYLOAD"

	s.listObjectsV2Outputs = []*s3.ListObjectsV2Output{
		{KeyCount: aws.Int64(1)},
		{
			KeyCount: aws.Int64(5),
			Contents: []*s3.Object{
				{Key: aws.String("test/prefix/UUID-1")},
				{Key: aws.String("test/prefix/UUID-2")},
				{Key: aws.String("test/prefix/UUID-3")},
				{Key: aws.String("test/prefix/UUID-4")},
				{Key: aws.String("test/prefix/UUID-5")},
			},
		},
		{
			KeyCount: aws.Int64(5),
			Contents: []*s3.Object{
				{Key: aws.String("test/prefix/UUID-6")},
				{Key: aws.String("test/prefix/UUID-7")},
				{Key: aws.String("test/prefix/UUID-8")},
				{Key: aws.String("test/prefix/UUID-9")},
				{Key: aws.String("test/prefix/UUID-10")},
			},
		},
	}
	p, err := r.GetAll()
	assert.NoError(t, err)
	payload, err := ioutil.ReadAll(&p)
	assert.NoError(t, err)
	assert.Equal(t, `PAYLOAD0
PAYLOAD1
PAYLOAD2
PAYLOAD3
PAYLOAD4
PAYLOAD5
PAYLOAD6
PAYLOAD7
PAYLOAD8
PAYLOAD9
`, string(payload[:]))

}

func TestReaderHandler_HandleGetAllOKWithLotsOfWorkers(t *testing.T) {
	r, s := getReaderwithMultipleWorkers()
	s.payload = "PAYLOAD"
	s.listObjectsV2Outputs = []*s3.ListObjectsV2Output{
		{KeyCount: aws.Int64(1)},
		getListObjectsV2Output(5, 0),
		getListObjectsV2Output(5, 5),
		getListObjectsV2Output(5, 15),
		getListObjectsV2Output(5, 20),
		getListObjectsV2Output(5, 25),
	}
	p, err := r.GetAll()
	assert.NoError(t, err)
	payload, err := ioutil.ReadAll(&p)
	assert.NoError(t, err)
	assert.Equal(t, 25, strings.Count(string(payload[:]), "PAYLOAD"))
}

func TestS3Reader_Head(t *testing.T) {
	r, s := getReader()
	t.Run("Found", func(t *testing.T) {
		found, err := r.Head(expectedUUID)
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "test/prefix/123e4567/e89b/12d3/a456/426655440000", *s.headObjectInput.Key)
		assert.Equal(t, "testBucket", *s.headObjectInput.Bucket)

	})

	t.Run("NotFound", func(t *testing.T) {
		s.s3error = awserr.New("NotFound", "message", errors.New("Some error"))
		found, err := r.Head(expectedUUID)
		assert.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("Random Error", func(t *testing.T) {
		s.s3error = errors.New("Random error")
		found, err := r.Head(expectedUUID)
		assert.Error(t, err)
		assert.False(t, found)
		assert.Equal(t, s.s3error, err)
	})
}

func getListObjectsV2Output(keyCount int64, start int) *s3.ListObjectsV2Output {
	contents := []*s3.Object{}
	for i := start; i < start+int(keyCount); i++ {
		contents = append(contents, &s3.Object{Key: aws.String("test/prefix/UUID-" + strconv.Itoa(i))})
	}
	return &s3.ListObjectsV2Output{
		KeyCount: aws.Int64(keyCount),
		Contents: contents,
	}
}

func TestS3Reader_GetAllFails(t *testing.T) {
	r, s := getReader()
	s.s3error = errors.New("Some error")
	_, err := r.GetAll()
	assert.Error(t, err)
	assert.Equal(t, s.s3error, err)
}

func TestDelete(t *testing.T) {
	var w Writer
	var s *mockS3Client

	t.Run("With prefix", func(t *testing.T) {
		w, s = getWriter()
		err := w.Delete(expectedUUID)
		assert.NoError(t, err)
		assert.Equal(t, "test/prefix/123e4567/e89b/12d3/a456/426655440000", *s.deleteObjectInput.Key)
		assert.Equal(t, "testBucket", *s.deleteObjectInput.Bucket)
	})

	t.Run("Without prefix", func(t *testing.T) {
		w, s = getWriterNoPrefix()
		err := w.Delete(expectedUUID)
		assert.NoError(t, err)
		assert.Equal(t, "/123e4567/e89b/12d3/a456/426655440000", *s.deleteObjectInput.Key)
		assert.Equal(t, "testBucket", *s.deleteObjectInput.Bucket)
	})

	t.Run("Fails", func(t *testing.T) {
		w, s = getWriter()
		s.s3error = errors.New("Some S3 error")
		err := w.Delete(expectedUUID)
		assert.Error(t, err)
		assert.Equal(t, s.s3error, err)
	})
}

func getReader() (Reader, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Reader(s, "testBucket", "test/prefix", 1), s
}

func getReaderwithMultipleWorkers() (Reader, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Reader(s, "testBucket", "test/prefix", 15), s
}

func getReaderNoPrefix() (Reader, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Reader(s, "testBucket", "", 1), s
}

func getWriter() (Writer, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Writer(s, "testBucket", "test/prefix"), s
}

func getWriterNoPrefix() (Writer, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Writer(s, "testBucket", ""), s
}
