package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Config names the bucket of an S3-compatible object store (AWS S3, R2 or MinIO).
type S3Config struct {
	Endpoint string `json:"endpoint"`
	Bucket   string `json:"bucket"`
	Region   string `json:"region"`
	Secure   bool   `json:"secure"`
}

// s3Store keeps objects in one bucket at their key.
type s3Store struct {
	client *minio.Client
	bucket string
	dated  *datedTransport
}

// datedTransport keeps the store's clock's offset from this one, by the Date of its last response.
type datedTransport struct {
	http.RoundTripper
	offset atomic.Int64
}

func (t *datedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.RoundTripper.RoundTrip(r)
	if err == nil {
		if date, err := http.ParseTime(resp.Header.Get("Date")); err == nil {
			t.offset.Store(int64(time.Until(date)))
		}
	}
	return resp, err
}

// Now is the store's clock, to the second its Date headers keep, or this one's before it answers.
func (s *s3Store) Now() time.Time { return time.Now().Add(time.Duration(s.dated.offset.Load())) }

// NewS3 returns a Store over cfg's existing bucket, with credentials from AWS_* or MINIO_*
// environment keys, then IAM or web identity.
func NewS3(cfg S3Config) (Store, error) {
	if cfg.Endpoint == "" || cfg.Bucket == "" {
		return nil, errors.New("storage: s3 endpoint and bucket are required")
	}
	creds := credentials.NewChainCredentials([]credentials.Provider{
		&credentials.EnvAWS{},
		&credentials.EnvMinio{},
		&credentials.IAM{Client: &http.Client{Transport: http.DefaultTransport}},
	})
	transport, err := minio.DefaultTransport(cfg.Secure)
	if err != nil {
		return nil, fmt.Errorf("storage: s3 transport: %w", err)
	}
	dated := &datedTransport{RoundTripper: transport}
	client, err := minio.New(cfg.Endpoint, &minio.Options{Creds: creds, Secure: cfg.Secure, Region: cfg.Region, Transport: dated})
	if err != nil {
		return nil, fmt.Errorf("storage: s3 client: %w", err)
	}
	return &s3Store{client: client, bucket: cfg.Bucket, dated: dated}, nil
}

func (s *s3Store) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	var opts minio.GetObjectOptions
	if err := opts.SetRange(offset, offset+length-1); err != nil {
		return nil, fmt.Errorf("storage: read %s at %d+%d: %w", key, offset, length, err)
	}
	return s.get(ctx, key, opts)
}

func (s *s3Store) GetSuffix(ctx context.Context, key string, n int64) ([]byte, error) {
	var opts minio.GetObjectOptions
	if err := opts.SetRange(0, -n); err != nil {
		return nil, fmt.Errorf("storage: read last %d bytes of %s: %w", n, key, err)
	}
	return s.get(ctx, key, opts)
}

func (s *s3Store) get(ctx context.Context, key string, opts minio.GetObjectOptions) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, opts)
	if err != nil {
		return nil, fmt.Errorf("storage: get %s: %w", key, err)
	}
	body, err := io.ReadAll(obj)
	if err = errors.Join(err, obj.Close()); err != nil {
		return nil, fmt.Errorf("storage: get %s: %w", key, err)
	}
	return body, nil
}

func (s *s3Store) Put(ctx context.Context, key string, body []byte) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("storage: put %s: %w", key, err)
	}
	return nil
}

// PutIfAbsent is a PUT with If-None-Match: *, which the store refuses with 412 when an
// object is already stored at the key.
func (s *s3Store) PutIfAbsent(ctx context.Context, key string, body []byte) error {
	var opts minio.PutObjectOptions
	opts.SetMatchETagExcept("*")
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(body), int64(len(body)), opts)
	var resp minio.ErrorResponse
	if errors.As(err, &resp) && resp.StatusCode == http.StatusPreconditionFailed {
		return fmt.Errorf("%s: %w", key, ErrExists)
	}
	if err != nil {
		return fmt.Errorf("storage: put %s: %w", key, err)
	}
	return nil
}

// List relies on S3 listing keys in ascending order.
func (s *s3Store) List(ctx context.Context, prefix, after string) ([]string, error) {
	var keys []string
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, StartAfter: after, Recursive: true}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("storage: list %s: %w", prefix, obj.Err)
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}
