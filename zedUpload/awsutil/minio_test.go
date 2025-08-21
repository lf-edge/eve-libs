//go:build integration
// +build integration

// Copyright(c) 2025 Zededa,
// All rights reserved.

package awsutil

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/joho/godotenv/autoload"
	"github.com/lf-edge/eve-libs/zedUpload/types"
)

// --- helpers ---

func getenvOrSkip(t *testing.T, key string) string {
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("Skipping test: %s not set", key)
	}
	return v
}

// setupMinio creates a context pointing to a MinIO S3-compatible endpoint
// using the same awsutil.S3ctx but with BaseEndpoint override.
func setupMinio(t *testing.T) (*S3ctx, string, string, bool) {
	t.Helper()

	endpoint := getenvOrSkip(t, "MINIO_ENDPOINT")
	accessKey := getenvOrSkip(t, "MINIO_ACCESS_KEY_ID")
	secretKey := getenvOrSkip(t, "MINIO_SECRET_ACCESS_KEY")
	region := getenvOrSkip(t, "MINIO_REGION")
	bucket := getenvOrSkip(t, "MINIO_TEST_BUCKET")
	_, useIPv6 := os.LookupEnv("MINIO_USE_IPV6")

	// NOTE: NewAwsCtx signature here is assumed to be:
	// NewAwsCtx(id, secret, region, endpointOverride string, useIPv6 bool, hctx *http.Client)
	// If your signature differs, adjust this call accordingly.
	s, err := NewAwsCtx(accessKey, secretKey, region, useIPv6, endpoint, nil)
	if err != nil {
		t.Fatalf("NewAwsCtx (MinIO) failed: %v", err)
	}

	// Ensure bucket exists (ignore if already present).
	_ = s.CreateBucket(bucket)
	s.WaitUntilBucketExists(bucket)

	return s, bucket, region, useIPv6
}

// --- tests ---

func TestMinioUploadFile(t *testing.T) {
	s, bucket, _, _ := setupMinio(t)

	key := "test-object"
	t.Cleanup(func() {
		_ = s.DeleteObject(bucket, key)
		time.Sleep(500 * time.Millisecond)
		_ = s.DeleteBucket(bucket)
	})

	// create a small payload
	tmpf, err := os.CreateTemp("", "minio-upload-*.txt")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmpf.Name())
	payload := []byte("minio integration payload")
	if _, err := tmpf.Write(payload); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	tmpf.Close()

	_, err = s.UploadFile(tmpf.Name(), bucket, key, false, nil)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	// Verify object exists
	if _, err := s.client.HeadObject(s.ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}); err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
}

func TestMinioDownloadFile(t *testing.T) {
	s, bucket, _, _ := setupMinio(t)

	key := "dl-object"
	t.Cleanup(func() {
		_ = s.DeleteObject(bucket, key)
		time.Sleep(500 * time.Millisecond)
		_ = s.DeleteBucket(bucket)
	})

	// Upload original
	orig := []byte("minio download test payload")
	src := filepath.Join(t.TempDir(), "orig.txt")
	if err := os.WriteFile(src, orig, 0644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if _, err := s.UploadFile(src, bucket, key, false, nil); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}

	// Download to dest
	dest := filepath.Join(t.TempDir(), "downloaded.txt")
	_, err := s.DownloadFile(dest, bucket, key, 0, types.DownloadedParts{PartSize: S3PartSize}, nil)
	if err != nil {
		t.Fatalf("DownloadFile: %v", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(orig, got) {
		t.Errorf("content mismatch; want %q, got %q", orig, got)
	}
}

func TestMinioListImages(t *testing.T) {
	s, bucket, _, _ := setupMinio(t)

	keys := []string{"imgA", "imgB"}
	t.Cleanup(func() {
		for _, k := range keys {
			_ = s.DeleteObject(bucket, k)
		}
		time.Sleep(500 * time.Millisecond)
		_ = s.DeleteBucket(bucket)
	})

	// Upload
	for _, k := range keys {
		src := filepath.Join(t.TempDir(), k+".txt")
		if err := os.WriteFile(src, []byte(k), 0644); err != nil {
			t.Fatalf("write %s: %v", k, err)
		}
		if _, err := s.UploadFile(src, bucket, k, false, nil); err != nil {
			t.Fatalf("UploadFile %s: %v", k, err)
		}
	}

	listed, err := s.ListImages(bucket, nil)
	if err != nil {
		t.Fatalf("ListImages: %v", err)
	}
	seen := map[string]bool{}
	for _, k := range listed {
		seen[k] = true
	}
	for _, k := range keys {
		if !seen[k] {
			t.Errorf("missing key %q in ListImages result: %v", k, listed)
		}
	}
}

func TestMinioGetObjectMetaData(t *testing.T) {
	s, bucket, _, _ := setupMinio(t)

	key := "meta-object"
	t.Cleanup(func() {
		_ = s.DeleteObject(bucket, key)
		time.Sleep(500 * time.Millisecond)
		_ = s.DeleteBucket(bucket)
	})

	orig := []byte("minio metadata payload")
	src := filepath.Join(t.TempDir(), "meta.txt")
	if err := os.WriteFile(src, orig, 0644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if _, err := s.UploadFile(src, bucket, key, false, nil); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}

	size, md5sum, err := s.GetObjectMetaData(bucket, key)
	if err != nil {
		t.Fatalf("GetObjectMetaData: %v", err)
	}
	if size != int64(len(orig)) {
		t.Errorf("size=%d, want %d", size, len(orig))
	}
	h := md5.Sum(orig)
	want := hex.EncodeToString(h[:])
	if md5sum != want {
		t.Errorf("md5=%q, want %q", md5sum, want)
	}
}

func TestMinioDownloadFileByChunks(t *testing.T) {
	s, bucket, _, _ := setupMinio(t)

	key := "chunk-object"
	t.Cleanup(func() {
		_ = s.DeleteObject(bucket, key)
		time.Sleep(500 * time.Millisecond)
		_ = s.DeleteBucket(bucket)
	})

	orig := []byte("this is a by-chunks minio payload long enough to span multiple reads")
	src := filepath.Join(t.TempDir(), "bychunks.txt")
	if err := os.WriteFile(src, orig, 0644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if _, err := s.UploadFile(src, bucket, key, false, nil); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}

	rc, size, err := s.DownloadFileByChunks(src, bucket, key)
	if err != nil {
		t.Fatalf("DownloadFileByChunks: %v", err)
	}
	defer rc.Close()
	if size != int64(len(orig)) {
		t.Errorf("size=%d, want %d", size, len(orig))
	}

	var buf bytes.Buffer
	chunk := make([]byte, 16)
	for {
		n, err := rc.Read(chunk)
		if n > 0 {
			buf.Write(chunk[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read chunk: %v", err)
		}
	}
	if !bytes.Equal(buf.Bytes(), orig) {
		t.Errorf("content mismatch;\n got: %q\nwant: %q", buf.Bytes(), orig)
	}
}

func TestMinioUploadPartCreateUploadID(t *testing.T) {
	s, bucket, _, _ := setupMinio(t)

	// Ensure bucket exists for multipart tests
	if err := s.CreateBucket(bucket); err != nil {
		// MinIO may already have it; ignore existing errors if needed
	}

	key := "mpu-create-id"
	chunk := []byte("first-part")
	t.Cleanup(func() {
		_ = s.DeleteObject(bucket, key)
		time.Sleep(500 * time.Millisecond)
		_ = s.DeleteBucket(bucket)
	})

	etag, uploadID, err := s.UploadPart(bucket, key, chunk, 1, "")
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	if uploadID == "" {
		t.Error("expected non-empty uploadID")
	}
	if etag == "" {
		t.Error("expected non-empty ETag")
	}
}

func TestMinioUploadPartMultipartComplete(t *testing.T) {
	s, bucket, _, _ := setupMinio(t)

	// Ensure bucket exists
	if err := s.CreateBucket(bucket); err != nil {
		// ignore if exists
	}
	key := "mpu-complete"
	t.Cleanup(func() {
		_ = s.DeleteObject(bucket, key)
		time.Sleep(500 * time.Millisecond)
		_ = s.DeleteBucket(bucket)
	})

	const minPartSize = 5 * 1024 * 1024
	part1 := make([]byte, minPartSize)
	for i := range part1 {
		part1[i] = 'A'
	}
	part2 := []byte("LAST")

	etag1, uploadID, err := s.UploadPart(bucket, key, part1, 1, "")
	if err != nil {
		t.Fatalf("UploadPart #1: %v", err)
	}
	etag2, uploadID2, err := s.UploadPart(bucket, key, part2, 2, uploadID)
	if err != nil {
		t.Fatalf("UploadPart #2: %v", err)
	}
	if uploadID != uploadID2 {
		t.Fatalf("uploadID mismatch: %q vs %q", uploadID, uploadID2)
	}

	if err := s.CompleteUploadedParts(bucket, key, uploadID, []string{etag1, etag2}); err != nil {
		t.Fatalf("CompleteUploadedParts: %v", err)
	}

	// Download and verify assembled object
	resp, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		t.Fatalf("read object: %v", err)
	}
	want := append(part1, part2...)
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("assembled content mismatch")
	}
}

func TestMinioGetSignedURL(t *testing.T) {
	s, bucket, _, _ := setupMinio(t)

	key := "signed-object"
	t.Cleanup(func() {
		_ = s.DeleteObject(bucket, key)
		time.Sleep(500 * time.Millisecond)
		_ = s.DeleteBucket(bucket)
	})

	orig := []byte("presigned url minio test")
	src := filepath.Join(t.TempDir(), "signed.txt")
	if err := os.WriteFile(src, orig, 0644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if _, err := s.UploadFile(src, bucket, key, false, nil); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}

	urlStr, err := s.GetSignedURL(bucket, key, 5*time.Minute)
	if err != nil {
		t.Fatalf("GetSignedURL: %v", err)
	}

	resp, err := http.Get(urlStr)
	if err != nil {
		t.Fatalf("GET presigned: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(body, orig) {
		t.Errorf("downloaded content mismatch; want %q, got %q", orig, body)
	}
}
