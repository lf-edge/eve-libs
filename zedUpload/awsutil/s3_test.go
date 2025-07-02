// Copyright(c) 2025 Zededa, Inc.
// All rights reserved.

package awsutil

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/joho/godotenv/autoload"
	"github.com/lf-edge/eve-libs/zedUpload/types"
)

// helper to get env or skip
func getEnvOrSkip(t *testing.T, key string) string {
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("Skipping test: environment variable %s not set", key)
	}
	return v
}

// setupTestBucket creates a bucket whose name is prefix-<timestamp>
func setupTests(t *testing.T) (*S3ctx, string, string, bool) {
	t.Helper()

	// Cleanup: delete every object, then the bucket.
	baseBucket := getEnvOrSkip(t, "AWS_TEST_BUCKET")
	if baseBucket == "" {
		t.Skip("AWS_TEST_BUCKET not set; skipping integration test")
	}
	accessKey := getEnvOrSkip(t, "AWS_ACCESS_KEY_ID")
	secretKey := getEnvOrSkip(t, "AWS_SECRET_ACCESS_KEY")
	region := getEnvOrSkip(t, "AWS_REGION")
	_, useIPv6 := os.LookupEnv("AWS_USE_IPV6")

	// Initialize a real S3ctx
	s, err := NewAwsCtx(accessKey, secretKey, region, useIPv6, nil)
	if err != nil {
		t.Fatalf("NewAwsCtx failed: %v", err)
	}

	return s, baseBucket, region, useIPv6
}

func TestUploadFile(t *testing.T) {
	// get context and bucket
	s, bucket, region, useIPv6 := setupTests(t)

	key := "test-object"
	// cleanup
	t.Cleanup(func() {
		if err := s.DeleteObject(bucket, key); err != nil {
			t.Logf("Cleanup: DeleteObject failed: %v", err)
		}
		time.Sleep(2 * time.Second)
		if err := s.DeleteBucket(bucket); err != nil {
			t.Logf("Cleanup: DeleteBucket failed: %v", err)
		}
	})

	// write a temp file
	tmpf, err := os.CreateTemp("", "upload-integ-*.txt")
	if err != nil {
		t.Fatalf("CreateTemp failed: %v", err)
	}
	defer os.Remove(tmpf.Name())
	payload := []byte("integration-test payload")
	if _, err := tmpf.Write(payload); err != nil {
		t.Fatalf("writing temp file: %v", err)
	}
	tmpf.Close()

	loc, err := s.UploadFile(tmpf.Name(), bucket, key, false, nil)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}
	// Parse the returned location and verify host and path separately:
	u, err := url.Parse(loc)
	if err != nil {
		t.Fatalf("could not parse Location %q: %v", loc, err)
	}

	// For non-us-east-1 regions the host is "<bucket>.s3.[dualstack.]<region>.amazonaws.com"
	var dualstack string
	if useIPv6 {
		dualstack = "dualstack."
	}
	wantHost := fmt.Sprintf("%s.s3.%s%s.amazonaws.com", bucket, dualstack, region)
	if u.Host != wantHost {
		t.Errorf("wrong host: got %q, want %q", u.Host, wantHost)
	}

	// And the path should be "/<key>"
	wantPath := "/" + key
	if u.Path != wantPath {
		t.Errorf("wrong path: got %q, want %q", u.Path, wantPath)
	}
	// verify object exists
	if _, err := s.client.HeadObject(s.ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}); err != nil {
		t.Errorf("HeadObject failed; object not found: %v", err)
	}
}

func TestDownloadFile(t *testing.T) {
	// get context and bucket
	s, bucket, region, useIPv6 := setupTests(t)

	key := "test-object"
	// cleanup
	t.Cleanup(func() {
		if err := s.DeleteObject(bucket, key); err != nil {
			t.Logf("Cleanup: DeleteObject failed: %v", err)
		}
		time.Sleep(2 * time.Second)
		if err := s.DeleteBucket(bucket); err != nil {
			t.Logf("Cleanup: DeleteBucket failed: %v", err)
		}
	})

	// write a small temp file and upload it
	orig := []byte("integration download test payload")
	tmp, err := os.CreateTemp("", "dl-int-*.txt")
	if err != nil {
		t.Fatalf("TempFile: %v", err)
	}
	if _, err := tmp.Write(orig); err != nil {
		t.Fatalf("writing temp file: %v", err)
	}
	tmp.Close()
	defer os.Remove(tmp.Name())

	loc, err := s.UploadFile(tmp.Name(), bucket, key, false, nil)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	// sanity-check the returned URL
	u, err := url.Parse(loc)
	if err != nil {
		t.Fatalf("parsing Location %q: %v", loc, err)
	}
	var dualstack string
	if useIPv6 {
		dualstack = "dualstack."
	}
	wantHost := fmt.Sprintf("%s.s3.%s%s.amazonaws.com", bucket, dualstack, region)
	if u.Host != wantHost {
		t.Errorf("unexpected host: got %q, want %q", u.Host, wantHost)
	}
	if u.Path != "/"+key {
		t.Errorf("unexpected path: got %q, want %q", u.Path, "/"+key)
	}

	// now download it
	destDir := t.TempDir()
	destPath := filepath.Join(destDir, "downloaded.txt")
	_, err = s.DownloadFile(destPath, bucket, key, 0,
		types.DownloadedParts{PartSize: S3PartSize}, nil,
	)
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}

	// verify the file content
	got, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(orig, got) {
		t.Errorf("downloaded content does not match original;\nwant: %q\n got: %q", orig, got)
	}
}

// TestListImages creates two objects and ensures ListImages returns their keys.
func TestListImages(t *testing.T) {
	// get context and bucket
	s, bucket, _, _ := setupTests(t)

	keys := []string{"imgA", "imgB"}
	// cleanup
	t.Cleanup(func() {
		for _, key := range keys {
			if err := s.DeleteObject(bucket, key); err != nil {
				t.Logf("Cleanup: DeleteObject failed: %v", err)
			}
		}
		time.Sleep(2 * time.Second)
		if err := s.DeleteBucket(bucket); err != nil {
			t.Logf("Cleanup: DeleteBucket failed: %v", err)
		}
	})

	// upload two objects
	contents := map[string][]byte{
		"imgA": []byte("foo"),
		"imgB": []byte("bar"),
	}
	for key, content := range contents {
		tmp := t.TempDir() + "/" + key + ".txt"
		if err := os.WriteFile(tmp, content, 0644); err != nil {
			t.Fatalf("writing temp file for %s: %v", key, err)
		}
		if _, err := s.UploadFile(tmp, bucket, key, false, nil); err != nil {
			t.Fatalf("UploadFile for %s failed: %v", key, err)
		}
	}

	// call ListImages and verify
	listed, err := s.ListImages(bucket, nil)
	if err != nil {
		t.Fatalf("ListImages failed: %v", err)
	}
	if len(listed) != len(contents) {
		t.Fatalf("expected %d objects, got %d: %v", len(contents), len(listed), listed)
	}
	seen := map[string]bool{}
	for _, k := range listed {
		seen[k] = true
	}
	for key := range contents {
		if !seen[key] {
			t.Errorf("ListImages missing key %q", key)
		}
	}
}

// TestGetObjectMetaData uploads one object and verifies GetObjectMetaData returns the right size & MD5.
func TestGetObjectMetaData(t *testing.T) {
	// get context and bucket
	s, bucket, region, useIPv6 := setupTests(t)

	key := "single-object"
	// cleanup
	t.Cleanup(func() {
		if err := s.DeleteObject(bucket, key); err != nil {
			t.Logf("Cleanup: DeleteObject failed: %v", err)
		}
		time.Sleep(2 * time.Second)
		if err := s.DeleteBucket(bucket); err != nil {
			t.Logf("Cleanup: DeleteBucket failed: %v", err)
		}
	})

	// upload one object
	orig := []byte("integration-metadata-test")
	tmp := t.TempDir() + "/obj.txt"
	if err := os.WriteFile(tmp, orig, 0644); err != nil {
		t.Fatalf("writing temp file: %v", err)
	}
	loc, err := s.UploadFile(tmp, bucket, key, false, nil)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}
	// sanity-check the returned URL
	u, err := url.Parse(loc)
	if err != nil {
		t.Fatalf("parsing Location: %v", err)
	}
	var dualstack string
	if useIPv6 {
		dualstack = "dualstack."
	}
	wantHost := fmt.Sprintf("%s.s3.%s%s.amazonaws.com", bucket, dualstack, region)
	if u.Host != wantHost {
		t.Errorf("unexpected host: got %q, want %q", u.Host, wantHost)
	}

	// call GetObjectMetaData and verify
	size, md5sum, err := s.GetObjectMetaData(bucket, key)
	if err != nil {
		t.Fatalf("GetObjectMetaData failed: %v", err)
	}
	if size != int64(len(orig)) {
		t.Errorf("size = %d, want %d", size, len(orig))
	}
	h := md5.Sum(orig)
	wantMD5 := hex.EncodeToString(h[:])

	if md5sum != wantMD5 {
		t.Errorf("md5 = %q, want %q", md5sum, wantMD5)
	}
}
func TestDownloadFileByChunks(t *testing.T) {
	// get context and bucket
	s, bucket, _, _ := setupTests(t)

	key := "chunk-object"
	// cleanup
	t.Cleanup(func() {
		if err := s.DeleteObject(bucket, key); err != nil {
			t.Logf("Cleanup: DeleteObject failed: %v", err)
		}
		time.Sleep(2 * time.Second)
		if err := s.DeleteBucket(bucket); err != nil {
			t.Logf("Cleanup: DeleteBucket failed: %v", err)
		}
	})

	// write & upload a test file
	orig := []byte("this is an integration-by-chunks test payload, long enough to span multiple reads")
	tmp := t.TempDir() + "/orig.txt"
	if err := os.WriteFile(tmp, orig, 0644); err != nil {
		t.Fatalf("writing temp file: %v", err)
	}
	if _, err := s.UploadFile(tmp, bucket, key, false, nil); err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	// download by chunks
	rc, size, err := s.DownloadFileByChunks(tmp, bucket, key)
	if err != nil {
		t.Fatalf("DownloadFileByChunks failed: %v", err)
	}
	defer rc.Close()

	// verify reported size
	if size != int64(len(orig)) {
		t.Errorf("DownloadFileByChunks size = %d; want %d", size, len(orig))
	}

	// read it back in fixed-size chunks
	var buf bytes.Buffer
	chunk := make([]byte, 16) // arbitrary chunk size
	for {
		n, err := rc.Read(chunk)
		if n > 0 {
			buf.Write(chunk[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("reading chunk: %v", err)
		}
	}

	// verify full content
	got := buf.Bytes()
	if !bytes.Equal(got, orig) {
		t.Errorf("downloaded content mismatch:\n got: %q\nwant: %q", got, orig)
	}
}

// TestUploadPartCreateUploadID ensures the very first call to UploadPart
// (with uploadID="") returns a valid uploadID and non-empty ETag.
func TestUploadPartCreateUploadID(t *testing.T) {
	// get context and bucket
	s, bucket, _, _ := setupTests(t)
	// create the bucket
	err := s.CreateBucket(bucket)
	if err != nil {
		t.Fatalf("Could not create bucket %s: %v", bucket, err)
	}
	s.WaitUntilBucketExists(bucket)

	key := "part-object"
	chunk := []byte("chunk-1 data")
	// cleanup
	t.Cleanup(func() {
		if err := s.DeleteObject(bucket, key); err != nil {
			t.Logf("Cleanup: DeleteObject failed: %v", err)
		}
		time.Sleep(2 * time.Second)
		if err := s.DeleteBucket(bucket); err != nil {
			t.Logf("Cleanup: DeleteBucket failed: %v", err)
		}
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

// TestUploadPartMultipartComplete uploads two parts, completes the upload,
// then downloads the assembled object and verifies its full content.
func TestUploadPartMultipartComplete(t *testing.T) {
	// get context and bucket
	s, bucket, _, _ := setupTests(t)
	// create the bucket
	err := s.CreateBucket(bucket)
	if err != nil {
		t.Fatalf("Could not create bucket %s: %v", bucket, err)
	}
	s.WaitUntilBucketExists(bucket)

	key := "multipart-object"
	// cleanup
	t.Cleanup(func() {
		if err := s.DeleteObject(bucket, key); err != nil {
			t.Logf("Cleanup: DeleteObject failed: %v", err)
		}
		time.Sleep(2 * time.Second)
		if err := s.DeleteBucket(bucket); err != nil {
			t.Logf("Cleanup: DeleteBucket failed: %v", err)
		}
	})
	const minPartSize = 5 * 1024 * 1024 // 5 MB
	part1 := make([]byte, minPartSize)
	for i := range part1 {
		part1[i] = 'A'
	}
	part2 := []byte("LAST") // last part can be small

	// --- Upload parts ---
	etag1, uploadID, err := s.UploadPart(bucket, key, part1, 1, "")
	if err != nil {
		t.Fatalf("UploadPart #1: %v", err)
	}
	etag2, uploadID2, err := s.UploadPart(bucket, key, part2, 2, uploadID)
	if err != nil {
		t.Fatalf("UploadPart #2: %v", err)
	}
	if uploadID != uploadID2 {
		t.Errorf("expected same uploadID, got %q vs %q", uploadID, uploadID2)
	}

	// --- Complete the multipart upload using your helper ---
	err = s.CompleteUploadedParts(bucket, key, uploadID, []string{etag1, etag2})
	if err != nil {
		t.Fatalf("CompleteUploadedParts: %v", err)
	}

	// --- Download and verify ---
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
		t.Fatalf("reading object: %v", err)
	}

	want := append(part1, part2...)
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("object content = %q; want %q", buf.String(), string(want))
	}
}

// TestGetSignedURL uploads an object and then fetches it using a presigned URL.
func TestGetSignedURL(t *testing.T) {
	// get context and bucket
	s, bucket, _, _ := setupTests(t)

	key := "signed-object"
	// Prepare and upload a small file.
	orig := []byte("presigned URL integration test")
	tmp := t.TempDir() + "/signed.txt"
	if err := os.WriteFile(tmp, orig, 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	if _, err := s.UploadFile(tmp, bucket, key, false, nil); err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}
	// cleanup
	t.Cleanup(func() {
		if err := s.DeleteObject(bucket, key); err != nil {
			t.Logf("Cleanup: DeleteObject failed: %v", err)
		}
		time.Sleep(2 * time.Second)
		if err := s.DeleteBucket(bucket); err != nil {
			t.Logf("Cleanup: DeleteBucket failed: %v", err)
		}
	})

	// Generate a presigned URL valid for 5 minutes.
	urlStr, err := s.GetSignedURL(bucket, key, 5*time.Minute)
	if err != nil {
		t.Fatalf("GetSignedURL failed: %v", err)
	}

	// Fetch via HTTP GET.
	resp, err := http.Get(urlStr)
	if err != nil {
		t.Fatalf("HTTP GET on presigned URL failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected HTTP status: got %d, want %d", resp.StatusCode, http.StatusOK)
	}

	// Read and verify the body.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response body: %v", err)
	}
	if !bytes.Equal(body, orig) {
		t.Errorf("downloaded content = %q; want %q", body, orig)
	}
}
