package azure_test

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/joho/godotenv/autoload"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	azure "github.com/lf-edge/eve-libs/zedUpload/azureutil"
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

func newHTTPClient() *http.Client {
	return &http.Client{Timeout: 2 * time.Minute}
}

func randomBlobName(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, uuid.New().String())
}

// derive the service URL from the account name
func deriveAccountURL(accountName string) string {
	return fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
}

func TestListBlob(t *testing.T) {
	accountName := getEnvOrSkip(t, "TEST_AZURE_ACCOUNT_NAME")
	accountURL := deriveAccountURL(accountName)
	accountKey := getEnvOrSkip(t, "TEST_AZURE_ACCOUNT_KEY")
	container := getEnvOrSkip(t, "TEST_AZURE_CONTAINER")
	prefix := getEnvOrSkip(t, "TEST_AZURE_BLOB_PREFIX")

	httpClient := newHTTPClient()

	blobs, err := azure.ListAzureBlob(accountURL, accountName, accountKey, container, httpClient)
	require.NoError(t, err)

	// Filter client-side to just that sub-directory:
	var filtered []string
	for _, b := range blobs {
		if strings.HasPrefix(b, prefix) {
			filtered = append(filtered, b)
		}
	}

	require.NotEmpty(t, filtered, "Expected at least one blob under "+prefix)
	for _, b := range filtered {
		t.Logf("Found blob: %s", b)
	}
}

// TestListAndDeleteBlob tests listing and deleting a blob
func TestListAndDeleteBlob(t *testing.T) {
	accountName := getEnvOrSkip(t, "TEST_AZURE_ACCOUNT_NAME")
	accountURL := deriveAccountURL(accountName)
	accountKey := getEnvOrSkip(t, "TEST_AZURE_ACCOUNT_KEY")
	container := getEnvOrSkip(t, "TEST_AZURE_CONTAINER")

	httpClient := newHTTPClient()

	// Create a unique blob
	blobName := randomBlobName("test-list")
	// Upload something
	content := "hello Azure"
	localFile := t.TempDir() + "/tmp.txt"
	err := os.WriteFile(localFile, []byte(content), 0644)
	require.NoError(t, err)

	url, err := azure.UploadAzureBlob(accountURL, accountName, accountKey, container, blobName, localFile, httpClient)
	require.NoError(t, err)
	require.Contains(t, url, blobName)

	// List and assert presence
	blobs, err := azure.ListAzureBlob(accountURL, accountName, accountKey, container, httpClient)
	require.NoError(t, err)
	found := false
	for _, b := range blobs {
		if b == blobName {
			found = true
			break
		}
	}
	require.True(t, found, "uploaded blob should appear in list")

	// Delete blob
	err = azure.DeleteAzureBlob(accountURL, accountName, accountKey, container, blobName, httpClient)
	require.NoError(t, err)

	// List again and assert absence
	blobs, err = azure.ListAzureBlob(accountURL, accountName, accountKey, container, httpClient)
	require.NoError(t, err)
	for _, b := range blobs {
		require.NotEqual(t, blobName, b)
	}
}

// TestUploadAndGetMetaData tests UploadAzureBlob and GetAzureBlobMetaData
func TestUploadAndGetMetaData(t *testing.T) {
	accountName := getEnvOrSkip(t, "TEST_AZURE_ACCOUNT_NAME")
	accountURL := deriveAccountURL(accountName)
	accountKey := getEnvOrSkip(t, "TEST_AZURE_ACCOUNT_KEY")
	container := getEnvOrSkip(t, "TEST_AZURE_CONTAINER")

	httpClient := newHTTPClient()

	blobName := randomBlobName("test-meta")
	localFile := t.TempDir() + "/data.bin"
	data := []byte{0, 1, 2, 3, 4, 5}
	err := os.WriteFile(localFile, data, 0644)
	require.NoError(t, err)

	// Upload
	_, err = azure.UploadAzureBlob(accountURL, accountName, accountKey, container, blobName, localFile, httpClient)
	require.NoError(t, err)

	// Get metadata
	length, md5, err := azure.GetAzureBlobMetaData(accountURL, accountName, accountKey, container, blobName, httpClient)
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), length)
	require.NotEmpty(t, md5)

	// Cleanup
	err = azure.DeleteAzureBlob(accountURL, accountName, accountKey, container, blobName, httpClient)
	require.NoError(t, err)
}

// TestGenerateBlobSasURI ensures SAS URI is generated and accessible
func TestGenerateBlobSasURI(t *testing.T) {
	accountName := getEnvOrSkip(t, "TEST_AZURE_ACCOUNT_NAME")
	accountURL := deriveAccountURL(accountName)
	accountKey := getEnvOrSkip(t, "TEST_AZURE_ACCOUNT_KEY")
	container := getEnvOrSkip(t, "TEST_AZURE_CONTAINER")
	httpClient := newHTTPClient()

	blobName := randomBlobName("test-sas")
	localFile := t.TempDir() + "/file.txt"
	err := os.WriteFile(localFile, []byte("sas content"), 0644)
	require.NoError(t, err)

	_, err = azure.UploadAzureBlob(accountURL, accountName, accountKey, container, blobName, localFile, httpClient)
	require.NoError(t, err)

	// Generate SAS
	sasURL, err := azure.GenerateBlobSasURI(accountURL, accountName, accountKey, container, blobName, httpClient, 5*time.Minute)
	require.NoError(t, err)
	require.Contains(t, sasURL, "?")

	// Try to GET via HTTP
	resp, err := http.Get(sasURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "sas content", string(body))

	// Cleanup
	err = azure.DeleteAzureBlob(accountURL, accountName, accountKey, container, blobName, httpClient)
	require.NoError(t, err)
}

// TestDownloadAzureBlobByChunks verifies the streaming downloader returns correct size & data.
func TestDownloadAzureBlobByChunks(t *testing.T) {
	accountName := getEnvOrSkip(t, "TEST_AZURE_ACCOUNT_NAME")
	accountURL := deriveAccountURL(accountName)
	accountKey := getEnvOrSkip(t, "TEST_AZURE_ACCOUNT_KEY")
	container := getEnvOrSkip(t, "TEST_AZURE_CONTAINER")
	httpClient := newHTTPClient()

	// tiny payload
	content := []byte("chunked download test payload")
	blobName := randomBlobName("dl-chunked")
	srcPath := filepath.Join(t.TempDir(), "small.txt")
	require.NoError(t, os.WriteFile(srcPath, content, 0644))

	// upload
	_, err := azure.UploadAzureBlob(accountURL, accountName, accountKey, container, blobName, srcPath, httpClient)
	require.NoError(t, err)

	// **prepare a dummy localFile path**
	localFile := filepath.Join(t.TempDir(), "downloaded.txt")

	// stream download
	rc, size, err := azure.DownloadAzureBlobByChunks(
		accountURL, accountName, accountKey,
		container, blobName,
		localFile,
		httpClient,
	)
	require.NoError(t, err)
	defer rc.Close()

	// verify size and content
	require.Equal(t, int64(len(content)), size)
	buf, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, content, buf)

	// cleanup
	require.NoError(t, azure.DeleteAzureBlob(accountURL, accountName, accountKey, container, blobName, httpClient))
}

// TestBlockBlobStageAndCommit exercises UploadPartByChunk + UploadBlockListToBlob.
func TestUploadPartAndBlockList(t *testing.T) {
	accountName := getEnvOrSkip(t, "TEST_AZURE_ACCOUNT_NAME")
	accountURL := deriveAccountURL(accountName)
	accountKey := getEnvOrSkip(t, "TEST_AZURE_ACCOUNT_KEY")
	container := getEnvOrSkip(t, "TEST_AZURE_CONTAINER")
	httpClient := newHTTPClient()

	blobName := randomBlobName("block-blob")
	data := []byte("Hello, Block Blob!")
	partA := data[:10]
	partB := data[10:]

	idA := base64.StdEncoding.EncodeToString([]byte("0001"))
	idB := base64.StdEncoding.EncodeToString([]byte("0002"))

	// stage blocks
	require.NoError(t, azure.UploadPartByChunk(
		accountURL, accountName, accountKey, container, blobName, idA,
		httpClient, bytes.NewReader(partA),
	))
	require.NoError(t, azure.UploadPartByChunk(
		accountURL, accountName, accountKey, container, blobName, idB,
		httpClient, bytes.NewReader(partB),
	))

	// commit in order
	require.NoError(t, azure.UploadBlockListToBlob(
		accountURL, accountName, accountKey, container, blobName,
		httpClient, []string{idA, idB},
	))

	// **HERE**: give it a file path
	localFile := filepath.Join(t.TempDir(), "downloaded.bin")

	rc, size, err := azure.DownloadAzureBlobByChunks(
		accountURL, accountName, accountKey,
		container, blobName,
		localFile,
		httpClient,
	)
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), size)
	require.Equal(t, data, got)

	// cleanup
	require.NoError(t, azure.DeleteAzureBlob(
		accountURL, accountName, accountKey, container, blobName, httpClient,
	))
}

// TestDownloadAzureBlob tests DownloadAzureBlob end-to-end against real Azure Blob Storage.
func TestDownloadAzureBlob(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreAnyFunction("go.opencensus.io/stats/view.(*worker).start"), goleak.IgnoreCurrent())
	accountName := getEnvOrSkip(t, "TEST_AZURE_ACCOUNT_NAME")
	accountURL := deriveAccountURL(accountName)
	accountKey := getEnvOrSkip(t, "TEST_AZURE_ACCOUNT_KEY")
	container := getEnvOrSkip(t, "TEST_AZURE_CONTAINER")

	httpClient := newHTTPClient()

	// Create a unique blob and local files
	blobName := randomBlobName("test-dl")
	srcPath := filepath.Join(t.TempDir(), "src.bin")
	content := []byte("Integration download test payload")
	require.NoError(t, os.WriteFile(srcPath, content, 0644))

	// Upload the blob
	_, err := azure.UploadAzureBlob(
		accountURL, accountName, accountKey,
		container, blobName, srcPath, httpClient,
	)
	require.NoError(t, err)

	// Download to a new file
	dstPath := filepath.Join(t.TempDir(), "dst.bin")
	parts, err := azure.DownloadAzureBlob(
		accountURL,
		accountName,
		accountKey,
		container,
		blobName,
		dstPath,
		0, // no max size limit
		httpClient,
		types.DownloadedParts{},
		nil, // no progress channel
	)
	require.NoError(t, err)

	// Validate downloaded content
	data, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	require.Equal(t, content, data)

	// Validate parts metadata
	require.Len(t, parts.Parts, 1)
	require.Equal(t, int64(0), parts.Parts[0].Ind)
	require.Equal(t, int64(len(content)), parts.Parts[0].Size)

	// Cleanup
	require.NoError(t, azure.DeleteAzureBlob(
		accountURL, accountName, accountKey,
		container, blobName, httpClient,
	))
}
