// Copyright(c) 2026 Zededa, Inc.
// All rights reserved.

package zedUpload_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lf-edge/eve-libs/zedUpload"
)

// TestHTTPURLConstruction verifies that URLs are constructed correctly
// using url.JoinPath, avoiding double slashes or missing slashes.
func TestHTTPURLConstruction(t *testing.T) {
	var receivedPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		// Return some dummy content so downloads don't fail immediately on Body read
		_, err := w.Write([]byte("some data"))
		if err != nil {
			t.Fatalf("Failed to write response: %v", err)
		}
	}))
	defer ts.Close()

	serverURL := ts.URL

	tests := []struct {
		name           string
		remoteDir      string
		remoteFilename string
		operation      zedUpload.SyncOpType
		expectedPath   string
	}{
		// Download Tests (URL = serverURL + remoteDir + remoteFilename)
		{
			name:           "Download: Normal",
			remoteDir:      "folder",
			remoteFilename: "file.txt",
			operation:      zedUpload.SyncOpDownload,
			expectedPath:   "/folder/file.txt",
		},
		{
			name:           "Download: Empty dir",
			remoteDir:      "",
			remoteFilename: "file.txt",
			operation:      zedUpload.SyncOpDownload,
			expectedPath:   "/file.txt",
		},
		{
			name:           "Download: Dir with trailing slash",
			remoteDir:      "folder/",
			remoteFilename: "file.txt",
			operation:      zedUpload.SyncOpDownload,
			expectedPath:   "/folder/file.txt",
		},
		{
			name:           "Download: Dir with leading slash",
			remoteDir:      "/folder",
			remoteFilename: "file.txt",
			operation:      zedUpload.SyncOpDownload,
			expectedPath:   "/folder/file.txt",
		},
		{
			name:           "Download: Filename with leading slash",
			remoteDir:      "folder",
			remoteFilename: "/file.txt",
			operation:      zedUpload.SyncOpDownload,
			expectedPath:   "/folder/file.txt",
		},
		{
			name:           "Download: Empty dir, filename with slash",
			remoteDir:      "",
			remoteFilename: "/file.txt",
			operation:      zedUpload.SyncOpDownload,
			expectedPath:   "/file.txt",
		},
		// Upload Tests (URL = serverURL + remoteDir)
		{
			name:           "Upload: Normal",
			remoteDir:      "upload",
			remoteFilename: "data.bin",
			operation:      zedUpload.SyncOpUpload,
			expectedPath:   "/upload",
		},
		{
			name:           "Upload: Empty dir",
			remoteDir:      "",
			remoteFilename: "data.bin",
			operation:      zedUpload.SyncOpUpload,
			expectedPath:   "/",
		},
		{
			name:           "Upload: Dir with trailing slash",
			remoteDir:      "upload/",
			remoteFilename: "data.bin",
			operation:      zedUpload.SyncOpUpload,
			expectedPath:   "/upload/",
		},
		// List Tests (URL = serverURL + remoteDir)
		{
			name:           "List: Normal",
			remoteDir:      "images",
			remoteFilename: "",
			operation:      zedUpload.SyncOpList,
			expectedPath:   "/images",
		},
		{
			name:           "List: Empty dir",
			remoteDir:      "",
			remoteFilename: "",
			operation:      zedUpload.SyncOpList,
			expectedPath:   "/",
		},
		// MetaData Tests (URL = serverURL + remoteDir + remoteFilename)
		{
			name:           "MetaData: Normal",
			remoteDir:      "meta",
			remoteFilename: "image.img",
			operation:      zedUpload.SyncOpGetObjectMetaData,
			expectedPath:   "/meta/image.img",
		},
		{
			name:           "MetaData: Empty dir",
			remoteDir:      "",
			remoteFilename: "image.img",
			operation:      zedUpload.SyncOpGetObjectMetaData,
			expectedPath:   "/image.img",
		},
		{
			name:           "MetaData: Dir with trailing slash",
			remoteDir:      "meta/",
			remoteFilename: "image.img",
			operation:      zedUpload.SyncOpGetObjectMetaData,
			expectedPath:   "/meta/image.img",
		},
		{
			name:           "MetaData: Filename with leading slash",
			remoteDir:      "meta",
			remoteFilename: "/image.img",
			operation:      zedUpload.SyncOpGetObjectMetaData,
			expectedPath:   "/meta/image.img",
		},
	}

	tmpDir := t.TempDir()
	// Create a dummy local file for upload
	localFile := filepath.Join(tmpDir, "dummy_local")
	if err := os.WriteFile(localFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create dummy local file: %v", err)
	}
	// For download destination
	// We need to make sure we don't overwrite the upload source if we use same var,
	// but here we use localFile as source for Upload and destination for Download.
	// That's fine.

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			receivedPath = ""

			httpAuth := &zedUpload.AuthInput{AuthType: "http"}
			ctx, err := zedUpload.NewDronaCtx("test-url-constr", 0)
			if ctx == nil || err != nil {
				t.Fatalf("NewDronaCtx failed: %v", err)
			}
			// defer ctx.Close()

			// Create Endpoint
			ep, err := ctx.NewSyncerDest(zedUpload.SyncHttpTr, serverURL, tt.remoteDir, httpAuth)
			if err != nil {
				t.Fatalf("NewSyncerDest failed: %v", err)
			}

			respChan := make(chan *zedUpload.DronaRequest)

			req := ep.NewRequest(tt.operation, tt.remoteFilename, localFile, 0, true, respChan)
			if req == nil {
				t.Fatalf("NewRequest returned nil")
			}

			if err := req.Post(); err != nil {
				t.Fatalf("req.Post() failed: %v", err)
			}

			// Wait for response
			timeout := time.After(2 * time.Second)
			done := false
			for !done {
				select {
				case resp := <-respChan:
					if !resp.IsDnUpdate() {
						done = true
					}
				case <-timeout:
					t.Fatalf("Timed out waiting for operation")
				}
			}

			// Check path
			if receivedPath != tt.expectedPath {
				t.Errorf("Path mismatch.\nExpected: %q\nGot:      %q", tt.expectedPath, receivedPath)
			}
		})
	}
}
