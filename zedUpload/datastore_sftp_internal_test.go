// Copyright(c) 2026 Zededa, Inc.
// All rights reserved.

package zedUpload

import (
	"testing"
)

func TestSftpResolvePath(t *testing.T) {
	tests := []struct {
		name         string
		basePath     string
		fileName     string
		expectedPath string
	}{
		{
			name:         "clean join",
			basePath:     "/home/user",
			fileName:     "file.txt",
			expectedPath: "/home/user/file.txt",
		},
		{
			name:         "trailing slash in base",
			basePath:     "/home/user/",
			fileName:     "file.txt",
			expectedPath: "/home/user/file.txt",
		},
		{
			name:         "leading slash in file",
			basePath:     "/home/user",
			fileName:     "/file.txt",
			expectedPath: "/home/user/file.txt",
		},
		{
			name:         "both slashes",
			basePath:     "/home/user/",
			fileName:     "/file.txt",
			expectedPath: "/home/user/file.txt",
		},
		{
			name:         "empty base",
			basePath:     "",
			fileName:     "file.txt",
			expectedPath: "file.txt",
		},
		{
			name:         "relative base",
			basePath:     "uploads",
			fileName:     "image.png",
			expectedPath: "uploads/image.png",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ep := &SftpTransportMethod{path: tt.basePath}
			result := ep.resolvePath(tt.fileName)
			if result != tt.expectedPath {
				t.Errorf("expected %q, got %q", tt.expectedPath, result)
			}
		})
	}
}
