package zedUpload_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/lf-edge/eve-libs/zedUpload"
)

const (
	// parameters for HTTP datastore
	httpUploadFile  = uploadFile
	httpDownloadDir = "./test/output/httpDownload/"
)

type logger interface {
	Logf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

func TestHTTPDatastore(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("setup error: %v", err)
	}
	if err := os.MkdirAll(httpDownloadDir, 0755); err != nil {
		t.Fatalf("unable to make download directory: %v", err)
	}
	if err := os.MkdirAll(nettraceFolder, 0755); err != nil {
		t.Fatalf("unable to make nettrace log directory: %v", err)
	}
	t.Run("API", testHTTPDatastoreAPI)
	t.Run("Negative", testHTTPDatastoreNegative)
	t.Run("Functional", testHTTPDatastoreFunctional)
	t.Run("Repeat", testHTTPDatastoreRepeat)
}

// operationHTTP simple utility function for performing HTTP operations during testing. Parameters are:
// - localPath: local file path
// - remoteFilename: remote file name
// - url: remote URL to server (not including path)
// - remoteDir: remote directory path
// - operation: operation to perform
// - local: whether to use local IP address
// - syncerOpts: optional syncer options
// func operationHTTP(t *testing.T, localPath, remoteFilename, url, remoteDir string, operation zedUpload.SyncOpType, local bool, syncerOpts ...zedUpload.SyncerDestOption) (bool, string) {
func operationHTTP(l logger, operation zedUpload.SyncOpType, url, remoteDir, remoteFilename, localPath string, local bool, syncerOpts ...zedUpload.SyncerDestOption) (bool, string) {
	respChan := make(chan *zedUpload.DronaRequest)

	httpAuth := &zedUpload.AuthInput{AuthType: "http"}
	ctx, err := zedUpload.NewDronaCtx("zuploader", 0)
	if ctx == nil {
		return true, err.Error()
	}

	// create Endpoint
	dEndPoint, err := ctx.NewSyncerDest(zedUpload.SyncHttpTr, url, nettraceFolder, remoteDir, httpAuth, syncerOpts...)
	if err == nil && dEndPoint != nil {
		if local {
			var lIP net.IP
			err = lIP.UnmarshalText([]byte("127.0.0.1"))
			if err != nil {
				return true, err.Error()
			}
			err = dEndPoint.WithSrcIP(lIP)
			if err != nil {
				return true, err.Error()
			}
		}
		// create Request
		req := dEndPoint.NewRequest(operation, remoteFilename, localPath, 0, true, respChan)
		if req != nil {
			_ = req.Post()
		}
	}

	var (
		isErr           bool
		status          string
		lastCurrentSize int64
		lastUpdate      = time.Now()
	)
	for resp := range respChan {
		if resp.IsDnUpdate() {
			currentSize, totalSize, _ := resp.Progress()
			if currentSize != lastCurrentSize {
				l.Logf("Update progress for %v: %v/%v",
					resp.GetLocalName(), currentSize, totalSize)
				lastCurrentSize = currentSize
				lastUpdate = time.Now()
			}
			if time.Now().After(lastUpdate.Add(20 * time.Minute)) {
				l.Errorf("No update during 20 minutes")
				break
			}
			continue
		}
		isErr, status = resp.IsError(), resp.GetStatus()
		break
	}
	return isErr, status
}

func listHTTPFiles(t *testing.T, url, dir string) (bool, string) {
	respChan := make(chan *zedUpload.DronaRequest)

	httpAuth := &zedUpload.AuthInput{AuthType: "http"}
	ctx, err := zedUpload.NewDronaCtx("zlister", 0)
	if ctx == nil {
		return true, err.Error()
	}

	// create Endpoint
	dEndPoint, err := ctx.NewSyncerDest(zedUpload.SyncHttpTr, url, nettraceFolder, dir, httpAuth)
	if err == nil && dEndPoint != nil {
		// create Request
		req := dEndPoint.NewRequest(zedUpload.SyncOpList, "", "", 0, true, respChan)
		if req != nil {
			_ = req.Post()
		}
	}

	var (
		isErr  bool
		status string
	)
	for resp := range respChan {
		if resp.IsDnUpdate() {
			continue
		}
		isErr, status = resp.IsError(), resp.GetStatus()
		break
	}
	return isErr, status
}

// getHTTPObjectMetaData simple utility function for requesting metadata information about a remote object
func getHTTPObjectMetaData(t *testing.T, objloc string, objkey string, url, dir string) (bool, string, int64) {
	respChan := make(chan *zedUpload.DronaRequest)

	httpAuth := &zedUpload.AuthInput{AuthType: "http"}
	ctx, err := zedUpload.NewDronaCtx("zuploader", 0)
	if ctx == nil {
		return true, err.Error(), 0
	}

	// create Endpoint
	dEndPoint, err := ctx.NewSyncerDest(zedUpload.SyncHttpTr, url, nettraceFolder, dir, httpAuth)
	if err == nil && dEndPoint != nil {
		// create Request
		req := dEndPoint.NewRequest(zedUpload.SyncOpGetObjectMetaData, objkey, objloc, 0, true, respChan)
		if req != nil {
			_ = req.Post()
		}
	}

	var (
		isErr  bool
		status string
		length int64
	)
	for resp := range respChan {
		if resp.IsDnUpdate() {
			continue
		}
		isErr, status, length = resp.IsError(), resp.GetStatus(), resp.GetContentLength()
		break
	}
	return isErr, status, length
}

func testHTTPDatastoreAPI(t *testing.T) {
	// create the test server
	// pretty simple:
	// - any POST gets written to tempDir; the test should check the file
	// - any GET gets checked in the local filesystem; the test should create the file. If it is a directory, it lists the file names
	tempDir := t.TempDir()
	r := chi.NewRouter()
	r.Post("/*", func(w http.ResponseWriter, r *http.Request) {
		dir := filepath.Join(tempDir, r.URL.Path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := r.ParseMultipartForm(20 * 1024 * 1024); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		for filename := range r.MultipartForm.File {
			file, _, err := r.FormFile(filename)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			defer file.Close()
			fullpath := filepath.Join(dir, filename)
			f, err := os.Create(fullpath)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			defer f.Close()
			if _, err := io.Copy(f, file); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		w.WriteHeader(http.StatusCreated)
	})
	r.Get("/*", func(w http.ResponseWriter, r *http.Request) {
		p := filepath.Join(tempDir, r.URL.Path)
		// not found? return 404
		if _, err := os.Stat(p); err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// directory? send filenames; file? list filenames
		http.ServeFile(w, r, p)
	})
	ts := httptest.NewServer(r)
	defer ts.Close()
	t.Run("Upload", func(t *testing.T) {
		t.Run("valid", func(t *testing.T) {
			filename := "httpteststuff"
			status, msg := operationHTTP(t, zedUpload.SyncOpUpload, ts.URL, "", filename, httpUploadFile, false)
			if status {
				t.Fatalf("%v", msg)
			}
			// check that the contents of the file match; it should gave been placed in tempDir
			inHash, err := sha256File(httpUploadFile)
			if err != nil {
				t.Errorf("%v", err)
			}
			outHash, err := sha256File(filepath.Join(tempDir, filename))
			if err != nil {
				t.Fatalf("%v", err)
			}
			if inHash != outHash {
				t.Errorf("hash mismatch infile %s outfile %s", inHash, outHash)
			}
		})
		//t.Run("Upload=1", func(t *testing.T) { operationHTTP(t, httpUploadFile, "release/httpteststuff", httpPostRegion, zedUpload.SyncOpUpload) })
		//t.Run("Upload=2", func(t *testing.T) {
		//	operationHTTP(t, httpUploadFile, "release/1.0/httpteststuff", httpPostRegion, zedUpload.SyncOpUpload)
		//})
	})
	t.Run("Download", func(t *testing.T) {
		var downloadTest = func(filename, localName string, size int) error {
			target := filepath.Join(httpDownloadDir, localName)
			infile := filepath.Join(tempDir, filename)
			_, inHash, err := createRandomFile(infile, size)
			if err != nil {
				return fmt.Errorf("unable to create random file %s: %v", filename, err)
			}
			defer os.Remove(target)
			defer os.Remove(infile)
			status, msg := operationHTTP(t, zedUpload.SyncOpDownload, ts.URL, "", filename, target, false)
			if status {
				return fmt.Errorf("%v", msg)
			}
			outHash, err := sha256File(target)
			if err != nil {
				return fmt.Errorf("unable to hash download file %s: %v", target, err)
			}
			if inHash != outHash {
				return fmt.Errorf("hash mismatch infile %s outfile %s", inHash, outHash)
			}
			return nil
		}
		t.Run("valid", func(t *testing.T) {
			if err := downloadTest("bionic/release-20210804/ubuntu-18.04-server-cloudimg-s390x-lxd.tar.xz", "file0", 1024*1024*100); err != nil {
				t.Error(err)
			}
		})
		t.Run("another", func(t *testing.T) {
			if err := downloadTest("minimal/releases/bionic/release-20210803/ubuntu-18.04-minimal-cloudimg-amd64-root.tar.xz", "file1", 1024*1024*100); err != nil {
				t.Error(err)
			}
		})
		t.Run("one more", func(t *testing.T) {
			if err := downloadTest("xenial/release/ubuntu-16.04-server-cloudimg-amd64-disk1.img", "file2", 1024*1024*100); err != nil {
				t.Error(err)
			}
		})
	})
	t.Run("List", func(t *testing.T) {
		t.Run("non-existent URL", func(t *testing.T) {
			status, _ := listHTTPFiles(t, "http://1.2.3.4:80", "randompath")
			if !status {
				t.Errorf("Non-existent URL seems to exist")
			}
		})
		//t.Run("List=1", func(t *testing.T) { listHTTPFiles(t, "http://192.168.0.147:80") })
		dirpath := "listfiles"
		filenames := []string{"file1", "file2", "file3"}
		for _, filename := range filenames {
			fullFile := filepath.Join(tempDir, dirpath, filename)
			if _, _, err := createRandomFile(fullFile, 1024*1); err != nil {
				t.Fatalf("unable to create random file %s: %v", fullFile, err)
			}
		}
		t.Run("valid", func(t *testing.T) {
			status, msg := listHTTPFiles(t, ts.URL, dirpath)
			if status {
				t.Errorf("%v", msg)
			}
		})
	})
	t.Run("Delete", func(t *testing.T) {
		//t.Run("Delete=0", func(t *testing.T) { operationHTTP(t, "", "httpteststuff", zedUpload.SyncOpDelete) })
		//t.Run("Delete=1", func(t *testing.T) { operationHTTP(t, "", "release/httpteststuff", zedUpload.SyncOpDelete) })
		//t.Run("Delete=2", func(t *testing.T) { operationHTTP(t, "", "release/1.0/httpteststuff", zedUpload.SyncOpDelete) })
	})
}

func testHTTPDatastoreFunctional(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping HTTP Extended test suite.")
	} else {
		t.Log("Running HTTP Extended test suite.")
		t.Run("XtraSmall", func(t *testing.T) {
			err := testHTTPObjectWithFile(t, filepath.Join(httpDownloadDir, "file1"), 1024*1)
			if err != nil {
				t.Errorf("%v", err)
			}
		})
		t.Run("Small", func(t *testing.T) {
			err := testHTTPObjectWithFile(t, filepath.Join(httpDownloadDir, "file2"), 1024*1024*1)
			if err != nil {
				t.Errorf("%v", err)
			}
		})
	}
}

// testHTTPObjectWithFile get a remote object's metadata, then download it, and compare the sizes
func testHTTPObjectWithFile(t *testing.T, localPath string, size int) error {
	tempDir := t.TempDir()
	r := chi.NewRouter()
	r.Get("/*", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join(tempDir, r.URL.Path))
	})
	r.Head("/*", func(w http.ResponseWriter, r *http.Request) {
		filename := filepath.Join(tempDir, r.URL.Path)
		stat, err := os.Stat(filename)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", stat.Size()))
	})
	ts := httptest.NewServer(r)
	defer ts.Close()

	// just create a random download file
	filename := "file1"
	infile := filepath.Join(tempDir, filename)
	if _, _, err := createRandomFile(infile, size); err != nil {
		return fmt.Errorf("unable to create random file %s: %v", infile, err)
	}
	defer os.Remove(infile)
	status, msg := operationHTTP(t, zedUpload.SyncOpDownload, ts.URL, "", filename, localPath, false)
	if status {
		return fmt.Errorf("%v", msg)
	}
	statusMeta, msgMeta, remoteSize := getHTTPObjectMetaData(t, localPath, filename, ts.URL, "")
	if statusMeta {
		return errors.New(msgMeta)
	}
	statusDownload, msgDownload := operationHTTP(t, zedUpload.SyncOpDownload, ts.URL, "", filename, localPath, false)
	if statusDownload {
		return errors.New(msgDownload)
	}
	stat, err := os.Stat(localPath)
	if err != nil {
		return err
	}
	localSize := stat.Size()
	if remoteSize != localSize {
		return fmt.Errorf("Downloaded size %v didn't match metadata remote size %v", localSize, remoteSize)
	}
	return nil

}

func testHTTPDatastoreNegative(t *testing.T) {
	// create the test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()
	t.Run("InvalidTransport", func(t *testing.T) {
		status, _ := operationHTTP(t, zedUpload.SyncOpUnknown, ts.URL, "", "httpteststuff", httpUploadFile, false)
		if !status {
			t.Errorf("Processing invalid transporter")
		}
	})
	t.Run("InvalidUpload", func(t *testing.T) {
		status, _ := operationHTTP(t, zedUpload.SyncOpUpload, ts.URL, "", "httpteststuff", uploadDir+"InvalidFile", false)
		if !status {
			t.Errorf("Uploading non existent file")
		}
	})
	t.Run("InvalidDownload", func(t *testing.T) {
		status, _ := operationHTTP(t, zedUpload.SyncOpDownload, ts.URL, "", "InvalidFile", filepath.Join(httpDownloadDir, "file0"), false)
		if !status {
			t.Errorf("Downloading non existent file")
		}
	})
}

// testHTTPDatastoreRepeat tests that the client can handle lost packets
// during a large download stream and recover, by using the Range HTTP header.
// The tests sets up an "unstable" proxy, which is configured to start dropping and
// delaying packets after a certain number of bytes have been transferred.
//
// The test works as follows:
//
//		1- create a large input file with random data, e.g. 100MB
//		2- hash the input file
//		3- create a test http server configured to serve the input file
//		4- start the unstable proxy, pointing to the test http server as the origin,
//		   configured to drop packets for a fixed period of time, e.g. 40 seconds, after half the file has been transferred
//		5- download the file using the unstable proxy, which should download half the file, fail for the fixed period of time,
//	    and then successfully download the rest
//		6- when download completes, hash the downloaded file
//		7- check that the hashes match
func testHTTPDatastoreRepeat(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping HTTP repeat test suite.")
	} else {
		t.Log("Running HTTP repeat test suite.")

		var (
			inactivityTimeout = 10 * time.Second
			unstableDelay     = 15 * time.Second // this just needs to be a little longer than the previous
		)

		// make a random file
		infile := httpDownloadDir + "input"
		inSize, inHash, err := createRandomFile(infile, 1024*1024*100)
		if err != nil {
			t.Fatalf("unable to create random file %v", err)
		}
		defer os.RemoveAll(infile)

		// create the test server
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			f, err := os.Open(infile)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			defer f.Close()
			http.ServeContent(w, r, "input", time.Now(), newUnstableFileReader(f, uint64(inSize/2), unstableDelay, 100, t.Logf))
		}))
		defer ts.Close()

		outfile := filepath.Join(httpDownloadDir, "repeat2")
		status, msg := operationHTTP(t, zedUpload.SyncOpDownload, ts.URL, "", "path/does/not/matter/with/fixed/server", outfile, true, zedUpload.WithHTTPInactivityTimeout(inactivityTimeout))
		if status {
			t.Errorf("%v", msg)
		}
		// get the number of bytes transferred
		stat, err := os.Stat(outfile)
		if err != nil {
			t.Fatalf("unable to stat file %s %v", outfile, err)
		}
		outSize := stat.Size()
		if outSize != inSize {
			t.Fatalf("expected %d bytes, got %d", inSize, outSize)
		}

		hashSum, err := sha256File(outfile)
		if err != nil {
			t.Errorf("%v", err)
		} else {
			if hashSum != inHash {
				t.Errorf("hash mismatch infile %s outfile %s", inHash, hashSum)
			}
		}
	}
}

func sha256File(filePath string) (string, error) {
	hasher := sha256.New()
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := io.Copy(hasher, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// createRandomFile creates a file of the given size at the given path, and returns the actual size of the file,
// string of the file hash, and error. Will create all parent directories, if required
func createRandomFile(p string, size int) (int64, string, error) {
	// create the parent directories
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return 0, "", fmt.Errorf("unable to create parent directories for %s %v", p, err)
	}
	if _, err := os.Stat(p); err == nil {
		if err := os.Remove(p); err != nil {
			return 0, "", fmt.Errorf("unable to remove existing file %s %v", p, err)
		}
	}
	f, err := os.Create(p)
	if err != nil {
		return 0, "", fmt.Errorf("unable to create file %s %v", p, err)
	}
	defer f.Close()
	randReader := io.LimitReader(rand.New(rand.NewSource(time.Now().UnixNano())), int64(size))
	if _, err := io.Copy(f, randReader); err != nil {
		return 0, "", fmt.Errorf("unable to copy random data to file %s %v", p, err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		return 0, "", fmt.Errorf("unable to seek to beginning of file %s %v", p, err)
	}
	// get the final file size
	stat, err := os.Stat(p)
	if err != nil {
		return 0, "", fmt.Errorf("unable to stat file %s %v", p, err)
	}
	actualSize := stat.Size()

	hash, err := sha256File(p)
	if err != nil {
		return 0, "", fmt.Errorf("unable to hash file %s: %v", p, err)
	}
	return actualSize, hash, nil
}
