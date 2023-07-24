// Copyright(c) 2017-2018 Zededa, Inc.
// All rights reserved.

package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	logutils "github.com/lf-edge/eve/pkg/pillar/utils/logging"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/html"
)

const (
	chunkSize  int64 = 64 * 1024
	maxRetries       = 10
	maxDelay         = time.Minute
	// if chunkSize cannot be transmitted after inactivityTimeout we will re-schedule download, so we expect more than 218 B/s
	inactivityTimeout = 5 * time.Minute
)

type UpdateStats struct {
	Size          int64    // complete size to upload/download
	Asize         int64    // current size uploaded/downloaded
	List          []string //list of images at given path
	Error         error
	BodyLength    int   // Body legth in http response
	ContentLength int64 // Content length in http response
}

type NotifChan chan UpdateStats

var userAgent = "UnityNetworkReporter/" + " (" + runtime.GOOS + " " + runtime.GOARCH + ")"

func getHttpClient() *http.Client {
	tr := &http.Transport{
		TLSNextProto: make(map[string]func(s string, conn *tls.Conn) http.RoundTripper),
	}
	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // Do _NOT_ follow redirects!
		},
		Transport: tr,
	}
}

func getHref(token html.Token) (ok bool, href string) {
	// Iterate over all of the Token's attributes until we find an "href"
	for _, attr := range token.Attr {
		if attr.Key == "href" {
			href = attr.Val
			ok = true
		}
	}

	return
}

// ExecCmd performs various commands such as "ls", "get", etc.
// Note that "host" needs to contain the URL in the case of a get
func ExecCmd(ctx context.Context, cmd, host, remoteFile, localFile string, objSize int64,
	prgNotify NotifChan, client *http.Client) UpdateStats {
	if ctx == nil {
		ctx = context.Background()
	}
	var imgList []string
	stats := UpdateStats{}
	if client == nil {
		client = getHttpClient()
	}
	switch cmd {
	case "ls":
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, host, nil)
		if err != nil {
			stats.Error = fmt.Errorf("request failed for ls %s: %s",
				host, err)
			return stats
		}
		resp, err := client.Do(req)
		if err != nil {
			stats.Error = fmt.Errorf("get failed for ls %s: %s",
				host, err)
			return stats
		}

		if resp.StatusCode != 200 {
			stats.Error = fmt.Errorf("bad response code for ls %s: %d",
				host, resp.StatusCode)
			return stats
		}

		tokenizer := html.NewTokenizer(resp.Body)

		for tokenType := tokenizer.Next(); tokenType != html.ErrorToken; tokenType = tokenizer.Next() {
			if tokenType == html.StartTagToken {
				token := tokenizer.Token()
				// Check if the token is an <a> tag
				isAnchor := token.Data == "a"
				if !isAnchor {
					continue
				}
				// Extract the href value, if there is one
				ok, url := getHref(token)
				if !ok {
					continue
				}

				imgList = append(imgList, url)
			}
		}

		resp.Body.Close()
		stats.List = imgList
		if prgNotify != nil {
			select {
			case prgNotify <- stats:
			default: //ignore we cannot write
			}
		}
		return stats
	case "get":
		var copiedSize int64
		stats.Size = objSize
		dirErr := os.MkdirAll(filepath.Dir(localFile), 0755)
		if dirErr != nil {
			stats.Error = dirErr
			return stats
		}
		local, fileErr := os.Create(localFile)
		if fileErr != nil {
			stats.Error = fileErr
			return stats
		}
		defer local.Close()

		var errorList []string
		done := false
		supportRange := false //is server supports ranges requests, false for the first request
		forceRestart := false
		NoSuitableAddrFound := false
		delay := time.Second
		lastModified := ""
		appendToErrorList := func(attempt int, err error) {
			errorList = append(errorList, fmt.Sprintf("(attempt %d/%d): %v", attempt, maxRetries, err))
			logrus.Warnf("ExecCmd get %s failed (attempt %d/%d): %v", host, attempt, maxRetries, err)
		}
		for attempt := 0; attempt < maxRetries; attempt++ {
			//check context error on every attempt
			if ctx.Err() != nil {
				appendToErrorList(attempt, ctx.Err())
				break
			}
			if attempt > 0 {
				time.Sleep(delay)
				if delay < maxDelay {
					delay = delay * 2
				}
			}

			// restart from the beginning if server do not support ranges or we forced to restart
			if !supportRange || forceRestart {
				err := local.Truncate(0)
				if err != nil {
					appendToErrorList(attempt, fmt.Errorf("failed truncate file: %s", err))
					continue
				}
				_, err = local.Seek(0, 0)
				if err != nil {
					appendToErrorList(attempt, fmt.Errorf("failed seek file: %s", err))
					continue
				}
				copiedSize = 0
				forceRestart = false
			}
			// we need innerCtx cancel to call in case of inactivity
			innerCtx, innerCtxCancel := context.WithCancel(ctx)
			inactivityTimer := time.AfterFunc(inactivityTimeout, func() {
				//keep it to call cancel regardless of logic to releases resources
				innerCtxCancel()
			})
			req, err := http.NewRequestWithContext(innerCtx, http.MethodGet, host, nil)
			if err != nil {
				stats.Error = fmt.Errorf("request failed for get %s: %s",
					host, err)
				return stats
			}
			req.Header.Set("User-Agent", userAgent)
			req.Header.Set("Content-Type", "application/octet-stream")

			withRange := false
			//add Range header if server supports it and we already receive data
			if supportRange && copiedSize > 0 {
				withRange = true
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-", copiedSize))
			}
			resp, err := client.Do(req)
			if err != nil {
				// skip the error from http *net.DNSError has the suffix of "no suitable address found"
				// for a cleaner error string output for http download failure
				if !logutils.IsNoSuitableAddrErr(err) {
					appendToErrorList(attempt, fmt.Errorf("client.Do failed: %s", err))
				} else {
					if !NoSuitableAddrFound {
						NoSuitableAddrFound = true
						appendToErrorList(attempt, fmt.Errorf(logutils.NoSuitableAddrStr))
					}
				}
				continue
			}

			// supportRange indicates if server supports range requests
			supportRange = resp.Header.Get("Accept-Ranges") == "bytes"

			//if we not receive StatusOK for request without Range header or StatusPartialContent for request with range
			//it indicates that server misconfigured
			if !withRange && resp.StatusCode != http.StatusOK || withRange && resp.StatusCode != http.StatusPartialContent {
				respErr := fmt.Errorf("bad response code: %d", resp.StatusCode)
				err = resp.Body.Close()
				if err != nil {
					respErr = fmt.Errorf("respErr: %v; close Body error: %v", respErr, err)
				}
				appendToErrorList(attempt, respErr)
				//we do not want to process server misconfiguration here
				break
			}
			newLastModified := resp.Header.Get("Last-Modified")
			if lastModified != "" && newLastModified != lastModified {
				// last modified changed, retry from the beginning
				lastModified = newLastModified
				forceRestart = true
				continue
			}
			if resp.StatusCode == http.StatusOK {
				// we received StatusOK which is the response for the whole content, not for the partial one
				stats.BodyLength = int(resp.ContentLength)
			}
			//reset to be not affected by the client.Do timeouts
			inactivityTimer.Reset(inactivityTimeout)
			var written int64
			for {
				var copyErr error
				if written, copyErr = io.CopyN(local, resp.Body, chunkSize); copyErr != nil && copyErr != io.EOF {
					copiedSize += written
					if innerCtx.Err() != nil {
						// the error comes from canceled context, which indicates inactivity timeout
						appendToErrorList(attempt, fmt.Errorf("inactivity for %s", inactivityTimeout))
					} else {
						appendToErrorList(attempt, fmt.Errorf("error from CopyN: %v", copyErr))
					}
					break
				}
				copiedSize += written
				// we read chunk of data from response on each iteration, if data length is a multiple of chunkSize
				// on the last iteration we will read 0 bytes and will hit written != chunkSize
				if written != chunkSize {
					// Must have reached EOF
					done = true
					break
				}
				//we received data so re-schedule inactivity timer
				inactivityTimer.Reset(inactivityTimeout)
				stats.Asize = copiedSize
				if prgNotify != nil {
					select {
					case prgNotify <- stats:
					default: //ignore we cannot write
					}
				}
			}
			if done {
				break
			}
			err = resp.Body.Close()
			if err != nil {
				appendToErrorList(attempt, fmt.Errorf("error close Body: %v", err))
			}
		}
		if !done {
			stats.Error = fmt.Errorf("%s: %s", host, strings.Join(errorList, "; "))
		}
		return stats
	case "post":
		file, err := os.Open(localFile)
		if err != nil {
			stats.Error = err
			return stats
		}
		defer file.Close()
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		part, err := writer.CreateFormFile(remoteFile, filepath.Base(localFile))
		if err != nil {
			stats.Error = err
			return stats
		}
		_, err = io.Copy(part, file)
		if err != nil {
			stats.Error = err
			return stats
		}
		err = writer.Close()
		if err != nil {
			stats.Error = err
			return stats
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, host, body)
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Accept", "*/*")
		req.Header.Set("Content-Type", writer.FormDataContentType())
		resp, err := client.Do(req)
		if err != nil {
			stats.Error = fmt.Errorf("request failed for post %s: %s",
				host, err)
			return stats
		} else {
			BODY := &bytes.Buffer{}
			_, err := BODY.ReadFrom(resp.Body)
			if err != nil {
				stats.Error = fmt.Errorf("post failed for %s: %s",
					host, err)
				return stats
			}
			resp.Body.Close()
		}
		Body, _ := ioutil.ReadAll(resp.Body)
		stats.Asize = int64(len(Body))
		if prgNotify != nil {
			select {
			case prgNotify <- stats:
			default: //ignore we cannot write
			}
		}
		stats.BodyLength = len(Body)
		return stats
	case "meta":
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, host, nil)
		if err != nil {
			stats.Error = fmt.Errorf("request failed for meta %s: %s",
				host, err)
			return stats
		}
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Content-Type", "application/octet-stream")

		resp, err := client.Do(req)
		if err != nil {
			stats.Error = fmt.Errorf("head failed for meta %s: %s",
				host, err)
			return stats
		}
		stats.ContentLength = resp.ContentLength
		return stats
	default:
		stats.Error = fmt.Errorf("unknown subcommand: %v", cmd)
		return stats
	}
}
