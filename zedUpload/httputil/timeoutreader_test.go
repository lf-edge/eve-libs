package http

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestTimeoutReaderSucceed(t *testing.T) {
	// Test that TimeoutReader succeeds when the underlying reader succeeds.
	r := NewTimeoutReader(1*time.Second, strings.NewReader("hello world"))
	p := make([]byte, 11)
	n, err := r.Read(p)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if n != 11 {
		t.Errorf("unexpected read size: %d", n)
	}
	if string(p) != "hello world" {
		t.Errorf("unexpected read: %q", string(p))
	}
}

func TestTimeoutReaderFail(t *testing.T) {
	// Test that TimeoutReader fails when the underlying reader fails.
	r := NewTimeoutReader(1*time.Second, blockedReader{delay: 2 * time.Second, reader: strings.NewReader("hello world")})
	p := make([]byte, 12)
	_, err := r.Read(p)
	if err == nil {
		t.Errorf("unexpected success")
	}
	if !errors.Is(err, &ErrTimeout{}) {
		t.Errorf("error was %v and not ErrTimeout", err)
	}
}

type blockedReader struct {
	delay  time.Duration
	reader io.Reader
}

func (r blockedReader) Read(p []byte) (n int, err error) {
	time.Sleep(r.delay)
	return r.reader.Read(p)
}
