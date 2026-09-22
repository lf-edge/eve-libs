// Copyright (c) 2026 Zededa, Inc.
// SPDX-License-Identifier: Apache-2.0

package nettrace

import (
	"context"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// stoppableTracer is a tracerWithDial that records published traces and whose
// tracing ends when done is closed.
type stoppableTracer struct {
	mu     sync.Mutex
	traces []networkTrace
	done   chan struct{}
}

func (st *stoppableTracer) getTracerID() TraceID            { return "stoppable-tracer" }
func (st *stoppableTracer) getRelTimestamp() Timestamp      { return Timestamp{IsRel: true} }
func (st *stoppableTracer) tracingDone() <-chan struct{}    { return st.done }
func (st *stoppableTracer) traceNewSocket(sock *inetSocket) {}
func (st *stoppableTracer) publishTrace(t networkTrace) {
	st.mu.Lock()
	st.traces = append(st.traces, t)
	st.mu.Unlock()
}

// TestDialContextWatcherStopsWithTracing checks that the goroutine watching a
// dial's context for closure does not outlive the tracer when that context is
// never cancelled, as is the case for some HTTP clients.
func TestDialContextWatcherStopsWithTracing(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() {
		err := listener.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			err = conn.Close()
			if err != nil {
				panic(err)
			}
		}
	}()

	tracer := &stoppableTracer{done: make(chan struct{})}
	dialer := newTracedDialer(tracer, logrus.New(), nil, time.Second, 0, false, nil, nil)
	// Cancellable, so that the dialer watches it, but never cancelled while
	// the tracer is in use.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baseline := runtime.NumGoroutine()
	const dials = 20
	for range dials {
		conn, err := dialer.dial(ctx, "tcp", listener.Addr().String())
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		err = conn.Close()
		if err != nil {
			panic(err)
		}
	}
	if got := runtime.NumGoroutine(); got < baseline+dials {
		t.Fatalf("expected one watcher per dial, goroutines went from %d to %d",
			baseline, got)
	}

	close(tracer.done)
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline+2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > baseline+2 {
		t.Fatalf("%d goroutines still running after tracing stopped, baseline %d",
			got, baseline)
	}
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	for _, trace := range tracer.traces {
		if dial, ok := trace.(DialTraceEnv); ok && dial.CTXClosed {
			t.Fatal("a context-closed trace was published although the context was never cancelled")
		}
	}
}
