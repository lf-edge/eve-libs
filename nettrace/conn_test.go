// Copyright (c) 2026 Zededa, Inc.
// SPDX-License-Identifier: Apache-2.0

package nettrace

import (
	"sync"
	"testing"
	"time"
)

// DNS message bytes used across the malformed-parsing tests.
var (
	// malformedQueryReservedPrefix: valid 12-byte header (QDCOUNT=1) followed
	// by a question name whose first label byte is 0x40 (01xxxxxx = reserved),
	// which causes dnsmessage.Parser.Question() to fail without advancing the
	// parser offset.  The old code used "continue" here and looped forever.
	malformedQueryReservedPrefix = []byte{
		0x00, 0x01, // ID = 1
		0x01, 0x00, // Flags: RD=1 (standard recursive query)
		0x00, 0x01, // QDCOUNT = 1
		0x00, 0x00, // ANCOUNT = 0
		0x00, 0x00, // NSCOUNT = 0
		0x00, 0x00, // ARCOUNT = 0
		0x40, 0x00, // question name: 0x40 = reserved label prefix → parse error
	}

	// malformedReplyBadAnswerName: valid header (ANCOUNT=1, QDCOUNT=0) where
	// the answer resource-header name starts with the reserved prefix 0x40.
	// dnsmessage.Parser.AnswerHeader() fails without advancing; the old code
	// used "continue" here and looped forever.
	malformedReplyBadAnswerName = []byte{
		0x00, 0x02, // ID = 2
		0x84, 0x00, // Flags: QR=1, AA=1
		0x00, 0x00, // QDCOUNT = 0
		0x00, 0x01, // ANCOUNT = 1
		0x00, 0x00, // NSCOUNT = 0
		0x00, 0x00, // ARCOUNT = 0
		0x40, 0x00, // answer name: reserved prefix → AnswerHeader() error
	}

	// malformedReplyCNAMEBadRdata: header (ANCOUNT=1), one CNAME answer with a
	// valid resource header (name=root, type=CNAME, RDLENGTH=2) but malformed
	// RDATA (canonical name starts with reserved prefix 0x40).
	// After AnswerHeader() succeeds, CNAMEResource() fails without advancing;
	// the old code used "continue" here and looped forever.
	// The fix calls SkipAnswer() (which uses the known RDLENGTH=2 to advance
	// by byte-count, without re-parsing the name) before continuing the loop.
	malformedReplyCNAMEBadRdata = []byte{
		0x00, 0x03, // ID = 3
		0x84, 0x00, // Flags: QR=1, AA=1
		0x00, 0x00, // QDCOUNT = 0
		0x00, 0x01, // ANCOUNT = 1
		0x00, 0x00, // NSCOUNT = 0
		0x00, 0x00, // ARCOUNT = 0
		// Answer (CNAME):
		0x00,       // name: root label (".")
		0x00, 0x05, // TYPE: CNAME
		0x00, 0x01, // CLASS: IN
		0x00, 0x00, 0x00, 0x3c, // TTL: 60
		0x00, 0x02, // RDLENGTH: 2
		0x40, 0x00, // RDATA: canonical name with reserved prefix
	}

	// malformedCNAMEThenValidA: header (ANCOUNT=2): first a CNAME with malformed
	// RDATA, then a well-formed A record for 127.0.0.1.
	// The fix must skip the broken CNAME via SkipAnswer() and still parse the A.
	malformedCNAMEThenValidA = []byte{
		0x00, 0x04, // ID = 4
		0x84, 0x00, // Flags: QR=1, AA=1
		0x00, 0x00, // QDCOUNT = 0
		0x00, 0x02, // ANCOUNT = 2
		0x00, 0x00, // NSCOUNT = 0
		0x00, 0x00, // ARCOUNT = 0
		// Answer 1 (CNAME, malformed RDATA):
		0x00,       // name: root label
		0x00, 0x05, // TYPE: CNAME
		0x00, 0x01, // CLASS: IN
		0x00, 0x00, 0x00, 0x3c, // TTL: 60
		0x00, 0x02, // RDLENGTH: 2
		0x40, 0x00, // RDATA: reserved prefix → CNAMEResource() error
		// Answer 2 (A record: 127.0.0.1):
		0x00,       // name: root label
		0x00, 0x01, // TYPE: A
		0x00, 0x01, // CLASS: IN
		0x00, 0x00, 0x00, 0x3c, // TTL: 60
		0x00, 0x04, // RDLENGTH: 4
		0x7f, 0x00, 0x00, 0x01, // RDATA: 127.0.0.1
	}
)

// testTracer is a minimal networkTracer that records published traces.
type testTracer struct {
	mu     sync.Mutex
	traces []networkTrace
}

func (tt *testTracer) getTracerID() TraceID       { return "test-tracer" }
func (tt *testTracer) getRelTimestamp() Timestamp { return Timestamp{IsRel: true, Rel: 0} }
func (tt *testTracer) publishTrace(t networkTrace) {
	tt.mu.Lock()
	tt.traces = append(tt.traces, t)
	tt.mu.Unlock()
}
func (tt *testTracer) published() []networkTrace {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	cp := make([]networkTrace, len(tt.traces))
	copy(cp, tt.traces)
	return cp
}

// runWithTimeout runs fn in a goroutine and fails the test if fn does not
// return within the given deadline.  This catches infinite loops.
func runWithTimeout(t *testing.T, deadline time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
		// completed in time
	case <-time.After(deadline):
		t.Fatalf("function did not return within %v — likely an infinite loop", deadline)
	}
}

const parseTimeout = 3 * time.Second

// TestParseMalformedDNSQuery verifies that parseDNSQuery terminates (does not
// loop forever) when the question name contains a reserved label prefix.
func TestParseMalformedDNSQuery(t *testing.T) {
	tracer := &testTracer{}
	tc := newTracedConn(tracer, "test-conn", nil, &nilLogger{}, true, true)

	runWithTimeout(t, parseTimeout, func() {
		tc.parseDNSQuery(malformedQueryReservedPrefix, Timestamp{IsRel: true})
	})

	// A DNSQueryTraceEnv is still published (with no questions) so the caller
	// knows a query was sent — just verify the questions list is empty.
	var queryTrace *DNSQueryTraceEnv
	for _, tr := range tracer.published() {
		if qt, ok := tr.(DNSQueryTraceEnv); ok {
			queryTrace = &qt
			break
		}
	}
	if queryTrace == nil {
		t.Fatal("expected a DNSQueryTraceEnv to be published, got none")
	}
	if len(queryTrace.Questions) != 0 {
		t.Errorf("expected no parsed questions, got %d", len(queryTrace.Questions))
	}
}

// TestParseMalformedDNSReplyBadAnswerName verifies that parseDNSReply
// terminates when the answer resource-header name itself is malformed
// (AnswerHeader fails).
func TestParseMalformedDNSReplyBadAnswerName(t *testing.T) {
	tracer := &testTracer{}
	tc := newTracedConn(tracer, "test-conn", nil, &nilLogger{}, true, true)

	runWithTimeout(t, parseTimeout, func() {
		tc.parseDNSReply(malformedReplyBadAnswerName, Timestamp{IsRel: true})
	})
}

// TestParseMalformedDNSReplyCNAMEBadRdata verifies that parseDNSReply
// terminates when a CNAME answer has a well-formed resource header but
// malformed RDATA (reserved label prefix in the canonical name).
func TestParseMalformedDNSReplyCNAMEBadRdata(t *testing.T) {
	tracer := &testTracer{}
	tc := newTracedConn(tracer, "test-conn", nil, &nilLogger{}, true, true)

	runWithTimeout(t, parseTimeout, func() {
		tc.parseDNSReply(malformedReplyCNAMEBadRdata, Timestamp{IsRel: true})
	})
}

// TestParseMalformedDNSReplySkipsAndContinues verifies that when a CNAME answer
// has malformed RDATA, parseDNSReply skips it (via SkipAnswer) and still
// records the valid A record that follows it.
func TestParseMalformedDNSReplySkipsAndContinues(t *testing.T) {
	tracer := &testTracer{}
	tc := newTracedConn(tracer, "test-conn", nil, &nilLogger{}, true, true)

	runWithTimeout(t, parseTimeout, func() {
		tc.parseDNSReply(malformedCNAMEThenValidA, Timestamp{IsRel: true})
	})

	// A DNSReplyTrace must have been published containing the A record answer.
	var replyTrace *DNSReplyTrace
	for _, tr := range tracer.published() {
		if rt, ok := tr.(DNSReplyTrace); ok {
			replyTrace = &rt
			break
		}
	}
	if replyTrace == nil {
		t.Fatal("expected a DNSReplyTrace to be published, got none")
	}
	if len(replyTrace.Answers) != 1 {
		t.Fatalf("expected 1 answer (the valid A record), got %d", len(replyTrace.Answers))
	}
	got := replyTrace.Answers[0]
	if got.Type != DNSResTypeA {
		t.Errorf("expected answer type A, got %v", got.Type)
	}
	if got.ResolvedVal != "127.0.0.1" {
		t.Errorf("expected resolved value 127.0.0.1, got %q", got.ResolvedVal)
	}
}
