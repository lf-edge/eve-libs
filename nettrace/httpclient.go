// Copyright (c) 2022 Zededa, Inc.
// SPDX-License-Identifier: Apache-2.0

package nettrace

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/golang-design/lockfree"
	"github.com/google/uuid"
	"github.com/mdlayher/netlink"
	"github.com/sirupsen/logrus"
	"github.com/ti-mo/conntrack"
	"go.etcd.io/bbolt"
	"golang.org/x/net/http2"
	"golang.org/x/sys/unix"
)

// HTTPClient wraps and enhances the standard HTTP client with tracing
// capabilities, i.e. monitoring and recording of network events related to the operations
// of the HTTP client, including HTTP requests made, TCP connections opened/attempted,
// TLS tunnels established/attempted, DNS queries sent, DNS answers received, etc.
type HTTPClient struct {
	// This lock protects all attributes of the HTTPClient except for lockfree Queues
	// which do not require locking.
	sync.Mutex
	id TraceID

	// The standard HTTP client is embedded and can be accessed simply as .Client
	// DO NOT change the Client.Transport field (to customize the HTTP client
	// behaviour), otherwise tracing functionality may get broken. Instead, configure
	// the desired behaviour of the HTTP client inside the HTTPClientCfg argument
	// of the HTTPClient constructor.
	*http.Client
	httpTransp *http.Transport

	// From the constructor config
	log                  Logger
	sourceIP             net.IP
	skipNameserver       NameserverSelector
	netProxy             func(req *http.Request) (*url.URL, error)
	withSockTrace        bool
	withDNSTrace         bool
	tcpHandshakeTimeout  time.Duration
	tcpKeepAliveInterval time.Duration

	// Network tracing
	nfConn           *conntrack.Conn
	tracingWG        sync.WaitGroup
	tracingCtx       context.Context
	cancelTracing    context.CancelFunc
	tracingStartedAt Timestamp
	pendingTraces    *lockfree.Queue // value: networkTrace
	noConnSockets    []*inetSocket   // not-yet connected AF_INET sockets
	connections      *evictingMap    // stores *connection, bucket="connections"
	dials            *evictingMap    // stores *dial, bucket="dials"
	tlsTuns          *evictingMap    // stores *tlsTun, bucket="tlsTuns"
	dnsQueries       *evictingMap    // stores *DNSQueryTrace, bucket="dnsQueries"
	httpReqs         *evictingMap    // stores *HTTPReqTrace, bucket="httpReqs"
	db               *bbolt.DB

	// Packet capture
	packetCapturer *packetCapturer // nil if disabled

	// unique UUID per HTTPClient instance
	uuid string
}

const (
	bucketConns         = "Connections"
	bucketDials         = "Dials"
	bucketTLSTuns       = "TlsTuns"
	bucketDNSQueries    = "DNSQueries"
	bucketHTTPReqs      = "HTTPReqs"
	bucketTCPConns      = "TCPConns"
	bucketUDPConns      = "UDPConns"
	bucketNoConnSockets = "NoConnSockets"
	netTraceFolder      = "/persist/nettrace/"
)

// NameserverSelector is a function that for a given nameserver decides
// whether it should be used for name resolution or skipped.
type NameserverSelector func(ipAddr net.IP, port uint16) (skip bool, reason string)

// HTTPClientCfg : configuration for the embedded HTTP client.
// This is not related to tracing but how the standard HTTP client itself should behave.
// Normally, HTTP client is configured by customizing the client's Transport
// (see https://pkg.go.dev/net/http#Transport).
// However, for the HTTP client tracing to function properly, Client.Transport,
// as installed and customized by the NewHTTPClient() constructor, should not be modified.
// The only allowed action is to additionally wrap the Transport with a RoundTripper
// implementation, which is allowed to for example modify HTTP requests/responses,
// but still should call the wrapped Transport for the HTTP request execution.
// An example of this is Transport from the oauth2 package, adding an Authorization
// header with a token: https://pkg.go.dev/golang.org/x/oauth2#Transport
type HTTPClientCfg struct {
	// PreferHTTP2, if true, will make the HTTP client to chose HTTP/2 as the preferred
	// HTTP version during the Application-Layer Protocol Negotiation (ALPN).
	PreferHTTP2 bool
	// SourceIP : source IP address to use for all connections and packets sent.
	// This includes all TCP connections opened for HTTP requests and UDP
	// packets sent with DNS requests.
	// Leave as nil to not bind sockets to any source IP address and instead let
	// the kernel to select the source IP address for each connection based on
	// the routing decision.
	SourceIP net.IP
	// SkipNameserver can be optionally provided as a callback to exclude some
	// of the system-wide configured DNS server(s) that would be otherwise used
	// for DNS queries.
	// The callback is called for every configured DNS server just before it is
	// queried. If the callback returns true, the server is skipped and the resolver
	// moves to the next one.
	// Every skipped nameserver is recorded in DialTrace.SkippedNameservers.
	SkipNameserver NameserverSelector
	// Proxy specifies a callback to return an address of a network proxy that
	// should be used for the given HTTP request.
	// If Proxy is nil or returns a nil *URL, no proxy is used.
	Proxy func(*http.Request) (*url.URL, error)
	// TLSClientConfig specifies the TLS configuration to use for TLS tunnels.
	// If nil, the default configuration is used.
	TLSClientConfig *tls.Config
	// ReqTimeout specifies a time limit for requests made by the HTTP client.
	// The timeout includes connection time, any redirects, and reading the response body.
	// The timer remains running after Get, Head, Post, or Do return and will interrupt
	// reading of the Response.Body.
	ReqTimeout time.Duration
	// TCPHandshakeTimeout specifies the maximum amount of time to wait for a TCP handshake
	// to complete. Zero means no timeout.
	TCPHandshakeTimeout time.Duration
	// TCPKeepAliveInterval specifies the interval between keep-alive probes for an active
	// TCP connection. If zero, keep-alive probes are sent with a default value (15 seconds),
	// if supported by the operating system.
	// If negative, keep-alive probes are disabled.
	TCPKeepAliveInterval time.Duration
	// TLSHandshakeTimeout specifies the maximum amount of time to wait for a TLS handshake
	// to complete. Zero means no timeout.
	TLSHandshakeTimeout time.Duration
	// DisableKeepAlive, if true, disables HTTP keep-alive and will only use the connection
	// to the server for a single HTTP request.
	DisableKeepAlive bool
	// DisableCompression, if true, prevents the Transport from requesting compression with
	// an "Accept-Encoding: gzip" request header when the Request contains no existing
	// Accept-Encoding value.
	DisableCompression bool
	// MaxIdleConns controls the maximum number of idle (keep-alive) connections across
	// all hosts. Zero means no limit.
	MaxIdleConns int
	// MaxIdleConnsPerHost, if non-zero, controls the maximum idle (keep-alive) connections
	// to keep per-host. If zero, DefaultMaxIdleConnsPerHost from the http package is used.
	MaxIdleConnsPerHost int
	// MaxConnsPerHost optionally limits the total number of connections per host,
	// including connections in the dialing, active, and idle states. On limit violation,
	// dials will block.
	// Zero means no limit.
	MaxConnsPerHost int
	// IdleConnTimeout is the maximum amount of time an idle (keep-alive) connection will
	// remain idle before closing itself.
	// Zero means no limit.
	IdleConnTimeout time.Duration
	// ResponseHeaderTimeout, if non-zero, specifies the amount of time to wait for a server's
	// response headers after fully writing the request (including its body, if any).
	// This time does not include the time to read the response body.
	ResponseHeaderTimeout time.Duration
	// ExpectContinueTimeout, if non-zero, specifies the amount of time to wait for a server's
	// first response headers after fully writing the request headers if the request has an
	// "Expect: 100-continue" header. Zero means no timeout and causes the body to be sent
	// immediately, without waiting for the server to approve.
	// This time does not include the time to send the request header.
	ExpectContinueTimeout time.Duration
}

// AF_INET socket.
// Used only until connection is made.
type inetSocket struct {
	addrTuple
	addrUpdateAt   Timestamp
	origFD         int
	dupFD          int // duplicated origFD; used to get socket name even after origFD was closed
	fromDial       TraceID
	fromResolvDial TraceID   // undefined if this socket was not opened by resolver
	createdAt      Timestamp // for TCP this is just before handshake
	origClosed     bool
	origClosedAt   Timestamp
	dupClosed      bool
	conntrack      conntrackEntry
}

type conntrackEntry struct {
	flow       *conntrack.Flow
	capturedAt Timestamp
	queriedAt  Timestamp // includes failed attempts
}

// TCP or UDP connection.
// Source/destination is from the client side.
type connection struct {
	addrTuple
	ID             TraceID
	SockCreatedAt  Timestamp
	ConnectedAt    Timestamp // for TCP this is just after handshake
	ClosedAt       Timestamp
	Reused         bool
	Closed         bool
	DialID         TraceID
	FromResolver   bool
	Conntrack      conntrackEntry
	TotalRecvBytes uint64
	TotalSentBytes uint64
	SocketOps      []SocketOp
}

// Single attempt to establish TCP connection.
type dial struct {
	DialTrace
	httpReqID TraceID
}

// Single TLS tunnel.
type tlsTun struct {
	TLSTunnelTrace // TCPConn is not always set here
	httpReqID      TraceID
}

// NewHTTPClient creates a new instance of HTTPClient, enhancing the standard
// http.Client with tracing capabilities.
// Tracing starts immediately:
//   - a background Go routine collecting traces is started
//   - packet capture starts on selected interfaces if WithPacketCapture option was passed
func NewHTTPClient(config HTTPClientCfg, traceOpts ...TraceOpt) (*HTTPClient, error) {
	client := &HTTPClient{
		id:             IDGenerator(),
		log:            &nilLogger{},
		sourceIP:       config.SourceIP,
		skipNameserver: config.SkipNameserver,
		netProxy:       config.Proxy,
		pendingTraces:  lockfree.NewQueue(),
	}

	// create netTrace folder
	if err := os.MkdirAll(netTraceFolder, 0755); err != nil {
		return nil, err
	}
	// Generate a unique UUID for this HTTPClient instance. This is important for
	// the netrace files to be unique and not overwrite each other.
	client.uuid = uuid.New().String()

	err := client.resetTraces(true) // initialize maps
	if err != nil {
		return nil, err
	}

	client.tracingCtx, client.cancelTracing = context.WithCancel(context.Background())
	client.tcpHandshakeTimeout = config.TCPHandshakeTimeout
	client.tcpKeepAliveInterval = config.TCPKeepAliveInterval
	client.httpTransp = &http.Transport{
		Proxy:                 client.proxyForRequest,
		DialContext:           client.dial,
		TLSClientConfig:       config.TLSClientConfig,
		TLSHandshakeTimeout:   config.TLSHandshakeTimeout,
		DisableKeepAlives:     config.DisableKeepAlive,
		DisableCompression:    config.DisableCompression,
		MaxIdleConns:          config.MaxIdleConns,
		MaxIdleConnsPerHost:   config.MaxIdleConnsPerHost,
		MaxConnsPerHost:       config.MaxConnsPerHost,
		IdleConnTimeout:       config.IdleConnTimeout,
		ResponseHeaderTimeout: config.ResponseHeaderTimeout,
		ForceAttemptHTTP2:     config.PreferHTTP2,
		ExpectContinueTimeout: config.ExpectContinueTimeout,
	}
	if config.PreferHTTP2 {
		err := http2.ConfigureTransport(client.httpTransp)
		if err != nil {
			return nil, err
		}
	}
	var withPcap *WithPacketCapture
	var withHTTP *WithHTTPReqTrace
	for _, traceOpt := range traceOpts {
		if topt, withDefaults := traceOpt.(TraceOptWithDefaults); withDefaults {
			topt.setDefaults()
		}
		switch opt := traceOpt.(type) {
		case *WithLogging:
			if opt.CustomLogger != nil {
				client.log = opt.CustomLogger
			} else {
				client.log = logrus.New()
			}
		case *WithConntrack:
			client.nfConn, err = conntrack.Dial(&netlink.Config{})
			if err != nil {
				return nil, fmt.Errorf("nettrace: failed to connect to netfilter: %v", err)
			}
		case *WithSockTrace:
			client.withSockTrace = true
		case *WithDNSQueryTrace:
			client.withDNSTrace = true
		case *WithHTTPReqTrace:
			withHTTP = opt
		case *WithPacketCapture:
			withPcap = opt
		}
	}
	if withPcap != nil {
		client.packetCapturer = newPacketCapturer(client, client.log, *withPcap)
	}
	var rt http.RoundTripper
	if withHTTP != nil {
		rt = newTracedRoundTripper(client, *withHTTP)
	} else {
		rt = client.httpTransp
	}
	client.Client = &http.Client{
		Transport: rt,
		Timeout:   config.ReqTimeout,
	}
	if client.packetCapturer != nil {
		err := client.packetCapturer.startPcap(client.tracingCtx, &client.tracingWG)
		if err != nil {
			return nil, err
		}
	}
	client.tracingWG.Add(1)
	go client.runTracing()
	client.log.Tracef("nettrace: created new HTTPClient id=%s", client.id)
	return client, nil
}

func (c *HTTPClient) flushEvictedBatch(batch []finalizedTrace) {
	err := c.writeBatchToBolt(batch)
	if err != nil {
		c.log.Errorf("Failed to write eviction batch to Bolt: %v", err)
	}
}

func (c *HTTPClient) getTracerID() TraceID {
	return c.id
}

// Get timestamp for the current time relative to when racing started.
func (c *HTTPClient) getRelTimestamp() Timestamp {
	c.Lock()
	defer c.Unlock()
	return c.getRelTimestampNolock()
}

// Get timestamp for the current time relative to when racing started.
func (c *HTTPClient) getRelTimestampNolock() Timestamp {
	return c.tracingStartedAt.Elapsed()
}

// Publish newly recorded networkTrace into the queue for processing.
func (c *HTTPClient) publishTrace(t networkTrace) {
	c.pendingTraces.Enqueue(t)
}

// resetTraces : recreates all maps holding recorded network traces and pcaps.
func (c *HTTPClient) resetTraces(delOpenConns bool) error {
	c.Lock()
	defer c.Unlock()

	// 2) Open DB on first use.
	if c.db == nil {
		db, err := bbolt.Open(netTraceFolder+"nettrace_"+c.uuid+".db", 0666, nil)
		if err != nil {
			return fmt.Errorf("open db: %w", err)
		}
		c.db = db
	}
	db := c.db

	// 3) In one transaction, ensure Connections exists,
	//    then wipe & recreate the other buckets (and Connections if requested).
	if err := db.Update(func(tx *bbolt.Tx) error {
		// Always ensure Connections bucket exists
		if tx.Bucket([]byte(bucketConns)) == nil {
			if _, err := tx.CreateBucket([]byte(bucketConns)); err != nil {
				return fmt.Errorf("create bucket %s: %w", bucketConns, err)
			}
		}

		// Buckets we always reset:
		buckets := []string{
			bucketDials,
			bucketTLSTuns,
			bucketDNSQueries,
			bucketHTTPReqs,
			bucketConns,
			bucketTCPConns,
			bucketUDPConns,
		}

		// Conditionally reset Connections as well
		if delOpenConns {
			buckets = append(buckets, bucketConns)
		}

		for _, name := range buckets {
			// Delete old bucket (ignore if missing)
			if err := tx.DeleteBucket([]byte(name)); err != nil && err != bbolt.ErrBucketNotFound {
				return fmt.Errorf("delete bucket %s: %w", name, err)
			}
			// Recreate it empty
			if _, err := tx.CreateBucket([]byte(name)); err != nil {
				return fmt.Errorf("create bucket %s: %w", name, err)
			}
		}
		return nil
	}); err != nil {
		c.log.Errorf("db.Update failed: %w", err)
	}
	// Make sure that all pending traces for open connections are processed.
	prevStart := c.tracingStartedAt
	c.tracingStartedAt = Timestamp{Abs: time.Now()}
	c.noConnSockets = []*inetSocket{}
	c.dials = newEvictingMap(5000, "dials", 100, c.flushEvictedBatch)
	c.tlsTuns = newEvictingMap(5000, "tlsTuns", 100, c.flushEvictedBatch)
	c.dnsQueries = newEvictingMap(5000, "dnsQueries", 100, c.flushEvictedBatch)
	c.httpReqs = newEvictingMap(5000, "httpReqs", 100, c.flushEvictedBatch)
	if delOpenConns {
		c.connections = newEvictingMap(5000, "connections", 100, c.flushEvictedBatch)
	} else {
		// Keep open connections, just turn relative timestamps into absolute ones.
		// (otherwise they would turn negative)
		for _, id := range c.connections.order {
			val, ok := c.connections.Get(id)
			if !ok {
				continue
			}
			conn, ok := val.(*connection)
			if !ok {
				continue
			}
			if !conn.Closed {
				conn.Reused = true
				if !conn.SockCreatedAt.Undefined() && conn.SockCreatedAt.IsRel {
					conn.SockCreatedAt = prevStart.Add(conn.SockCreatedAt)
				}
				if !conn.ConnectedAt.Undefined() && conn.ConnectedAt.IsRel {
					conn.ConnectedAt = prevStart.Add(conn.ConnectedAt)
				}
				if !conn.ClosedAt.Undefined() && conn.ClosedAt.IsRel {
					conn.ClosedAt = prevStart.Add(conn.ClosedAt)
				}
				if !conn.Conntrack.capturedAt.Undefined() && conn.Conntrack.capturedAt.IsRel {
					conn.Conntrack.capturedAt = prevStart.Add(conn.Conntrack.capturedAt)
				}
				conn.Conntrack.queriedAt = Timestamp{} // Reset to undefined timestamp.
			} else {
				c.connections.Delete(id)
			}
		}
	}
	c.processPendingTraces(delOpenConns)

	if c.packetCapturer != nil {
		c.packetCapturer.clearPcap()
	}

	return nil
}

// ExportNetTraceToJSONFile exports the nettrace to a JSON file.
func (c *HTTPClient) ExportNetTraceToJSONFile(filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	if _, err := file.Write([]byte(`{"description":"nettrace_cache",`)); err != nil {
		c.log.Errorf("Failed to write JSON header: %v", err)
	}

	// --- TraceBeginAt and TraceEndAt ---
	if _, err := file.Write([]byte(`"traceBeginAt":`)); err != nil {
		c.log.Errorf("Failed to write traceBeginAt: %v", err)
	}
	if err := enc.Encode(c.tracingStartedAt); err != nil {
		return err
	}
	if _, err := file.Write([]byte(`,"traceEndAt":`)); err != nil {
		c.log.Errorf("Failed to write traceEndAt: %v", err)
	}
	if err := enc.Encode(c.getRelTimestampNolock()); err != nil {
		return err
	}

	// --- Dials ---
	if _, err := file.Write([]byte(`,"dials":[`)); err != nil {
		c.log.Errorf("Failed to write dials header: %v", err)
	}
	first := true
	for _, id := range c.dials.order {
		if dial, ok := c.dials.store[id].(*dial); ok {
			if !first {
				if _, err := file.Write([]byte(",")); err != nil {
					c.log.Errorf("Failed to write comma separator in dials: %v", err)
					return err
				}
			}
			if err := enc.Encode(dial.DialTrace); err != nil {
				return err
			}
			first = false
		}
	}
	if err := c.streamBoltBucketToJSON("dials", file, enc, &first); err != nil {
		return err
	}
	if _, err := file.Write([]byte(`],`)); err != nil {
		c.log.Errorf("Failed to write dials end: %v", err)
	}

	// --- Connections ---
	if _, err := file.Write([]byte(`"tcpConns":[`)); err != nil {
		c.log.Errorf("Failed to write tcpConns header: %v", err)
	}
	first = true
	for _, id := range c.connections.order {
		if conn, ok := c.connections.store[id].(*connection); ok && conn.proto == syscall.IPPROTO_TCP {
			if !first {
				if _, err := file.Write([]byte(",")); err != nil {
					c.log.Errorf("Failed to write connection comma: %v", err)
				}
			}
			tcp := TCPConnTrace{
				TraceID:          conn.ID,
				FromDial:         conn.DialID,
				FromResolver:     conn.FromResolver,
				HandshakeBeginAt: conn.SockCreatedAt,
				HandshakeEndAt:   conn.ConnectedAt,
				Connected:        true,
				ConnCloseAt:      conn.ClosedAt,
				AddrTuple:        conn.addrTuple.toExportedAddrTuple(),
				Reused:           conn.Reused,
				TotalSentBytes:   conn.TotalSentBytes,
				TotalRecvBytes:   conn.TotalRecvBytes,
				Conntract:        conntrackToExportedEntry(conn.Conntrack.flow, conn.Conntrack.capturedAt),
				SocketTrace:      &SocketTrace{SocketOps: conn.SocketOps},
			}
			if err := enc.Encode(tcp); err != nil {
				return err
			}
			first = false
		}
	}
	if err := c.streamBoltConnectionsToJSON(syscall.IPPROTO_TCP, file, enc, &first); err != nil {
		return err
	}
	if _, err := file.Write([]byte(`],`)); err != nil {
		c.log.Errorf("Failed to write tcpConns end: %v", err)
	}

	if _, err := file.Write([]byte(`"udpConns":[`)); err != nil {
		c.log.Errorf("Failed to write udpConns header: %v", err)
	}
	first = true
	for _, id := range c.connections.order {
		if conn, ok := c.connections.store[id].(*connection); ok && conn.proto == syscall.IPPROTO_UDP {
			if !first {
				if _, err := file.Write([]byte(",")); err != nil {
					c.log.Errorf("Failed to write comma separator in udpConns: %v", err)
				}
			}
			udp := UDPConnTrace{
				TraceID:        conn.ID,
				FromDial:       conn.DialID,
				FromResolver:   conn.FromResolver,
				SocketCreateAt: conn.SockCreatedAt,
				ConnCloseAt:    conn.ClosedAt,
				AddrTuple:      conn.addrTuple.toExportedAddrTuple(),
				TotalSentBytes: conn.TotalSentBytes,
				TotalRecvBytes: conn.TotalRecvBytes,
				Conntract:      conntrackToExportedEntry(conn.Conntrack.flow, conn.Conntrack.capturedAt),
				SocketTrace:    &SocketTrace{SocketOps: conn.SocketOps},
			}
			if err := enc.Encode(udp); err != nil {
				return err
			}
			first = false
		}
	}
	if err := c.streamBoltConnectionsToJSON(syscall.IPPROTO_UDP, file, enc, &first); err != nil {
		return err
	}
	if _, err := file.Write([]byte(`],`)); err != nil {
		c.log.Errorf("Failed to write udpConns end: %v", err)
	}

	// --- DNS Queries ---
	if _, err := file.Write([]byte(`"dnsQueries":[`)); err != nil {
		c.log.Errorf("Failed to write dnsQueries header: %v", err)
	}
	first = true
	for _, id := range c.dnsQueries.order {
		if dnsQuery, ok := c.dnsQueries.store[id].(*DNSQueryTrace); ok {
			if !first {
				if !first {
					if _, err := file.Write([]byte(",")); err != nil {
						c.log.Errorf("Failed to write comma separator in dnsQueries: %v", err)
					}
				}
			}
			if err := enc.Encode(dnsQuery); err != nil {
				return err
			}
			first = false
		}
	}
	if err := c.streamBoltBucketToJSON("dnsQueries", file, enc, &first); err != nil {
		return err
	}
	if _, err := file.Write([]byte(`],`)); err != nil {
		c.log.Errorf("Failed to write dnsQueries end: %v", err)
	}

	// --- HTTP Requests ---
	if _, err := file.Write([]byte(`"httpRequests":[`)); err != nil {
		c.log.Errorf("Failed to write httpRequests header: %v", err)
	}
	first = true
	for _, id := range c.httpReqs.order {
		if req, ok := c.httpReqs.store[id].(*HTTPReqTrace); ok {
			if !first {
				if _, err := file.Write([]byte(",")); err != nil {
					c.log.Errorf("Failed to write comma separator in httpRequests: %v", err)
				}
			}
			if err := enc.Encode(req); err != nil {
				return err
			}
			first = false
		}
	}
	if err := c.streamBoltBucketToJSON("httpReqs", file, enc, &first); err != nil {
		return err
	}
	if _, err := file.Write([]byte(`],`)); err != nil {
		c.log.Errorf("Failed to write httpRequests end: %v", err)
	}

	// --- TLS Tunnels ---
	if _, err := file.Write([]byte(`"tlsTunnels":[`)); err != nil {
		c.log.Errorf("Failed to write tlsTunnels header: %v", err)
	}
	first = true
	for _, id := range c.tlsTuns.order {
		if tun, ok := c.tlsTuns.store[id].(*tlsTun); ok {
			if !first {
				if _, err := file.Write([]byte(",")); err != nil {
					c.log.Errorf("Failed to write comma separator in tlsTunnels: %v", err)
				}
			}
			if err := enc.Encode(tun.TLSTunnelTrace); err != nil {
				return err
			}
			first = false
		}
	}
	if err := c.streamBoltBucketToJSON("tlsTuns", file, enc, &first); err != nil {
		return err
	}
	if _, err := file.Write([]byte(`]`)); err != nil {
		c.log.Errorf("Failed to write tlsTunnels end: %v", err)
	}

	if _, err := file.Write([]byte(`}`)); err != nil {
		c.log.Errorf("Failed to write final JSON end: %v", err)
	}
	return nil
}

func (c *HTTPClient) streamBoltConnectionsToJSON(proto uint8, file *os.File, enc *json.Encoder, first *bool) error {
	return c.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketConns))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var conn connection
			if err := json.Unmarshal(v, &conn); err != nil {
				return err
			}
			if conn.proto != proto {
				return nil
			}
			if !*first {
				if _, err := file.Write([]byte(",")); err != nil {
					c.log.Errorf("Failed to write comma separator in bolt connections: %v", err)
				}
			}
			if proto == syscall.IPPROTO_TCP {
				tcp := TCPConnTrace{
					TraceID:          conn.ID,
					FromDial:         conn.DialID,
					FromResolver:     conn.FromResolver,
					HandshakeBeginAt: conn.SockCreatedAt,
					HandshakeEndAt:   conn.ConnectedAt,
					Connected:        true,
					ConnCloseAt:      conn.ClosedAt,
					AddrTuple:        conn.addrTuple.toExportedAddrTuple(),
					Reused:           conn.Reused,
					TotalSentBytes:   conn.TotalSentBytes,
					TotalRecvBytes:   conn.TotalRecvBytes,
					Conntract:        conntrackToExportedEntry(conn.Conntrack.flow, conn.Conntrack.capturedAt),
					SocketTrace:      &SocketTrace{SocketOps: conn.SocketOps},
				}
				if err := enc.Encode(tcp); err != nil {
					return err
				}
			} else {
				udp := UDPConnTrace{
					TraceID:        conn.ID,
					FromDial:       conn.DialID,
					FromResolver:   conn.FromResolver,
					SocketCreateAt: conn.SockCreatedAt,
					ConnCloseAt:    conn.ClosedAt,
					AddrTuple:      conn.addrTuple.toExportedAddrTuple(),
					TotalSentBytes: conn.TotalSentBytes,
					TotalRecvBytes: conn.TotalRecvBytes,
					Conntract:      conntrackToExportedEntry(conn.Conntrack.flow, conn.Conntrack.capturedAt),
					SocketTrace:    &SocketTrace{SocketOps: conn.SocketOps},
				}
				if err := enc.Encode(udp); err != nil {
					return err
				}
			}
			*first = false
			return nil
		})
	})
}

func (c *HTTPClient) streamBoltBucketToJSON(bucketName string, file *os.File, enc *json.Encoder, first *bool) error {
	return c.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return nil // Empty bucket, nothing to write
		}
		return b.ForEach(func(k, v []byte) error {
			switch bucketName {
			case "dials":
				var dial DialTrace
				if err := json.Unmarshal(v, &dial); err != nil {
					return err
				}
				if !*first {
					if _, err := file.Write([]byte(",")); err != nil {
						c.log.Errorf("Failed to write comma separator in dials: %v", err)
					}
				}
				if err := enc.Encode(dial); err != nil {
					return err
				}
			case "dnsQueries":
				var dns DNSQueryTrace
				if err := json.Unmarshal(v, &dns); err != nil {
					return err
				}
				if !*first {
					if _, err := file.Write([]byte(",")); err != nil {
						c.log.Errorf("Failed to write comma separator in dnsQueries: %v", err)
					}
				}
				if err := enc.Encode(dns); err != nil {
					return err
				}
			case "httpReqs":
				var req HTTPReqTrace
				if err := json.Unmarshal(v, &req); err != nil {
					return err
				}
				if !*first {
					if _, err := file.Write([]byte(",")); err != nil {
						c.log.Errorf("Failed to write comma separator in httpReqs: %v", err)
					}
				}
				if err := enc.Encode(req); err != nil {
					return err
				}
			case "tlsTuns":
				var tls TLSTunnelTrace
				if err := json.Unmarshal(v, &tls); err != nil {
					return err
				}
				if !*first {
					if _, err := file.Write([]byte(",")); err != nil {
						c.log.Errorf("Failed to write comma separator in tlsTunnels: %v", err)
					}
				}
				if err := enc.Encode(tls); err != nil {
					return err
				}
			default:
				// Unknown or unsupported bucket; skip silently or log a warning if desired
				return nil
			}
			*first = false
			return nil
		})
	})
}

// GetTrace returns a summary of all network and HTTP trace records (aka HTTPTrace),
// collected since the tracing last (re)started (either when the client was created
// or when the last ClearTrace() was called).
// This will include packet capture for every selected interface if it was enabled.
// The method allows to insert some description into the returned HTTPTrace
// (e.g. “download image XYZ”).
// Note that .TraceEndAt of the returned HTTPTrace is set to the current time.
// Also note that this does not stop tracing or clears the collected traces - use Close()
// or ClearTrace() for that.
func (c *HTTPClient) GetTrace(description string) (HTTPTrace, []PacketCapture, error) {
	c.Lock()
	defer c.Unlock()

	// export net traces to JSON file
	nettraceName := "nettrace_" + c.uuid + ".json"
	err := c.ExportNetTraceToJSONFile(netTraceFolder + nettraceName)
	if err != nil {
		c.log.Errorf("Failed to export net trace to JSON file: %v", err)
	}
	// Last-minute processing of collected traces...
	c.processPendingTraces(false)
	c.periodicSockUpdate(true)
	c.periodicConnUpdate(true)

	// Collect captured packets.
	var pcaps []PacketCapture
	if c.packetCapturer != nil {
		pcaps = c.packetCapturer.getPcap()
	}

	// Combine all network traces into one HTTPTrace.
	httpTrace := HTTPTrace{NetTrace: NetTrace{
		Description:  description,
		TraceBeginAt: c.tracingStartedAt,
		TraceEndAt:   c.getRelTimestampNolock(),
		UUID:         c.uuid,
	}}

	ids := append([]TraceID(nil), c.dials.order...) // safe copy

	for _, id := range ids {
		if dial, ok := c.dials.store[id].(*dial); ok {
			httpTrace.Dials = append(httpTrace.Dials, dial.DialTrace)
		}
		c.dials.Delete(id)
	}

	for _, sock := range c.noConnSockets {
		conntrack := conntrackToExportedEntry(sock.conntrack.flow, sock.conntrack.capturedAt)
		switch sock.proto {
		case syscall.IPPROTO_TCP:
			httpTrace.TCPConns = append(httpTrace.TCPConns, TCPConnTrace{
				TraceID:          IDGenerator(),
				FromDial:         sock.fromDial,
				FromResolver:     !sock.fromResolvDial.Undefined(),
				HandshakeBeginAt: sock.createdAt,
				HandshakeEndAt:   sock.origClosedAt,
				Connected:        false,
				AddrTuple:        sock.addrTuple.toExportedAddrTuple(),
				Reused:           false,
				Conntract:        conntrack,
			})
		case syscall.IPPROTO_UDP:
			httpTrace.UDPConns = append(httpTrace.UDPConns, UDPConnTrace{
				TraceID:        IDGenerator(),
				FromDial:       sock.fromDial,
				FromResolver:   !sock.fromResolvDial.Undefined(),
				SocketCreateAt: sock.createdAt,
				AddrTuple:      sock.addrTuple.toExportedAddrTuple(),
				Conntract:      conntrack,
			})
		}
	}

	ids = append([]TraceID(nil), c.connections.order...) // safe copy

	for _, id := range ids {
		connIface := c.connections.store[id]
		conn, ok := connIface.(*connection)
		if !ok {
			continue
		}
		conntrack := conntrackToExportedEntry(conn.Conntrack.flow, conn.Conntrack.capturedAt)
		var socketTrace *SocketTrace
		if c.withSockTrace {
			socketTrace = &SocketTrace{SocketOps: conn.SocketOps}
		}
		switch conn.proto {
		case syscall.IPPROTO_TCP:
			httpTrace.TCPConns = append(httpTrace.TCPConns, TCPConnTrace{
				TraceID:          conn.ID,
				FromDial:         conn.DialID,
				FromResolver:     conn.FromResolver,
				HandshakeBeginAt: conn.SockCreatedAt,
				HandshakeEndAt:   conn.ConnectedAt,
				Connected:        true,
				ConnCloseAt:      conn.ClosedAt,
				AddrTuple:        conn.addrTuple.toExportedAddrTuple(),
				Reused:           conn.Reused,
				TotalSentBytes:   conn.TotalSentBytes,
				TotalRecvBytes:   conn.TotalRecvBytes,
				Conntract:        conntrack,
				SocketTrace:      socketTrace,
			})
		case syscall.IPPROTO_UDP:
			httpTrace.UDPConns = append(httpTrace.UDPConns, UDPConnTrace{
				TraceID:        conn.ID,
				FromDial:       conn.DialID,
				FromResolver:   conn.FromResolver,
				SocketCreateAt: conn.SockCreatedAt,
				ConnCloseAt:    conn.ClosedAt,
				AddrTuple:      conn.addrTuple.toExportedAddrTuple(),
				TotalSentBytes: conn.TotalSentBytes,
				TotalRecvBytes: conn.TotalRecvBytes,
				Conntract:      conntrack,
				SocketTrace:    socketTrace,
			})
		}

		// Safe deletion after using the copied slice
		c.connections.Delete(id)
	}

	ids = append([]TraceID(nil), c.dnsQueries.order...) // copy order slice
	for _, id := range ids {
		dnsIface := c.dnsQueries.store[id]
		dnsQuery, ok := dnsIface.(*DNSQueryTrace)
		if !ok {
			continue
		}
		if dnsQuery.FromDial.Undefined() {
			if connIface, ok := c.connections.store[dnsQuery.Connection]; ok {
				if connTrace, ok := connIface.(*connection); ok {
					dnsQuery.FromDial = connTrace.DialID
				}
			}
		}
		httpTrace.DNSQueries = append(httpTrace.DNSQueries, *dnsQuery)
		c.dnsQueries.Delete(id)
	}

	ids = append([]TraceID(nil), c.httpReqs.order...) // reuse ids variable
	for _, id := range ids {
		reqIface := c.httpReqs.store[id]
		httpReq, ok := reqIface.(*HTTPReqTrace)
		if !ok {
			continue
		}
		if httpReq.TCPConn.Undefined() {
			for _, dialID := range c.dials.order {
				dIface := c.dials.store[dialID]
				if dial, ok := dIface.(*dial); ok {
					if !dial.httpReqID.Undefined() && dial.httpReqID == httpReq.TraceID {
						httpReq.TCPConn = dial.EstablishedConn
					}
				}
			}
		}
		httpTrace.HTTPRequests = append(httpTrace.HTTPRequests, *httpReq)
		c.httpReqs.Delete(id)
	}

	ids = append([]TraceID(nil), c.tlsTuns.order...) // reuse ids variable
	for _, id := range ids {
		tIface := c.tlsTuns.store[id]
		tlsTun, ok := tIface.(*tlsTun)
		if !ok {
			continue
		}
		if tlsTun.TCPConn.Undefined() {
			if rIface, ok := c.httpReqs.store[tlsTun.httpReqID]; ok {
				if httpReq, ok := rIface.(*HTTPReqTrace); ok {
					tlsTun.TCPConn = httpReq.TCPConn
				}
			}
		}
		httpTrace.TLSTunnels = append(httpTrace.TLSTunnels, tlsTun.TLSTunnelTrace)
		c.tlsTuns.Delete(id)
	}

	return httpTrace, pcaps, nil
}

// ClearTrace effectively restarts tracing by removing all traces collected up to
// this point. If packet capture is enabled (WithPacketCapture), packets captured
// so far are deleted.
// However, note that if TCP connection is reused from a previous run, it will reappear
// in the HTTPTrace (returned by GetTrace()) with some attributes restored to their previously
// recorded values (like .HandshakeBeginAt) and some updated (for example .Reused will be set
// to true).
func (c *HTTPClient) ClearTrace() error {
	return c.resetTraces(false)
}

// Close stops tracing of the embedded HTTP client, including packet capture if it
// was enabled.
// After this, it would be invalid to call GetTrace(), ClearTrace() or even to keep using
// the embedded HTTP Client.
func (c *HTTPClient) Close() error {
	c.cancelTracing()
	c.tracingWG.Wait()
	return c.resetTraces(true)
}

// runTracing is a separate Go routine that:
//   - processes collected network traces
//   - runs filtering of captured packets
//   - tries to obtain source IP + port for every traced socket
//   - tries to update conntrack entry for every traced connection
func (c *HTTPClient) runTracing() {
	defer c.tracingWG.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.tracingCtx.Done():
			c.log.Tracef("nettrace: networkTracer id=%s: network tracing stopped\n", c.id)
			return
		case <-ticker.C:
			c.Lock()
			c.processPendingTraces(false)
			c.periodicSockUpdate(false)
			c.periodicConnUpdate(false)
			if c.packetCapturer != nil {
				if c.packetCapturer.readyToFilterPcap() {
					c.packetCapturer.filterPcap()
				}
			}
			c.Unlock()
		}
	}
}

// periodicSockUpdate periodically retries to get source IP+port for non-yet-connected
// AF_INET sockets (if still not available) and updates obtained conntrack entries.
// The function should be called with HTTPClient locked.
func (c *HTTPClient) periodicSockUpdate(gettingTrace bool) {
	now := c.getRelTimestampNolock()
	// How frequently to retry to get source IP and source port for an AF_INET socket.
	const addrRetryPeriod = 3 * time.Second
	// How frequently to update conntrack entry for not-yet-established connection.
	const conntrackUpdatePeriod = 5 * time.Second
	for _, sock := range c.noConnSockets {
		if sock.dupClosed {
			// This socket is no longer actively traced.
			continue
		}
		// Check if the original FD is still open.
		if !sock.origClosed {
			ret, err := unix.FcntlInt(uintptr(sock.origFD), unix.F_GETFD, 0)
			if errno, ok := err.(syscall.Errno); ret == -1 && ok {
				if errno == syscall.EBADF {
					sock.origClosed = true
					sock.origClosedAt = now
				}
			}
		}
		// Try to get source IP + port if we still do not have it for this socket.
		if !sock.withSrcAddr() {
			if gettingTrace || sock.origClosed || sock.addrUpdateAt.Undefined() ||
				now.Sub(sock.addrUpdateAt) >= addrRetryPeriod {
				c.getSockSrcAddr(sock)
			}
		}
		// Update conntrack entry if it is too old.
		if c.nfConn != nil && sock.withSrcAddr() {
			if gettingTrace || sock.origClosed || sock.conntrack.queriedAt.Undefined() ||
				now.Sub(sock.conntrack.queriedAt) >= conntrackUpdatePeriod {
				c.getConntrack(sock.addrTuple, &sock.conntrack, now)
			}
		}
		if sock.origClosed {
			c.closeSockDupFD(sock)
		}
	}
}

func (c *HTTPClient) getConntrack(addr addrTuple, entry *conntrackEntry, now Timestamp) {
	flow, err := c.nfConn.Get(conntrack.Flow{
		TupleOrig: conntrack.Tuple{
			IP: conntrack.IPTuple{
				SourceAddress:      addr.srcIP,
				DestinationAddress: addr.dstIP,
			},
			Proto: conntrack.ProtoTuple{
				Protocol:        addr.proto,
				SourcePort:      addr.srcPort,
				DestinationPort: addr.dstPort,
			},
		},
	})
	entry.queriedAt = now
	if err != nil {
		c.log.Warningf("nettrace: networkTracer id=%s: "+
			"failed to get conntrack entry for connection %v: %v",
			c.id, addr, err)
		return
	}
	entry.capturedAt = now
	entry.flow = &flow
}

func (c *HTTPClient) getSockSrcAddr(sock *inetSocket) {
	sa, err := syscall.Getsockname(sock.dupFD)
	if err != nil {
		c.log.Warningf("nettrace: networkTracer id=%s: "+
			"failed to get src IP+port for duplicated FD %d: %v",
			c.id, sock.dupFD, err)
	} else if sa != nil {
		if laddr4, ok := sa.(*syscall.SockaddrInet4); ok {
			sock.srcPort = uint16(laddr4.Port)
			sock.srcIP = laddr4.Addr[:]
		} else if laddr6, ok := sa.(*syscall.SockaddrInet6); ok {
			sock.srcPort = uint16(laddr6.Port)
			sock.srcIP = laddr6.Addr[:]
		}
	}
}

func (c *HTTPClient) closeSockDupFD(sock *inetSocket) {
	err := syscall.Close(sock.dupFD)
	if err != nil {
		c.log.Warningf("nettrace: networkTracer id=%s: "+
			"failed to close duplicated FD %d: %v", c.id, sock.dupFD, err)
	}
	sock.dupClosed = true
}

// periodicConnUpdate periodically updates obtained conntrack entries
// for established connections.
// The function should be called with HTTPClient locked.
func (c *HTTPClient) periodicConnUpdate(gettingTrace bool) {
	now := c.getRelTimestampNolock()
	const conntrackUpdatePeriod = 20 * time.Second

	for _, id := range c.connections.order {
		connIface := c.connections.store[id]
		conn, ok := connIface.(*connection)
		if !ok || conn.Closed {
			continue
		}
		if c.nfConn != nil {
			if gettingTrace || conn.Conntrack.queriedAt.Undefined() ||
				now.Sub(conn.Conntrack.queriedAt) >= conntrackUpdatePeriod {
				c.getConntrack(conn.addrTuple, &conn.Conntrack, now)
			}
		}
	}
}

type finalizedTrace struct {
	Bucket string
	Key    TraceID
	Value  interface{}
}

func (c *HTTPClient) writeBatchToBolt(batch []finalizedTrace) error {
	return c.db.Update(func(tx *bbolt.Tx) error {
		for _, entry := range batch {
			b := tx.Bucket([]byte(entry.Bucket))
			if b == nil {
				var err error
				b, err = tx.CreateBucketIfNotExists([]byte(entry.Bucket))
				if err != nil {
					return err
				}
			}
			data, err := json.Marshal(entry.Value)
			if err != nil {
				return err
			}
			if err := b.Put([]byte(fmt.Sprintf("%v", entry.Key)), data); err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *HTTPClient) processPendingTraces(dropAll bool) {
	var i uint64
	traceCount := c.pendingTraces.Length()
	var batch []finalizedTrace

	for i = 0; i < traceCount; i++ {
		item := c.pendingTraces.Dequeue()
		now := c.getRelTimestampNolock()
		if dropAll {
			continue
		}
		switch t := item.(networkTrace).(type) {
		case dialTrace:
			dial := c.getOrAddDialTrace(t.TraceID)
			if t.ctxClosed {
				dial.CtxCloseAt = t.CtxCloseAt
				continue
			} else if t.justBegan {
				dial.httpReqID = t.httpReqID
				dial.DialBeginAt = t.DialBeginAt
				dial.SourceIP = t.SourceIP
				dial.DstAddress = t.DstAddress
				continue
			} else {
				dial.httpReqID = t.httpReqID
				dial.DialErr = t.DialErr
				dial.DialBeginAt = t.DialBeginAt
				dial.DialEndAt = t.DialEndAt
				dial.EstablishedConn = t.EstablishedConn
				dial.SourceIP = t.SourceIP
				dial.DstAddress = t.DstAddress
			}
			connAddrTuple := addrTupleFromConn(t.conn)
			connSockIdx := -1
			for idx, sock := range c.noConnSockets {
				if sock.fromDial == dial.TraceID {
					c.finalizeNoConnSocket(sock, t.conn != nil, now)
					if !connAddrTuple.undefined() && sock.addrTuple.equal(connAddrTuple) {
						connSockIdx = idx
					}
				}
			}
			if t.conn != nil {
				connection := &connection{
					ID:          t.EstablishedConn,
					addrTuple:   connAddrTuple,
					ConnectedAt: t.DialEndAt,
					DialID:      dial.TraceID,
				}
				if connSockIdx != -1 {
					connection.SockCreatedAt = c.noConnSockets[connSockIdx].createdAt
					connection.Conntrack = c.noConnSockets[connSockIdx].conntrack
				}
				if c.nfConn != nil {
					c.getConntrack(connAddrTuple, &connection.Conntrack, now)
				}
				c.connections.Set(t.EstablishedConn, connection)
			}
			if connSockIdx != -1 {
				c.delNoConnSocket(connSockIdx)
			}

		case resolverDialTrace:
			dial := c.getOrAddDialTrace(t.parentDial)
			dial.ResolverDials = append(dial.ResolverDials, ResolverDialTrace{
				DialBeginAt:     t.dialBeginAt,
				DialEndAt:       t.dialEndAt,
				DialErr:         errToString(t.dialErr),
				Nameserver:      t.nameserver,
				EstablishedConn: t.connID,
			})
			// Stop monitoring sockets opened by this call to resolver's Dial.
			connAddrTuple := addrTupleFromConn(t.conn) // undefined if dial failed
			connSockIdx := -1
			for idx, sock := range c.noConnSockets {
				if sock.fromDial == t.parentDial && sock.fromResolvDial == t.resolvDial {
					c.finalizeNoConnSocket(sock, t.conn != nil, now)
					if !connAddrTuple.undefined() && sock.addrTuple.equal(connAddrTuple) {
						connSockIdx = idx
					}
				}
			}
			if t.conn != nil {
				// Add entry for newly created connection.
				connection := &connection{
					ID:           t.connID,
					addrTuple:    connAddrTuple,
					ConnectedAt:  t.dialEndAt,
					DialID:       t.parentDial,
					FromResolver: true,
				}
				if connSockIdx != -1 {
					connection.SockCreatedAt = c.noConnSockets[connSockIdx].createdAt
					connection.Conntrack = c.noConnSockets[connSockIdx].conntrack
				}
				if c.nfConn != nil {
					c.getConntrack(connAddrTuple, &connection.Conntrack, now)
				}
				c.connections.Set(t.connID, connection)
			}
			if connSockIdx != -1 {
				// Socket is connected - remove it from the noConnSockets slice.
				c.delNoConnSocket(connSockIdx)
			}

		case resolverCloseTrace:
			dial := c.getOrAddDialTrace(t.parentDial)
			dial.SkippedNameservers = t.skippedServers
			batch = append(batch, finalizedTrace{Key: t.parentDial, Bucket: "dials", Value: dial})
			c.dials.Delete(t.parentDial)

		case socketOpTrace:
			connIface, ok := c.connections.Get(t.connID)
			if !ok {
				break
			}
			connection := connIface.(*connection)
			if t.closed {
				connection.Closed = true
				connection.ClosedAt = t.ReturnAt
				if c.nfConn != nil {
					c.getConntrack(connection.addrTuple, &connection.Conntrack, now)
				}
				batch = append(batch, finalizedTrace{Key: connection.ID, Bucket: "connections", Value: connection})
				c.connections.Delete(connection.ID)
			} else {
				switch t.SocketOp.Type {
				case SocketOpTypeRead, SocketOpTypeReadFrom:
					connection.TotalRecvBytes += uint64(t.SocketOp.DataLen)
				case SocketOpTypeWrite, SocketOpTypeWriteTo:
					connection.TotalSentBytes += uint64(t.SocketOp.DataLen)
				}
				if c.withSockTrace {
					connection.SocketOps = append(connection.SocketOps, t.SocketOp)
				}
			}

		case tlsTrace:
			tlsTunTrace := c.getOrAddTLSTunTrace(t.TraceID)
			tlsTunTrace.TLSTunnelTrace = t.TLSTunnelTrace
			tlsTunTrace.httpReqID = t.httpReqID
			httpReqTrace := c.getOrAddHTTPReqTrace(t.httpReqID)
			if t.forProxy {
				httpReqTrace.ProxyTLSTunnel = t.TraceID
			} else {
				httpReqTrace.TLSTunnel = t.TraceID
			}

		case dnsQueryTrace:
			dnsTrace := c.getOrAddDNSTrace(t.connID)
			dnsTrace.DNSQueryMsgs = append(dnsTrace.DNSQueryMsgs, t.DNSQueryMsg)

		case dnsReplyTrace:
			dnsTrace := c.getOrAddDNSTrace(t.connID)
			dnsTrace.DNSReplyMsgs = append(dnsTrace.DNSReplyMsgs, t.DNSReplyMsg)
			if len(dnsTrace.DNSQueryMsgs) > 0 && len(dnsTrace.DNSReplyMsgs) > 0 {
				batch = append(batch, finalizedTrace{Key: dnsTrace.TraceID, Bucket: "dnsQueries", Value: dnsTrace})
				c.dnsQueries.Delete(dnsTrace.TraceID)
			}

		case httpBodyTrace:
			httpReqTrace := c.getOrAddHTTPReqTrace(t.httpReqID)
			if t.isRequest {
				httpReqTrace.ReqContentLen = t.readBodyLen
			} else {
				httpReqTrace.RespContentLen = t.readBodyLen
			}

		case httpConnTrace:
			httpReqTrace := c.getOrAddHTTPReqTrace(t.httpReqID)
			if connTrace := c.lookupConnTrace(t.conn); connTrace != nil {
				httpReqTrace.TCPConn = connTrace.ID
			}

		case httpReqTrace:
			httpReqTrace := c.getOrAddHTTPReqTrace(t.httpReqID)
			// Prefer proto versions from the response if already provided.
			if httpReqTrace.RespRecvAt.Undefined() {
				httpReqTrace.ProtoMajor = t.protoMajor
				httpReqTrace.ProtoMinor = t.protoMinor
			}
			httpReqTrace.ReqSentAt = t.sentAt
			httpReqTrace.ReqMethod = t.reqMethod
			httpReqTrace.ReqURL = t.reqURL
			httpReqTrace.ReqHeader = t.header
			httpReqTrace.NetworkProxy = t.netProxy

		case httpRespTrace:
			httpReqTrace := c.getOrAddHTTPReqTrace(t.httpReqID)
			if t.rtErr != nil {
				httpReqTrace.ReqError = errToString(t.rtErr)
				continue
			}
			httpReqTrace.ProtoMajor = t.protoMajor
			httpReqTrace.ProtoMinor = t.protoMinor
			httpReqTrace.ReqError = ""
			httpReqTrace.RespRecvAt = t.recvAt
			httpReqTrace.RespStatusCode = t.statusCode
			httpReqTrace.RespHeader = t.header
			if !httpReqTrace.ReqSentAt.Undefined() && !httpReqTrace.RespRecvAt.Undefined() {
				batch = append(batch, finalizedTrace{Key: httpReqTrace.TraceID, Bucket: "httpReqs", Value: httpReqTrace})
				c.httpReqs.Delete(httpReqTrace.TraceID)
			}
		}
	}

	if err := c.writeBatchToBolt(batch); err != nil {
		c.log.Errorf("nettrace: failed to write batch to Bolt: %v", err)
	}

}

func (c *HTTPClient) getOrAddDialTrace(id TraceID) *dial {
	if val, ok := c.dials.Get(id); ok {
		if d, ok := val.(*dial); ok {
			return d
		}
	}
	d := &dial{DialTrace: DialTrace{TraceID: id}}
	c.dials.Set(id, d)
	return d
}

func (c *HTTPClient) getOrAddDNSTrace(connID TraceID) *DNSQueryTrace {
	for _, id := range c.dnsQueries.order {
		entry, ok := c.dnsQueries.Get(id)
		if !ok {
			continue
		}
		if dnsQuery, ok := entry.(*DNSQueryTrace); ok && dnsQuery.Connection == connID {
			return dnsQuery
		}
	}
	trace := &DNSQueryTrace{
		TraceID:    IDGenerator(),
		Connection: connID,
	}
	c.dnsQueries.Set(trace.TraceID, trace)
	return trace
}

func (c *HTTPClient) getOrAddTLSTunTrace(id TraceID) *tlsTun {
	if val, ok := c.tlsTuns.Get(id); ok {
		if t, ok := val.(*tlsTun); ok {
			return t
		}
	}
	t := &tlsTun{TLSTunnelTrace: TLSTunnelTrace{TraceID: id}}
	c.tlsTuns.Set(id, t)
	return t
}

func (c *HTTPClient) getOrAddHTTPReqTrace(id TraceID) *HTTPReqTrace {
	if val, ok := c.httpReqs.Get(id); ok {
		if r, ok := val.(*HTTPReqTrace); ok {
			return r
		}
	}
	r := &HTTPReqTrace{TraceID: id}
	c.httpReqs.Set(id, r)
	return r
}

func (c *HTTPClient) finalizeNoConnSocket(sock *inetSocket, connected bool, now Timestamp) {
	if sock.dupClosed {
		return
	}
	if !sock.withSrcAddr() {
		c.getSockSrcAddr(sock)
	}
	if c.nfConn != nil && sock.withSrcAddr() {
		c.getConntrack(sock.addrTuple, &sock.conntrack, now)
	}
	if !connected && sock.origClosedAt.Undefined() {
		sock.origClosedAt = now
	}
	c.closeSockDupFD(sock)
}

func (c *HTTPClient) delNoConnSocket(idx int) {
	sockCount := len(c.noConnSockets)
	c.noConnSockets[idx] = c.noConnSockets[sockCount-1]
	c.noConnSockets[sockCount-1] = nil
	c.noConnSockets = c.noConnSockets[:sockCount-1]
}

func (c *HTTPClient) lookupConnTrace(conn net.Conn) *connection {
	addr := addrTupleFromConn(conn)
	for _, id := range c.connections.order {
		val, ok := c.connections.Get(id)
		if !ok {
			continue
		}
		connTrace, ok := val.(*connection)
		if !ok {
			continue
		}
		if connTrace.addrTuple.equal(addr) {
			return connTrace
		}
	}
	return nil
}

func (c *HTTPClient) getHTTPTransport() http.RoundTripper {
	return c.httpTransp
}

func (c *HTTPClient) iterNoConnSockets(iterCb connIterCallback) {
	for _, socket := range c.noConnSockets {
		stop := iterCb(socket.addrTuple, socket.conntrack.flow)
		if stop {
			return
		}
	}
}

func (c *HTTPClient) iterConnections(iterCb connIterCallback) {
	for _, id := range c.connections.order {
		val, ok := c.connections.Get(id)
		if !ok {
			continue
		}
		conn, ok := val.(*connection)
		if !ok {
			continue
		}
		stop := iterCb(conn.addrTuple, conn.Conntrack.flow)
		if stop {
			return
		}
	}
}

func (c *HTTPClient) proxyForRequest(req *http.Request) (*url.URL, error) {
	if c.netProxy == nil {
		return nil, nil
	}
	return c.netProxy(req)
}

func (c *HTTPClient) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := newTracedDialer(c, c.log, c.sourceIP, c.tcpHandshakeTimeout,
		c.tcpKeepAliveInterval, c.withDNSTrace, c.skipNameserver)
	return dialer.dial(ctx, network, addr)
}

// Start tracing a newly created AF_INET socket.
// This is done synchronously with HTTPClient locked (i.e. not using queue) to ensure
// that HTTPClient will not accidentally filter out first packets produced by this socket
// due to a race condition between trace processing and packet filtering.
func (c *HTTPClient) traceNewSocket(sock *inetSocket) {
	c.Lock()
	defer c.Unlock()
	now := c.getRelTimestampNolock()
	for _, oldSock := range c.noConnSockets {
		if !oldSock.origClosed && oldSock.origFD == sock.origFD {
			// oldSock.origFD was closed and got reused.
			oldSock.origClosed = true
			oldSock.origClosedAt = now
			break
		}
	}
	c.noConnSockets = append(c.noConnSockets, sock)
}
