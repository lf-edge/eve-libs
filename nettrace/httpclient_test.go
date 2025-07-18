// Copyright (c) 2022 Zededa, Inc.
// SPDX-License-Identifier: Apache-2.0

package nettrace_test

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/lf-edge/eve-libs/nettrace"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
	"github.com/vishvananda/netlink"
)

func relTimeIsInBetween(t *GomegaWithT, timestamp, lowerBound, upperBound nettrace.Timestamp) {
	t.Expect(timestamp.IsRel).To(BeTrue())
	t.Expect(lowerBound.IsRel).To(BeTrue())
	t.Expect(upperBound.IsRel).To(BeTrue())
	t.Expect(timestamp.Rel >= lowerBound.Rel).To(BeTrue())
	t.Expect(timestamp.Rel <= upperBound.Rel).To(BeTrue())
}

func checkCapturedTCPHandshakeForHTTPS(t *testing.T, pcap []gopacket.Packet) {
	var foundSYN, foundSYNACK, foundACK bool

	for _, packet := range pcap {
		tcpLayer := packet.Layer(layers.LayerTypeTCP)
		ipv4Layer := packet.Layer(layers.LayerTypeIPv4)
		ipv6Layer := packet.Layer(layers.LayerTypeIPv6)
		if tcpLayer == nil || (ipv4Layer == nil && ipv6Layer == nil) {
			continue
		}
		tcp, _ := tcpLayer.(*layers.TCP)
		var srcIP, dstIP net.IP
		if ipv4Layer != nil {
			srcIP = ipv4Layer.(*layers.IPv4).SrcIP
			dstIP = ipv4Layer.(*layers.IPv4).SrcIP
		}
		if ipv6Layer != nil {
			srcIP = ipv6Layer.(*layers.IPv6).SrcIP
			dstIP = ipv6Layer.(*layers.IPv6).SrcIP
		}

		// SYN packet from client to server
		if tcp.SYN && !tcp.ACK && tcp.DstPort == 443 {
			t.Logf("Found SYN to port 443: %s:%d -> %s:%d",
				srcIP, tcp.SrcPort, dstIP, tcp.DstPort)
			foundSYN = true
		}

		// SYN-ACK packet from server to client
		if tcp.SYN && tcp.ACK && tcp.SrcPort == 443 {
			t.Logf("Found SYN-ACK from port 443: %s:%d -> %s:%d",
				srcIP, tcp.SrcPort, dstIP, tcp.DstPort)
			foundSYNACK = true
		}

		// Final ACK completing the handshake
		if !foundACK && !tcp.SYN && tcp.ACK && tcp.DstPort == 443 {
			t.Logf("Found ACK to port 443: %s:%d -> %s:%d",
				srcIP, tcp.SrcPort, dstIP, tcp.DstPort)
			foundACK = true
		}
	}

	if !foundSYN {
		t.Error("Did not find SYN packet to port 443")
	}
	if !foundSYNACK {
		t.Error("Did not find SYN-ACK packet from port 443")
	}
	if !foundACK {
		t.Error("Did not find final ACK to port 443")
	}
}

func TestHTTPTracing(test *testing.T) {
	defaultLink, err := getLinkForDefaultRoute()
	if err != nil {
		test.Skipf("Skipping test: no default route found (%v)", err)
	}
	rootUser := os.Geteuid() == 0
	var withConntrackAcct bool
	if rootUser {
		fmt.Println("Running as root - will additionally test conntrack and pcap")
		// Check if packet/byte conntrack accounting is enabled.
		const conntrackAcctOpt = "/proc/sys/net/netfilter/nf_conntrack_acct"
		data, err := os.ReadFile(conntrackAcctOpt)
		if err != nil {
			fmt.Printf("Failed to read conntrack accounting setting: %v\n", err)
		} else if strings.TrimSpace(string(data)) == "1" {
			fmt.Printf("%s is enabled, will check packet/byte counters recorded by conntrack\n",
				conntrackAcctOpt)
			withConntrackAcct = true
		}
	}
	startTime := time.Now()
	t := NewWithT(test)

	// Options that do not require administrative privileges.
	opts := []nettrace.TraceOpt{
		&nettrace.WithLogging{
			CustomLogger: logrus.New(),
		},
		&nettrace.WithHTTPReqTrace{
			HeaderFields: nettrace.HdrFieldsOptWithValues,
		},
		&nettrace.WithSockTrace{},
		&nettrace.WithDNSQueryTrace{},
	}
	if rootUser {
		opts = append(opts, &nettrace.WithConntrack{})
		opts = append(opts, &nettrace.WithPacketCapture{
			Interfaces: []string{defaultLink.Attrs().Name},
		})
	}
	client, err := nettrace.NewHTTPClient(nettrace.HTTPClientCfg{
		PreferHTTP2:      true,
		ReqTimeout:       5 * time.Second,
		DisableKeepAlive: true,
	}, opts...)
	t.Expect(err).ToNot(HaveOccurred())

	req, err := http.NewRequest("GET", "https://www.example.com", nil)
	t.Expect(err).ToNot(HaveOccurred())
	req.Header.Set("Accept", "text/html")
	resp, err := client.Do(req)
	t.Expect(err).ToNot(HaveOccurred())
	t.Expect(resp).ToNot(BeNil())
	t.Expect(resp.StatusCode).To(Equal(200))
	t.Expect(resp.Body).ToNot(BeNil())
	body := new(strings.Builder)
	_, err = io.Copy(body, resp.Body)
	t.Expect(err).ToNot(HaveOccurred())
	err = resp.Body.Close()
	t.Expect(err).ToNot(HaveOccurred())
	t.Expect(body.String()).To(ContainSubstring("<html>"))
	t.Expect(body.String()).To(ContainSubstring("</html>"))

	if rootUser {
		// Give pcap some time to complete capture of all the packets.
		time.Sleep(3 * time.Second)
	}
	trace, pcap, err := client.GetTrace("GET www.example.com over HTTPS")
	t.Expect(err).ToNot(HaveOccurred())
	if rootUser {
		t.Expect(pcap).To(HaveLen(1))
		t.Expect(pcap[0].InterfaceName).To(Equal(defaultLink.Attrs().Name))
		t.Expect(pcap[0].WithTCPPayload).To(BeTrue())
		t.Expect(pcap[0].Packets).ToNot(BeEmpty())
		// Most of the captured packets are encrypted with TLS, but we can at least
		// check that TCP handshake is included in the trace.
		checkCapturedTCPHandshakeForHTTPS(test, pcap[0].Packets)
	} else {
		t.Expect(pcap).To(BeEmpty())
	}

	t.Expect(trace.Description).To(Equal("GET www.example.com over HTTPS"))
	t.Expect(trace.TraceBeginAt.IsRel).To(BeFalse())
	t.Expect(trace.TraceBeginAt.Abs.After(startTime)).To(BeTrue())
	t.Expect(trace.TraceBeginAt.Abs.Before(time.Now())).To(BeTrue())
	traceBeginAsRel := nettrace.Timestamp{IsRel: true, Rel: 0}
	t.Expect(trace.TraceEndAt.IsRel).To(BeTrue())
	t.Expect(trace.TraceEndAt.Rel > 0).To(BeTrue())

	// Dial trace
	t.Expect(trace.Dials).To(HaveLen(1)) // no redirects
	dial := trace.Dials[0]
	t.Expect(dial.TraceID).ToNot(BeZero())
	relTimeIsInBetween(t, dial.DialBeginAt, traceBeginAsRel, trace.TraceEndAt)
	relTimeIsInBetween(t, dial.DialEndAt, dial.DialBeginAt, trace.TraceEndAt)
	t.Expect(dial.DialErr).To(BeZero())
	t.Expect(dial.SourceIP).To(BeZero())
	t.Expect(dial.DstAddress).To(Equal("www.example.com:443"))
	t.Expect(dial.ResolverDials).ToNot(BeEmpty())
	for _, resolvDial := range dial.ResolverDials {
		relTimeIsInBetween(t, resolvDial.DialBeginAt, dial.DialBeginAt, dial.DialEndAt)
		relTimeIsInBetween(t, resolvDial.DialEndAt, resolvDial.DialBeginAt, dial.DialEndAt)
		t.Expect(resolvDial.Nameserver).ToNot(BeZero())
		if !resolvDial.EstablishedConn.Undefined() {
			t.Expect(resolvDial.DialErr).To(BeZero())
			t.Expect(trace.UDPConns.Get(resolvDial.EstablishedConn)).ToNot(BeNil())
		}
	}
	t.Expect(dial.EstablishedConn).ToNot(BeZero())
	t.Expect(trace.TCPConns.Get(dial.EstablishedConn)).ToNot(BeNil())

	// DNS trace
	t.Expect(trace.DNSQueries).ToNot(BeEmpty())
	for _, dnsQuery := range trace.DNSQueries {
		t.Expect(dnsQuery.FromDial == dial.TraceID).To(BeTrue())
		t.Expect(dnsQuery.TraceID).ToNot(BeZero())
		udpConn := trace.UDPConns.Get(dnsQuery.Connection)
		t.Expect(udpConn).ToNot(BeNil())

		t.Expect(dnsQuery.DNSQueryMsgs).To(HaveLen(1))
		dnsMsg := dnsQuery.DNSQueryMsgs[0]
		relTimeIsInBetween(t, dnsMsg.SentAt, udpConn.SocketCreateAt, udpConn.ConnCloseAt)
		t.Expect(dnsMsg.Questions).To(HaveLen(1))
		t.Expect(dnsMsg.Questions[0].Name).To(Equal("www.example.com."))
		t.Expect(dnsMsg.Questions[0].Type).To(Or(
			Equal(nettrace.DNSResTypeA), Equal(nettrace.DNSResTypeAAAA)))
		t.Expect(dnsMsg.Truncated).To(BeFalse())

		t.Expect(dnsQuery.DNSReplyMsgs).To(HaveLen(1))
		dnsReply := dnsQuery.DNSReplyMsgs[0]
		relTimeIsInBetween(t, dnsReply.RecvAt, dnsMsg.SentAt, udpConn.ConnCloseAt)
		t.Expect(dnsReply.ID == dnsMsg.ID).To(BeTrue())
		t.Expect(dnsReply.RCode).To(Equal(nettrace.DNSRCodeNoError))
		t.Expect(dnsReply.Answers).ToNot(BeEmpty())
		t.Expect(dnsReply.Truncated).To(BeFalse())
	}

	// UDP connection trace
	t.Expect(trace.UDPConns).ToNot(BeEmpty())
	for _, udpConn := range trace.UDPConns {
		t.Expect(udpConn.TraceID).ToNot(BeZero())
		t.Expect(udpConn.FromDial == dial.TraceID).To(BeTrue())
		relTimeIsInBetween(t, udpConn.SocketCreateAt, dial.DialBeginAt, dial.DialEndAt)
		relTimeIsInBetween(t, udpConn.ConnCloseAt, udpConn.SocketCreateAt, dial.DialEndAt)
		t.Expect(net.ParseIP(udpConn.AddrTuple.SrcIP)).ToNot(BeNil())
		t.Expect(net.ParseIP(udpConn.AddrTuple.DstIP)).ToNot(BeNil())
		t.Expect(udpConn.AddrTuple.SrcPort).ToNot(BeZero())
		t.Expect(udpConn.AddrTuple.DstPort).ToNot(BeZero())
		t.Expect(udpConn.SocketTrace).ToNot(BeNil())
		t.Expect(udpConn.SocketTrace.SocketOps).ToNot(BeEmpty())
		for _, socketOp := range udpConn.SocketTrace.SocketOps {
			relTimeIsInBetween(t, socketOp.CallAt, udpConn.SocketCreateAt, udpConn.ConnCloseAt)
			relTimeIsInBetween(t, socketOp.ReturnAt, socketOp.CallAt, udpConn.ConnCloseAt)
		}
		if rootUser {
			t.Expect(udpConn.Conntract).ToNot(BeNil())
			if withConntrackAcct {
				t.Expect(udpConn.Conntract.PacketsRecv).ToNot(BeZero())
				t.Expect(udpConn.Conntract.PacketsSent).ToNot(BeZero())
				t.Expect(udpConn.Conntract.BytesRecv).ToNot(BeZero())
				t.Expect(udpConn.Conntract.BytesSent).ToNot(BeZero())
			}
			t.Expect(udpConn.Conntract.AddrOrig.DstPort).To(BeEquivalentTo(53))
			t.Expect(udpConn.Conntract.AddrReply.SrcPort).To(BeEquivalentTo(53))
			relTimeIsInBetween(t, udpConn.Conntract.CapturedAt, traceBeginAsRel, trace.TraceEndAt)
			status := uint32(udpConn.Conntract.Status)
			t.Expect(status & nettrace.ConntrackFlags["seen-reply"]).ToNot(BeZero())
			t.Expect(status & nettrace.ConntrackFlags["confirmed"]).ToNot(BeZero())
		} else {
			// WithConntrack requires root privileges
			t.Expect(udpConn.Conntract).To(BeNil())
		}
		t.Expect(udpConn.TotalRecvBytes).ToNot(BeZero())
		t.Expect(udpConn.TotalSentBytes).ToNot(BeZero())
	}

	// HTTP request trace
	t.Expect(trace.HTTPRequests).To(HaveLen(1))
	httpReq := trace.HTTPRequests[0]
	t.Expect(httpReq.TraceID).ToNot(BeZero())
	t.Expect(httpReq.TCPConn.Undefined()).To(BeFalse())
	usedTCPConn := trace.TCPConns.Get(httpReq.TCPConn)
	t.Expect(usedTCPConn).ToNot(BeNil())
	t.Expect(httpReq.ProtoMajor).To(BeEquivalentTo(2))
	t.Expect(httpReq.ProtoMinor).To(BeEquivalentTo(0))
	t.Expect(httpReq.NetworkProxy).To(BeZero())
	relTimeIsInBetween(t, httpReq.ReqSentAt, traceBeginAsRel, trace.TraceEndAt)
	t.Expect(httpReq.ReqError).To(BeZero())
	t.Expect(httpReq.ReqMethod).To(Equal("GET"))
	t.Expect(httpReq.ReqURL).To(Equal("https://www.example.com"))
	t.Expect(httpReq.ReqHeader).ToNot(BeEmpty())
	acceptHdr := httpReq.ReqHeader.Get("Accept")
	t.Expect(acceptHdr).ToNot(BeNil())
	t.Expect(acceptHdr.FieldVal).To(Equal("text/html"))
	t.Expect(acceptHdr.FieldValLen).To(BeEquivalentTo(len(acceptHdr.FieldVal)))
	t.Expect(httpReq.ReqContentLen).To(BeZero())
	relTimeIsInBetween(t, httpReq.RespRecvAt, httpReq.ReqSentAt, trace.TraceEndAt)
	t.Expect(httpReq.RespStatusCode).To(Equal(200))
	t.Expect(httpReq.RespHeader).ToNot(BeEmpty())
	contentType := httpReq.RespHeader.Get("content-type")
	t.Expect(contentType).ToNot(BeNil())
	t.Expect(contentType.FieldVal).To(ContainSubstring("text/html"))
	t.Expect(contentType.FieldValLen).To(BeEquivalentTo(len(contentType.FieldVal)))
	t.Expect(httpReq.RespContentLen).ToNot(BeZero())

	// TCP connection traces
	// There can be multiple parallel connection attempts made as per Happy Eyeballs algorithm.
	t.Expect(trace.TCPConns).ToNot(BeEmpty())
	for _, tcpConn := range trace.TCPConns {
		t.Expect(tcpConn.TraceID).ToNot(BeZero())
		t.Expect(tcpConn.FromDial == dial.TraceID).To(BeTrue())
		t.Expect(tcpConn.Reused).To(BeFalse())
		t.Expect(net.ParseIP(tcpConn.AddrTuple.SrcIP)).ToNot(BeNil())
		t.Expect(net.ParseIP(tcpConn.AddrTuple.DstIP)).ToNot(BeNil())
		t.Expect(tcpConn.AddrTuple.SrcPort).ToNot(BeZero()) // TODO: this may fail for IPv6
		t.Expect(tcpConn.AddrTuple.DstPort).ToNot(BeZero())
		if rootUser {
			t.Expect(tcpConn.Conntract).ToNot(BeNil())
			if withConntrackAcct {
				t.Expect(tcpConn.Conntract.PacketsRecv).ToNot(BeZero())
				t.Expect(tcpConn.Conntract.PacketsSent).ToNot(BeZero())
				t.Expect(tcpConn.Conntract.BytesRecv).ToNot(BeZero())
				t.Expect(tcpConn.Conntract.BytesSent).ToNot(BeZero())
			}
			t.Expect(tcpConn.Conntract.AddrOrig.DstPort).To(BeEquivalentTo(443))
			t.Expect(tcpConn.Conntract.AddrReply.SrcPort).To(BeEquivalentTo(443))
			relTimeIsInBetween(t, tcpConn.Conntract.CapturedAt, traceBeginAsRel, trace.TraceEndAt)
			status := uint32(tcpConn.Conntract.Status)
			t.Expect(status & nettrace.ConntrackFlags["seen-reply"]).ToNot(BeZero())
			t.Expect(status & nettrace.ConntrackFlags["confirmed"]).ToNot(BeZero())
			t.Expect(tcpConn.Conntract.TCPState).To(Equal(nettrace.TCPStateClose))
		} else {
			// WithConntrack requires root privileges
			t.Expect(tcpConn.Conntract).To(BeNil())
		}
		if tcpConn.TraceID != usedTCPConn.TraceID {
			// Not used for HTTP request in the end.
			continue
		}
		relTimeIsInBetween(t, tcpConn.HandshakeBeginAt, dial.DialBeginAt, dial.DialEndAt)
		relTimeIsInBetween(t, tcpConn.HandshakeEndAt, tcpConn.HandshakeBeginAt, dial.DialEndAt)
		relTimeIsInBetween(t, tcpConn.ConnCloseAt, tcpConn.HandshakeEndAt, trace.TraceEndAt)
		t.Expect(tcpConn.SocketTrace).ToNot(BeNil())
		t.Expect(tcpConn.SocketTrace.SocketOps).ToNot(BeEmpty())
		for _, socketOp := range tcpConn.SocketTrace.SocketOps {
			relTimeIsInBetween(t, socketOp.CallAt, tcpConn.HandshakeEndAt, tcpConn.ConnCloseAt)
			relTimeIsInBetween(t, socketOp.ReturnAt, socketOp.CallAt, tcpConn.ConnCloseAt)
		}
		t.Expect(tcpConn.TotalRecvBytes).ToNot(BeZero())
		t.Expect(tcpConn.TotalSentBytes).ToNot(BeZero())
	}

	// TLS tunnel trace
	t.Expect(trace.TLSTunnels).To(HaveLen(1))
	tlsTun := trace.TLSTunnels[0]
	t.Expect(tlsTun.TraceID).ToNot(BeZero())
	t.Expect(tlsTun.TCPConn == usedTCPConn.TraceID).To(BeTrue())
	t.Expect(httpReq.TLSTunnel == tlsTun.TraceID).To(BeTrue())
	t.Expect(httpReq.ProxyTLSTunnel.Undefined()).To(BeTrue())
	t.Expect(tlsTun.DidResume).To(BeFalse())
	relTimeIsInBetween(t, tlsTun.HandshakeBeginAt, usedTCPConn.HandshakeEndAt, usedTCPConn.ConnCloseAt)
	relTimeIsInBetween(t, tlsTun.HandshakeEndAt, tlsTun.HandshakeBeginAt, usedTCPConn.ConnCloseAt)
	t.Expect(tlsTun.HandshakeErr).To(BeZero())
	t.Expect(tlsTun.ServerName).To(Equal("www.example.com"))
	t.Expect(tlsTun.NegotiatedProto).To(Equal("h2"))
	t.Expect(tlsTun.CipherSuite).ToNot(BeZero())
	t.Expect(tlsTun.PeerCerts).To(HaveLen(2))
	peerCert := tlsTun.PeerCerts[0]
	t.Expect(peerCert.IsCA).To(BeFalse())
	t.Expect(peerCert.Subject).To(Equal("CN=*.example.com,O=Internet Corporation for Assigned Names and Numbers,L=Los Angeles,ST=California,C=US"))
	t.Expect(peerCert.Issuer).To(Equal("CN=DigiCert Global G3 TLS ECC SHA384 2020 CA1,O=DigiCert Inc,C=US"))
	t.Expect(peerCert.NotBefore.Undefined()).To(BeFalse())
	t.Expect(peerCert.NotBefore.IsRel).To(BeFalse())
	t.Expect(peerCert.NotAfter.Undefined()).To(BeFalse())
	t.Expect(peerCert.NotAfter.IsRel).To(BeFalse())
	t.Expect(peerCert.NotBefore.Abs.Before(time.Now())).To(BeTrue())
	t.Expect(peerCert.NotAfter.Abs.After(time.Now())).To(BeTrue())
	peerCert = tlsTun.PeerCerts[1]
	t.Expect(peerCert.IsCA).To(BeTrue())
	t.Expect(peerCert.Subject).To(Equal("CN=DigiCert Global G3 TLS ECC SHA384 2020 CA1,O=DigiCert Inc,C=US"))
	t.Expect(peerCert.Issuer).To(Equal("CN=DigiCert Global Root G3,OU=www.digicert.com,O=DigiCert Inc,C=US"))
	t.Expect(peerCert.NotBefore.Undefined()).To(BeFalse())
	t.Expect(peerCert.NotBefore.IsRel).To(BeFalse())
	t.Expect(peerCert.NotAfter.Undefined()).To(BeFalse())
	t.Expect(peerCert.NotAfter.IsRel).To(BeFalse())
	t.Expect(peerCert.NotBefore.Abs.Before(time.Now())).To(BeTrue())
	t.Expect(peerCert.NotAfter.Abs.After(time.Now())).To(BeTrue())

	err = client.Close()
	t.Expect(err).ToNot(HaveOccurred())
}

// TestTLSCertErrors : test that even when TLS handshake fails due to a bad certificate,
// we still get the certificate issuer and the subject in the trace.
func TestTLSCertErrors(test *testing.T) {
	t := NewGomegaWithT(test)

	// Option required for TLS tracing.
	// WithLogging is not specified to test nilLogger.
	opts := []nettrace.TraceOpt{
		&nettrace.WithHTTPReqTrace{},
	}
	client, err := nettrace.NewHTTPClient(nettrace.HTTPClientCfg{
		PreferHTTP2: true,
		ReqTimeout:  5 * time.Second,
	}, opts...)
	t.Expect(err).ToNot(HaveOccurred())

	// Expired certificate
	req, err := http.NewRequest("GET", "https://expired.badssl.com/", nil)
	t.Expect(err).ToNot(HaveOccurred())
	resp, err := client.Do(req)
	t.Expect(err).To(HaveOccurred())
	t.Expect(resp).To(BeNil())
	trace, _, err := client.GetTrace("expired cert")
	t.Expect(err).ToNot(HaveOccurred())
	t.Expect(trace.TLSTunnels).To(HaveLen(1))
	tlsTun := trace.TLSTunnels[0]
	t.Expect(tlsTun.HandshakeErr).ToNot(BeZero())
	t.Expect(tlsTun.PeerCerts).To(HaveLen(1)) // when TLS fails, we only get the problematic cert
	peerCert := tlsTun.PeerCerts[0]
	t.Expect(peerCert.IsCA).To(BeFalse())
	t.Expect(peerCert.Issuer).To(Equal("CN=COMODO RSA Domain Validation Secure Server CA,O=COMODO CA Limited,L=Salford,ST=Greater Manchester,C=GB"))
	t.Expect(peerCert.Subject).To(Equal("CN=*.badssl.com,OU=Domain Control Validated+OU=PositiveSSL Wildcard"))
	t.Expect(peerCert.NotBefore.Abs.IsZero()).To(BeFalse())
	t.Expect(peerCert.NotAfter.Abs.Before(time.Now())).To(BeTrue())
	err = client.ClearTrace()
	t.Expect(err).ToNot(HaveOccurred())

	// Wrong Host
	req, err = http.NewRequest("GET", "https://wrong.host.badssl.com/", nil)
	t.Expect(err).ToNot(HaveOccurred())
	resp, err = client.Do(req)
	t.Expect(err).To(HaveOccurred())
	t.Expect(resp).To(BeNil())
	trace, _, err = client.GetTrace("wrong host")
	t.Expect(err).ToNot(HaveOccurred())
	t.Expect(trace.TLSTunnels).To(HaveLen(1))
	tlsTun = trace.TLSTunnels[0]
	t.Expect(tlsTun.HandshakeErr).ToNot(BeZero())
	t.Expect(tlsTun.PeerCerts).To(HaveLen(1))
	peerCert = tlsTun.PeerCerts[0]
	t.Expect(peerCert.IsCA).To(BeFalse())
	t.Expect(peerCert.Issuer).To(SatisfyAny(
		Equal("CN=R10,O=Let's Encrypt,C=US"),
		Equal("CN=R11,O=Let's Encrypt,C=US")))
	t.Expect(peerCert.Subject).To(Equal("CN=*.badssl.com"))
	t.Expect(peerCert.NotBefore.Abs.Before(time.Now())).To(BeTrue())
	t.Expect(peerCert.NotAfter.Abs.After(time.Now())).To(BeTrue())
	err = client.ClearTrace()
	t.Expect(err).ToNot(HaveOccurred())

	// Untrusted root
	req, err = http.NewRequest("GET", "https://untrusted-root.badssl.com/", nil)
	t.Expect(err).ToNot(HaveOccurred())
	resp, err = client.Do(req)
	t.Expect(err).To(HaveOccurred())
	t.Expect(resp).To(BeNil())
	trace, _, err = client.GetTrace("untrusted root")
	t.Expect(err).ToNot(HaveOccurred())
	t.Expect(trace.TLSTunnels).To(HaveLen(1))
	tlsTun = trace.TLSTunnels[0]
	t.Expect(tlsTun.HandshakeErr).ToNot(BeZero())
	t.Expect(tlsTun.PeerCerts).To(HaveLen(1))
	peerCert = tlsTun.PeerCerts[0]
	t.Expect(peerCert.IsCA).To(BeTrue())
	t.Expect(peerCert.Issuer).To(Equal("CN=BadSSL Untrusted Root Certificate Authority,O=BadSSL,L=San Francisco,ST=California,C=US"))
	t.Expect(peerCert.Subject).To(Equal("CN=BadSSL Untrusted Root Certificate Authority,O=BadSSL,L=San Francisco,ST=California,C=US"))
	t.Expect(peerCert.NotBefore.Abs.Before(time.Now())).To(BeTrue())
	t.Expect(peerCert.NotAfter.Abs.After(time.Now())).To(BeTrue())
	err = client.ClearTrace()
	t.Expect(err).ToNot(HaveOccurred())

	err = client.Close()
	t.Expect(err).ToNot(HaveOccurred())
}

// Trace HTTP request targeted at a non-existent host name.
func TestNonExistentHost(test *testing.T) {
	t := NewGomegaWithT(test)

	// Options that do not require administrative privileges.
	opts := []nettrace.TraceOpt{
		&nettrace.WithLogging{},
		&nettrace.WithHTTPReqTrace{
			HeaderFields: nettrace.HdrFieldsOptWithValues,
		},
		&nettrace.WithSockTrace{},
		&nettrace.WithDNSQueryTrace{},
	}
	client, err := nettrace.NewHTTPClient(nettrace.HTTPClientCfg{
		ReqTimeout: 5 * time.Second,
	}, opts...)
	t.Expect(err).ToNot(HaveOccurred())

	req, err := http.NewRequest("GET", "https://non-existent-host.com", nil)
	t.Expect(err).ToNot(HaveOccurred())
	resp, err := client.Do(req)
	t.Expect(err).To(HaveOccurred())
	t.Expect(resp).To(BeNil())
	trace, _, err := client.GetTrace("non-existent host")
	t.Expect(err).ToNot(HaveOccurred())
	traceBeginAsRel := nettrace.Timestamp{IsRel: true, Rel: 0}

	// Dial trace
	t.Expect(trace.Dials).To(HaveLen(1)) // one failed Dial (DNS failed)
	dial := trace.Dials[0]
	t.Expect(dial.TraceID).ToNot(BeZero())
	relTimeIsInBetween(t, dial.DialBeginAt, traceBeginAsRel, trace.TraceEndAt)
	relTimeIsInBetween(t, dial.DialEndAt, dial.DialBeginAt, trace.TraceEndAt)
	t.Expect(dial.DstAddress).To(Equal("non-existent-host.com:443"))
	t.Expect(dial.ResolverDials).ToNot(BeEmpty())
	for _, resolvDial := range dial.ResolverDials {
		relTimeIsInBetween(t, resolvDial.DialBeginAt, dial.DialBeginAt, dial.DialEndAt)
		relTimeIsInBetween(t, resolvDial.DialEndAt, resolvDial.DialBeginAt, dial.DialEndAt)
		t.Expect(resolvDial.Nameserver).ToNot(BeZero())
		if !resolvDial.EstablishedConn.Undefined() {
			t.Expect(resolvDial.DialErr).To(BeZero())
			t.Expect(trace.UDPConns.Get(resolvDial.EstablishedConn)).ToNot(BeNil())
		}
	}
	t.Expect(dial.DialErr).ToNot(BeZero())
	t.Expect(dial.EstablishedConn).To(BeZero())

	// DNS trace
	t.Expect(trace.DNSQueries).ToNot(BeEmpty())
	for _, dnsQuery := range trace.DNSQueries {
		t.Expect(dnsQuery.FromDial == dial.TraceID).To(BeTrue())
		t.Expect(dnsQuery.TraceID).ToNot(BeZero())
		udpConn := trace.UDPConns.Get(dnsQuery.Connection)
		t.Expect(udpConn).ToNot(BeNil())

		t.Expect(dnsQuery.DNSQueryMsgs).To(HaveLen(1))
		dnsMsg := dnsQuery.DNSQueryMsgs[0]
		relTimeIsInBetween(t, dnsMsg.SentAt, udpConn.SocketCreateAt, udpConn.ConnCloseAt)
		t.Expect(dnsMsg.Questions).To(HaveLen(1))
		t.Expect(dnsMsg.Questions[0].Name).To(HavePrefix("non-existent-host.com."))
		t.Expect(dnsMsg.Questions[0].Type).To(Or(
			Equal(nettrace.DNSResTypeA), Equal(nettrace.DNSResTypeAAAA)))
		t.Expect(dnsMsg.Truncated).To(BeFalse())

		t.Expect(dnsQuery.DNSReplyMsgs).To(HaveLen(1))
		dnsReply := dnsQuery.DNSReplyMsgs[0]
		relTimeIsInBetween(t, dnsReply.RecvAt, dnsMsg.SentAt, udpConn.ConnCloseAt)
		t.Expect(dnsReply.ID == dnsMsg.ID).To(BeTrue())
		t.Expect(dnsReply.RCode).To(Equal(nettrace.DNSRCodeNXDomain))
		t.Expect(dnsReply.Answers).To(BeEmpty())
		t.Expect(dnsReply.Truncated).To(BeFalse())
	}

	// UDP connection trace
	t.Expect(trace.UDPConns).ToNot(BeEmpty())
	for _, udpConn := range trace.UDPConns {
		t.Expect(udpConn.TraceID).ToNot(BeZero())
		t.Expect(udpConn.FromDial == dial.TraceID).To(BeTrue())
		relTimeIsInBetween(t, udpConn.SocketCreateAt, dial.DialBeginAt, dial.DialEndAt)
		relTimeIsInBetween(t, udpConn.ConnCloseAt, udpConn.SocketCreateAt, dial.DialEndAt)
		t.Expect(net.ParseIP(udpConn.AddrTuple.SrcIP)).ToNot(BeNil())
		t.Expect(net.ParseIP(udpConn.AddrTuple.DstIP)).ToNot(BeNil())
		t.Expect(udpConn.AddrTuple.SrcPort).ToNot(BeZero())
		t.Expect(udpConn.AddrTuple.DstPort).ToNot(BeZero())
		t.Expect(udpConn.SocketTrace).ToNot(BeNil())
		t.Expect(udpConn.SocketTrace.SocketOps).ToNot(BeEmpty())
		for _, socketOp := range udpConn.SocketTrace.SocketOps {
			relTimeIsInBetween(t, socketOp.CallAt, udpConn.SocketCreateAt, udpConn.ConnCloseAt)
			relTimeIsInBetween(t, socketOp.ReturnAt, socketOp.CallAt, udpConn.ConnCloseAt)
		}
		t.Expect(udpConn.Conntract).To(BeNil()) // WithConntrack requires root privileges
		t.Expect(udpConn.TotalRecvBytes).ToNot(BeZero())
		t.Expect(udpConn.TotalSentBytes).ToNot(BeZero())
	}

	// TCP connection trace
	t.Expect(trace.TCPConns).To(BeEmpty())

	// TLS tunnel trace
	t.Expect(trace.TLSTunnels).To(BeEmpty())

	// HTTP request trace
	t.Expect(trace.HTTPRequests).To(HaveLen(1))
	httpReq := trace.HTTPRequests[0]
	t.Expect(httpReq.TraceID).ToNot(BeZero())
	t.Expect(httpReq.TCPConn).To(BeZero())
	t.Expect(httpReq.ProtoMajor).To(BeEquivalentTo(1))
	t.Expect(httpReq.ProtoMinor).To(BeEquivalentTo(1))
	relTimeIsInBetween(t, httpReq.ReqSentAt, traceBeginAsRel, trace.TraceEndAt)
	t.Expect(httpReq.ReqError).ToNot(BeZero())
	t.Expect(httpReq.ReqMethod).To(Equal("GET"))
	t.Expect(httpReq.ReqURL).To(Equal("https://non-existent-host.com"))
	t.Expect(httpReq.ReqHeader).To(BeEmpty())
	t.Expect(httpReq.ReqContentLen).To(BeZero())
	t.Expect(httpReq.RespRecvAt.Undefined()).To(BeTrue())
	t.Expect(httpReq.RespStatusCode).To(BeZero())
	t.Expect(httpReq.RespHeader).To(BeEmpty())
	t.Expect(httpReq.RespContentLen).To(BeZero())

	err = client.Close()
	t.Expect(err).ToNot(HaveOccurred())
}

// Trace HTTP request targeted at a non-responsive destination (nobody is listening).
func TestUnresponsiveDest(test *testing.T) {
	t := NewGomegaWithT(test)

	// Options that do not require administrative privileges.
	opts := []nettrace.TraceOpt{
		&nettrace.WithLogging{},
		&nettrace.WithHTTPReqTrace{
			HeaderFields: nettrace.HdrFieldsOptWithValues,
		},
		&nettrace.WithSockTrace{},
		&nettrace.WithDNSQueryTrace{},
	}
	client, err := nettrace.NewHTTPClient(nettrace.HTTPClientCfg{
		ReqTimeout: 5 * time.Second,
	}, opts...)
	t.Expect(err).ToNot(HaveOccurred())

	req, err := http.NewRequest("GET", "https://198.51.100.100", nil)
	t.Expect(err).ToNot(HaveOccurred())
	resp, err := client.Do(req)
	t.Expect(err).To(HaveOccurred())
	t.Expect(resp).To(BeNil())
	time.Sleep(time.Second)
	trace, _, err := client.GetTrace("unresponsive dest")
	t.Expect(err).ToNot(HaveOccurred())
	traceBeginAsRel := nettrace.Timestamp{IsRel: true, Rel: 0}

	// Dial trace
	t.Expect(trace.Dials).To(HaveLen(1)) // one failed Dial (DNS failed)
	dial := trace.Dials[0]
	t.Expect(dial.TraceID).ToNot(BeZero())
	relTimeIsInBetween(t, dial.DialBeginAt, traceBeginAsRel, trace.TraceEndAt)
	// DialEndAt and CtxCloseAt never get set, because when the http.Client.Timeout
	// is reached, all dialers are dropped. The dialer itself does not return an error.
	//relTimeIsInBetween(t, dial.DialEndAt, dial.DialBeginAt, trace.TraceEndAt)
	//relTimeIsInBetween(t, dial.CtxCloseAt, dial.DialBeginAt, trace.TraceEndAt)
	t.Expect(dial.DstAddress).To(Equal("198.51.100.100:443"))
	t.Expect(dial.ResolverDials).To(BeEmpty())
	// although we would like this to not be zero, technically, there was no dial error
	// the whole client timed out
	t.Expect(dial.DialErr).To(BeZero())
	t.Expect(dial.EstablishedConn).To(BeZero())

	// DNS trace
	t.Expect(trace.DNSQueries).To(BeEmpty())

	// UDP connection trace
	t.Expect(trace.UDPConns).To(BeEmpty())

	// TCP connection trace
	t.Expect(trace.TCPConns).To(HaveLen(1))
	tcpConn := trace.TCPConns[0]
	t.Expect(tcpConn.TraceID).ToNot(BeZero())
	t.Expect(tcpConn.FromDial == dial.TraceID).To(BeTrue())
	t.Expect(tcpConn.Reused).To(BeFalse())
	// dial.DialEndAt is not set, so we can't check if the handshake began after the dial
	// relTimeIsInBetween(t, tcpConn.HandshakeBeginAt, dial.DialBeginAt, dial.DialEndAt)
	// killed from outside of Dial
	// relTimeIsInBetween(t, tcpConn.HandshakeEndAt, tcpConn.HandshakeBeginAt, trace.TraceEndAt)
	t.Expect(tcpConn.ConnCloseAt.Undefined()).To(BeTrue())
	t.Expect(net.ParseIP(tcpConn.AddrTuple.SrcIP)).ToNot(BeNil())
	t.Expect(net.ParseIP(tcpConn.AddrTuple.DstIP)).ToNot(BeNil())
	t.Expect(tcpConn.AddrTuple.SrcPort).ToNot(BeZero()) // btw. not easy to get when TLS handshake fails
	t.Expect(tcpConn.AddrTuple.DstPort).ToNot(BeZero())
	t.Expect(tcpConn.SocketTrace).To(BeZero())
	t.Expect(tcpConn.Conntract).To(BeNil())
	t.Expect(tcpConn.TotalRecvBytes).To(BeZero())
	t.Expect(tcpConn.TotalSentBytes).To(BeZero())

	// TLS tunnel trace
	t.Expect(trace.TLSTunnels).To(BeEmpty())

	// HTTP request trace
	t.Expect(trace.HTTPRequests).To(HaveLen(1))
	httpReq := trace.HTTPRequests[0]
	t.Expect(httpReq.TraceID).ToNot(BeZero())
	t.Expect(httpReq.TCPConn).To(BeZero())
	t.Expect(httpReq.ProtoMajor).To(BeEquivalentTo(1))
	t.Expect(httpReq.ProtoMinor).To(BeEquivalentTo(1))
	relTimeIsInBetween(t, httpReq.ReqSentAt, traceBeginAsRel, trace.TraceEndAt)
	t.Expect(httpReq.ReqError).ToNot(BeZero())
	t.Expect(httpReq.ReqMethod).To(Equal("GET"))
	t.Expect(httpReq.ReqURL).To(Equal("https://198.51.100.100"))
	t.Expect(httpReq.ReqHeader).To(BeEmpty())
	t.Expect(httpReq.ReqContentLen).To(BeZero())
	t.Expect(httpReq.RespRecvAt.Undefined()).To(BeTrue())
	t.Expect(httpReq.RespStatusCode).To(BeZero())
	t.Expect(httpReq.RespHeader).To(BeEmpty())
	t.Expect(httpReq.RespContentLen).To(BeZero())

	err = client.Close()
	t.Expect(err).ToNot(HaveOccurred())
}

func TestReusedTCPConn(test *testing.T) {
	t := NewWithT(test)

	// Options that do not require administrative privileges.
	opts := []nettrace.TraceOpt{
		&nettrace.WithLogging{},
		&nettrace.WithHTTPReqTrace{
			HeaderFields: nettrace.HdrFieldsOptWithValues,
		},
		&nettrace.WithSockTrace{},
		&nettrace.WithDNSQueryTrace{},
	}
	client, err := nettrace.NewHTTPClient(nettrace.HTTPClientCfg{
		DisableKeepAlive: false, // allow TCP conn to be reused between HTTP requests
	}, opts...)
	t.Expect(err).ToNot(HaveOccurred())

	// First GET request
	req, err := http.NewRequest("GET", "https://www.example.com", nil)
	t.Expect(err).ToNot(HaveOccurred())
	resp, err := client.Do(req)
	t.Expect(err).ToNot(HaveOccurred())
	t.Expect(resp).ToNot(BeNil())
	t.Expect(resp.StatusCode).To(Equal(200))
	t.Expect(resp.Body).ToNot(BeNil())
	body := new(strings.Builder)
	_, err = io.Copy(body, resp.Body)
	t.Expect(err).ToNot(HaveOccurred())
	err = resp.Body.Close()
	t.Expect(err).ToNot(HaveOccurred())

	trace, _, err := client.GetTrace("GET www.example.com over HTTPS for the first time")
	t.Expect(err).ToNot(HaveOccurred())

	// Dial trace
	t.Expect(trace.Dials).To(HaveLen(1)) // no redirects
	dial := trace.Dials[0]
	t.Expect(dial.TraceID).ToNot(BeZero())
	t.Expect(dial.DstAddress).To(Equal("www.example.com:443"))

	// HTTP request trace
	t.Expect(trace.HTTPRequests).To(HaveLen(1))
	httpReq := trace.HTTPRequests[0]
	t.Expect(httpReq.TraceID).ToNot(BeZero())
	t.Expect(httpReq.TCPConn.Undefined()).To(BeFalse())
	usedTCPConn := trace.TCPConns.Get(httpReq.TCPConn)
	t.Expect(usedTCPConn).ToNot(BeNil())
	t.Expect(usedTCPConn.FromDial == dial.TraceID).To(BeTrue())
	t.Expect(usedTCPConn.Reused).To(BeFalse())
	t.Expect(usedTCPConn.ConnCloseAt.Undefined()).To(BeTrue())
	t.Expect(usedTCPConn.TotalRecvBytes).ToNot(BeZero())
	t.Expect(usedTCPConn.TotalSentBytes).ToNot(BeZero())

	// TLS tunnel trace.
	t.Expect(trace.TLSTunnels).To(HaveLen(1))
	tlsTun := trace.TLSTunnels[0]
	t.Expect(tlsTun.TraceID).ToNot(BeZero())
	t.Expect(tlsTun.TCPConn == usedTCPConn.TraceID).To(BeTrue())

	// Idle TCP connection should not be removed from the trace
	err = client.ClearTrace()
	t.Expect(err).ToNot(HaveOccurred())

	// Second request to the same destination
	req, err = http.NewRequest("GET", "https://www.example.com", nil)
	t.Expect(err).ToNot(HaveOccurred())
	resp, err = client.Do(req)
	t.Expect(err).ToNot(HaveOccurred())
	t.Expect(resp).ToNot(BeNil())
	t.Expect(resp.StatusCode).To(Equal(200))
	t.Expect(resp.Body).ToNot(BeNil())
	body = new(strings.Builder)
	_, err = io.Copy(body, resp.Body)
	t.Expect(err).ToNot(HaveOccurred())
	err = resp.Body.Close()
	t.Expect(err).ToNot(HaveOccurred())

	trace, _, err = client.GetTrace("GET www.example.com over HTTPS for the second time")
	t.Expect(err).ToNot(HaveOccurred())
	traceBeginAsRel := nettrace.Timestamp{IsRel: true, Rel: 0}

	// No dialing this time - connection is reused.
	t.Expect(trace.Dials).To(BeEmpty())
	t.Expect(trace.DNSQueries).To(BeEmpty())
	t.Expect(trace.UDPConns).To(BeEmpty())
	t.Expect(trace.TLSTunnels).To(BeEmpty())

	// HTTP request trace
	t.Expect(trace.HTTPRequests).To(HaveLen(1))
	httpReq = trace.HTTPRequests[0]
	t.Expect(httpReq.TraceID).ToNot(BeZero())
	t.Expect(httpReq.TCPConn == usedTCPConn.TraceID).To(BeTrue())
	t.Expect(httpReq.ProtoMajor).To(BeEquivalentTo(1))
	t.Expect(httpReq.ProtoMinor).To(BeEquivalentTo(1))
	t.Expect(httpReq.NetworkProxy).To(BeZero())
	relTimeIsInBetween(t, httpReq.ReqSentAt, traceBeginAsRel, trace.TraceEndAt)
	t.Expect(httpReq.ReqError).To(BeZero())
	t.Expect(httpReq.ReqMethod).To(Equal("GET"))
	t.Expect(httpReq.ReqURL).To(Equal("https://www.example.com"))
	t.Expect(httpReq.ReqHeader).To(BeEmpty())
	t.Expect(httpReq.ReqContentLen).To(BeZero())
	relTimeIsInBetween(t, httpReq.RespRecvAt, httpReq.ReqSentAt, trace.TraceEndAt)
	t.Expect(httpReq.RespStatusCode).To(Equal(200))
	t.Expect(httpReq.RespHeader).ToNot(BeEmpty())
	contentType := httpReq.RespHeader.Get("content-type")
	t.Expect(contentType).ToNot(BeNil())
	t.Expect(contentType.FieldVal).To(ContainSubstring("text/html"))
	t.Expect(contentType.FieldValLen).To(BeEquivalentTo(len(contentType.FieldVal)))
	t.Expect(httpReq.RespContentLen).ToNot(BeZero())

	// Reused TCP connection trace
	usedTCPConn = trace.TCPConns.Get(usedTCPConn.TraceID)
	t.Expect(usedTCPConn).ToNot(BeNil())
	t.Expect(usedTCPConn.FromDial == dial.TraceID).To(BeTrue())
	t.Expect(usedTCPConn.Reused).To(BeTrue())
	t.Expect(usedTCPConn.HandshakeBeginAt.IsRel).To(BeFalse())
	t.Expect(usedTCPConn.HandshakeEndAt.IsRel).To(BeFalse())
	t.Expect(usedTCPConn.HandshakeBeginAt.Abs.Before(usedTCPConn.HandshakeEndAt.Abs)).To(BeTrue())
	t.Expect(usedTCPConn.HandshakeEndAt.Abs.Before(trace.TraceBeginAt.Abs)).To(BeTrue())
	t.Expect(usedTCPConn.ConnCloseAt.Undefined()).To(BeTrue())
	t.Expect(usedTCPConn.TotalRecvBytes).ToNot(BeZero())
	t.Expect(usedTCPConn.TotalSentBytes).ToNot(BeZero())

	err = client.Close()
	t.Expect(err).ToNot(HaveOccurred())
}

func TestAllNameserversSkipped(test *testing.T) {
	t := NewWithT(test)

	opts := []nettrace.TraceOpt{
		&nettrace.WithHTTPReqTrace{},
		&nettrace.WithDNSQueryTrace{},
	}
	client, err := nettrace.NewHTTPClient(nettrace.HTTPClientCfg{
		SkipNameserver: func(ipAddr net.IP, port uint16) (skip bool, reason string) {
			return true, "skipping any configured nameserver"
		},
	}, opts...)
	t.Expect(err).ToNot(HaveOccurred())

	req, err := http.NewRequest("GET", "https://www.example.com", nil)
	t.Expect(err).ToNot(HaveOccurred())
	resp, err := client.Do(req)
	t.Expect(err).To(HaveOccurred())
	t.Expect(resp).To(BeNil())

	trace, _, err := client.GetTrace("GET www.example.com but skip all nameservers")
	t.Expect(err).ToNot(HaveOccurred())

	// Dial trace
	t.Expect(trace.Dials).To(HaveLen(1))
	dial := trace.Dials[0]
	t.Expect(dial.TraceID).ToNot(BeZero())
	t.Expect(dial.DstAddress).To(Equal("www.example.com:443"))
	t.Expect(dial.DialErr).To(ContainSubstring("skipping any configured nameserver"))
	t.Expect(dial.ResolverDials).To(BeEmpty())
	t.Expect(dial.SkippedNameservers).ToNot(BeEmpty())

	t.Expect(trace.DNSQueries).To(BeEmpty())
	t.Expect(trace.UDPConns).To(BeEmpty())
	t.Expect(trace.TCPConns).To(BeEmpty())
	t.Expect(trace.TLSTunnels).To(BeEmpty())

	t.Expect(trace.HTTPRequests).To(HaveLen(1))
	httpReq := trace.HTTPRequests[0]
	t.Expect(httpReq.TraceID).ToNot(BeZero())
	t.Expect(httpReq.TCPConn.Undefined()).To(BeTrue())
	t.Expect(httpReq.ReqMethod).To(Equal("GET"))
	t.Expect(httpReq.ReqURL).To(Equal("https://www.example.com"))
	t.Expect(httpReq.ReqError).To(ContainSubstring("skipping any configured nameserver"))

	err = client.Close()
	t.Expect(err).ToNot(HaveOccurred())
}

func getLinkForDefaultRoute() (netlink.Link, error) {
	routes, err := netlink.RouteList(nil, netlink.FAMILY_ALL)
	if err != nil {
		return nil, err
	}
	for _, r := range routes {
		if (r.Dst == nil || r.Dst.IP.IsUnspecified()) && r.Gw != nil {
			link, err := netlink.LinkByIndex(r.LinkIndex)
			if err != nil {
				return nil, err
			}
			return link, nil
		}
	}
	return nil, os.ErrNotExist
}

func getSourceIPForDefaultRoute(t *testing.T) net.IP {
	link, err := getLinkForDefaultRoute()
	if err != nil {
		t.Skipf("Skipping test: no default route found (%v)", err)
	}
	ifName := link.Attrs().Name

	addrs, err := netlink.AddrList(link, netlink.FAMILY_ALL)
	if err != nil {
		t.Skipf("Skipping test: failed to list addresses for %s: %v", ifName, err)
	}

	for _, addr := range addrs {
		if addr.IP == nil || addr.IP.IsLinkLocalUnicast() {
			continue
		}
		return addr.IP
	}

	t.Skipf("Skipping test: failed to get usable IP address for %s: %v", ifName, err)
	return nil
}

func TestWithSourceIP(test *testing.T) {
	t := NewWithT(test)

	opts := []nettrace.TraceOpt{
		&nettrace.WithLogging{
			CustomLogger: logrus.New(),
		},
		&nettrace.WithHTTPReqTrace{},
		&nettrace.WithDNSQueryTrace{},
	}
	sourceIP := getSourceIPForDefaultRoute(test)
	fmt.Println(sourceIP)
	client, err := nettrace.NewHTTPClient(nettrace.HTTPClientCfg{
		PreferHTTP2:      true,
		ReqTimeout:       5 * time.Second,
		DisableKeepAlive: true,
		SourceIP:         sourceIP,
	}, opts...)
	t.Expect(err).ToNot(HaveOccurred())

	req, err := http.NewRequest("GET", "https://www.example.com", nil)
	t.Expect(err).ToNot(HaveOccurred())
	req.Header.Set("Accept", "text/html")
	resp, err := client.Do(req)
	t.Expect(err).ToNot(HaveOccurred())
	t.Expect(resp).ToNot(BeNil())
	t.Expect(resp.StatusCode).To(Equal(200))
	t.Expect(resp.Body).ToNot(BeNil())
	body := new(strings.Builder)
	_, err = io.Copy(body, resp.Body)
	t.Expect(err).ToNot(HaveOccurred())
	err = resp.Body.Close()
	t.Expect(err).ToNot(HaveOccurred())
	t.Expect(body.String()).To(ContainSubstring("<html>"))
	t.Expect(body.String()).To(ContainSubstring("</html>"))

	trace, pcap, err := client.GetTrace("GET www.example.com with source IP set")
	t.Expect(err).ToNot(HaveOccurred())
	t.Expect(pcap).To(BeEmpty())

	t.Expect(trace.Dials).To(HaveLen(1))
	dial := trace.Dials[0]
	t.Expect(dial.DstAddress).To(Equal("www.example.com:443"))
	t.Expect(dial.EstablishedConn).ToNot(BeZero())
	t.Expect(trace.DNSQueries).ToNot(BeEmpty())
	for _, dnsQuery := range trace.DNSQueries {
		t.Expect(dnsQuery.DNSQueryMsgs).To(HaveLen(1))
		t.Expect(dnsQuery.DNSReplyMsgs).To(HaveLen(1))
	}
	t.Expect(trace.UDPConns).ToNot(BeEmpty())
	t.Expect(trace.TCPConns).ToNot(BeEmpty())
	for _, tcpConn := range trace.TCPConns {
		t.Expect(tcpConn.AddrTuple.SrcIP).To(Equal(sourceIP.String()))
		t.Expect(tcpConn.AddrTuple.DstIP).ToNot(BeEmpty())
	}
	t.Expect(trace.TLSTunnels).To(HaveLen(1))
	tlsTun := trace.TLSTunnels[0]
	t.Expect(tlsTun.ServerName).To(Equal("www.example.com"))
	t.Expect(tlsTun.NegotiatedProto).To(Equal("h2"))
	t.Expect(tlsTun.PeerCerts).ToNot(BeEmpty())
	t.Expect(trace.HTTPRequests).To(HaveLen(1))
	httpReq := trace.HTTPRequests[0]
	t.Expect(httpReq.RespStatusCode).To(Equal(200))

	err = client.Close()
	t.Expect(err).ToNot(HaveOccurred())
}
