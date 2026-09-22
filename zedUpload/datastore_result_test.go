// Copyright (c) 2026 Zededa, Inc.
// SPDX-License-Identifier: Apache-2.0

package zedUpload

import (
	"testing"
	"time"

	"github.com/lf-edge/eve-libs/zedUpload/types"
)

func TestPostResponseReachesALiveRequester(t *testing.T) {
	req := &DronaRequest{name: "blob", result: make(chan *DronaRequest)}
	received := make(chan *DronaRequest, 1)
	go func() { received <- <-req.result }()
	(&DronaCtx{}).postResponse(req, nil)
	select {
	case got := <-received:
		if got != req {
			t.Fatalf("requester received %p, want %p", got, req)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the requester never received the response")
	}
}

// TestPostResponseGivesUpOnAGoneRequester takes as long as the transport
// waits for a requester before giving up on it.
func TestPostResponseGivesUpOnAGoneRequester(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the result delivery timeout")
	}
	// Nobody ever reads this channel, as after the requester has returned.
	req := &DronaRequest{name: "blob", result: make(chan *DronaRequest)}
	returned := make(chan struct{})
	go func() {
		(&DronaCtx{}).postResponse(req, nil)
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Minute):
		t.Fatal("postResponse stayed blocked on a result channel nobody reads")
	}
}

func TestStatsUpdaterDoesNotPostAfterTheTransferEnds(t *testing.T) {
	// Nobody reads the result channel: after the transfer ends the requester
	// only reads the final response, which the transport posts separately.
	req := &DronaRequest{name: "blob", result: make(chan *DronaRequest)}
	prgNotif := make(types.StatsNotifChan)
	returned := make(chan struct{})
	go func() {
		statsUpdater(req, &DronaCtx{}, prgNotif)
		close(returned)
	}()
	prgNotif <- types.UpdateStats{Size: 4096, Asize: 4096}
	close(prgNotif)

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("statsUpdater did not return after the progress channel was closed, " +
			"it is posting an update nobody will read")
	}
	req.RLock()
	defer req.RUnlock()
	if req.objectSize != 4096 || req.asize != 4096 {
		t.Fatalf("final stats not recorded on the request: size %d/%d, want 4096/4096",
			req.asize, req.objectSize)
	}
}
