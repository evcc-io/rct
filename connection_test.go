package rct

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestHandleIgnoresUnsolicitedResponses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &Connection{
		cache:   newCache(),
		pending: make(map[Identifier]struct{}),
	}

	dgC := make(chan *Datagram)
	go c.handle(ctx, dgC)

	foreign := &Datagram{Response, TotalEnergySolarGenBWh, []byte{0x4b, 0x8a, 0x90, 0x16}} // 18161708 Wh
	own := &Datagram{Response, TotalEnergySolarGenBWh, []byte{0x40, 0xe2, 0x25, 0xbc}}     // 7.07 Wh

	// unsolicited response must not be cached
	dgC <- foreign
	dgC <- &Datagram{Cmd: Read} // barrier: handle has processed foreign once this is received
	if dg, _ := c.cache.Get(TotalEnergySolarGenBWh); dg != nil {
		t.Fatalf("unsolicited response cached: %v", dg)
	}

	// response to an outstanding read is cached
	c.expectResponse(TotalEnergySolarGenBWh)
	dgC <- own
	waitFor(t, func() bool {
		dg, _ := c.cache.Get(TotalEnergySolarGenBWh)
		return dg != nil
	})

	// a second, unsolicited response must not overwrite the cached value
	dgC <- foreign
	dgC <- &Datagram{Cmd: Read}
	dg, _ := c.cache.Get(TotalEnergySolarGenBWh)
	if !bytes.Equal(dg.Data, own.Data) {
		t.Fatalf("cache overwritten by unsolicited response: %v", dg)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timeout")
		}
		time.Sleep(time.Millisecond)
	}
}
