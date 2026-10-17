package rct

import (
	"bytes"
	"context"
	"testing"
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

	// unbuffered channel: once handle receives this, the previous datagram is fully processed
	barrier := &Datagram{Cmd: Read}

	foreign := &Datagram{Response, TotalEnergySolarGenBWh, []byte{0x4b, 0x8a, 0x90, 0x16}} // 18161708 Wh
	own := &Datagram{Response, TotalEnergySolarGenBWh, []byte{0x40, 0xe2, 0x25, 0xbc}}     // 7.07 Wh

	// unsolicited response must not be cached
	dgC <- foreign
	dgC <- barrier
	if dg, _ := c.cache.Get(TotalEnergySolarGenBWh); dg != nil {
		t.Fatalf("unsolicited response cached: %v", dg)
	}

	// response to an outstanding read is cached
	c.mu.Lock()
	c.pending[TotalEnergySolarGenBWh] = struct{}{}
	c.mu.Unlock()
	dgC <- own
	dgC <- barrier
	if dg, _ := c.cache.Get(TotalEnergySolarGenBWh); dg == nil {
		t.Fatal("requested response not cached")
	}

	// a second, unsolicited response must not overwrite the cached value
	dgC <- foreign
	dgC <- barrier
	if dg, _ := c.cache.Get(TotalEnergySolarGenBWh); !bytes.Equal(dg.Data, own.Data) {
		t.Fatalf("cache overwritten by unsolicited response: %v", dg)
	}
}
