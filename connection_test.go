package rct

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/evcc-io/rct/internal"
)

func TestWriteVerified(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer func(d time.Duration) { VerifyTimeout = d }(VerifyTimeout)
	VerifyTimeout = 200 * time.Millisecond

	local, remote := net.Pipe()
	defer local.Close()
	go io.Copy(io.Discard, remote)

	c := &Connection{
		conn:    local,
		cache:   newCache(),
		pending: make(map[Identifier]struct{}),
		broker:  internal.NewBroker[*Datagram](),
		logger:  func(string, ...any) {},
	}
	go c.broker.Start(ctx)

	written := []byte{0x3d, 0x8f, 0x5c, 0x29}  // 0.07
	previous := []byte{0x3f, 0x3a, 0x50, 0xf1} // 0.73

	// respond with the given values once WriteVerified has subscribed and sent its datagrams
	respond := func(values ...[]byte) {
		go func() {
			time.Sleep(50 * time.Millisecond)
			for _, v := range values {
				c.broker.Publish(&Datagram{Response, BatterySoCTargetMin, v})
			}
		}()
	}

	respond(previous, written)
	if err := c.WriteVerified(BatterySoCTargetMin, written); err != nil {
		t.Fatalf("expected confirmed write, got %v", err)
	}

	respond(previous)
	if err := c.WriteVerified(BatterySoCTargetMin, written); err == nil || !strings.Contains(err.Error(), "not applied") {
		t.Fatalf("expected not applied error, got %v", err)
	}

	if err := c.WriteVerified(BatterySoCTargetMin, written); err == nil || !strings.Contains(err.Error(), "not confirmed") {
		t.Fatalf("expected not confirmed error, got %v", err)
	}
}

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
