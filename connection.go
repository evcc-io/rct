package rct

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/evcc-io/rct/internal"
)

var (
	// DialTimeout is the timeout for network connection
	DialTimeout = 5 * time.Second
	// SuccessTimeout is the timeout for data returned from device
	SuccessTimeout = 30 * time.Second
	// ErrMustRetry is returned when a query result was not found in the cache.
	// It's up to the client application to decide the timing for retrying the query.
	ErrMustRetry = errors.New("must retry")
)

// Connection to a RCT device
type Connection struct {
	mu      sync.Mutex
	conn    net.Conn
	cache   *cache
	broker  *internal.Broker[*Datagram]
	errCB   func(error)
	timeout time.Duration
	logger  func(format string, a ...any)
}

// WithErrorCallback sets the error callback. It is only invoked after initial connection succeeds.
func WithErrorCallback(cb func(error)) func(*Connection) {
	return func(c *Connection) {
		c.errCB = cb
	}
}

// WithTimeout sets the query timeout
func WithTimeout(timeout time.Duration) func(*Connection) {
	return func(c *Connection) {
		c.timeout = timeout
	}
}

// WithLogger sets the query timeout
func WithLogger(logger func(format string, a ...any)) func(*Connection) {
	return func(c *Connection) {
		c.logger = logger
	}
}

// Creates a new connection to a RCT device at the given address.
// Must not be called concurrently.
func NewConnection(ctx context.Context, host string, opt ...func(*Connection)) (*Connection, error) {
	conn := &Connection{
		cache:  newCache(),
		broker: internal.NewBroker[*Datagram](),
	}

	for _, o := range opt {
		o(conn)
	}

	bufC := make(chan byte, 1024)
	errC := make(chan error, 1)

	go conn.receive(ctx, net.JoinHostPort(host, "8899"), bufC, errC)

	// wait up to SuccessTimeout for first non-error response, i.e. successful read
	var lastErr error
	t := time.NewTimer(SuccessTimeout)
INIT:
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, errors.New("timeout")
		case err := <-errC:
			if err != nil {
				lastErr = err
				continue
			}
			break INIT
		}
	}

	go func() {
		for err := range errC {
			if conn.errCB != nil {
				conn.errCB(err)
			}
		}
	}()

	go conn.broker.Start(ctx)
	go ParseStream(ctx, bufC, conn.broker.PublishChan())
	go conn.handle(ctx, conn.broker.Subscribe())

	if conn.logger != nil {
		go func() {
			for dg := range conn.broker.Subscribe() {
				conn.log("recv", dg)
			}
		}()
	}

	return conn, nil
}

// receive streams received bytes from the connection
func (c *Connection) receive(ctx context.Context, addr string, bufC chan<- byte, errC chan<- error) {
	buf := make([]byte, 1024)
	defer c.close()

	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = 10 * time.Second

	for {
		conn, err := c.connect(ctx, addr)
		if err != nil {
			errC <- err
			time.Sleep(bo.NextBackOff())
			continue
		}

		n, err := conn.Read(buf)
		if err != nil {
			c.mu.Lock()
			c.close()
			c.mu.Unlock()

			errC <- err
			time.Sleep(bo.NextBackOff())
			continue
		}

		// ack data received
		errC <- nil
		bo.Reset()

		// stream received data
		for _, b := range buf[:n] {
			bufC <- b
		}
	}
}

// handle is the receiver go routine
func (c *Connection) connect(ctx context.Context, addr string) (net.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return c.conn, nil
	}

	ctx, cancel := context.WithTimeout(ctx, DialTimeout)
	defer cancel()

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err == nil {
		c.conn = conn
	}

	return conn, err
}

func (c *Connection) handle(ctx context.Context, dgC <-chan *Datagram) {
	for {
		select {
		case <-ctx.Done():
			return
		case dg := <-dgC:
			if dg.Cmd == Response || dg.Cmd == LongResponse {
				c.cache.Put(dg)
			}
		}
	}
}

// log is the logger go routine
func (c *Connection) log(action string, dg *Datagram) {
	c.logger(action + ": " + dg.String())
}

// Closes the connection
func (c *Connection) close() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *Connection) Send(dg *Datagram) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// ensure active connection
	if c.conn == nil {
		return 0, errors.New("disconnected")
	}

	c.log("send", dg)

	var rdb DatagramBuilder
	rdb.Build(dg)

	c.conn.SetWriteDeadline(time.Now().Add(DialTimeout))
	n, err := c.conn.Write(rdb.Bytes())
	if err != nil {
		c.close()
	}

	return n, err
}

func (c *Connection) Subscribe() chan *Datagram {
	return c.broker.Subscribe()
}

func (c *Connection) Unsubscribe(ch chan *Datagram) {
	c.broker.Unsubscribe(ch)
}

func (c *Connection) Get(id Identifier) (*Datagram, time.Time) {
	return c.cache.Get(id)
}

// Queries the given identifier on the RCT device, returning its value as a datagram
func (c *Connection) Query(id Identifier) (*Datagram, error) {
	if dg, ts := c.cache.Get(id); dg != nil && time.Since(ts) < c.timeout {
		return dg, nil
	}

	if _, err := c.Send(&Datagram{Read, id, nil}); err != nil {
		return nil, err
	}

	return nil, ErrMustRetry
}

// Queries the given identifier on the RCT device, returning its value as a float32
func (c *Connection) QueryFloat32(id Identifier) (float32, error) {
	dg, err := c.Query(id)
	if err != nil {
		return 0, err
	}
	return dg.Float32()
}

// Queries the given identifier on the RCT device, returning its value as a uint8
func (c *Connection) QueryInt32(id Identifier) (int32, error) {
	dg, err := c.Query(id)
	if err != nil {
		return 0, err
	}
	return dg.Int32()
}

// Queries the given identifier on the RCT device, returning its value as a uint16
func (c *Connection) QueryUint16(id Identifier) (uint16, error) {
	dg, err := c.Query(id)
	if err != nil {
		return 0, err
	}
	return dg.Uint16()
}

// Queries the given identifier on the RCT device, returning its value as a uint8
func (c *Connection) QueryUint8(id Identifier) (uint8, error) {
	dg, err := c.Query(id)
	if err != nil {
		return 0, err
	}
	return dg.Uint8()
}

// Writes the given identifier with the given value on the RCT device
func (c *Connection) Write(id Identifier, data []byte) error {
	_, err := c.Send(&Datagram{Write, id, data})
	return err
}
