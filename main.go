package mux

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/sagernet/sing/common/buf"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

const (
	defaultBufferSize = 4096
	headerSize        = 6 // 2 bytes for sequence number, 4 bytes for payload length
)

type Client struct {
	dialer         N.Dialer
	logger         logger.Logger
	maxConnections int
}

type Options struct {
	Dialer         N.Dialer
	Logger         logger.Logger
	MaxConnections int
}

func NewClient(options Options) (*Client, error) {
	if options.MaxConnections <= 0 {
		return nil, E.New("inverse mux: invalid MaxConnections")
	}
	return &Client{
		dialer:         options.Dialer,
		logger:         options.Logger,
		maxConnections: options.MaxConnections,
	}, nil
}

func (c *Client) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if network != "tcp" {
		return nil, E.New("inverse mux: only TCP is supported")
	}

	conns := make([]net.Conn, c.maxConnections)
	for i := 0; i < c.maxConnections; i++ {
		conn, err := c.dialer.DialContext(ctx, network, destination)
		if err != nil {
			for j := 0; j < i; j++ {
				conns[j].Close()
			}
			return nil, err
		}
		conns[i] = conn
	}

	return newInverseMultiplexConn(conns, c.logger), nil
}

type inverseMultiplexConn struct {
	conns       []net.Conn
	logger      logger.Logger
	writeMu     sync.Mutex
	readMu      sync.Mutex
	writeSeq    uint16
	readSeq     uint16
	readBuffers map[uint16]*buf.Buffer
	readChan    chan *buf.Buffer
	closeChan   chan struct{}
	closeOnce   sync.Once
}

func newInverseMultiplexConn(conns []net.Conn, logger logger.Logger) *inverseMultiplexConn {
	c := &inverseMultiplexConn{
		conns:       conns,
		logger:      logger,
		readBuffers: make(map[uint16]*buf.Buffer),
		readChan:    make(chan *buf.Buffer, len(conns)),
		closeChan:   make(chan struct{}),
	}
	for i := range conns {
		go c.readLoop(i)
	}
	return c
}

func (c *inverseMultiplexConn) readLoop(connIndex int) {
	for {
		header := make([]byte, headerSize)
		_, err := io.ReadFull(c.conns[connIndex], header)
		if err != nil {
			c.logger.Error("inverse mux: read header error:", err)
			c.Close()
			return
		}

		seq := uint16(header[0])<<8 | uint16(header[1])
		length := int(header[2])<<24 | int(header[3])<<16 | int(header[4])<<8 | int(header[5])

		buffer := buf.NewSize(length)
		_, err = io.ReadFull(c.conns[connIndex], buffer.Bytes())
		if err != nil {
			c.logger.Error("inverse mux: read payload error:", err)
			buffer.Release()
			c.Close()
			return
		}

		c.readMu.Lock()
		c.readBuffers[seq] = buffer
		for {
			if buffer, ok := c.readBuffers[c.readSeq]; ok {
				select {
				case c.readChan <- buffer:
					delete(c.readBuffers, c.readSeq)
					c.readSeq++
				case <-c.closeChan:
					c.readMu.Unlock()
					return
				}
			} else {
				break
			}
		}
		c.readMu.Unlock()
	}
}

func (c *inverseMultiplexConn) Read(b []byte) (n int, err error) {
	select {
	case buffer := <-c.readChan:
		n = copy(b, buffer.Bytes())
		buffer.Release()
		return n, nil
	case <-c.closeChan:
		return 0, io.EOF
	}
}

func (c *inverseMultiplexConn) Write(b []byte) (n int, err error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	for len(b) > 0 {
		chunkSize := defaultBufferSize
		if chunkSize > len(b) {
			chunkSize = len(b)
		}

		header := make([]byte, headerSize)
		header[0] = byte(c.writeSeq >> 8)
		header[1] = byte(c.writeSeq)
		header[2] = byte(chunkSize >> 24)
		header[3] = byte(chunkSize >> 16)
		header[4] = byte(chunkSize >> 8)
		header[5] = byte(chunkSize)

		conn := c.conns[c.writeSeq%uint16(len(c.conns))]
		_, err = conn.Write(header)
		if err != nil {
			return n, err
		}

		written, err := conn.Write(b[:chunkSize])
		n += written
		if err != nil {
			return n, err
		}

		b = b[chunkSize:]
		c.writeSeq++
	}

	return n, nil
}

func (c *inverseMultiplexConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closeChan)
		for _, conn := range c.conns {
			conn.Close()
		}
	})
	return nil
}

func (c *inverseMultiplexConn) LocalAddr() net.Addr  { return c.conns[0].LocalAddr() }
func (c *inverseMultiplexConn) RemoteAddr() net.Addr { return c.conns[0].RemoteAddr() }

func (c *inverseMultiplexConn) SetDeadline(t time.Time) error {
	for _, conn := range c.conns {
		if err := conn.SetDeadline(t); err != nil {
			return err
		}
	}
	return nil
}

func (c *inverseMultiplexConn) SetReadDeadline(t time.Time) error {
	for _, conn := range c.conns {
		if err := conn.SetReadDeadline(t); err != nil {
			return err
		}
	}
	return nil
}

func (c *inverseMultiplexConn) SetWriteDeadline(t time.Time) error {
	for _, conn := range c.conns {
		if err := conn.SetWriteDeadline(t); err != nil {
			return err
		}
	}
	return nil
}

// Service and Router implementations remain the same as in the previous version
