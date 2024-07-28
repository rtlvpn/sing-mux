package mux

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

const (
	defaultBufferSize = 4096
	headerSize        = 8 // 4 bytes for length, 4 bytes for sequence number
)

// Options struct to maintain compatibility
type Options struct {
	Dialer         N.Dialer
	Logger         logger.Logger
	Protocol       string
	MaxConnections int
	MinStreams     int
	MaxStreams     int
	Padding        bool
	Brutal         BrutalOptions
}

// BrutalOptions struct to maintain compatibility
type BrutalOptions struct {
	Enabled    bool
	SendBPS    uint64
	ReceiveBPS uint64
}

// Client represents the client-side of the inverse multiplexer
type Client struct {
	dialer         N.Dialer
	logger         logger.Logger
	protocol       string
	maxConnections int
	minStreams     int
	maxStreams     int
	padding        bool
	brutal         BrutalOptions
	connections    []*subConn
	mu             sync.Mutex
	nextSeq        uint32
}

// NewClient creates a new inverse multiplexing client
func NewClient(options Options) (*Client, error) {
	return &Client{
		dialer:         options.Dialer,
		logger:         options.Logger,
		protocol:       options.Protocol,
		maxConnections: options.MaxConnections,
		minStreams:     options.MinStreams,
		maxStreams:     options.MaxStreams,
		padding:        options.Padding,
		brutal:         options.Brutal,
	}, nil
}

// DialContext establishes the inverse multiplexed connection
func (c *Client) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for len(c.connections) < c.maxConnections {
		conn, err := c.dialer.DialContext(ctx, network, destination)
		if err != nil {
			return nil, err
		}
		subConn := &subConn{Conn: conn, id: c.nextSeq}
		c.connections = append(c.connections, subConn)
		c.nextSeq++
	}

	imuxConn := &imuxConn{
		client:     c,
		readBuffer: make(chan []byte, c.maxConnections),
		closeChan:  make(chan struct{}),
	}

	for _, conn := range c.connections {
		go imuxConn.readRoutine(conn)
	}

	return imuxConn, nil
}

// Dummy methods to satisfy the interface
func (c *Client) Reset()       {}
func (c *Client) Close() error { return nil }

type subConn struct {
	net.Conn
	id uint32
}

type imuxConn struct {
	client     *Client
	readBuffer chan []byte
	writeMu    sync.Mutex
	closeChan  chan struct{}
	closeOnce  sync.Once
}

func (c *imuxConn) readRoutine(conn *subConn) {
	defer conn.Close()

	for {
		header := make([]byte, headerSize)
		_, err := io.ReadFull(conn, header)
		if err != nil {
			return
		}

		length := binary.BigEndian.Uint32(header[:4])
		data := make([]byte, length)
		_, err = io.ReadFull(conn, data)
		if err != nil {
			return
		}

		select {
		case c.readBuffer <- data:
		case <-c.closeChan:
			return
		}
	}
}

func (c *imuxConn) Read(b []byte) (n int, err error) {
	select {
	case data := <-c.readBuffer:
		return copy(b, data), nil
	case <-c.closeChan:
		return 0, io.EOF
	}
}

func (c *imuxConn) Write(b []byte) (n int, err error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	for len(b) > 0 {
		chunk := b
		if len(chunk) > defaultBufferSize {
			chunk = chunk[:defaultBufferSize]
		}

		header := make([]byte, headerSize)
		binary.BigEndian.PutUint32(header[:4], uint32(len(chunk)))
		binary.BigEndian.PutUint32(header[4:], c.client.nextSeq)
		c.client.nextSeq++

		for _, conn := range c.client.connections {
			if _, err := conn.Write(header); err != nil {
				return n, err
			}
			if _, err := conn.Write(chunk); err != nil {
				return n, err
			}
		}

		n += len(chunk)
		b = b[len(chunk):]
	}

	return n, nil
}

func (c *imuxConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closeChan)
		for _, conn := range c.client.connections {
			conn.Close()
		}
	})
	return nil
}

func (c *imuxConn) LocalAddr() net.Addr  { return c.client.connections[0].LocalAddr() }
func (c *imuxConn) RemoteAddr() net.Addr { return c.client.connections[0].RemoteAddr() }

func (c *imuxConn) SetDeadline(t time.Time) error {
	for _, conn := range c.client.connections {
		if err := conn.SetDeadline(t); err != nil {
			return err
		}
	}
	return nil
}

func (c *imuxConn) SetReadDeadline(t time.Time) error {
	for _, conn := range c.client.connections {
		if err := conn.SetReadDeadline(t); err != nil {
			return err
		}
	}
	return nil
}

func (c *imuxConn) SetWriteDeadline(t time.Time) error {
	for _, conn := range c.client.connections {
		if err := conn.SetWriteDeadline(t); err != nil {
			return err
		}
	}
	return nil
}

// Service represents the server-side of the inverse multiplexer
type Service struct {
	newStreamContext func(context.Context, net.Conn) context.Context
	logger           logger.ContextLogger
	handler          ServiceHandler
	padding          bool
	brutal           BrutalOptions
}

type ServiceOptions struct {
	NewStreamContext func(context.Context, net.Conn) context.Context
	Logger           logger.ContextLogger
	Handler          ServiceHandler
	Padding          bool
	Brutal           BrutalOptions
}

type ServiceHandler interface {
	N.TCPConnectionHandler
	N.UDPConnectionHandler
}

func NewService(options ServiceOptions) (*Service, error) {
	return &Service{
		newStreamContext: options.NewStreamContext,
		logger:           options.Logger,
		handler:          options.Handler,
		padding:          options.Padding,
		brutal:           options.Brutal,
	}, nil
}

// Dummy method to satisfy the interface
func (s *Service) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	return nil
}

// Dummy functions to maintain compatibility
func WriteBrutalRequest(writer io.Writer, receiveBPS uint64) error { return nil }
func ReadBrutalRequest(reader io.Reader) (uint64, error)           { return 0, nil }
func WriteBrutalResponse(writer io.Writer, receiveBPS uint64, ok bool, message string) error {
	return nil
}
func ReadBrutalResponse(reader io.Reader) (uint64, error)  { return 0, nil }
func SetBrutalOptions(conn net.Conn, sendBPS uint64) error { return nil }

// Dummy structs and interfaces to maintain compatibility
type StreamRequest struct{}
type StreamResponse struct{}

func ReadStreamRequest(reader io.Reader) (*StreamRequest, error)   { return nil, nil }
func ReadStreamResponse(reader io.Reader) (*StreamResponse, error) { return nil, nil }

type ExtendedConn interface {
	net.Conn
	ReaderReplaceable() bool
	WriterReplaceable() bool
	Upstream() any
}

type ExtendedServerConn interface {
	ExtendedConn
	NeedAdditionalReadDeadline() bool
}

type ServerConn struct{}

func (c *ServerConn) NeedAdditionalReadDeadline() bool { return false }
func (c *ServerConn) ReaderReplaceable() bool          { return false }
func (c *ServerConn) WriterReplaceable() bool          { return false }
func (c *ServerConn) Upstream() any                    { return nil }

// Dummy buffer-related functions to maintain compatibility
func (c *imuxConn) WriteBuffer(buffer *buf.Buffer) error {
	_, err := c.Write(buffer.Bytes())
	return err
}

func (c *imuxConn) ReadBuffer(buffer *buf.Buffer) error {
	n, err := c.Read(buffer.FreeBytes())
	buffer.Truncate(n)
	return err
}

func (c *imuxConn) WriteTo(w io.Writer) (n int64, err error) {
	return bufio.Copy(w, c)
}

// Dummy error for compatibility
var ErrInvalidData = E.New("invalid data")
