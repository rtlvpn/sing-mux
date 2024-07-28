package invmux

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

type Client struct {
	dialer         N.Dialer
	logger         logger.Logger
	protocol       byte
	maxConnections int
	minStreams     int
	maxStreams     int
	padding        bool
	brutal         BrutalOptions
	connections    []*inverseConn
	writeChan      chan []byte
	readChan       chan []byte
	mu             sync.Mutex
}

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

type BrutalOptions struct {
	Enabled    bool
	SendBPS    uint64
	ReceiveBPS uint64
}

func NewClient(options Options) (*Client, error) {
	if options.MaxConnections <= 0 {
		options.MaxConnections = 1
	}
	return &Client{
		dialer:         options.Dialer,
		logger:         options.Logger,
		protocol:       protocolFromString(options.Protocol),
		maxConnections: options.MaxConnections,
		minStreams:     options.MinStreams,
		maxStreams:     options.MaxStreams,
		padding:        options.Padding,
		brutal:         options.Brutal,
		writeChan:      make(chan []byte, 100),
		readChan:       make(chan []byte, 100),
	}, nil
}

func protocolFromString(protocol string) byte {
	switch protocol {
	case "h2mux":
		return 0
	case "smux":
		return 1
	case "yamux":
		return 2
	default:
		return 0
	}
}

func (c *Client) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := 0; i < c.maxConnections; i++ {
		conn, err := c.dialer.DialContext(ctx, network, destination)
		if err != nil {
			c.logger.Error("failed to dial connection:", err)
			continue
		}
		ic := &inverseConn{conn: conn}
		c.connections = append(c.connections, ic)
		go c.readLoop(ic)
		go c.writeLoop(ic)
	}

	if len(c.connections) == 0 {
		return nil, io.ErrNoProgress
	}

	return &clientConn{client: c, destination: destination}, nil
}

func (c *Client) readLoop(ic *inverseConn) {
	for {
		buffer := buf.New()
		_, err := buffer.ReadFrom(ic.conn)
		if err != nil {
			c.logger.Error("read error:", err)
			return
		}
		c.readChan <- buffer.Bytes()
		buffer.Release()
	}
}

func (c *Client) writeLoop(ic *inverseConn) {
	for data := range c.writeChan {
		_, err := ic.conn.Write(data)
		if err != nil {
			c.logger.Error("write error:", err)
			return
		}
	}
}

type inverseConn struct {
	conn net.Conn
}

type clientConn struct {
	client      *Client
	destination M.Socksaddr
	buffer      []byte
}

func (cc *clientConn) Read(b []byte) (n int, err error) {
	if len(cc.buffer) > 0 {
		n = copy(b, cc.buffer)
		cc.buffer = cc.buffer[n:]
		return
	}

	select {
	case data := <-cc.client.readChan:
		n = copy(b, data)
		if n < len(data) {
			cc.buffer = data[n:]
		}
	default:
		err = io.EOF
	}
	return
}

func (cc *clientConn) Write(b []byte) (n int, err error) {
	cc.client.writeChan <- b
	return len(b), nil
}

func (cc *clientConn) Close() error {
	close(cc.client.writeChan)
	for _, conn := range cc.client.connections {
		conn.conn.Close()
	}
	return nil
}

func (cc *clientConn) LocalAddr() net.Addr                { return cc.client.connections[0].conn.LocalAddr() }
func (cc *clientConn) RemoteAddr() net.Addr               { return cc.destination.TCPAddr() }
func (cc *clientConn) SetDeadline(t time.Time) error      { return nil }
func (cc *clientConn) SetReadDeadline(t time.Time) error  { return nil }
func (cc *clientConn) SetWriteDeadline(t time.Time) error { return nil }

func (c *Client) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.connections {
		conn.conn.Close()
	}
	c.connections = nil
}

func (c *Client) Close() error {
	c.Reset()
	return nil
}

type Service struct {
	newStreamContext func(context.Context, net.Conn) context.Context
	logger           logger.ContextLogger
	handler          ServiceHandler
	padding          bool
	brutal           BrutalOptions
}

type ServiceHandler interface {
	N.TCPConnectionHandler
	N.UDPConnectionHandler
}

type ServiceOptions struct {
	NewStreamContext func(context.Context, net.Conn) context.Context
	Logger           logger.ContextLogger
	Handler          ServiceHandler
	Padding          bool
	Brutal           BrutalOptions
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

var Destination = M.Socksaddr{Fqdn: "inverse-mux"}

func (s *Service) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	return s.handler.NewConnection(ctx, conn, metadata)
}

func (s *Service) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata M.Metadata) error {
	return s.handler.NewPacketConnection(ctx, conn, metadata)
}

// Dummy implementations for additional functions to satisfy the original interface

func SetBrutalOptions(conn net.Conn, sendBPS uint64) error {
	// Dummy implementation
	return nil
}

const (
	BrutalAvailable   = true
	BrutalMinSpeedBPS = 65536
)

func WriteBrutalRequest(writer io.Writer, receiveBPS uint64) error {
	// Dummy implementation
	return nil
}

func ReadBrutalRequest(reader io.Reader) (uint64, error) {
	// Dummy implementation
	return 0, nil
}

func WriteBrutalResponse(writer io.Writer, receiveBPS uint64, ok bool, message string) error {
	// Dummy implementation
	return nil
}

func ReadBrutalResponse(reader io.Reader) (uint64, error) {
	// Dummy implementation
	return 0, nil
}

// Additional structs and functions to satisfy the original interface

type Request struct {
	Version  byte
	Protocol byte
	Padding  bool
}

func ReadRequest(reader io.Reader) (*Request, error) {
	// Dummy implementation
	return &Request{}, nil
}

type StreamRequest struct {
	Network     string
	Destination M.Socksaddr
	PacketAddr  bool
}

func ReadStreamRequest(reader io.Reader) (*StreamRequest, error) {
	// Dummy implementation
	return &StreamRequest{}, nil
}

func EncodeStreamRequest(request StreamRequest, buffer *buf.Buffer) error {
	// Dummy implementation
	return nil
}

type StreamResponse struct {
	Status  uint8
	Message string
}

func ReadStreamResponse(reader io.Reader) (*StreamResponse, error) {
	// Dummy implementation
	return &StreamResponse{}, nil
}

func (c *Client) DialPacket(ctx context.Context, destination M.Socksaddr) (N.PacketConn, error) {
	// Dummy implementation
	return nil, E.New("not implemented")
}

func (s *Service) NewPacket(ctx context.Context, conn N.PacketConn, metadata M.Metadata) error {
	// Dummy implementation
	return E.New("not implemented")
}
