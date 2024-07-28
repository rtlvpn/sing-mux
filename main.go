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
	DefaultChunkSize = 16384
)

type Client struct {
	dialer         N.Dialer
	logger         logger.ContextLogger
	maxConnections int
	minStreams     int
	maxStreams     int
	padding        bool
	chunkSize      int
	access         sync.Mutex
	sessions       map[string]*imuxSession
	brutal         BrutalOptions
}

type Options struct {
	Dialer         N.Dialer
	Logger         logger.ContextLogger
	Protocol       string
	MaxConnections int
	MinStreams     int
	MaxStreams     int
	Padding        bool
	ChunkSize      int
	Brutal         BrutalOptions
}

type BrutalOptions struct {
	Enabled    bool
	SendBPS    uint64
	ReceiveBPS uint64
}

func NewClient(options Options) (*Client, error) {
	if options.ChunkSize == 0 {
		options.ChunkSize = DefaultChunkSize
	}
	return &Client{
		dialer:         options.Dialer,
		logger:         options.Logger,
		maxConnections: options.MaxConnections,
		minStreams:     options.MinStreams,
		maxStreams:     options.MaxStreams,
		padding:        options.Padding,
		chunkSize:      options.ChunkSize,
		sessions:       make(map[string]*imuxSession),
		brutal:         options.Brutal,
	}, nil
}

func (c *Client) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	c.access.Lock()
	defer c.access.Unlock()

	sessionKey := destination.String()
	session, ok := c.sessions[sessionKey]
	if !ok {
		session = &imuxSession{
			client:      c,
			destination: destination,
			connections: make([]*imuxConn, 0, c.maxConnections),
			chunks:      make(chan *buf.Buffer, 100),
		}
		c.sessions[sessionKey] = session
	}

	if len(session.connections) < c.maxConnections {
		for i := len(session.connections); i < c.maxConnections; i++ {
			conn, err := c.dialer.DialContext(ctx, network, destination)
			if err != nil {
				c.logger.ErrorContext(ctx, "failed to establish connection:", err)
				continue
			}
			imuxConn := &imuxConn{
				Conn:    conn,
				session: session,
			}
			session.connections = append(session.connections, imuxConn)
			go session.readLoop(imuxConn)
		}
	}

	if len(session.connections) == 0 {
		return nil, io.ErrNoProgress
	}

	return &imuxClientConn{
		session: session,
	}, nil
}

func (c *Client) Reset() {
	c.access.Lock()
	defer c.access.Unlock()
	for _, session := range c.sessions {
		session.Close()
	}
	c.sessions = make(map[string]*imuxSession)
}

func (c *Client) Close() error {
	c.Reset()
	return nil
}

type imuxSession struct {
	client      *Client
	destination M.Socksaddr
	connections []*imuxConn
	chunks      chan *buf.Buffer
	nextConn    int
	access      sync.Mutex
}

func (s *imuxSession) readLoop(conn *imuxConn) {
	for {
		buffer := buf.New()
		_, err := buffer.ReadFrom(conn)
		if err != nil {
			buffer.Release()
			s.removeConnection(conn)
			return
		}
		s.chunks <- buffer
	}
}

func (s *imuxSession) removeConnection(conn *imuxConn) {
	s.access.Lock()
	defer s.access.Unlock()
	for i, c := range s.connections {
		if c == conn {
			s.connections = append(s.connections[:i], s.connections[i+1:]...)
			break
		}
	}
	conn.Close()
}

func (s *imuxSession) Write(b []byte) (int, error) {
	s.access.Lock()
	defer s.access.Unlock()
	if len(s.connections) == 0 {
		return 0, io.ErrClosedPipe
	}
	conn := s.connections[s.nextConn]
	s.nextConn = (s.nextConn + 1) % len(s.connections)
	return conn.Write(b)
}

func (s *imuxSession) Close() error {
	s.access.Lock()
	defer s.access.Unlock()
	for _, conn := range s.connections {
		conn.Close()
	}
	s.connections = nil
	close(s.chunks)
	return nil
}

type imuxConn struct {
	net.Conn
	session *imuxSession
}

type imuxClientConn struct {
	session *imuxSession
	buffer  *buf.Buffer
}

func (c *imuxClientConn) Read(b []byte) (n int, err error) {
	if c.buffer == nil {
		select {
		case c.buffer = <-c.session.chunks:
		case <-time.After(30 * time.Second):
			return 0, E.New("read timeout")
		}
	}

	n, err = c.buffer.Read(b)
	if c.buffer.IsEmpty() {
		c.buffer.Release()
		c.buffer = nil
	}
	return
}

func (c *imuxClientConn) Write(b []byte) (n int, err error) {
	return c.session.Write(b)
}

func (c *imuxClientConn) Close() error {
	return c.session.Close()
}

func (c *imuxClientConn) LocalAddr() net.Addr                { return nil }
func (c *imuxClientConn) RemoteAddr() net.Addr               { return c.session.destination }
func (c *imuxClientConn) SetDeadline(t time.Time) error      { return nil }
func (c *imuxClientConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *imuxClientConn) SetWriteDeadline(t time.Time) error { return nil }

// Implement additional interfaces
func (c *imuxClientConn) NeedAdditionalReadDeadline() bool { return false }
func (c *imuxClientConn) Upstream() any                    { return c.session }
func (c *imuxClientConn) ReaderReplaceable() bool          { return false }
func (c *imuxClientConn) WriterReplaceable() bool          { return false }

type Service struct {
	newStreamContext func(context.Context, net.Conn) context.Context
	handler          ServiceHandler
	logger           logger.ContextLogger
	padding          bool
	brutal           BrutalOptions
}

type ServiceHandler interface {
	N.TCPConnectionHandler
	N.UDPConnectionHandler
}

type ServiceOptions struct {
	NewStreamContext func(context.Context, net.Conn) context.Context
	Handler          ServiceHandler
	Logger           logger.ContextLogger
	Padding          bool
	Brutal           BrutalOptions
}

func NewService(options ServiceOptions) (*Service, error) {
	return &Service{
		newStreamContext: options.NewStreamContext,
		handler:          options.Handler,
		logger:           options.Logger,
		padding:          options.Padding,
		brutal:           options.Brutal,
	}, nil
}

func (s *Service) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	if s.newStreamContext != nil {
		ctx = s.newStreamContext(ctx, conn)
	}
	return s.handler.NewConnection(ctx, conn, metadata)
}

// Implement additional methods and structures to match sing-mux

type StreamRequest struct {
	Network     string
	Destination M.Socksaddr
	PacketAddr  bool
}

type StreamResponse struct {
	Status  uint8
	Message string
}

func ReadStreamRequest(reader io.Reader) (*StreamRequest, error) {
	// Implement actual reading logic
	return &StreamRequest{}, nil
}

func ReadStreamResponse(reader io.Reader) (*StreamResponse, error) {
	// Implement actual reading logic
	return &StreamResponse{}, nil
}

func WriteBrutalRequest(writer io.Writer, receiveBPS uint64) error {
	// Implement if needed
	return nil
}

func ReadBrutalRequest(reader io.Reader) (uint64, error) {
	// Implement if needed
	return 0, nil
}

func WriteBrutalResponse(writer io.Writer, receiveBPS uint64, ok bool, message string) error {
	// Implement if needed
	return nil
}

func ReadBrutalResponse(reader io.Reader) (uint64, error) {
	// Implement if needed
	return 0, nil
}

const (
	BrutalAvailable = false
)

func SetBrutalOptions(conn net.Conn, sendBPS uint64) error {
	// Implement if needed
	return nil
}
