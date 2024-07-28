package mux

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

const (
	defaultMaxConnections = 4
	defaultChunkSize      = 16384
	chunkHeaderSize       = 8 // 4 bytes for ID, 4 bytes for length
)

type Client struct {
	dialer         N.Dialer
	logger         logger.Logger
	maxConnections int
	chunkSize      int
	pool           *connectionPool
}

type Options struct {
	Dialer         N.Dialer
	Logger         logger.Logger
	MaxConnections int
	ChunkSize      int
}

func NewClient(options Options) (*Client, error) {
	client := &Client{
		dialer:         options.Dialer,
		logger:         options.Logger,
		maxConnections: defaultMaxConnections,
		chunkSize:      defaultChunkSize,
	}
	if options.MaxConnections > 0 {
		client.maxConnections = options.MaxConnections
	}
	if options.ChunkSize > 0 {
		client.chunkSize = options.ChunkSize
	}
	client.pool = newConnectionPool(client)
	return client, nil
}

func (c *Client) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return c.dialIMUXConn(ctx, network, destination), nil
}

func (c *Client) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return &imuxPacketConn{
		imuxConn: c.dialIMUXConn(ctx, "udp", destination),
	}, nil
}

func (c *Client) dialIMUXConn(ctx context.Context, network string, destination M.Socksaddr) *imuxConn {
	conns := make([]net.Conn, c.maxConnections)
	for i := 0; i < c.maxConnections; i++ {
		conn, err := c.pool.get(ctx, network, destination)
		if err != nil {
			c.logger.Error("Failed to get connection from pool:", err)
			continue
		}
		conns[i] = conn
	}

	imuxConn := &imuxConn{
		client:      c,
		conns:       conns,
		readChan:    make(chan chunk, c.maxConnections),
		writeChan:   make(chan chunk, c.maxConnections),
		closeChan:   make(chan struct{}),
		readBuffer:  make(map[uint32][]byte),
		nextReadID:  0,
		nextWriteID: 0,
	}

	for i := 0; i < c.maxConnections; i++ {
		if conns[i] != nil {
			go imuxConn.readRoutine(i)
			go imuxConn.writeRoutine(i)
		}
	}

	return imuxConn
}

type imuxConn struct {
	client      *Client
	conns       []net.Conn
	readChan    chan chunk
	writeChan   chan chunk
	closeChan   chan struct{}
	readBuffer  map[uint32][]byte
	nextReadID  uint32
	nextWriteID uint32
	readMu      sync.Mutex
	writeMu     sync.Mutex
}

type chunk struct {
	id   uint32
	data []byte
}

func (c *imuxConn) Read(b []byte) (n int, err error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	for {
		if data, ok := c.readBuffer[c.nextReadID]; ok {
			n = copy(b, data)
			if n < len(data) {
				c.readBuffer[c.nextReadID] = data[n:]
			} else {
				delete(c.readBuffer, c.nextReadID)
				c.nextReadID++
			}
			return
		}

		select {
		case chunk := <-c.readChan:
			c.readBuffer[chunk.id] = chunk.data
		case <-c.closeChan:
			return 0, io.EOF
		}
	}
}

func (c *imuxConn) Write(b []byte) (n int, err error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	for n < len(b) {
		end := n + c.client.chunkSize
		if end > len(b) {
			end = len(b)
		}

		select {
		case c.writeChan <- chunk{id: c.nextWriteID, data: b[n:end]}:
			n = end
			c.nextWriteID++
		case <-c.closeChan:
			return n, io.ErrClosedPipe
		}
	}
	return
}

func (c *imuxConn) Close() error {
	select {
	case <-c.closeChan:
		return nil
	default:
		close(c.closeChan)
		for _, conn := range c.conns {
			if conn != nil {
				conn.Close()
			}
		}
		return nil
	}
}

func (c *imuxConn) LocalAddr() net.Addr                { return c.conns[0].LocalAddr() }
func (c *imuxConn) RemoteAddr() net.Addr               { return c.conns[0].RemoteAddr() }
func (c *imuxConn) SetDeadline(t time.Time) error      { return nil }
func (c *imuxConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *imuxConn) SetWriteDeadline(t time.Time) error { return nil }

func (c *imuxConn) readRoutine(index int) {
	conn := c.conns[index]
	headerBuf := make([]byte, chunkHeaderSize)
	for {
		_, err := io.ReadFull(conn, headerBuf)
		if err != nil {
			c.client.logger.Error("Read header error:", err)
			return
		}

		id := binary.BigEndian.Uint32(headerBuf[:4])
		length := binary.BigEndian.Uint32(headerBuf[4:])

		data := make([]byte, length)
		_, err = io.ReadFull(conn, data)
		if err != nil {
			c.client.logger.Error("Read data error:", err)
			return
		}

		select {
		case c.readChan <- chunk{id: id, data: data}:
		case <-c.closeChan:
			return
		}
	}
}

func (c *imuxConn) writeRoutine(index int) {
	conn := c.conns[index]
	for {
		select {
		case chunk := <-c.writeChan:
			header := make([]byte, chunkHeaderSize)
			binary.BigEndian.PutUint32(header[:4], chunk.id)
			binary.BigEndian.PutUint32(header[4:], uint32(len(chunk.data)))

			_, err := conn.Write(header)
			if err != nil {
				c.client.logger.Error("Write header error:", err)
				return
			}

			_, err = conn.Write(chunk.data)
			if err != nil {
				c.client.logger.Error("Write data error:", err)
				return
			}
		case <-c.closeChan:
			return
		}
	}
}

type imuxPacketConn struct {
	imuxConn *imuxConn
}

func (c *imuxPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	n, err = c.imuxConn.Read(p)
	return n, c.imuxConn.RemoteAddr(), err
}

func (c *imuxPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	return c.imuxConn.Write(p)
}

func (c *imuxPacketConn) Close() error        { return c.imuxConn.Close() }
func (c *imuxPacketConn) LocalAddr() net.Addr { return c.imuxConn.LocalAddr() }
func (c *imuxPacketConn) SetDeadline(t time.Time) error {
	return c.imuxConn.SetDeadline(t)
}
func (c *imuxPacketConn) SetReadDeadline(t time.Time) error {
	return c.imuxConn.SetReadDeadline(t)
}
func (c *imuxPacketConn) SetWriteDeadline(t time.Time) error {
	return c.imuxConn.SetWriteDeadline(t)
}

type Service struct {
	logger  logger.ContextLogger
	handler N.TCPConnectionHandler
}

type ServiceOptions struct {
	Logger  logger.ContextLogger
	Handler N.TCPConnectionHandler
}

func NewService(options ServiceOptions) (*Service, error) {
	return &Service{
		logger:  options.Logger,
		handler: options.Handler,
	}, nil
}

func (s *Service) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	s.logger.InfoContext(ctx, "New connection from", conn.RemoteAddr())

	serverSession := &serverSession{
		conn:   conn,
		logger: s.logger,
	}

	return serverSession.serve(ctx, s.handler, metadata)
}

type serverSession struct {
	conn    net.Conn
	logger  logger.ContextLogger
	streams sync.Map
}

func (s *serverSession) serve(ctx context.Context, handler N.TCPConnectionHandler, metadata M.Metadata) error {
	for {
		stream, err := s.acceptStream()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return E.Cause(err, "accept stream")
		}

		go func() {
			err := handler.NewConnection(ctx, stream, metadata)
			if err != nil {
				s.logger.ErrorContext(ctx, E.Cause(err, "handle connection"))
			}
		}()
	}
}

func (s *serverSession) acceptStream() (net.Conn, error) {
	headerBuf := make([]byte, chunkHeaderSize)
	_, err := io.ReadFull(s.conn, headerBuf)
	if err != nil {
		return nil, err
	}

	id := binary.BigEndian.Uint32(headerBuf[:4])
	length := binary.BigEndian.Uint32(headerBuf[4:])

	data := make([]byte, length)
	_, err = io.ReadFull(s.conn, data)
	if err != nil {
		return nil, err
	}

	stream := &serverStream{
		session: s,
		id:      id,
		readBuf: data,
	}
	s.streams.Store(id, stream)
	return stream, nil
}

type serverStream struct {
	session *serverSession
	id      uint32
	readBuf []byte
	closed  bool
	mu      sync.Mutex
}

func (s *serverStream) Read(b []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, io.EOF
	}

	if len(s.readBuf) > 0 {
		n = copy(b, s.readBuf)
		s.readBuf = s.readBuf[n:]
		return n, nil
	}

	headerBuf := make([]byte, chunkHeaderSize)
	_, err = io.ReadFull(s.session.conn, headerBuf)
	if err != nil {
		return 0, err
	}

	length := binary.BigEndian.Uint32(headerBuf[4:])
	data := make([]byte, length)
	_, err = io.ReadFull(s.session.conn, data)
	if err != nil {
		return 0, err
	}

	n = copy(b, data)
	if n < len(data) {
		s.readBuf = data[n:]
	}
	return n, nil
}

func (s *serverStream) Write(b []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, io.ErrClosedPipe
	}

	header := make([]byte, chunkHeaderSize)
	binary.BigEndian.PutUint32(header[:4], s.id)
	binary.BigEndian.PutUint32(header[4:], uint32(len(b)))

	_, err = s.session.conn.Write(header)
	if err != nil {
		return 0, err
	}

	return s.session.conn.Write(b)
}

func (s *serverStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	s.session.streams.Delete(s.id)
	return nil
}

func (s *serverStream) LocalAddr() net.Addr  { return s.session.conn.LocalAddr() }
func (s *serverStream) RemoteAddr() net.Addr { return s.session.conn.RemoteAddr() }
func (s *serverStream) SetDeadline(t time.Time) error {
	return s.session.conn.SetDeadline(t)
}
func (s *serverStream) SetReadDeadline(t time.Time) error {
	return s.session.conn.SetReadDeadline(t)
}
func (s *serverStream) SetWriteDeadline(t time.Time) error {
	return s.session.conn.SetWriteDeadline(t)
}

// Connection pool implementation (simplified)
type connectionPool struct {
	client *Client
	conns  []net.Conn
	mu     sync.Mutex
}

func newConnectionPool(client *Client) *connectionPool {
	return &connectionPool{
		client: client,
		conns:  make([]net.Conn, 0),
	}
}

func (p *connectionPool) get(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.conns) > 0 {
		conn := p.conns[len(p.conns)-1]
		p.conns = p.conns[:len(p.conns)-1]
		return conn, nil
	}

	return p.client.dialer.DialContext(ctx, network, destination)
}

func (p *connectionPool) put(conn net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.conns = append(p.conns, conn)
}
