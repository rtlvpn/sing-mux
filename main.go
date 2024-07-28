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
	BrutalMinSpeedBPS     = 65536
	chunkHeaderSize       = 8 // 4 bytes for ID, 4 bytes for length
)

type Client struct {
	dialer         N.Dialer
	logger         logger.Logger
	maxConnections int
	minStreams     int
	maxStreams     int
	padding        bool
	brutal         BrutalOptions
	chunkSize      int
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
		options.MaxConnections = defaultMaxConnections
	}

	options.Logger.Info("New mux client created with", options.MaxConnections, "max connections")

	return &Client{
		dialer:         options.Dialer,
		logger:         options.Logger,
		maxConnections: options.MaxConnections,
		minStreams:     options.MinStreams,
		maxStreams:     options.MaxStreams,
		padding:        options.Padding,
		brutal:         options.Brutal,
		chunkSize:      defaultChunkSize,
	}, nil
}

type chunk struct {
	id   uint32
	data []byte
}

type imuxConn struct {
	client       *Client
	conns        []net.Conn
	readChan     chan chunk
	writeChan    chan chunk
	closeChan    chan struct{}
	closeOnce    sync.Once
	nextReadID   uint32
	nextWriteID  uint32
	readBuffer   map[uint32][]byte
	readBufferMu sync.RWMutex
	writeMu      sync.Mutex
}

func (c *Client) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	c.logger.Info("DialContext called for network:", network, "destination:", destination.String())
	return c.dialIMUXConn(ctx, network, destination)
}

func (c *Client) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	c.logger.Info("ListenPacket called for destination:", destination.String())
	conn, err := c.dialIMUXConn(ctx, "udp", destination)
	if err != nil {
		return nil, err
	}
	return &imuxPacketConn{imuxConn: conn}, nil
}

func (c *Client) dialIMUXConn(ctx context.Context, network string, destination M.Socksaddr) (*imuxConn, error) {
	c.logger.Info("Dialing IMUX connection for network:", network, "destination:", destination.String())
	conns := make([]net.Conn, 0, c.maxConnections)
	for i := 0; i < c.maxConnections; i++ {
		conn, err := c.dialer.DialContext(ctx, network, destination)
		if err != nil {
			c.logger.Error("Failed to dial connection:", err, "index:", i)
			continue
		}
		c.logger.Debug("Successfully dialed connection", i)
		conns = append(conns, conn)
	}

	if len(conns) == 0 {
		return nil, E.New("failed to establish any connections")
	}

	imuxConn := &imuxConn{
		client:     c,
		conns:      conns,
		readChan:   make(chan chunk, c.maxConnections),
		writeChan:  make(chan chunk, c.maxConnections),
		closeChan:  make(chan struct{}),
		readBuffer: make(map[uint32][]byte),
	}

	for i := range conns {
		go imuxConn.readRoutine(i)
		go imuxConn.writeRoutine(i)
	}

	c.logger.Info("IMUX connection created with", len(conns), "active connections")
	return imuxConn, nil
}

func (c *imuxConn) readRoutine(index int) {
	c.client.logger.Debug("Starting read routine for connection", index)
	headerBuf := make([]byte, chunkHeaderSize)
	for {
		_, err := io.ReadFull(c.conns[index], headerBuf)
		if err != nil {
			if err != io.EOF {
				c.client.logger.Error("Read header error:", err, "connection:", index)
			}
			return
		}

		id := binary.BigEndian.Uint32(headerBuf[:4])
		length := binary.BigEndian.Uint32(headerBuf[4:])

		data := make([]byte, length)
		_, err = io.ReadFull(c.conns[index], data)
		if err != nil {
			c.client.logger.Error("Read data error:", err, "connection:", index)
			return
		}

		select {
		case c.readChan <- chunk{id: id, data: data}:
			c.client.logger.Debug("Chunk received - ID:", id, "Length:", len(data), "Connection:", index)
		case <-c.closeChan:
			return
		}
	}
}

func (c *imuxConn) writeRoutine(index int) {
	c.client.logger.Debug("Starting write routine for connection", index)
	for {
		select {
		case chunk := <-c.writeChan:
			header := make([]byte, chunkHeaderSize)
			binary.BigEndian.PutUint32(header[:4], chunk.id)
			binary.BigEndian.PutUint32(header[4:], uint32(len(chunk.data)))

			_, err := c.conns[index].Write(header)
			if err != nil {
				c.client.logger.Error("Write header error:", err, "connection:", index)
				return
			}

			_, err = c.conns[index].Write(chunk.data)
			if err != nil {
				c.client.logger.Error("Write data error:", err, "connection:", index)
				return
			}

			c.client.logger.Debug("Chunk written - ID:", chunk.id, "Length:", len(chunk.data), "Connection:", index)

		case <-c.closeChan:
			return
		}
	}
}

func (c *imuxConn) Read(b []byte) (n int, err error) {
	c.client.logger.Debug("Read called, buffer size:", len(b))
	for {
		c.readBufferMu.RLock()
		data, ok := c.readBuffer[c.nextReadID]
		c.readBufferMu.RUnlock()

		if ok {
			n = copy(b, data)
			if n < len(data) {
				c.readBufferMu.Lock()
				c.readBuffer[c.nextReadID] = data[n:]
				c.readBufferMu.Unlock()
			} else {
				c.readBufferMu.Lock()
				delete(c.readBuffer, c.nextReadID)
				c.nextReadID++
				c.readBufferMu.Unlock()
			}
			c.client.logger.Debug("Read from buffer - ID:", c.nextReadID-1, "Bytes read:", n)
			return
		}

		select {
		case chunk := <-c.readChan:
			c.readBufferMu.Lock()
			c.readBuffer[chunk.id] = chunk.data
			c.readBufferMu.Unlock()
		case <-c.closeChan:
			return 0, io.EOF
		}
	}
}

func (c *imuxConn) Write(b []byte) (n int, err error) {
	c.client.logger.Debug("Write called, data length:", len(b))
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	remaining := len(b)
	for remaining > 0 {
		chunkSize := c.client.chunkSize
		if remaining < chunkSize {
			chunkSize = remaining
		}
		chunk := chunk{
			id:   c.nextWriteID,
			data: b[n : n+chunkSize],
		}
		select {
		case c.writeChan <- chunk:
			c.client.logger.Debug("Chunk sent to write channel - ID:", chunk.id, "Length:", chunkSize)
			n += chunkSize
			remaining -= chunkSize
			c.nextWriteID++
		case <-c.closeChan:
			return n, E.New("connection closed")
		}
	}
	c.client.logger.Debug("Write completed, total bytes written:", n)
	return n, nil
}

func (c *imuxConn) Close() error {
	c.client.logger.Info("Closing IMUX connection")
	c.closeOnce.Do(func() {
		close(c.closeChan)
		for _, conn := range c.conns {
			conn.Close()
		}
	})
	return nil
}

func (c *imuxConn) LocalAddr() net.Addr                { return c.conns[0].LocalAddr() }
func (c *imuxConn) RemoteAddr() net.Addr               { return c.conns[0].RemoteAddr() }
func (c *imuxConn) SetDeadline(t time.Time) error      { return nil }
func (c *imuxConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *imuxConn) SetWriteDeadline(t time.Time) error { return nil }

type imuxPacketConn struct {
	*imuxConn
}

func (c *imuxPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	n, err = c.imuxConn.Read(p)
	return n, c.imuxConn.RemoteAddr(), err
}

func (c *imuxPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	return c.imuxConn.Write(p)
}

func (c *Client) Reset() {}
func (c *Client) Close() error {
	return nil
}

type Service struct {
	newStreamContext func(context.Context, net.Conn) context.Context
	logger           logger.ContextLogger
	handler          N.TCPConnectionHandler
	padding          bool
	brutal           BrutalOptions
	listener         net.Listener
	connChan         chan net.Conn
	closeChan        chan struct{}
}

type ServiceOptions struct {
	NewStreamContext func(context.Context, net.Conn) context.Context
	Logger           logger.ContextLogger
	Handler          N.TCPConnectionHandler
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
		connChan:         make(chan net.Conn),
		closeChan:        make(chan struct{}),
	}, nil
}

func (s *Service) Start(listener net.Listener) error {
	s.listener = listener
	go s.acceptLoop()
	return nil
}

func (s *Service) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.closeChan:
				return
			default:
				s.logger.Error("Failed to accept connection:", err)
				continue
			}
		}
		go s.handleConnection(conn)
	}
}

func (s *Service) handleConnection(conn net.Conn) {
	defer conn.Close()
	ctx := s.newStreamContext(context.Background(), conn)
	err := s.NewConnection(ctx, conn, M.Metadata{})
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to handle connection:", err)
	}
}

func (s *Service) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	s.logger.InfoContext(ctx, "New connection from", conn.RemoteAddr())
	return s.handler.NewConnection(ctx, conn, metadata)
}

func (s *Service) Close() error {
	close(s.closeChan)
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

var Destination = M.Socksaddr{Fqdn: "inverse-mux"}

const (
	ProtocolH2Mux = iota
	ProtocolSmux
	ProtocolYAMux
)

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
	// Implement actual reading logic here
	return &StreamRequest{}, nil
}

func ReadStreamResponse(reader io.Reader) (*StreamResponse, error) {
	// Implement actual reading logic here
	return &StreamResponse{}, nil
}

func WriteBrutalRequest(writer io.Writer, receiveBPS uint64) error {
	// Implement actual writing logic here
	return nil
}

func ReadBrutalRequest(reader io.Reader) (uint64, error) {
	// Implement actual reading logic here
	return 0, nil
}

func WriteBrutalResponse(writer io.Writer, receiveBPS uint64, ok bool, message string) error {
	// Implement actual writing logic here
	return nil
}

func ReadBrutalResponse(reader io.Reader) (uint64, error) {
	// Implement actual reading logic here
	return 0, nil
}

const (
	BrutalAvailable = false
)

func SetBrutalOptions(conn net.Conn, sendBPS uint64) error {
	// Implement Brutal options setting logic here
	return nil
}
