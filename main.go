package mux

import (
	"context"
	"encoding/binary"
	"fmt"
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
	if options.Dialer == nil {
		return nil, E.New("dialer is required")
	}
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
	readBufferMu sync.Mutex
	writeMu      sync.Mutex
}

func (c *Client) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return c.dialIMUXConn(ctx, network, destination)
}

func (c *Client) dialIMUXConn(ctx context.Context, network string, destination M.Socksaddr) (*imuxConn, error) {
	conns := make([]net.Conn, 0, c.maxConnections)
	for i := 0; i < c.maxConnections; i++ {
		conn, err := c.dialer.DialContext(ctx, network, destination)
		if err != nil {
			c.logger.Warn("Failed to dial connection:", err)
			continue
		}
		conns = append(conns, conn)
	}

	if len(conns) == 0 {
		return nil, E.New("failed to establish any connections")
	}

	imuxConn := &imuxConn{
		client:     c,
		conns:      conns,
		readChan:   make(chan chunk, len(conns)),
		writeChan:  make(chan chunk, len(conns)),
		closeChan:  make(chan struct{}),
		readBuffer: make(map[uint32][]byte),
	}

	for i := range conns {
		go imuxConn.readRoutine(i)
		go imuxConn.writeRoutine(i)
	}

	return imuxConn, nil
}

func (c *imuxConn) readRoutine(index int) {
	headerBuf := make([]byte, chunkHeaderSize)
	for {
		_, err := io.ReadFull(c.conns[index], headerBuf)
		if err != nil {
			if err != io.EOF {
				c.client.logger.Error("Read header error:", err)
			}
			return
		}

		id := binary.BigEndian.Uint32(headerBuf[:4])
		length := binary.BigEndian.Uint32(headerBuf[4:])

		data := make([]byte, length)
		_, err = io.ReadFull(c.conns[index], data)
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
	for {
		select {
		case chunk := <-c.writeChan:
			header := make([]byte, chunkHeaderSize)
			binary.BigEndian.PutUint32(header[:4], chunk.id)
			binary.BigEndian.PutUint32(header[4:], uint32(len(chunk.data)))

			_, err := c.conns[index].Write(header)
			if err != nil {
				c.client.logger.Error("Write header error:", err)
				return
			}

			_, err = c.conns[index].Write(chunk.data)
			if err != nil {
				c.client.logger.Error("Write data error:", err)
				return
			}

		case <-c.closeChan:
			return
		}
	}
}

func (c *imuxConn) Read(b []byte) (n int, err error) {
	for {
		c.readBufferMu.Lock()
		if data, ok := c.readBuffer[c.nextReadID]; ok {
			n = copy(b, data)
			if n < len(data) {
				c.readBuffer[c.nextReadID] = data[n:]
			} else {
				delete(c.readBuffer, c.nextReadID)
				c.nextReadID++
			}
			c.readBufferMu.Unlock()
			return
		}
		c.readBufferMu.Unlock()

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
			n += chunkSize
			remaining -= chunkSize
			c.nextWriteID++
		case <-c.closeChan:
			return n, E.New("connection closed")
		}
	}
	return n, nil
}

func (c *imuxConn) Close() error {
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

func (c *Client) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	conn, err := c.dialIMUXConn(ctx, "udp", destination)
	if err != nil {
		return nil, err
	}
	return &imuxPacketConn{conn}, nil
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
}

type ServiceOptions struct {
	NewStreamContext func(context.Context, net.Conn) context.Context
	Logger           logger.ContextLogger
	Handler          N.TCPConnectionHandler
	Padding          bool
	Brutal           BrutalOptions
}

func NewService(options ServiceOptions) (*Service, error) {
	if options.Handler == nil {
		return nil, E.New("handler is required")
	}
	return &Service{
		newStreamContext: options.NewStreamContext,
		logger:           options.Logger,
		handler:          options.Handler,
		padding:          options.Padding,
		brutal:           options.Brutal,
	}, nil
}

func (s *Service) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	return s.handler.NewConnection(ctx, conn, metadata)
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
	// Implement the actual reading logic here
	return &StreamRequest{}, nil
}

func ReadStreamResponse(reader io.Reader) (*StreamResponse, error) {
	// Implement the actual reading logic here
	return &StreamResponse{}, nil
}

func WriteBrutalRequest(writer io.Writer, receiveBPS uint64) error {
	return binary.Write(writer, binary.BigEndian, receiveBPS)
}

func ReadBrutalRequest(reader io.Reader) (uint64, error) {
	var receiveBPS uint64
	err := binary.Read(reader, binary.BigEndian, &receiveBPS)
	return receiveBPS, err
}

func WriteBrutalResponse(writer io.Writer, receiveBPS uint64, ok bool, message string) error {
	if err := binary.Write(writer, binary.BigEndian, ok); err != nil {
		return err
	}
	if ok {
		return binary.Write(writer, binary.BigEndian, receiveBPS)
	}
	return WriteVString(writer, message)
}

func ReadBrutalResponse(reader io.Reader) (uint64, error) {
	var ok bool
	if err := binary.Read(reader, binary.BigEndian, &ok); err != nil {
		return 0, err
	}
	if ok {
		var receiveBPS uint64
		err := binary.Read(reader, binary.BigEndian, &receiveBPS)
		return receiveBPS, err
	}
	message, err := ReadVString(reader)
	if err != nil {
		return 0, err
	}
	return 0, fmt.Errorf(message)
}

func WriteVString(writer io.Writer, s string) error {
	if err := binary.Write(writer, binary.BigEndian, uint16(len(s))); err != nil {
		return err
	}
	_, err := writer.Write([]byte(s))
	return err
}

func ReadVString(reader io.Reader) (string, error) {
	var length uint16
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return "", err
	}
	bytes := make([]byte, length)
	_, err := io.ReadFull(reader, bytes)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

const (
	BrutalAvailable = false
)

func SetBrutalOptions(conn net.Conn, sendBPS uint64) error {
	// Implement the actual setting of brutal options here
	return nil
}
