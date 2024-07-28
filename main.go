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
	fmt.Printf("New mux client created with %d max connections\n", options.MaxConnections)

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
	wg           sync.WaitGroup
	mu           sync.Mutex
	isClosed     bool
}

func (c *Client) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	fmt.Printf("DialContext called for network: %s, destination: %s\n", network, destination.String())
	return c.dialIMUXConn(ctx, network, destination), nil
}

func (c *Client) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	fmt.Printf("ListenPacket called for destination: %s\n", destination.String())
	return &imuxPacketConn{
		imuxConn: c.dialIMUXConn(ctx, "udp", destination),
	}, nil
}

func (c *Client) dialIMUXConn(ctx context.Context, network string, destination M.Socksaddr) *imuxConn {
	c.logger.Info("Dialing IMUX connection for network:", network, "destination:", destination.String())
	fmt.Printf("Dialing IMUX connection for network: %s, destination: %s\n", network, destination.String())
	conns := make([]net.Conn, c.maxConnections)
	for i := 0; i < c.maxConnections; i++ {
		conn, err := c.dialer.DialContext(ctx, network, destination)
		if err != nil {
			c.logger.Error("Failed to dial connection:", err, "index:", i)
			fmt.Printf("Failed to dial connection: %v, index: %d\n", err, i)
			continue
		}
		c.logger.Debug("Successfully dialed connection", i)
		fmt.Printf("Successfully dialed connection %d\n", i)
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

	c.logger.Info("IMUX connection created with", len(conns), "active connections")
	fmt.Printf("IMUX connection created with %d active connections\n", len(conns))
	return imuxConn
}

func (c *imuxConn) readRoutine(index int) {
	c.wg.Add(1)
	defer c.wg.Done()
	c.client.logger.Debug("Starting read routine for connection", index)
	fmt.Printf("Starting read routine for connection %d\n", index)
	headerBuf := make([]byte, chunkHeaderSize)
	for {
		select {
		case <-c.closeChan:
			c.client.logger.Debug("Read routine closing for connection", index)
			fmt.Printf("Read routine closing for connection %d\n", index)
			return
		default:
			_, err := io.ReadFull(c.conns[index], headerBuf)
			if err != nil {
				if err != io.EOF {
					c.client.logger.Error("Read header error:", err, "connection:", index)
					fmt.Printf("Read header error: %v, connection: %d\n", err, index)
				} else {
					c.client.logger.Debug("EOF reached for connection", index)
					fmt.Printf("EOF reached for connection %d\n", index)
				}
				return
			}

			id := binary.BigEndian.Uint32(headerBuf[:4])
			length := binary.BigEndian.Uint32(headerBuf[4:])

			c.client.logger.Debug("Received chunk header - ID:", id, "Length:", length, "Connection:", index)
			fmt.Printf("Received chunk header - ID: %d, Length: %d, Connection: %d\n", id, length, index)

			data := make([]byte, length)
			_, err = io.ReadFull(c.conns[index], data)
			if err != nil {
				c.client.logger.Error("Read data error:", err, "connection:", index)
				fmt.Printf("Read data error: %v, connection: %d\n", err, index)
				return
			}

			c.client.logger.Debug("Received chunk data - ID:", id, "Length:", len(data), "Connection:", index)
			fmt.Printf("Received chunk data - ID: %d, Length: %d, Connection: %d\n", id, len(data), index)

			select {
			case c.readChan <- chunk{id: id, data: data}:
				c.client.logger.Debug("Chunk sent to read channel - ID:", id, "Connection:", index)
				fmt.Printf("Chunk sent to read channel - ID: %d, Connection: %d\n", id, index)
			case <-c.closeChan:
				c.client.logger.Debug("Read routine closing for connection", index)
				fmt.Printf("Read routine closing for connection %d\n", index)
				return
			}
		}
	}
}

func (c *imuxConn) writeRoutine(index int) {
	c.wg.Add(1)
	defer c.wg.Done()
	c.client.logger.Debug("Starting write routine for connection", index)
	fmt.Printf("Starting write routine for connection %d\n", index)
	for {
		select {
		case chunk := <-c.writeChan:
			header := make([]byte, chunkHeaderSize)
			binary.BigEndian.PutUint32(header[:4], chunk.id)
			binary.BigEndian.PutUint32(header[4:], uint32(len(chunk.data)))

			c.client.logger.Debug("Writing chunk header - ID:", chunk.id, "Length:", len(chunk.data), "Connection:", index)
			fmt.Printf("Writing chunk header - ID: %d, Length: %d, Connection: %d\n", chunk.id, len(chunk.data), index)

			_, err := c.conns[index].Write(header)
			if err != nil {
				c.client.logger.Error("Write header error:", err, "connection:", index)
				fmt.Printf("Write header error: %v, connection: %d\n", err, index)
				return
			}

			_, err = c.conns[index].Write(chunk.data)
			if err != nil {
				c.client.logger.Error("Write data error:", err, "connection:", index)
				fmt.Printf("Write data error: %v, connection: %d\n", err, index)
				return
			}

			c.client.logger.Debug("Chunk written successfully - ID:", chunk.id, "Length:", len(chunk.data), "Connection:", index)
			fmt.Printf("Chunk written successfully - ID: %d, Length: %d, Connection: %d\n", chunk.id, len(chunk.data), index)

		case <-c.closeChan:
			c.client.logger.Debug("Write routine closing for connection", index)
			fmt.Printf("Write routine closing for connection %d\n", index)
			return
		}
	}
}

func (c *imuxConn) Read(b []byte) (n int, err error) {
	c.mu.Lock()
	if c.isClosed {
		c.mu.Unlock()
		return 0, io.EOF
	}
	c.mu.Unlock()

	c.client.logger.Debug("Read called, buffer size:", len(b))
	fmt.Printf("Read called, buffer size: %d\n", len(b))
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
			c.client.logger.Debug("Read from buffer - ID:", c.nextReadID-1, "Bytes read:", n)
			fmt.Printf("Read from buffer - ID: %d, Bytes read: %d\n", c.nextReadID-1, n)
			return
		}
		c.readBufferMu.Unlock()

		select {
		case chunk := <-c.readChan:
			c.client.logger.Debug("Received chunk from read channel - ID:", chunk.id)
			fmt.Printf("Received chunk from read channel - ID: %d\n", chunk.id)
			c.readBufferMu.Lock()
			c.readBuffer[chunk.id] = chunk.data
			c.readBufferMu.Unlock()
		case <-c.closeChan:
			c.client.logger.Debug("Read operation cancelled, connection closed")
			fmt.Printf("Read operation cancelled, connection closed\n")
			return 0, io.EOF
		}
	}
}

func (c *imuxConn) Write(b []byte) (n int, err error) {
	c.mu.Lock()
	if c.isClosed {
		c.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	c.mu.Unlock()

	c.client.logger.Debug("Write called, data length:", len(b))
	fmt.Printf("Write called, data length: %d\n", len(b))
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
			fmt.Printf("Chunk sent to write channel - ID: %d, Length: %d\n", chunk.id, chunkSize)
			n += chunkSize
			remaining -= chunkSize
			c.nextWriteID++
		case <-c.closeChan:
			c.client.logger.Debug("Write operation cancelled, connection closed")
			fmt.Printf("Write operation cancelled, connection closed\n")
			return n, E.New("connection closed")
		}
	}
	c.client.logger.Debug("Write completed, total bytes written:", n)
	fmt.Printf("Write completed, total bytes written: %d\n", n)
	return n, nil
}

func (c *imuxConn) Close() error {
	c.client.logger.Info("Closing IMUX connection")
	fmt.Println("Closing IMUX connection")
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.isClosed = true
		c.mu.Unlock()

		close(c.closeChan)
		c.wg.Wait() // Wait for all goroutines to finish

		for i, conn := range c.conns {
			if conn != nil {
				c.client.logger.Debug("Closing sub-connection", i)
				fmt.Printf("Closing sub-connection %d\n", i)
				conn.Close()
			}
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

func (c *Client) Reset()       {}
func (c *Client) Close() error { return nil }

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
	return &Service{
		newStreamContext: options.NewStreamContext,
		logger:           options.Logger,
		handler:          options.Handler,
		padding:          options.Padding,
		brutal:           options.Brutal,
	}, nil
}

func (s *Service) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	s.logger.InfoContext(ctx, "New IMUX connection from ", metadata.Source)
	fmt.Printf("New IMUX connection from %s\n", metadata.Source)

	imuxConn := &imuxServerConn{
		Service:   s,
		conn:      conn,
		metadata:  metadata,
		readChan:  make(chan chunk, defaultMaxConnections),
		closeChan: make(chan struct{}),
	}

	go imuxConn.readRoutine()
	go imuxConn.writeRoutine()

	return s.handler.NewConnection(ctx, imuxConn, metadata)
}

type imuxServerConn struct {
	*Service
	conn         net.Conn
	metadata     M.Metadata
	readChan     chan chunk
	closeChan    chan struct{}
	nextReadID   uint32
	nextWriteID  uint32
	readBuffer   map[uint32][]byte
	readBufferMu sync.Mutex
	wg           sync.WaitGroup
	mu           sync.Mutex
	isClosed     bool
}

func (c *imuxServerConn) readRoutine() {
	c.wg.Add(1)
	defer c.wg.Done()
	headerBuf := make([]byte, chunkHeaderSize)
	for {
		select {
		case <-c.closeChan:
			c.logger.Debug("Server read routine closing")
			fmt.Println("Server read routine closing")
			return
		default:
			_, err := io.ReadFull(c.conn, headerBuf)
			if err != nil {
				if err != io.EOF {
					c.logger.Error("Server read header error:", err)
					fmt.Printf("Server read header error: %v\n", err)
				}
				return
			}

			id := binary.BigEndian.Uint32(headerBuf[:4])
			length := binary.BigEndian.Uint32(headerBuf[4:])

			data := make([]byte, length)
			_, err = io.ReadFull(c.conn, data)
			if err != nil {
				c.logger.Error("Server read data error:", err)
				fmt.Printf("Server read data error: %v\n", err)
				return
			}

			select {
			case c.readChan <- chunk{id: id, data: data}:
			case <-c.closeChan:
				return
			}
		}
	}
}

func (c *imuxServerConn) writeRoutine() {
	c.wg.Add(1)
	defer c.wg.Done()
	for {
		select {
		case <-c.closeChan:
			c.logger.Debug("Server write routine closing")
			fmt.Println("Server write routine closing")
			return
		default:
			chunk := <-c.readChan
			header := make([]byte, chunkHeaderSize)
			binary.BigEndian.PutUint32(header[:4], chunk.id)
			binary.BigEndian.PutUint32(header[4:], uint32(len(chunk.data)))

			_, err := c.conn.Write(header)
			if err != nil {
				c.logger.Error("Server write header error:", err)
				fmt.Printf("Server write header error: %v\n", err)
				return
			}

			_, err = c.conn.Write(chunk.data)
			if err != nil {
				c.logger.Error("Server write data error:", err)
				fmt.Printf("Server write data error: %v\n", err)
				return
			}
		}
	}
}

func (c *imuxServerConn) Read(b []byte) (n int, err error) {
	c.mu.Lock()
	if c.isClosed {
		c.mu.Unlock()
		return 0, io.EOF
	}
	c.mu.Unlock()

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

func (c *imuxServerConn) Write(b []byte) (n int, err error) {
	c.mu.Lock()
	if c.isClosed {
		c.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	c.mu.Unlock()

	remaining := len(b)
	for remaining > 0 {
		chunkSize := defaultChunkSize
		if remaining < chunkSize {
			chunkSize = remaining
		}
		chunk := chunk{
			id:   c.nextWriteID,
			data: b[n : n+chunkSize],
		}
		header := make([]byte, chunkHeaderSize)
		binary.BigEndian.PutUint32(header[:4], chunk.id)
		binary.BigEndian.PutUint32(header[4:], uint32(chunkSize))

		_, err := c.conn.Write(header)
		if err != nil {
			return n, err
		}

		_, err = c.conn.Write(chunk.data)
		if err != nil {
			return n, err
		}

		n += chunkSize
		remaining -= chunkSize
		c.nextWriteID++
	}
	return n, nil
}

func (c *imuxServerConn) Close() error {
	c.mu.Lock()
	if c.isClosed {
		c.mu.Unlock()
		return nil
	}
	c.isClosed = true
	c.mu.Unlock()

	close(c.closeChan)
	c.wg.Wait()
	return c.conn.Close()
}

func (c *imuxServerConn) LocalAddr() net.Addr                { return c.conn.LocalAddr() }
func (c *imuxServerConn) RemoteAddr() net.Addr               { return c.conn.RemoteAddr() }
func (c *imuxServerConn) SetDeadline(t time.Time) error      { return c.conn.SetDeadline(t) }
func (c *imuxServerConn) SetReadDeadline(t time.Time) error  { return c.conn.SetReadDeadline(t) }
func (c *imuxServerConn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }

func (s *Service) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata M.Metadata) error {
	return E.New("packet connection is not supported in IMUX")
}
