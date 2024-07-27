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
	maxConnections int
	minStreams     int
	maxStreams     int
	padding        bool
	brutal         BrutalOptions
	chunkSize      int
}

type Options struct {
	Dialer         N.Dialer
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
}

func NewClient(options Options) (*Client, error) {
	if options.MaxConnections <= 0 {
		options.MaxConnections = defaultMaxConnections
	}
	client := &Client{
		dialer:         options.Dialer,
		maxConnections: options.MaxConnections,
		minStreams:     options.MinStreams,
		maxStreams:     options.MaxStreams,
		padding:        options.Padding,
		brutal:         options.Brutal,
		chunkSize:      defaultChunkSize,
	}
	fmt.Printf("New mux client created with %d max connections\n", options.MaxConnections)
	return client, nil
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
	fmt.Printf("Dialing IMUX connection for network: %s, destination: %s\n", network, destination.String())
	conns := make([]net.Conn, c.maxConnections)
	for i := 0; i < c.maxConnections; i++ {
		conn, err := c.dialer.DialContext(ctx, network, destination)
		if err != nil {
			fmt.Printf("Failed to dial connection: %v, index: %d\n", err, i)
			continue
		}
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

	fmt.Printf("IMUX connection created with %d active connections\n", len(conns))
	return imuxConn
}

func (c *imuxConn) readRoutine(index int) {
	fmt.Printf("Starting read routine for connection %d\n", index)
	headerBuf := make([]byte, chunkHeaderSize)
	for {
		_, err := io.ReadFull(c.conns[index], headerBuf)
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Read header error: %v, connection: %d\n", err, index)
			} else {
				fmt.Printf("EOF reached for connection %d\n", index)
			}
			return
		}

		id := binary.BigEndian.Uint32(headerBuf[:4])
		length := binary.BigEndian.Uint32(headerBuf[4:])

		fmt.Printf("Received chunk header - ID: %d, Length: %d, Connection: %d\n", id, length, index)

		data := make([]byte, length)
		_, err = io.ReadFull(c.conns[index], data)
		if err != nil {
			fmt.Printf("Read data error: %v, connection: %d\n", err, index)
			return
		}

		fmt.Printf("Received chunk data - ID: %d, Length: %d, Connection: %d\n", id, len(data), index)

		select {
		case c.readChan <- chunk{id: id, data: data}:
			fmt.Printf("Chunk sent to read channel - ID: %d, Connection: %d\n", id, index)
		case <-c.closeChan:
			fmt.Printf("Read routine closing for connection %d\n", index)
			return
		}
	}
}

func (c *imuxConn) writeRoutine(index int) {
	fmt.Printf("Starting write routine for connection %d\n", index)
	for {
		select {
		case chunk := <-c.writeChan:
			header := make([]byte, chunkHeaderSize)
			binary.BigEndian.PutUint32(header[:4], chunk.id)
			binary.BigEndian.PutUint32(header[4:], uint32(len(chunk.data)))

			fmt.Printf("Writing chunk header - ID: %d, Length: %d, Connection: %d\n", chunk.id, len(chunk.data), index)

			_, err := c.conns[index].Write(header)
			if err != nil {
				fmt.Printf("Write header error: %v, connection: %d\n", err, index)
				return
			}

			_, err = c.conns[index].Write(chunk.data)
			if err != nil {
				fmt.Printf("Write data error: %v, connection: %d\n", err, index)
				return
			}

			fmt.Printf("Chunk written successfully - ID: %d, Length: %d, Connection: %d\n", chunk.id, len(chunk.data), index)

		case <-c.closeChan:
			fmt.Printf("Write routine closing for connection %d\n", index)
			return
		}
	}
}

func (c *imuxConn) Read(b []byte) (n int, err error) {
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
			fmt.Printf("Read from buffer - ID: %d, Bytes read: %d\n", c.nextReadID-1, n)
			return
		}
		c.readBufferMu.Unlock()

		select {
		case chunk := <-c.readChan:
			fmt.Printf("Received chunk from read channel - ID: %d\n", chunk.id)
			c.readBufferMu.Lock()
			c.readBuffer[chunk.id] = chunk.data
			c.readBufferMu.Unlock()
		case <-c.closeChan:
			fmt.Printf("Read operation cancelled, connection closed\n")
			return 0, io.EOF
		}
	}
}

func (c *imuxConn) Write(b []byte) (n int, err error) {
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
			fmt.Printf("Chunk sent to write channel - ID: %d, Length: %d\n", chunk.id, chunkSize)
			n += chunkSize
			remaining -= chunkSize
			c.nextWriteID++
		case <-c.closeChan:
			fmt.Printf("Write operation cancelled, connection closed\n")
			return n, E.New("connection closed")
		}
	}
	fmt.Printf("Write completed, total bytes written: %d\n", n)
	return n, nil
}

func (c *imuxConn) Close() error {
	fmt.Println("Closing IMUX connection")
	c.closeOnce.Do(func() {
		close(c.closeChan)
		for i, conn := range c.conns {
			if conn != nil {
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

func (c *Client) Reset() {
	fmt.Println("Resetting mux client")
}

func (c *Client) Close() error {
	fmt.Println("Closing mux client")
	return nil
}

// Placeholder implementations for compatibility
type Service struct{}
type ServiceOptions struct{}

func NewService(options ServiceOptions) (*Service, error) { return &Service{}, nil }
func (s *Service) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
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

func ReadStreamRequest(reader io.Reader) (*StreamRequest, error)   { return &StreamRequest{}, nil }
func ReadStreamResponse(reader io.Reader) (*StreamResponse, error) { return &StreamResponse{}, nil }
func WriteBrutalRequest(writer io.Writer, receiveBPS uint64) error { return nil }
func ReadBrutalRequest(reader io.Reader) (uint64, error)           { return 0, nil }
func WriteBrutalResponse(writer io.Writer, receiveBPS uint64, ok bool, message string) error {
	return nil
}
func ReadBrutalResponse(reader io.Reader) (uint64, error) { return 0, nil }

const BrutalAvailable = false

func SetBrutalOptions(conn net.Conn, sendBPS uint64) error { return nil }
