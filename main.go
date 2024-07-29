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
	DefaultMaxConnections = 4
	DefaultMinStreams     = 2
	DefaultMaxStreams     = 8
	DefaultChunkSize      = 16384
	DefaultKeepAlive      = 30 * time.Second
	DefaultIdleTimeout    = 5 * time.Minute
)

type Client struct {
	dialer         N.Dialer
	logger         logger.Logger
	maxConnections int
	minStreams     int
	maxStreams     int
	chunkSize      int
	keepAlive      time.Duration
	idleTimeout    time.Duration
	sessions       []*Session
	sessionMu      sync.Mutex
}

type Options struct {
	Dialer         N.Dialer
	Logger         logger.Logger
	MaxConnections int
	MinStreams     int
	MaxStreams     int
	ChunkSize      int
	KeepAlive      time.Duration
	IdleTimeout    time.Duration
}

func NewClient(options Options) (*Client, error) {
	if options.MaxConnections == 0 {
		options.MaxConnections = DefaultMaxConnections
	}
	if options.MinStreams == 0 {
		options.MinStreams = DefaultMinStreams
	}
	if options.MaxStreams == 0 {
		options.MaxStreams = DefaultMaxStreams
	}
	if options.ChunkSize == 0 {
		options.ChunkSize = DefaultChunkSize
	}
	if options.KeepAlive == 0 {
		options.KeepAlive = DefaultKeepAlive
	}
	if options.IdleTimeout == 0 {
		options.IdleTimeout = DefaultIdleTimeout
	}
	return &Client{
		dialer:         options.Dialer,
		logger:         options.Logger,
		maxConnections: options.MaxConnections,
		minStreams:     options.MinStreams,
		maxStreams:     options.MaxStreams,
		chunkSize:      options.ChunkSize,
		keepAlive:      options.KeepAlive,
		idleTimeout:    options.IdleTimeout,
	}, nil
}

func (c *Client) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()

	var session *Session
	if len(c.sessions) < c.maxConnections {
		newSession, err := c.createSession(ctx, destination)
		if err != nil {
			return nil, err
		}
		c.sessions = append(c.sessions, newSession)
		session = newSession
	} else {
		session = c.leastLoadedSession()
	}

	stream, err := session.OpenStream()
	if err != nil {
		return nil, err
	}

	return stream, nil
}

func (c *Client) createSession(ctx context.Context, destination M.Socksaddr) (*Session, error) {
	conn, err := c.dialer.DialContext(ctx, "tcp", destination)
	if err != nil {
		return nil, err
	}
	return NewSession(conn, c.chunkSize, c.maxStreams, c.keepAlive, c.idleTimeout, c.logger), nil
}

func (c *Client) leastLoadedSession() *Session {
	var leastLoaded *Session
	minStreams := c.maxStreams

	for _, session := range c.sessions {
		if session.StreamCount() < minStreams {
			leastLoaded = session
			minStreams = session.StreamCount()
		}
	}

	return leastLoaded
}

type Session struct {
	conn         net.Conn
	streams      map[uint32]*Stream
	nextID       uint32
	chunkSize    int
	maxStreams   int
	keepAlive    time.Duration
	idleTimeout  time.Duration
	logger       logger.Logger
	mu           sync.Mutex
	writeChan    chan *Chunk
	closeChan    chan struct{}
	lastActivity time.Time
}

func NewSession(conn net.Conn, chunkSize, maxStreams int, keepAlive, idleTimeout time.Duration, logger logger.Logger) *Session {
	s := &Session{
		conn:         conn,
		streams:      make(map[uint32]*Stream),
		chunkSize:    chunkSize,
		maxStreams:   maxStreams,
		keepAlive:    keepAlive,
		idleTimeout:  idleTimeout,
		logger:       logger,
		writeChan:    make(chan *Chunk, 50),
		closeChan:    make(chan struct{}),
		lastActivity: time.Now(),
	}
	go s.readLoop()
	go s.writeLoop()
	go s.keepAliveLoop()
	go s.idleTimeoutLoop()
	return s
}

func (s *Session) OpenStream() (net.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.streams) >= s.maxStreams {
		return nil, E.New("too many streams")
	}

	id := s.nextID
	s.nextID++

	stream := NewStream(id, s)
	s.streams[id] = stream

	s.lastActivity = time.Now()
	return stream, nil
}

func (s *Session) StreamCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

func (s *Session) readLoop() {
	header := make([]byte, 8)
	for {
		_, err := io.ReadFull(s.conn, header)
		if err != nil {
			s.logger.Error("read error:", err)
			s.Close()
			return
		}

		id := binary.BigEndian.Uint32(header[:4])
		length := binary.BigEndian.Uint32(header[4:])

		data := make([]byte, length)
		_, err = io.ReadFull(s.conn, data)
		if err != nil {
			s.logger.Error("read error:", err)
			s.Close()
			return
		}

		s.mu.Lock()
		s.lastActivity = time.Now()
		if stream, ok := s.streams[id]; ok {
			stream.readChan <- data
		}
		s.mu.Unlock()
	}
}

func (s *Session) writeLoop() {
	for {
		select {
		case chunk := <-s.writeChan:
			header := make([]byte, 8)
			binary.BigEndian.PutUint32(header[:4], chunk.StreamID)
			binary.BigEndian.PutUint32(header[4:], uint32(len(chunk.Data)))
			_, err := s.conn.Write(append(header, chunk.Data...))
			if err != nil {
				s.logger.Error("write error:", err)
				s.Close()
				return
			}
			s.mu.Lock()
			s.lastActivity = time.Now()
			s.mu.Unlock()
		case <-s.closeChan:
			return
		}
	}
}

func (s *Session) keepAliveLoop() {
	ticker := time.NewTicker(s.keepAlive)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			if time.Since(s.lastActivity) >= s.keepAlive {
				s.writeChan <- &Chunk{StreamID: 0, Data: []byte("keepalive")}
			}
			s.mu.Unlock()
		case <-s.closeChan:
			return
		}
	}
}

func (s *Session) idleTimeoutLoop() {
	ticker := time.NewTicker(s.idleTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			if time.Since(s.lastActivity) >= s.idleTimeout {
				s.logger.Info("session idle timeout")
				s.Close()
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
		case <-s.closeChan:
			return
		}
	}
}

func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn == nil {
		return nil
	}

	close(s.closeChan)
	for _, stream := range s.streams {
		stream.Close()
	}
	err := s.conn.Close()
	s.conn = nil
	return err
}

type Stream struct {
	id       uint32
	session  *Session
	readChan chan []byte
	readBuf  []byte
	closed   bool
	mu       sync.Mutex
}

func NewStream(id uint32, session *Session) *Stream {
	return &Stream{
		id:       id,
		session:  session,
		readChan: make(chan []byte, 10),
	}
}

func (s *Stream) Read(p []byte) (int, error) {
	if len(s.readBuf) > 0 {
		n := copy(p, s.readBuf)
		s.readBuf = s.readBuf[n:]
		return n, nil
	}

	select {
	case data := <-s.readChan:
		n := copy(p, data)
		if n < len(data) {
			s.readBuf = data[n:]
		}
		return n, nil
	case <-time.After(30 * time.Second):
		return 0, E.New("read timeout")
	}
}

func (s *Stream) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, E.New("stream closed")
	}

	totalWritten := 0
	for len(p) > 0 {
		chunk := &Chunk{
			StreamID: s.id,
			Data:     p,
		}
		if len(p) > s.session.chunkSize {
			chunk.Data = p[:s.session.chunkSize]
			p = p[s.session.chunkSize:]
		} else {
			totalWritten += len(p)
			p = nil
		}
		select {
		case s.session.writeChan <- chunk:
		case <-time.After(30 * time.Second):
			return totalWritten, E.New("write timeout")
		}
	}

	return totalWritten, nil
}

func (s *Stream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	s.session.mu.Lock()
	delete(s.session.streams, s.id)
	s.session.mu.Unlock()
	close(s.readChan)
	return nil
}

func (s *Stream) LocalAddr() net.Addr                { return s.session.conn.LocalAddr() }
func (s *Stream) RemoteAddr() net.Addr               { return s.session.conn.RemoteAddr() }
func (s *Stream) SetDeadline(t time.Time) error      { return nil }
func (s *Stream) SetReadDeadline(t time.Time) error  { return nil }
func (s *Stream) SetWriteDeadline(t time.Time) error { return nil }

type Chunk struct {
	StreamID uint32
	Data     []byte
}

// Server implementation

type Server struct {
	handler  ServiceHandler
	logger   logger.Logger
	sessions map[net.Conn]*Session
	mu       sync.Mutex
}

type ServiceHandler interface {
	NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error
}

func NewServer(handler ServiceHandler, logger logger.Logger) *Server {
	return &Server{
		handler:  handler,
		logger:   logger,
		sessions: make(map[net.Conn]*Session),
	}
}

func (s *Server) Serve(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	session := NewSession(conn, DefaultChunkSize, DefaultMaxStreams, DefaultKeepAlive, DefaultIdleTimeout, s.logger)
	s.mu.Lock()
	s.sessions[conn] = session
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.sessions, conn)
		s.mu.Unlock()
		session.Close()
	}()

	for {
		stream, err := session.AcceptStream()
		if err != nil {
			s.logger.Error("accept stream error:", err)
			return
		}
		go s.handleStream(stream)
	}
}

func (s *Server) handleStream(stream *Stream) {
	metadata := M.Metadata{
		Source: M.SocksaddrFromNet(stream.RemoteAddr()),
	}
	err := s.handler.NewConnection(context.Background(), stream, metadata)
	if err != nil {
		s.logger.Error("handle connection error:", err)
		stream.Close()
	}
}

func (s *Session) AcceptStream() (*Stream, error) {
	select {
	case <-s.closeChan:
		return nil, E.New("session closed")
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.streams) >= s.maxStreams {
		return nil, E.New("too many streams")
	}

	id := s.nextID
	s.nextID++

	stream := NewStream(id, s)
	s.streams[id] = stream

	s.lastActivity = time.Now()
	return stream, nil
}
