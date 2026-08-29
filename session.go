package quicdc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/quic-go/quic-go/quicvarint"
)

type SendStream interface {
	io.Writer
	io.Closer
}

type ReceiveStream interface {
	io.Reader
	ID() int64
	CancelRead(uint64)
}

type Connection interface {
	OpenUniStream() (SendStream, error)
	OpenUniStreamSync(context.Context) (SendStream, error)
	AcceptUniStream(context.Context) (ReceiveStream, error)
	CloseWithError(uint64, string) error
}

type OnDataChannelHandler func(*DataChannel)

// errSessionClosed is returned by operations that were waiting when the
// session's read loop stopped.
var errSessionClosed = errors.New("session closed")

// errProtocolViolation marks an error caused by the peer breaking the
// protocol. It tears the session down.
var errProtocolViolation = errors.New("protocol violation")

type Session struct {
	conn     Connection
	acceptCh chan ReceiveStream

	channels    map[uint64]*DataChannel
	channelLock sync.Mutex

	dcHandler   OnDataChannelHandler
	handlerLock sync.Mutex

	closed    chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func NewSession(conn Connection) *Session {
	pc := &Session{
		conn:        conn,
		acceptCh:    make(chan ReceiveStream),
		channels:    map[uint64]*DataChannel{},
		channelLock: sync.Mutex{},
		closed:      make(chan struct{}),
	}
	return pc
}

// Read starts a loop reading incoming streams from the session's connection.
// It returns when ctx is done or the connection stops accepting streams. An
// error on a single stream is reported to the data channel the stream belongs
// to, a protocol violation closes the connection and tears the session down.
// Read must not be called if the connection is managed by the application.
func (c *Session) Read(ctx context.Context) error {
	for {
		s, err := c.conn.AcceptUniStream(ctx)
		if err != nil {
			c.close(err)
			return c.closeErr
		}
		// Each stream is handled in its own goroutine, so that a stream
		// waiting for the application to read a message does not block the
		// other channels of the session.
		go func() {
			id, err := quicvarint.Read(quicvarint.NewReader(s))
			if err != nil {
				log.Printf("dropping stream: failed to read channel ID: %v", err)
				return
			}
			if err := c.ReadStream(ctx, s, id); err != nil {
				if errors.Is(err, errProtocolViolation) {
					c.abort(err)
					return
				}
				dc, ok := c.getChannel(id)
				if !ok {
					log.Printf("dropping stream for channel ID %v: %v", id, err)
					return
				}
				dc.setError(err)
			}
		}()
	}
}

// close records why the session stopped, unblocks pending opens and fails all
// data channels. Only the first error is kept.
func (c *Session) close(err error) {
	c.closeOnce.Do(func() {
		c.closeErr = err
		close(c.closed)
		for _, dc := range c.allChannels() {
			dc.setError(err)
		}
	})
}

// abort tears the session down and tells the peer why.
func (c *Session) abort(err error) {
	c.close(err)
	_ = c.conn.CloseWithError(errorCodeProtocolViolation, err.Error())
}

// OpenDataChannel opens a new data channel and waits for the peer to
// acknowledge it. It returns when ctx is done or the session's read loop
// stopped.
func (s *Session) OpenDataChannel(ctx context.Context, channelID, priority uint64, ordered bool, rxTime time.Duration, label string, protocol string) (*DataChannel, error) {
	dc := newDataChannel(s.conn, channelID, priority, ordered, rxTime, label, protocol)
	if err := s.addChannel(channelID, dc); err != nil {
		return nil, err
	}
	if err := dc.open(ctx, s.closed); err != nil {
		s.removeChannel(channelID)
		if errors.Is(err, errSessionClosed) && s.closeErr != nil {
			return nil, fmt.Errorf("%w: %w", errSessionClosed, s.closeErr)
		}
		return nil, err
	}
	return dc, nil
}

func (s *Session) OnIncomingDataChannel(handler OnDataChannelHandler) {
	s.handlerLock.Lock()
	defer s.handlerLock.Unlock()
	s.dcHandler = handler
}

func (s *Session) ReadStream(ctx context.Context, stream ReceiveStream, channelID uint64) error {
	mt, err := quicvarint.Read(quicvarint.NewReader(stream))
	if err != nil {
		return err
	}
	switch mt {
	case uint64(dataChannelOpenMessageType):
		m := dataChannelOpenMessage{ChannelID: channelID}
		if err := m.parse(quicvarint.NewReader(stream)); err != nil {
			return err
		}
		ordered, rxTime := m.ChannelType.parameters(m.ReliabilityParameter)
		dc := newDataChannel(
			s.conn,
			channelID,
			m.Priority,
			ordered,
			rxTime,
			m.Label,
			m.Protocol,
		)
		if err := s.addChannel(channelID, dc); err != nil {
			return fmt.Errorf("%w: %w", errProtocolViolation, err)
		}
		ackStream, err := s.conn.OpenUniStreamSync(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = ackStream.Close() }()
		dcoom := dataChannelOpenOkMessage{
			ChannelID: channelID,
		}
		response := dcoom.append(make([]byte, 0, 16)) // 16 is max size for two varints
		if _, err := ackStream.Write(response); err != nil {
			return err
		}
		s.onDataChannel(dc)
		return nil
	case uint64(dataChannelOpenOkMessageType):
		log.Printf("received dataChannelOpenOkMessage for channel ID: %v", channelID)
		dc, ok := s.getChannel(channelID)
		if !ok {
			return fmt.Errorf("%w: got OpenOk message for unknown channel ID: %v", errProtocolViolation, channelID)
		}
		dc.handleAck()
		return nil
	case uint64(dataChannelMessageType):
		dc, ok := s.getChannel(channelID)
		if !ok {
			return fmt.Errorf("%w: got message for unknown channel ID: %v", errProtocolViolation, channelID)
		}
		return dc.handleIncomingMessageStream(ctx, stream)
	}
	return nil
}

func (s *Session) onDataChannel(dc *DataChannel) {
	s.handlerLock.Lock()
	defer s.handlerLock.Unlock()
	if s.dcHandler != nil {
		// TODO: Does this really need a new goroutine? We don't want to block
		// on a user provided handler, because it would block the ReadStream
		// method, but it may be required to run outside since ReadStream could
		// run in the user's main goroutine?
		// Alternative API: Provide a blocking method that allows users to
		// "read" incoming channels from the session.
		go s.dcHandler(dc)
	}
}

func (s *Session) addChannel(id uint64, dc *DataChannel) error {
	s.channelLock.Lock()
	defer s.channelLock.Unlock()
	_, ok := s.channels[id]
	if ok {
		return fmt.Errorf("duplicate channel id: %v", id)
	}
	s.channels[id] = dc
	return nil
}

func (s *Session) removeChannel(id uint64) {
	s.channelLock.Lock()
	defer s.channelLock.Unlock()
	delete(s.channels, id)
}

func (s *Session) allChannels() []*DataChannel {
	s.channelLock.Lock()
	defer s.channelLock.Unlock()
	channels := make([]*DataChannel, 0, len(s.channels))
	for _, dc := range s.channels {
		channels = append(channels, dc)
	}
	return channels
}

func (s *Session) getChannel(id uint64) (*DataChannel, bool) {
	s.channelLock.Lock()
	defer s.channelLock.Unlock()
	dc, ok := s.channels[id]
	return dc, ok
}
