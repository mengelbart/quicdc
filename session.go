package quicdc

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

type Connection interface {
	OpenUniStream() (*quic.SendStream, error)
	OpenUniStreamSync(context.Context) (*quic.SendStream, error)
	AcceptUniStream(context.Context) (*quic.ReceiveStream, error)
}

type OnDataChannelHandler func(*DataChannel)

type Session struct {
	conn     Connection
	acceptCh chan quic.ReceiveStream

	channels    map[uint64]*DataChannel
	channelLock sync.Mutex

	dcHandler   OnDataChannelHandler
	handlerLock sync.Mutex
}

func NewSession(conn Connection) *Session {
	pc := &Session{
		conn:        conn,
		acceptCh:    make(chan quic.ReceiveStream),
		channels:    map[uint64]*DataChannel{},
		channelLock: sync.Mutex{},
	}
	return pc
}

// Read starts a loop reading incoming streams from the session's connection.
// Read must not be called if the connection is managed by the application.
func (c *Session) Read() {
	for {
		s, err := c.conn.AcceptUniStream(context.Background())
		if err != nil {
			panic(err)
		}
		r := quicvarint.NewReader(s)
		id, err := quicvarint.Read(r)
		if err != nil {
			panic(err)
		}
		c.ReadStream(context.Background(), s, id)
	}
}

func (s *Session) OpenDataChannel(channelID, priority uint64, ordered bool, rxTime time.Duration, label string, protocol string) (*DataChannel, error) {
	dc := newDataChannel(s.conn, channelID, priority, ordered, rxTime, label, protocol)
	s.addChannel(channelID, dc)
	if err := dc.open(); err != nil {
		return nil, err
	}
	return dc, nil
}

func (s *Session) OnIncomingDataChannel(handler OnDataChannelHandler) {
	s.handlerLock.Lock()
	defer s.handlerLock.Unlock()
	s.dcHandler = handler
}

func (s *Session) ReadStream(ctx context.Context, stream *quic.ReceiveStream, channelID uint64) error {
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
			return err
		}
		ackStream, err := s.conn.OpenUniStreamSync(ctx)
		if err != nil {
			return err
		}
		defer ackStream.Close()
		dcoom := dataChannelOpenOkMessage{
			ChannelID: channelID,
		}
		response := dcoom.append(make([]byte, 0, 16)) // 16 is max size for two varints
		if _, err := ackStream.Write(response); err != nil {
			return err
		}
		s.onDataChannel(dc)
		return ackStream.Close()
	case uint64(dataChannelOpenOkMessageType):
		log.Printf("received dataChannelOpenOkMessage for channel ID: %v", channelID)
	case uint64(dataChannelMessageType):
		dc, ok := s.getChannel(channelID)
		if !ok {
			return fmt.Errorf("got message for unknown channel ID: %v", channelID)
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

func (s *Session) getChannel(id uint64) (*DataChannel, bool) {
	s.channelLock.Lock()
	defer s.channelLock.Unlock()
	dc, ok := s.channels[id]
	return dc, ok
}
