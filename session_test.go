package quicdc

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go/quicvarint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errFakeConnClosed = errors.New("fake connection closed")

type fakeSendStream struct {
	lock   sync.Mutex
	buf    bytes.Buffer
	closed bool
}

func (s *fakeSendStream) Write(p []byte) (int, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.buf.Write(p)
}

func (s *fakeSendStream) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.closed = true
	return nil
}

func (s *fakeSendStream) bytes() []byte {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.buf.Bytes()
}

type fakeReceiveStream struct {
	lock      sync.Mutex
	r         *bytes.Reader
	id        int64
	cancelled bool
}

func (s *fakeReceiveStream) Read(p []byte) (int, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.r.Read(p)
}

func (s *fakeReceiveStream) ID() int64 { return s.id }

func (s *fakeReceiveStream) CancelRead(uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.cancelled = true
}

func (s *fakeReceiveStream) wasCancelled() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.cancelled
}

type fakeConn struct {
	lock      sync.Mutex
	streams   []*fakeSendStream
	closed    bool
	closeCode uint64

	accept   chan ReceiveStream
	closedCh chan struct{}
}

func newFakeConn() *fakeConn {
	return &fakeConn{
		accept:   make(chan ReceiveStream),
		closedCh: make(chan struct{}),
	}
}

func (c *fakeConn) OpenUniStream() (SendStream, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.closed {
		return nil, errFakeConnClosed
	}
	s := &fakeSendStream{}
	c.streams = append(c.streams, s)
	return s, nil
}

func (c *fakeConn) OpenUniStreamSync(context.Context) (SendStream, error) {
	return c.OpenUniStream()
}

func (c *fakeConn) AcceptUniStream(ctx context.Context) (ReceiveStream, error) {
	select {
	case s := <-c.accept:
		return s, nil
	case <-c.closedCh:
		return nil, errFakeConnClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *fakeConn) CloseWithError(code uint64, _ string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.closed {
		c.closed = true
		c.closeCode = code
		close(c.closedCh)
	}
	return nil
}

func (c *fakeConn) sendStreams() []*fakeSendStream {
	c.lock.Lock()
	defer c.lock.Unlock()
	return append([]*fakeSendStream{}, c.streams...)
}

func (c *fakeConn) closeInfo() (bool, uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.closed, c.closeCode
}

func openStream(id int64, channelID uint64) *fakeReceiveStream {
	m := dataChannelOpenMessage{
		ChannelID: channelID,
		Label:     "label",
		Protocol:  "protocol",
	}
	return &fakeReceiveStream{r: bytes.NewReader(m.append(nil)), id: id}
}

func closeStream(id int64, channelID uint64) *fakeReceiveStream {
	m := dataChannelCloseMessage{ChannelID: channelID}
	return &fakeReceiveStream{r: bytes.NewReader(m.append(nil)), id: id}
}

// runSession starts a session on conn and returns a channel of incoming data
// channels and a channel carrying the read loop's error.
func runSession(conn *fakeConn) (*Session, chan *DataChannel, chan error) {
	s := NewSession(conn)
	dcs := make(chan *DataChannel, 1)
	s.OnIncomingDataChannel(func(dc *DataChannel) { dcs <- dc })
	done := make(chan error, 1)
	go func() { done <- s.Read(context.Background()) }()
	return s, dcs, done
}

func TestDataChannelClose(t *testing.T) {
	conn := newFakeConn()
	s, dcs, done := runSession(conn)
	conn.accept <- openStream(0, 1)
	dc := <-dcs

	require.NoError(t, dc.Close())

	_, ok := s.getChannel(1)
	assert.False(t, ok)

	_, err := dc.SendMessage(context.Background())
	assert.ErrorIs(t, err, ErrDataChannelClosed)
	_, err = dc.ReceiveMessage(context.Background())
	assert.ErrorIs(t, err, ErrDataChannelClosed)

	// Close is idempotent and sends the close message once.
	require.NoError(t, dc.Close())
	streams := conn.sendStreams()
	require.Len(t, streams, 2)
	expected := dataChannelCloseMessage{ChannelID: 1}
	assert.Equal(t, expected.append(nil), streams[1].bytes())

	require.NoError(t, s.Close())
	<-done
}

func TestIncomingDataChannelClose(t *testing.T) {
	conn := newFakeConn()
	s, dcs, done := runSession(conn)
	conn.accept <- openStream(0, 1)
	dc := <-dcs
	_, ok := s.getChannel(1)
	require.True(t, ok)

	conn.accept <- closeStream(2, 1)

	_, err := dc.ReceiveMessage(context.Background())
	assert.ErrorIs(t, err, ErrDataChannelClosed)
	assert.Eventually(t, func() bool {
		_, ok := s.getChannel(1)
		return !ok
	}, time.Second, time.Millisecond)

	// A close message for an unknown channel is ignored.
	conn.accept <- closeStream(4, 42)

	require.NoError(t, s.Close())
	assert.ErrorIs(t, <-done, errSessionClosed)
}

func TestSessionClose(t *testing.T) {
	conn := newFakeConn()
	s, dcs, done := runSession(conn)
	conn.accept <- openStream(0, 1)
	dc := <-dcs

	require.NoError(t, s.Close())

	assert.ErrorIs(t, <-done, errSessionClosed)
	_, err := dc.ReceiveMessage(context.Background())
	assert.ErrorIs(t, err, errSessionClosed)
	_, ok := s.getChannel(1)
	assert.False(t, ok)

	closed, code := conn.closeInfo()
	assert.True(t, closed)
	assert.Equal(t, uint64(errorCodeNoError), code)
}

func TestControlStreamIsReleased(t *testing.T) {
	conn := newFakeConn()
	s, dcs, done := runSession(conn)
	stream := openStream(0, 1)
	conn.accept <- stream
	<-dcs

	assert.Eventually(t, stream.wasCancelled, time.Second, time.Millisecond)

	require.NoError(t, s.Close())
	<-done
}

func TestUnknownMessageType(t *testing.T) {
	conn := newFakeConn()
	_, _, done := runSession(conn)
	buf := quicvarint.Append(nil, 1)   // channel ID
	buf = quicvarint.Append(buf, 0x42) // unknown message type
	conn.accept <- &fakeReceiveStream{r: bytes.NewReader(buf), id: 0}

	assert.ErrorIs(t, <-done, errProtocolViolation)
	closed, code := conn.closeInfo()
	assert.True(t, closed)
	assert.Equal(t, uint64(errorCodeProtocolViolation), code)
}
