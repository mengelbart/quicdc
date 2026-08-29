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

func messageStream(id int64, channelID, seqNr uint64, payload string) *fakeReceiveStream {
	m := dataChannelMessage{ChannelID: channelID, SequenceNumber: seqNr}
	return &fakeReceiveStream{r: bytes.NewReader(append(m.append(nil), payload...)), id: id}
}

// runSession starts a session on conn and returns a channel of incoming data
// channels and a channel carrying the Run loop's error.
func runSession(conn *fakeConn, opts ...Option) (*Session, chan *DataChannel, chan error) {
	s := NewSession(conn, opts...)
	dcs := make(chan *DataChannel, 1)
	s.OnIncomingDataChannel(func(dc *DataChannel) { dcs <- dc })
	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()
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

func TestOrderedDelivery(t *testing.T) {
	conn := newFakeConn()
	s, dcs, done := runSession(conn)
	conn.accept <- openStream(0, 1)
	dc := <-dcs

	conn.accept <- messageStream(2, 1, 0, "first")
	msg, err := dc.ReceiveMessage(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(0), msg.SequenceNumber)

	// The gap at 1 keeps the channel from delivering.
	conn.accept <- messageStream(4, 1, 2, "third")
	assert.Eventually(t, func() bool {
		return dc.reorderBuffer.size() == 1
	}, time.Second, time.Millisecond)

	// Filling the gap delivers both messages in order.
	conn.accept <- messageStream(6, 1, 1, "second")
	msg, err = dc.ReceiveMessage(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(1), msg.SequenceNumber)
	msg, err = dc.ReceiveMessage(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(2), msg.SequenceNumber)

	require.NoError(t, s.Close())
	<-done
}

func TestStaleSequenceNumberFailsSession(t *testing.T) {
	conn := newFakeConn()
	_, dcs, done := runSession(conn)
	conn.accept <- openStream(0, 1)
	dc := <-dcs

	conn.accept <- messageStream(2, 1, 0, "first")
	_, err := dc.ReceiveMessage(context.Background())
	require.NoError(t, err)

	// Sequence number 0 was delivered already.
	conn.accept <- messageStream(4, 1, 0, "stale")

	assert.ErrorIs(t, <-done, errProtocolViolation)
	closed, code := conn.closeInfo()
	assert.True(t, closed)
	assert.Equal(t, uint64(errorCodeProtocolViolation), code)
}

func TestDuplicateSequenceNumberFailsSession(t *testing.T) {
	conn := newFakeConn()
	_, dcs, done := runSession(conn)
	conn.accept <- openStream(0, 1)
	dc := <-dcs

	// Sequence number 1 stays in the reorder buffer, waiting for the gap at 0.
	conn.accept <- messageStream(2, 1, 1, "second")
	assert.Eventually(t, func() bool {
		return dc.reorderBuffer.size() == 1
	}, time.Second, time.Millisecond)

	conn.accept <- messageStream(4, 1, 1, "duplicate")

	assert.ErrorIs(t, <-done, errProtocolViolation)
	closed, code := conn.closeInfo()
	assert.True(t, closed)
	assert.Equal(t, uint64(errorCodeProtocolViolation), code)
}

func TestReorderBufferOverflow(t *testing.T) {
	const bufferLen = 4

	conn := newFakeConn()
	_, dcs, done := runSession(conn, WithMaxReorderBufferLen(bufferLen))
	conn.accept <- openStream(0, 1)
	dc := <-dcs

	// Sequence number 0 never arrives, so nothing is ever delivered.
	for i := 1; i <= bufferLen; i++ {
		conn.accept <- messageStream(int64(2*i), 1, uint64(i), "payload")
	}
	assert.Eventually(t, func() bool {
		return dc.reorderBuffer.size() == bufferLen
	}, time.Second, time.Millisecond)

	conn.accept <- messageStream(4242, 1, bufferLen+1, "payload")

	assert.ErrorIs(t, <-done, ErrReorderBufferOverflow)
	_, err := dc.ReceiveMessage(context.Background())
	assert.ErrorIs(t, err, ErrReorderBufferOverflow)
	closed, code := conn.closeInfo()
	assert.True(t, closed)
	assert.Equal(t, uint64(errorCodeExcessiveLoad), code)
}

func TestDefaultMaxReorderBufferLen(t *testing.T) {
	s := NewSession(newFakeConn())
	assert.Equal(t, defaultMaxReorderBufferLen, s.maxReorderBufferLen)

	// Values below one keep the default.
	s = NewSession(newFakeConn(), WithMaxReorderBufferLen(0))
	assert.Equal(t, defaultMaxReorderBufferLen, s.maxReorderBufferLen)
}
