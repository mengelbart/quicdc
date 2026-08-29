package quicdcquic

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/mengelbart/quicdc"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeSendStream struct {
	cancelCode quic.StreamErrorCode
	cancelled  bool
}

func (s *fakeSendStream) Write(p []byte) (int, error) { return len(p), nil }
func (s *fakeSendStream) Close() error                { return nil }
func (s *fakeSendStream) CancelWrite(code quic.StreamErrorCode) {
	s.cancelCode = code
	s.cancelled = true
}

type fakePrioritySendStream struct {
	fakeSendStream
	priority    uint32
	incremental bool
}

func (s *fakePrioritySendStream) SetPriority(p uint32)    { s.priority = p }
func (s *fakePrioritySendStream) SetIncremental(inc bool) { s.incremental = inc }

type fakeReceiveStream struct {
	id         quic.StreamID
	cancelCode quic.StreamErrorCode
	cancelled  bool
}

func (s *fakeReceiveStream) Read(p []byte) (int, error) { return 0, io.EOF }
func (s *fakeReceiveStream) StreamID() quic.StreamID    { return s.id }
func (s *fakeReceiveStream) CancelRead(code quic.StreamErrorCode) {
	s.cancelCode = code
	s.cancelled = true
}

func TestSendStreamForwardsCancelWrite(t *testing.T) {
	fake := &fakeSendStream{}
	s := newSendStream(fake)
	s.CancelWrite(42)
	assert.True(t, fake.cancelled)
	assert.Equal(t, quic.StreamErrorCode(42), fake.cancelCode)
}

func TestSendStreamWithoutPriorities(t *testing.T) {
	s := newSendStream(&fakeSendStream{})
	_, ok := s.(interface {
		SetPriority(uint32)
		SetIncremental(bool)
	})
	assert.False(t, ok)
}

func TestSendStreamForwardsPriorities(t *testing.T) {
	fake := &fakePrioritySendStream{}
	s := newSendStream(fake)
	ps, ok := s.(interface {
		SetPriority(uint32)
		SetIncremental(bool)
	})
	require.True(t, ok)
	ps.SetPriority(7)
	ps.SetIncremental(true)
	assert.Equal(t, uint32(7), fake.priority)
	assert.True(t, fake.incremental)

	s.CancelWrite(3)
	assert.Equal(t, quic.StreamErrorCode(3), fake.cancelCode)
}

func TestReceiveStream(t *testing.T) {
	fake := &fakeReceiveStream{id: 9}
	s := newReceiveStream(fake)
	assert.Equal(t, int64(9), s.ID())
	s.CancelRead(5)
	assert.True(t, fake.cancelled)
	assert.Equal(t, quic.StreamErrorCode(5), fake.cancelCode)
}

// TestEndToEnd runs a session over a real quic-go connection: it opens a data
// channel, sends a message and closes the channel again.
func TestEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLSConfig(t), nil)
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	type accepted struct {
		session *quicdc.Session
		err     error
	}
	acceptedCh := make(chan accepted, 1)
	channels := make(chan *quicdc.DataChannel, 1)
	go func() {
		conn, err := listener.Accept(ctx)
		if err != nil {
			acceptedCh <- accepted{err: err}
			return
		}
		session := quicdc.NewSession(NewConnection(conn))
		session.OnIncomingDataChannel(func(dc *quicdc.DataChannel) { channels <- dc })
		go func() { _ = session.Run(ctx) }()
		acceptedCh <- accepted{session: session}
	}()

	conn, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLSConfig(), nil)
	require.NoError(t, err)

	client := quicdc.NewSession(NewConnection(conn))
	go func() { _ = client.Run(ctx) }()
	defer func() { _ = client.Close() }()

	dc, err := client.OpenDataChannel(ctx, 1, 2, true, 0, "label", "protocol")
	require.NoError(t, err)

	acc := <-acceptedCh
	require.NoError(t, acc.err)
	defer func() { _ = acc.session.Close() }()

	w, err := dc.SendMessage(ctx)
	require.NoError(t, err)
	_, err = w.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	remote := <-channels
	assert.Equal(t, uint64(1), remote.ID())

	r, err := remote.ReceiveMessage(ctx)
	require.NoError(t, err)
	body, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(body))
	require.NoError(t, r.Close())

	require.NoError(t, dc.Close())
}

func serverTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "quicdc test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	require.NoError(t, err)
	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{der},
			PrivateKey:  key,
		}},
		NextProtos: []string{"quicdc-test"},
	}
}

func clientTLSConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"quicdc-test"},
	}
}
