// Package quicdcquic adapts quic-go connections and streams to the interfaces
// of the quicdc package.
package quicdcquic

import (
	"context"
	"io"

	"github.com/mengelbart/quicdc"
	"github.com/quic-go/quic-go"
)

var _ quicdc.Connection = (*Connection)(nil)

// Connection adapts a *quic.Conn to quicdc.Connection.
type Connection struct {
	conn *quic.Conn
}

// NewConnection wraps conn so that it can be passed to quicdc.NewSession.
func NewConnection(conn *quic.Conn) *Connection {
	return &Connection{conn: conn}
}

// Conn returns the wrapped connection.
func (c *Connection) Conn() *quic.Conn {
	return c.conn
}

func (c *Connection) OpenUniStream() (quicdc.SendStream, error) {
	s, err := c.conn.OpenUniStream()
	if err != nil {
		return nil, err
	}
	return newSendStream(s), nil
}

func (c *Connection) OpenUniStreamSync(ctx context.Context) (quicdc.SendStream, error) {
	s, err := c.conn.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return newSendStream(s), nil
}

func (c *Connection) AcceptUniStream(ctx context.Context) (quicdc.ReceiveStream, error) {
	s, err := c.conn.AcceptUniStream(ctx)
	if err != nil {
		return nil, err
	}
	return newReceiveStream(s), nil
}

func (c *Connection) CloseWithError(code uint64, desc string) error {
	return c.conn.CloseWithError(quic.ApplicationErrorCode(code), desc)
}

// quicSendStream is the part of *quic.SendStream the adapter uses.
type quicSendStream interface {
	io.Writer
	io.Closer
	CancelWrite(quic.StreamErrorCode)
}

// quicReceiveStream is the part of *quic.ReceiveStream the adapter uses.
type quicReceiveStream interface {
	io.Reader
	StreamID() quic.StreamID
	CancelRead(quic.StreamErrorCode)
}

// prioritySetter is implemented by streams that support stream priorities.
// quicdc looks for the same methods on the streams it is handed.
type prioritySetter interface {
	SetPriority(uint32)
	SetIncremental(bool)
}

type sendStream struct {
	quicSendStream
}

func (s *sendStream) CancelWrite(code uint64) {
	s.quicSendStream.CancelWrite(quic.StreamErrorCode(code))
}

// prioritySendStream is a sendStream over a stream that supports priorities.
type prioritySendStream struct {
	sendStream
	prioritySetter
}

// newSendStream wraps s, keeping the priority methods reachable if the
// underlying stream has them.
func newSendStream(s quicSendStream) quicdc.SendStream {
	if ps, ok := s.(prioritySetter); ok {
		return &prioritySendStream{
			sendStream:     sendStream{quicSendStream: s},
			prioritySetter: ps,
		}
	}
	return &sendStream{quicSendStream: s}
}

type receiveStream struct {
	quicReceiveStream
}

func (s *receiveStream) ID() int64 {
	return int64(s.StreamID())
}

func (s *receiveStream) CancelRead(code uint64) {
	s.quicReceiveStream.CancelRead(quic.StreamErrorCode(code))
}

// newReceiveStream wraps s for quicdc.
func newReceiveStream(s quicReceiveStream) quicdc.ReceiveStream {
	return &receiveStream{quicReceiveStream: s}
}
