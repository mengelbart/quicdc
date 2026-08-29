package quicdc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go/quicvarint"
)

type prioritySetter interface {
	SetPriority(uint32)
	SetIncremental(bool)
}

const (
	errorCodeNoError           = 0x00
	errorCodeUnknownFlowID     = 0x01
	errorCodeProtocolViolation = 0x02
)

// ErrDataChannelClosed is returned by operations on a data channel that was
// closed locally or by the peer.
var ErrDataChannelClosed = errors.New("data channel closed")

type DataChannel struct {
	session       *Session
	nextSendSeqNr atomic.Uint64

	// recvLock guards nextRecvSeqNr and the enqueue/drain sequence on the
	// reorder buffer.
	recvLock      sync.Mutex
	nextRecvSeqNr uint64
	reorderBuffer *messageHeap
	recvBuffer    chan *DataChannelReadMessage

	id       uint64
	priority uint64
	ordered  bool
	rxTime   time.Duration
	label    string
	protocol string

	ackChan chan struct{}
	ackOnce sync.Once

	closeOnce sync.Once

	// errChan is closed once err is set.
	errChan chan struct{}
	errOnce sync.Once
	err     error
}

func newDataChannel(
	session *Session,
	id uint64,
	priority uint64,
	ordered bool,
	rxTime time.Duration,
	label string,
	protocol string,
) *DataChannel {
	return &DataChannel{
		session:       session,
		nextRecvSeqNr: 0,
		reorderBuffer: &messageHeap{},
		recvBuffer:    make(chan *DataChannelReadMessage),
		id:            id,
		priority:      priority,
		ordered:       ordered,
		rxTime:        rxTime,
		label:         label,
		protocol:      protocol,
		ackChan:       make(chan struct{}),
		errChan:       make(chan struct{}),
	}
}

// open sends the DATA_CHANNEL_OPEN message and waits for the peer's
// acknowledgement. It returns when the channel fails, ctx is done or
// sessionClosed is closed.
func (d *DataChannel) open(ctx context.Context, sessionClosed <-chan struct{}) error {
	s, err := d.session.conn.OpenUniStream()
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	if ps, ok := s.(prioritySetter); ok {
		slog.Info("dc setting stream priority", "priority", d.priority, "incremental", true)
		ps.SetPriority(uint32(d.priority))
		ps.SetIncremental(true)
	}

	// send DATA_CHANNEL_OPEN message
	dcom := dataChannelOpenMessage{
		ChannelID:            d.id,
		ChannelType:          getChannelType(d.ordered, d.rxTime),
		Priority:             d.priority,
		ReliabilityParameter: uint64(d.rxTime.Milliseconds()),
		Label:                d.label,
		Protocol:             d.protocol,
	}
	buf := dcom.append(make([]byte, 0, 64))
	if _, err = s.Write(buf); err != nil {
		return err
	}

	// wait for DATA_CHANNEL_OPEN_ACK message
	select {
	case <-d.ackChan:
	case <-d.errChan:
		return d.err
	case <-ctx.Done():
		return ctx.Err()
	case <-sessionClosed:
		return errSessionClosed
	}

	return nil
}

// setError records a fatal error on the data channel and unblocks pending
// operations. Only the first error is kept.
func (d *DataChannel) setError(err error) {
	d.errOnce.Do(func() {
		d.err = err
		close(d.errChan)
	})
}

// handleAck informs the open() goroutine that the open ack message has been
// received. Repeated calls are no-ops.
func (d *DataChannel) handleAck() {
	d.ackOnce.Do(func() {
		close(d.ackChan)
	})
}

// Close closes the data channel and notifies the peer. Pending and future
// calls to SendMessage and ReceiveMessage return ErrDataChannelClosed, and
// messages that are still held in the reorder buffer are dropped. Repeated
// calls are no-ops and return nil.
func (d *DataChannel) Close() error {
	var err error
	d.closeOnce.Do(func() {
		err = d.sendClose()
		d.teardown(ErrDataChannelClosed)
	})
	return err
}

// closeWithError tears the data channel down without notifying the peer and
// makes pending operations return err.
func (d *DataChannel) closeWithError(err error) {
	d.closeOnce.Do(func() {
		d.teardown(err)
	})
}

// sendClose sends the DATA_CHANNEL_CLOSE message to the peer. It does not wait
// for stream credit, so it fails instead of blocking if the peer's stream
// limit is exhausted.
func (d *DataChannel) sendClose() error {
	s, err := d.session.conn.OpenUniStream()
	if err != nil {
		return err
	}
	dccm := dataChannelCloseMessage{
		ChannelID: d.id,
	}
	if _, err := s.Write(dccm.append(make([]byte, 0, 16))); err != nil {
		_ = s.Close()
		return err
	}
	return s.Close()
}

// teardown unblocks pending operations with err, removes the channel from its
// session and discards buffered messages.
func (d *DataChannel) teardown(err error) {
	d.setError(err)
	d.session.removeChannel(d.id)
	d.discardReorderBuffer()
}

// discardReorderBuffer cancels the streams of all messages that are still
// waiting for delivery.
func (d *DataChannel) discardReorderBuffer() {
	d.recvLock.Lock()
	defer d.recvLock.Unlock()
	for d.reorderBuffer.peek() != nil {
		_ = d.reorderBuffer.dequeue().Close()
	}
}

func (d *DataChannel) pushMessage(ctx context.Context, msg *DataChannelReadMessage) {
	select {
	case d.recvBuffer <- msg:
	case <-d.errChan:
		_ = msg.Close()
	case <-ctx.Done():
		_ = msg.Close()
	}
}

// drainReorderBuffer must be called with recvLock held.
func (d *DataChannel) drainReorderBuffer(ctx context.Context) {
	for {
		head := d.reorderBuffer.peek()
		if head == nil {
			return
		}
		if head.SequenceNumber != d.nextRecvSeqNr {
			return
		}
		d.pushMessage(ctx, d.reorderBuffer.dequeue())
		d.nextRecvSeqNr++
	}
}

func (d *DataChannel) handleIncomingMessageStream(ctx context.Context, s ReceiveStream) error {
	m := dataChannelMessage{}
	if err := m.parse(quicvarint.NewReader(s)); err != nil {
		return err
	}
	rm := &DataChannelReadMessage{
		SequenceNumber: m.SequenceNumber,
		stream:         s,
	}
	if d.ordered {
		d.recvLock.Lock()
		defer d.recvLock.Unlock()
		d.reorderBuffer.enqueue(rm)
		d.drainReorderBuffer(ctx)
	} else {
		d.pushMessage(ctx, rm)
	}
	return nil
}

func (d *DataChannel) ID() uint64 {
	return d.id
}

// SendMessage opens a new stream for the next message on the data channel. It
// is safe for concurrent use. It returns ErrDataChannelClosed once the channel
// has been closed.
func (d *DataChannel) SendMessage(ctx context.Context) (*DataChannelWriteMessage, error) {
	select {
	case <-d.errChan:
		return nil, d.err
	default:
	}
	s, err := d.session.conn.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	if ps, ok := s.(prioritySetter); ok {
		slog.Info("dc setting stream priority", "priority", d.priority, "incremental", true)
		ps.SetPriority(uint32(d.priority))
		ps.SetIncremental(true)
	}
	dcm := dataChannelMessage{
		ChannelID:      d.id,
		SequenceNumber: d.nextSendSeqNr.Add(1) - 1,
	}
	_, err = s.Write(dcm.append(make([]byte, 0, 32)))
	if err != nil {
		return nil, err
	}
	return &DataChannelWriteMessage{
		SequenceNumber: dcm.SequenceNumber,
		stream:         s,
	}, nil
}

func (d *DataChannel) ReceiveMessage(ctx context.Context) (*DataChannelReadMessage, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg := <-d.recvBuffer:
		return msg, nil
	case <-d.errChan:
		return nil, d.err
	}
}

func getChannelType(ordered bool, rxtime time.Duration) dataChannelType {
	if ordered {
		if rxtime > 0 {
			return dataChannelTypePartialReliableTimed
		}
		return dataChannelTypeReliable
	}

	if rxtime > 0 {
		return dataChannelTypePartialReliableTimedUnordered
	}
	return dataChannelTypeReliableUnordered
}

type DataChannelReadMessage struct {
	SequenceNumber uint64
	stream         ReceiveStream
}

// Close implements io.ReadCloser.
func (m *DataChannelReadMessage) Close() error {
	m.stream.CancelRead(errorCodeUnknownFlowID)
	return nil
}

// Read implements io.ReadCloser.
func (m *DataChannelReadMessage) Read(p []byte) (n int, err error) {
	return m.stream.Read(p)
}

type DataChannelWriteMessage struct {
	SequenceNumber uint64
	stream         io.WriteCloser
}

// Close implements io.WriteCloser.
func (m *DataChannelWriteMessage) Close() error {
	return m.stream.Close()
}

// Write implements io.WriteCloser.
func (m *DataChannelWriteMessage) Write(p []byte) (n int, err error) {
	return m.stream.Write(p)
}
