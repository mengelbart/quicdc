package quicdc

import (
	"context"
	"errors"
	"fmt"
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

// defaultMaxReorderBufferLen is the default for WithMaxReorderBufferLen.
const defaultMaxReorderBufferLen = 256

// Connection error codes.
const (
	errorCodeNoError           = 0x00
	errorCodeProtocolViolation = 0x02
	errorCodeExcessiveLoad     = 0x05
)

// Stream error codes, used when resetting the read side of a message stream.
const (
	// errorCodeMessageAborted says the application stopped reading the message.
	errorCodeMessageAborted = 0x03
	// errorCodeMessageDiscarded says the message will never be delivered to
	// the application.
	errorCodeMessageDiscarded = 0x04
)

// ErrDataChannelClosed is returned by operations on a data channel that was
// closed locally or by the peer.
var ErrDataChannelClosed = errors.New("data channel closed")

// ErrReorderBufferOverflow tears the session down when an ordered data
// channel's reorder buffer is full, which means the peer kept sending while a
// gap stayed unfilled.
var ErrReorderBufferOverflow = errors.New("reorder buffer overflow")

type DataChannel struct {
	session       *Session
	nextSendSeqNr atomic.Uint64

	// recvLock guards nextRecvSeqNr and the enqueue/drain sequence on the
	// reorder buffer.
	recvLock      sync.Mutex
	nextRecvSeqNr uint64
	reorderBuffer *messageHeap
	recvBuffer    chan *DataChannelReadMessage
	// gapTimer bounds how long an ordered partially reliable channel waits
	// for a missing message. It is nil while the reorder buffer holds no gap.
	gapTimer *time.Timer

	id       uint64
	priority uint64
	ordered  bool
	rxTime   time.Duration
	label    string
	protocol string

	ackChan chan struct{}
	ackOnce sync.Once

	logger *slog.Logger

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
		logger:        session.logger.With("channel_id", id),
	}
}

// open sends the DATA_CHANNEL_OPEN message and waits for the peer's
// acknowledgement. It returns when the channel fails, ctx is done or
// sessionClosed is closed.
func (d *DataChannel) open(ctx context.Context, sessionClosed <-chan struct{}) error {
	s, err := d.session.conn.OpenUniStreamSync(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	if ps, ok := s.(prioritySetter); ok {
		d.logger.Debug("dc setting stream priority", "priority", d.priority, "incremental", true)
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
	d.stopGapTimer()
	for d.reorderBuffer.peek() != nil {
		_ = d.reorderBuffer.dequeue().cancel(errorCodeMessageDiscarded)
	}
}

func (d *DataChannel) pushMessage(ctx context.Context, msg *DataChannelReadMessage) {
	select {
	case d.recvBuffer <- msg:
	case <-d.errChan:
		_ = msg.cancel(errorCodeMessageDiscarded)
	case <-ctx.Done():
		_ = msg.cancel(errorCodeMessageDiscarded)
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
	s = newBufferedStream(s)
	m := dataChannelMessage{}
	if err := m.parse(quicvarint.NewReader(s)); err != nil {
		return err
	}
	rm := &DataChannelReadMessage{
		SequenceNumber: m.SequenceNumber,
		stream:         s,
	}
	if !d.ordered {
		d.pushMessage(ctx, rm)
		return nil
	}
	return d.enqueueOrdered(ctx, rm)
}

// enqueueOrdered buffers rm and delivers whatever became deliverable. A
// sequence number that was buffered already is a protocol violation, since
// QUIC already retransmits lost data. On a reliable channel that holds for a
// sequence number that was delivered already too, and a peer that fills the
// reorder buffer is a protocol violation as well. Both tear the session down.
// On a partially reliable channel the sender may give up on a message, so a
// message that arrives after its gap expired is dropped and a full reorder
// buffer skips forward instead.
func (d *DataChannel) enqueueOrdered(ctx context.Context, rm *DataChannelReadMessage) error {
	d.recvLock.Lock()
	defer d.recvLock.Unlock()

	if d.reorderBuffer.contains(rm.SequenceNumber) {
		return fmt.Errorf("%w: repeated sequence number %v on channel %v", errProtocolViolation, rm.SequenceNumber, d.id)
	}
	if rm.SequenceNumber < d.nextRecvSeqNr {
		if !d.partiallyReliable() {
			return fmt.Errorf("%w: repeated sequence number %v on channel %v", errProtocolViolation, rm.SequenceNumber, d.id)
		}
		return d.dropExpired(rm)
	}
	if d.reorderBuffer.size() >= d.session.maxReorderBufferLen {
		if !d.partiallyReliable() {
			return fmt.Errorf("%w: channel %v is waiting for sequence number %v", ErrReorderBufferOverflow, d.id, d.nextRecvSeqNr)
		}
		d.skipToBufferHead(ctx)
		if rm.SequenceNumber < d.nextRecvSeqNr {
			return d.dropExpired(rm)
		}
	}

	d.reorderBuffer.enqueue(rm)
	d.drainReorderBuffer(ctx)
	d.armGapTimer()
	return nil
}

func (d *DataChannel) partiallyReliable() bool {
	return d.rxTime > 0
}

func (d *DataChannel) dropExpired(rm *DataChannelReadMessage) error {
	d.logger.Debug("dropping expired message", "sequence_number", rm.SequenceNumber, "next_sequence_number", d.nextRecvSeqNr)
	return rm.cancel(errorCodeMessageDiscarded)
}

func (d *DataChannel) skipGap() {
	d.recvLock.Lock()
	defer d.recvLock.Unlock()
	d.gapTimer = nil
	d.skipToBufferHead(context.Background())
	d.armGapTimer()
}

// skipToBufferHead gives up on the messages missing before the head of the
// reorder buffer and delivers what that made deliverable. It must be called
// with recvLock held.
func (d *DataChannel) skipToBufferHead(ctx context.Context) {
	head := d.reorderBuffer.peek()
	if head == nil {
		return
	}
	if head.SequenceNumber > d.nextRecvSeqNr {
		d.logger.Debug("skipping missing messages", "from", d.nextRecvSeqNr, "to", head.SequenceNumber)
		d.nextRecvSeqNr = head.SequenceNumber
	}
	d.drainReorderBuffer(ctx)
}

// armGapTimer starts the gap timer if the reorder buffer is waiting for a
// missing message and stops it otherwise. It must be called with recvLock held
// and after draining, so that a non empty buffer means a gap.
func (d *DataChannel) armGapTimer() {
	if !d.partiallyReliable() {
		return
	}
	if d.reorderBuffer.peek() == nil {
		d.stopGapTimer()
		return
	}
	if d.gapTimer == nil {
		d.gapTimer = time.AfterFunc(d.rxTime, d.skipGap)
	}
}

// stopGapTimer must be called with recvLock held.
func (d *DataChannel) stopGapTimer() {
	if d.gapTimer != nil {
		d.gapTimer.Stop()
		d.gapTimer = nil
	}
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
		d.logger.Debug("dc setting stream priority", "priority", d.priority, "incremental", true)
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
	msg := &DataChannelWriteMessage{
		SequenceNumber: dcm.SequenceNumber,
		stream:         s,
	}
	if d.partiallyReliable() {
		// The message lifetime starts here. Once it is over the stream is
		// reset, so QUIC stops retransmitting what is left of the message.
		time.AfterFunc(d.rxTime, msg.expire)
	}
	return msg, nil
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

// Close implements io.ReadCloser. It tells the peer that the application
// stopped reading the message.
func (m *DataChannelReadMessage) Close() error {
	return m.cancel(errorCodeMessageAborted)
}

// cancel resets the read side of the message stream with code.
func (m *DataChannelReadMessage) cancel(code uint64) error {
	m.stream.CancelRead(code)
	return nil
}

// Read implements io.ReadCloser.
func (m *DataChannelReadMessage) Read(p []byte) (n int, err error) {
	return m.stream.Read(p)
}

type DataChannelWriteMessage struct {
	SequenceNumber uint64
	stream         SendStream
}

// expire resets the message stream once the message outlived the channel's
// rxTime.
func (m *DataChannelWriteMessage) expire() {
	m.stream.CancelWrite(errorCodeMessageDiscarded)
}

// Close implements io.WriteCloser.
func (m *DataChannelWriteMessage) Close() error {
	return m.stream.Close()
}

// Write implements io.WriteCloser.
func (m *DataChannelWriteMessage) Write(p []byte) (n int, err error) {
	return m.stream.Write(p)
}
