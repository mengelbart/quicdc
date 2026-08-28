package quicdc

import (
	"context"
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
	errorCodeUnknownFlowID = 0x01
)

type DataChannel struct {
	connection    Connection
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
}

func newDataChannel(
	conn Connection,
	id uint64,
	priority uint64,
	ordered bool,
	rxTime time.Duration,
	label string,
	protocol string,
) *DataChannel {
	return &DataChannel{
		connection:    conn,
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
	}
}

func (d *DataChannel) open() error {
	s, err := d.connection.OpenUniStream()
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
	<-d.ackChan

	return nil
}

// handleAck informs the open() goroutine that the open ack message has been
// received. Repeated calls are no-ops.
func (d *DataChannel) handleAck() {
	d.ackOnce.Do(func() {
		close(d.ackChan)
	})
}

func (d *DataChannel) pushMessage(ctx context.Context, msg *DataChannelReadMessage) {
	select {
	case d.recvBuffer <- msg:
	case <-ctx.Done():
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
// is safe for concurrent use.
func (d *DataChannel) SendMessage(ctx context.Context) (*DataChannelWriteMessage, error) {
	s, err := d.connection.OpenUniStreamSync(ctx)
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
