package quicdc

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/quic-go/quic-go"
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
	header        []byte
	nextSendSeqNr uint64
	nextRecvSeqNr uint64
	reorderBuffer *messageHeap
	recvBuffer    chan *DataChannelReadMessage

	id       uint64
	priority uint64
	ordered  bool
	rxTime   time.Duration
	label    string
	protocol string
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
		header:        []byte{},
		nextSendSeqNr: 0,
		nextRecvSeqNr: 0,
		reorderBuffer: &messageHeap{},
		recvBuffer:    make(chan *DataChannelReadMessage),
		id:            id,
		priority:      priority,
		ordered:       ordered,
		rxTime:        rxTime,
		label:         label,
		protocol:      protocol,
	}
}

func (d *DataChannel) open() error {
	d.header = make([]byte, 0, 64_000)
	s, err := d.connection.OpenUniStream()
	if err != nil {
		return err
	}

	// if ps, ok := s.(prioritySetter); ok {
	// 	ps.SetPriority(uint32(d.priority))
	// 	ps.SetIncremental(false)
	// }
	dcom := dataChannelOpenMessage{
		ChannelID:            d.id,
		ChannelType:          getChannelType(d.ordered, d.rxTime),
		Priority:             d.priority,
		ReliabilityParameter: uint64(d.rxTime.Milliseconds()),
		Label:                d.label,
		Protocol:             d.protocol,
	}
	buf := dcom.append(d.header)
	if _, err = s.Write(buf); err != nil {
		return err
	}
	return s.Close()
}

func (d *DataChannel) pushMessage(ctx context.Context, msg *DataChannelReadMessage) {
	log.Printf("pushing message %v, recvBufferLen: %v", msg.SequenceNumber, len(d.recvBuffer))
	log.Printf("pushed message %v, recvBufferLen: %v", msg.SequenceNumber, len(d.recvBuffer))
	select {
	case d.recvBuffer <- msg:
	case <-ctx.Done():
	}
}

func (d *DataChannel) drainReorderBuffer(ctx context.Context) {
	log.Printf("reorder buffer: %v", d.reorderBuffer)
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

func (d *DataChannel) handleIncomingMessageStream(ctx context.Context, s *quic.ReceiveStream) error {
	m := dataChannelMessage{}
	if err := m.parse(quicvarint.NewReader(s)); err != nil {
		return err
	}
	log.Printf("handling incoming message for channel ID: %v, sequence number: %v, streamID: %v", d.id, m.SequenceNumber, s.StreamID())
	rm := &DataChannelReadMessage{
		SequenceNumber: m.SequenceNumber,
		stream:         s,
	}
	if d.ordered {
		d.reorderBuffer.enqueue(rm)
		d.drainReorderBuffer(ctx)
	} else {
		d.pushMessage(ctx, rm)
	}
	return nil
}

func (d *DataChannel) SendMessage(ctx context.Context) (*DataChannelWriteMessage, error) {
	s, err := d.connection.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	dcm := dataChannelMessage{
		ChannelID:      d.id,
		SequenceNumber: d.nextSendSeqNr,
	}
	d.nextSendSeqNr++
	_, err = s.Write(dcm.append(d.header))
	if err != nil {
		return nil, err
	}
	return &DataChannelWriteMessage{
		SequenceNumber: dcm.SequenceNumber,
		stream:         s,
	}, nil
}

func (d *DataChannel) ReceiveMessage(ctx context.Context) (*DataChannelReadMessage, error) {
	log.Printf("ReceiveMessage, recvBufferLen: %v", len(d.recvBuffer))
	defer log.Printf("ReceiveMessage done")
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
	stream         *quic.ReceiveStream
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
