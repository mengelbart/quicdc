package quicdc

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

const (
	errCodeDropped = 0x01
)

type transport interface {
	quic.Connection
}

type PeerConnection struct {
	t        transport
	acceptCh chan quic.ReceiveStream
	channels map[uint64]*DataChannel
	lock     sync.Mutex
}

func NewPeerConnection(t transport) *PeerConnection {
	pc := &PeerConnection{
		t:        t,
		acceptCh: make(chan quic.ReceiveStream),
		channels: map[uint64]*DataChannel{},
		lock:     sync.Mutex{},
	}
	go pc.read()
	return pc
}

func (c *PeerConnection) NewDataChannel(id uint64, priority uint64, ordered bool, rxTime time.Duration, rxCount uint64, label string, protocol string) (*DataChannel, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.channels[id]; ok {
		return nil, errors.New("datachannel with ID already exists")
	}

	dc := &DataChannel{
		t:             c.t,
		header:        []byte{},
		sendSeqNr:     0,
		recvSeqNr:     0,
		streamBuffer:  make(chan quic.ReceiveStream),
		reorderBuffer: map[uint64]*DataChannelReadMessage{},
		readMsgbuffer: make(chan *DataChannelReadMessage),
		ID:            id,
		Priority:      priority,
		Ordered:       ordered,
		RXTime:        rxTime,
		RXCount:       rxCount,
		Label:         label,
		Protocol:      protocol,
	}
	if err := dc.open(); err != nil {
		return nil, err
	}
	c.channels[dc.ID] = dc
	return dc, nil
}

type DataChannel struct {
	t             transport
	header        []byte
	sendSeqNr     uint64
	recvSeqNr     uint64
	streamBuffer  chan quic.ReceiveStream
	reorderBuffer map[uint64]*DataChannelReadMessage
	readMsgbuffer chan *DataChannelReadMessage

	ID       uint64
	Priority uint64
	Ordered  bool
	RXTime   time.Duration
	RXCount  uint64
	Label    string
	Protocol string
}

func (d *DataChannel) read() {
	for s := range d.streamBuffer {
		d.handleIncomingMessageStream(s)
	}
}

func (d *DataChannel) handleIncomingMessageStream(s quic.ReceiveStream) {
	m := dataChannelMessage{}
	if err := m.parse(quicvarint.NewReader(s)); err != nil {
		// TODO: Close channel?
		panic(err)
	}
	log.Printf("handling incoming message: %v", m)
	if !d.Ordered || m.SequenceNumber == d.recvSeqNr {
		d.readMsgbuffer <- &DataChannelReadMessage{
			s: s,
		}
		d.recvSeqNr++
		for {
			next, ok := d.reorderBuffer[d.recvSeqNr]
			if !ok {
				break
			}
			d.readMsgbuffer <- next
			d.recvSeqNr++
		}
		return
	}
	if _, ok := d.reorderBuffer[m.SequenceNumber]; ok {
		panic("TODO: got duplicate message sequence number?")
	}
	d.reorderBuffer[m.SequenceNumber] = &DataChannelReadMessage{
		s: s,
	}
}

func (d *DataChannel) open() error {
	d.header = make([]byte, 0, 64_000)
	s, err := d.t.OpenUniStream()
	if err != nil {
		return err
	}

	ct, err := getChannelType(d.Ordered, d.RXTime, d.RXCount)
	if err != nil {
		return err
	}
	dcom := dataChannelOpenMessage{
		ChannelID:            d.ID,
		ChannelType:          ct,
		Priority:             d.Priority,
		ReliabilityParameter: getReliabilityParameter(d.RXTime, d.RXCount),
		Label:                d.Label,
		Protocol:             d.Protocol,
	}
	go d.read()
	buf := dcom.append(d.header)
	_, err = s.Write(buf)
	return err
}

func (c *PeerConnection) Accept(ctx context.Context) (*DataChannel, error) {
	var s quic.ReceiveStream
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case s = <-c.acceptCh:
	}
	m := dataChannelOpenMessage{}
	if err := m.parse(quicvarint.NewReader(s)); err != nil {
		// TODO: Close channel?
		return nil, err
	}
	o, rt, rc := m.ChannelType.parameters(m.ReliabilityParameter)
	d := &DataChannel{
		t:             c.t,
		header:        make([]byte, 0, 4096),
		sendSeqNr:     0,
		recvSeqNr:     0,
		streamBuffer:  make(chan quic.ReceiveStream),
		reorderBuffer: map[uint64]*DataChannelReadMessage{},
		readMsgbuffer: make(chan *DataChannelReadMessage),
		ID:            m.ChannelID,
		Priority:      m.Priority,
		Ordered:       o,
		RXTime:        rt,
		RXCount:       rc,
		Label:         m.Label,
		Protocol:      m.Protocol,
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.channels[d.ID]; ok {
		// TODO: Close conn?
		return nil, errors.New("datachannel with ID already exists")
	}
	c.channels[d.ID] = d
	go d.read()

	return d, nil
}

func (c *PeerConnection) read() {
	for {
		s, err := c.t.AcceptUniStream(context.Background())
		if err != nil {
			panic(err)
		}
		r := quicvarint.NewReader(s)
		id, err := quicvarint.Read(r)
		if err != nil {
			panic(err)
		}
		mt, err := quicvarint.Read(r)
		if err != nil {
			panic(err)
		}
		switch mt {
		case uint64(dataChannelOpenMessageType):
			select {
			case c.acceptCh <- s:
			case <-time.After(time.Second):
				log.Println("dropping incoming data channel")
				s.CancelRead(errCodeDropped)
			}
		case uint64(dataChannelOpenOkMessageType):
			panic("TODO")
		case uint64(dataChannelMessageType):
			c.lock.Lock()
			dc, ok := c.channels[id]
			c.lock.Unlock()
			if !ok {
				panic("TODO: GOT MESSAGE FOR UNKNOWN CHANNEL, NEED TO BUFFER?")
			}
			dc.streamBuffer <- s
		}
	}
}

type DataChannelWriteMessage struct {
	s io.WriteCloser
}

// Close implements io.WriteCloser.
func (m *DataChannelWriteMessage) Close() error {
	return m.s.Close()
}

// Write implements io.WriteCloser.
func (m *DataChannelWriteMessage) Write(p []byte) (n int, err error) {
	return m.s.Write(p)
}

func (d *DataChannel) SendMessage() (io.WriteCloser, error) {
	s, err := d.t.OpenUniStream()
	if err != nil {
		return nil, err
	}
	dcm := dataChannelMessage{
		ChannelID:      d.ID,
		SequenceNumber: d.sendSeqNr,
	}
	d.sendSeqNr++
	_, err = s.Write(dcm.append(d.header))
	if err != nil {
		return nil, err
	}
	return &DataChannelWriteMessage{
		s: s,
	}, nil
}

type DataChannelReadMessage struct {
	s quic.ReceiveStream
}

// Close implements io.ReadCloser.
func (m *DataChannelReadMessage) Close() error {
	m.s.CancelRead(errCodeDropped)
	return nil
}

// Read implements io.ReadCloser.
func (m *DataChannelReadMessage) Read(p []byte) (n int, err error) {
	return m.s.Read(p)
}

func (d *DataChannel) ReceiveMessage(ctx context.Context) (io.ReadCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg := <-d.readMsgbuffer:
		return msg, nil
	}
}

func getChannelType(ordered bool, rxtime time.Duration, rxcount uint64) (dataChannelType, error) {
	if rxtime > 0 && rxcount > 0 {
		return 0, errors.New("invalid partial reliability setting")
	}
	if ordered {
		if rxtime > 0 {
			return dataChannelTypePartialReliableTimed, nil
		}
		if rxcount > 0 {
			return dataChannelTypePartialReliableRexmit, nil
		}
		return dataChannelTypeReliable, nil
	}

	if rxtime > 0 {
		return dataChannelTypePartialReliableTimedUnordered, nil
	}
	if rxcount > 0 {
		return dataChannelTypePartialReliableRexmitUnordered, nil
	}

	return dataChannelTypeReliableUnordered, nil
}

func getReliabilityParameter(rxtime time.Duration, rxcount uint64) uint64 {
	if rxtime > 0 {
		return uint64(rxtime.Milliseconds())
	}
	if rxcount > 0 {
		return rxcount
	}
	return 0
}
