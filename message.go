package quicdc

import (
	"io"
	"time"

	"github.com/quic-go/quic-go/quicvarint"
)

type messageType uint64

const (
	dataChannelOpenMessageType messageType = iota
	dataChannelOpenOkMessageType
	dataChannelMessageType
)

type dataChannelType uint64

const (
	dataChannelTypeReliable          dataChannelType = 0x00
	dataChannelTypeReliableUnordered dataChannelType = 0x80

	dataChannelTypePartialReliableTimed          dataChannelType = 0x02
	dataChannelTypePartialReliableTimedUnordered dataChannelType = 0x82
)

func (t dataChannelType) parameters(reliabilityParameter uint64) (ordered bool, rxTime time.Duration) {
	switch t {
	case dataChannelTypeReliable:
		return true, 0
	case dataChannelTypeReliableUnordered:
		return false, 0
	case dataChannelTypePartialReliableTimed:
		return true, time.Duration(reliabilityParameter) * time.Millisecond
	case dataChannelTypePartialReliableTimedUnordered:
		return false, time.Duration(reliabilityParameter) * time.Millisecond
	}
	return true, 0
}

type dataChannelOpenMessage struct {
	ChannelID            uint64
	ChannelType          dataChannelType
	Priority             uint64
	ReliabilityParameter uint64
	Label                string
	Protocol             string
}

func (m *dataChannelOpenMessage) append(b []byte) []byte {
	b = quicvarint.Append(b, m.ChannelID)
	b = quicvarint.Append(b, uint64(dataChannelOpenMessageType))
	b = quicvarint.Append(b, uint64(m.ChannelType))
	b = quicvarint.Append(b, m.Priority)
	b = quicvarint.Append(b, m.ReliabilityParameter)
	b = quicvarint.Append(b, uint64(len(m.Label)))
	b = append(b, []byte(m.Label)...)
	b = quicvarint.Append(b, uint64(len(m.Protocol)))
	return append(b, []byte(m.Protocol)...)
}

func (m *dataChannelOpenMessage) parse(r quicvarint.Reader) (err error) {
	channelType, err := quicvarint.Read(r)
	if err != nil {
		return err
	}
	m.ChannelType = dataChannelType(channelType)

	m.Priority, err = quicvarint.Read(r)
	if err != nil {
		return err
	}
	m.ReliabilityParameter, err = quicvarint.Read(r)
	if err != nil {
		return err
	}
	m.Label, err = parseVarIntString(r)
	if err != nil {
		return err
	}
	m.Protocol, err = parseVarIntString(r)
	if err != nil {
		return err
	}
	return nil
}

type dataChannelOpenOkMessage struct {
	ChannelID uint64
}

func (m *dataChannelOpenOkMessage) append(b []byte) []byte {
	b = quicvarint.Append(b, m.ChannelID)
	return quicvarint.Append(b, uint64(dataChannelOpenOkMessageType))
}

type dataChannelMessage struct {
	ChannelID      uint64
	SequenceNumber uint64
}

func (m *dataChannelMessage) parse(r quicvarint.Reader) (err error) {
	m.SequenceNumber, err = quicvarint.Read(r)
	return err
}

func (m *dataChannelMessage) append(b []byte) []byte {
	b = quicvarint.Append(b, m.ChannelID)
	b = quicvarint.Append(b, uint64(dataChannelMessageType))
	return quicvarint.Append(b, m.SequenceNumber)
}

func parseVarIntString(r quicvarint.Reader) (string, error) {
	l, err := quicvarint.Read(r)
	if err != nil {
		return "", err
	}
	if l == 0 {
		return "", nil
	}
	val := make([]byte, l)
	var m int
	m, err = r.Read(val)
	if err != nil {
		return "", err
	}
	if uint64(m) != l {
		return "", io.EOF
	}
	return string(val), nil
}
