package quicdc

import (
	"fmt"
	"io"
	"time"

	"github.com/quic-go/quic-go/quicvarint"
)

// maxVarIntStringLen bounds the length of a label or protocol string read from
// the wire, so a peer cannot force a large allocation.
const maxVarIntStringLen = 4096

type messageType uint64

const (
	dataChannelOpenMessageType messageType = iota
	dataChannelOpenOkMessageType
	dataChannelMessageType
	dataChannelCloseMessageType
)

type dataChannelType uint64

const (
	dataChannelTypeReliable          dataChannelType = 0x00
	dataChannelTypeReliableUnordered dataChannelType = 0x80

	dataChannelTypePartialReliableTimed          dataChannelType = 0x02
	dataChannelTypePartialReliableTimedUnordered dataChannelType = 0x82
)

func (t dataChannelType) parameters(reliabilityParameter uint64) (ordered bool, rxTime time.Duration, err error) {
	switch t {
	case dataChannelTypeReliable:
		return true, 0, nil
	case dataChannelTypeReliableUnordered:
		return false, 0, nil
	case dataChannelTypePartialReliableTimed:
		return true, time.Duration(reliabilityParameter) * time.Millisecond, nil
	case dataChannelTypePartialReliableTimedUnordered:
		return false, time.Duration(reliabilityParameter) * time.Millisecond, nil
	}
	return false, 0, fmt.Errorf("unknown data channel type: 0x%x", uint64(t))
}

func appendHeader(b []byte, channelID uint64, t messageType) []byte {
	b = quicvarint.Append(b, channelID)
	return quicvarint.Append(b, uint64(t))
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
	return m.appendPayload(appendHeader(b, m.ChannelID, dataChannelOpenMessageType))
}

func (m *dataChannelOpenMessage) appendPayload(b []byte) []byte {
	b = quicvarint.Append(b, uint64(m.ChannelType))
	b = quicvarint.Append(b, m.Priority)
	b = quicvarint.Append(b, m.ReliabilityParameter)
	b = quicvarint.Append(b, uint64(len(m.Label)))
	b = append(b, []byte(m.Label)...)
	b = quicvarint.Append(b, uint64(len(m.Protocol)))
	return append(b, []byte(m.Protocol)...)
}

func (m *dataChannelOpenMessage) parsePayload(r quicvarint.Reader) (err error) {
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
	return appendHeader(b, m.ChannelID, dataChannelOpenOkMessageType)
}

type dataChannelCloseMessage struct {
	ChannelID uint64
}

func (m *dataChannelCloseMessage) append(b []byte) []byte {
	return appendHeader(b, m.ChannelID, dataChannelCloseMessageType)
}

type dataChannelMessage struct {
	ChannelID      uint64
	SequenceNumber uint64
}

func (m *dataChannelMessage) parsePayload(r quicvarint.Reader) (err error) {
	m.SequenceNumber, err = quicvarint.Read(r)
	return err
}

func (m *dataChannelMessage) append(b []byte) []byte {
	return m.appendPayload(appendHeader(b, m.ChannelID, dataChannelMessageType))
}

func (m *dataChannelMessage) appendPayload(b []byte) []byte {
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
	if l > maxVarIntStringLen {
		return "", fmt.Errorf("string length %v exceeds maximum of %v", l, maxVarIntStringLen)
	}
	val := make([]byte, l)
	if _, err := io.ReadFull(r, val); err != nil {
		return "", err
	}
	return string(val), nil
}
