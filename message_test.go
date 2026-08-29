package quicdc

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go/quicvarint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// oneByteReader returns at most one byte per Read call.
type oneByteReader struct {
	r io.Reader
}

func (r *oneByteReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return r.r.Read(p[:1])
}

func (r *oneByteReader) ReadByte() (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r.r, b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}

func appendVarIntString(b []byte, s string) []byte {
	b = quicvarint.Append(b, uint64(len(s)))
	return append(b, []byte(s)...)
}

func TestParseVarIntString(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		s, err := parseVarIntString(quicvarint.NewReader(bytes.NewReader(appendVarIntString(nil, ""))))
		require.NoError(t, err)
		assert.Equal(t, "", s)
	})

	t.Run("short reads", func(t *testing.T) {
		buf := appendVarIntString(nil, "some-long-label-that-spans-reads")
		s, err := parseVarIntString(&oneByteReader{r: bytes.NewReader(buf)})
		require.NoError(t, err)
		assert.Equal(t, "some-long-label-that-spans-reads", s)
	})

	t.Run("truncated", func(t *testing.T) {
		buf := appendVarIntString(nil, "label")
		_, err := parseVarIntString(quicvarint.NewReader(bytes.NewReader(buf[:len(buf)-2])))
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("length too large", func(t *testing.T) {
		buf := quicvarint.Append(nil, maxVarIntStringLen+1)
		_, err := parseVarIntString(quicvarint.NewReader(bytes.NewReader(buf)))
		assert.Error(t, err)
	})
}

func TestChannelTypeRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ordered bool
		rxTime  time.Duration
		want    dataChannelType
	}{
		{"reliable ordered", true, 0, dataChannelTypeReliable},
		{"reliable unordered", false, 0, dataChannelTypeReliableUnordered},
		{"partial reliable ordered", true, 20 * time.Millisecond, dataChannelTypePartialReliableTimed},
		{"partial reliable unordered", false, 20 * time.Millisecond, dataChannelTypePartialReliableTimedUnordered},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ct := getChannelType(tc.ordered, tc.rxTime)
			assert.Equal(t, tc.want, ct)

			ordered, rxTime, err := ct.parameters(uint64(tc.rxTime.Milliseconds()))
			require.NoError(t, err)
			assert.Equal(t, tc.ordered, ordered)
			assert.Equal(t, tc.rxTime, rxTime)
		})
	}
}

func TestParametersUnknownChannelType(t *testing.T) {
	_, _, err := dataChannelType(0x42).parameters(0)
	assert.Error(t, err)
}

func TestDataChannelOpenMessageRoundTrip(t *testing.T) {
	// A label of 64 bytes or more needs a two byte varint length.
	long := strings.Repeat("a", 100)

	for _, tc := range []struct {
		name string
		m    dataChannelOpenMessage
	}{
		{"empty label and protocol", dataChannelOpenMessage{ChannelID: 1}},
		{"short strings", dataChannelOpenMessage{
			ChannelID: 4,
			Label:     "label",
			Protocol:  "protocol",
		}},
		{"multi byte lengths", dataChannelOpenMessage{
			ChannelID: 1 << 20,
			Label:     long,
			Protocol:  long + "b",
		}},
		{"non ascii", dataChannelOpenMessage{
			ChannelID: 2,
			Label:     "kanal-\u00fcber-quic",
			Protocol:  "\U0001f4e1",
		}},
		{"all fields", dataChannelOpenMessage{
			ChannelID:            63,
			ChannelType:          dataChannelTypePartialReliableTimedUnordered,
			Priority:             1 << 30,
			ReliabilityParameter: 5000,
			Label:                "label",
			Protocol:             "protocol",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := quicvarint.NewReader(bytes.NewReader(tc.m.append(nil)))

			channelID, err := quicvarint.Read(r)
			require.NoError(t, err)
			assert.Equal(t, tc.m.ChannelID, channelID)
			mt, err := quicvarint.Read(r)
			require.NoError(t, err)
			assert.Equal(t, uint64(dataChannelOpenMessageType), mt)

			parsed := dataChannelOpenMessage{ChannelID: channelID}
			require.NoError(t, parsed.parsePayload(r))
			assert.Equal(t, tc.m, parsed)
		})
	}
}

func TestDataChannelOpenMessageParsePayloadTruncated(t *testing.T) {
	m := dataChannelOpenMessage{ChannelID: 1, Label: "label", Protocol: "protocol"}
	buf := m.appendPayload(nil)

	for i := range buf {
		parsed := dataChannelOpenMessage{}
		err := parsed.parsePayload(quicvarint.NewReader(bytes.NewReader(buf[:i])))
		assert.Error(t, err, "payload truncated to %v bytes parsed without error", i)
	}
}

func TestDataChannelMessageRoundTrip(t *testing.T) {
	m := dataChannelMessage{ChannelID: 7, SequenceNumber: 1 << 40}
	r := quicvarint.NewReader(bytes.NewReader(m.append(nil)))

	channelID, err := quicvarint.Read(r)
	require.NoError(t, err)
	assert.Equal(t, m.ChannelID, channelID)
	mt, err := quicvarint.Read(r)
	require.NoError(t, err)
	assert.Equal(t, uint64(dataChannelMessageType), mt)

	parsed := dataChannelMessage{ChannelID: channelID}
	require.NoError(t, parsed.parsePayload(r))
	assert.Equal(t, m, parsed)
}
