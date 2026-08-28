package quicdc

import (
	"bytes"
	"io"
	"testing"

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
