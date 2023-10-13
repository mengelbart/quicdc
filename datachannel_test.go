package quicdc_test

import (
	"testing"

	"github.com/mengelbart/quicdc"
)

func TestDataChannelNilTransport(t *testing.T) {
	dc := &quicdc.DataChannel{
		ID:       0,
		Priority: 0,
		Ordered:  false,
		RXTime:   0,
		RXCount:  0,
		Label:    "",
		Protocol: "",
	}
}

func TestDataChannel(t *testing.T) {
	dc := &quicdc.DataChannel{}
}
