package quicdc

import "github.com/quic-go/quic-go"

type quicTransport struct {
	conn quic.Connection
}
