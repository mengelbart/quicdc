package quicdc_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"log"
	"math/big"
	"time"

	"github.com/mengelbart/quicdc"
	"github.com/mengelbart/quicdc/quicdcquic"
	"github.com/quic-go/quic-go"
)

const alpn = "quicdc-example"

// Example opens a reliable ordered data channel over a QUIC connection, sends
// a message on it and receives the message on the other side.
func Example() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLSConfig(), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	received := make(chan string, 1)
	go func() {
		if err := receive(ctx, listener, received); err != nil {
			log.Print(err)
		}
	}()

	conn, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLSConfig(), nil)
	if err != nil {
		log.Fatal(err)
	}
	session := quicdc.NewSession(quicdcquic.NewConnection(conn))
	go func() { _ = session.Run(ctx) }()
	defer func() { _ = session.Close() }()

	// channel ID 0, priority 0, ordered, no rxTime, so a reliable ordered
	// channel. A non-zero rxTime makes the channel partially reliable: a
	// message that cannot be delivered within that time is dropped.
	dc, err := session.OpenDataChannel(ctx, 0, 0, true, 0, "chat", "example")
	if err != nil {
		log.Fatal(err)
	}

	msg, err := dc.SendMessage(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := msg.Write([]byte("hello")); err != nil {
		log.Fatal(err)
	}
	if err := msg.Close(); err != nil {
		log.Fatal(err)
	}

	fmt.Println(<-received)

	if err := dc.Close(); err != nil {
		log.Fatal(err)
	}
	// Output: channel 0: hello
}

// receive accepts one connection, waits for the peer to open a data channel
// and reads one message from it.
func receive(ctx context.Context, listener *quic.Listener, received chan<- string) error {
	conn, err := listener.Accept(ctx)
	if err != nil {
		return err
	}
	session := quicdc.NewSession(quicdcquic.NewConnection(conn))

	channels := make(chan *quicdc.DataChannel, 1)
	session.OnIncomingDataChannel(func(dc *quicdc.DataChannel) {
		channels <- dc
	})
	go func() { _ = session.Run(ctx) }()
	defer func() { _ = session.Close() }()

	dc := <-channels
	msg, err := dc.ReceiveMessage(ctx)
	if err != nil {
		return err
	}
	body, err := io.ReadAll(msg)
	if err != nil {
		return err
	}
	if err := msg.Close(); err != nil {
		return err
	}
	received <- fmt.Sprintf("channel %v: %s", dc.ID(), body)
	return nil
}

func serverTLSConfig() *tls.Config {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "quicdc example"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		log.Fatal(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		NextProtos:   []string{alpn},
	}
}

func clientTLSConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{alpn},
	}
}
