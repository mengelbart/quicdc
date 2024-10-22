package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"time"

	"github.com/mengelbart/quicdc"
	"github.com/quic-go/quic-go"
)

func main() {
	server := flag.Bool("server", false, "run as server")
	flag.Parse()

	if *server {
		if err := runServer(); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := runClient(); err != nil {
		log.Fatal(err)
	}
}

func runServer() error {
	ctx := context.Background()

	l, err := quic.ListenAddr("127.0.0.1:1909", generateTLSConfig(), nil)
	if err != nil {
		return err
	}
	conn, err := l.Accept(ctx)
	if err != nil {
		return err
	}
	pc := quicdc.NewSession(conn)
	dc, err := pc.Accept(ctx)
	if err != nil {
		return err
	}
	return handleMessage(ctx, dc)
}

func handleMessage(ctx context.Context, dc *quicdc.DataChannel) error {
	for {
		m, err := dc.ReceiveMessage(ctx)
		if err != nil {
			return err
		}
		msg, err := io.ReadAll(m)
		if err != nil {
			return err
		}
		log.Printf("got message: %v", string(msg))
	}
}

func runClient() error {
	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"moq-00"},
	}
	c, err := quic.DialAddr(context.Background(), "localhost:1909", tlsConf, nil)
	if err != nil {
		return err
	}
	pc := quicdc.NewSession(c)
	dc, err := pc.NewDataChannel(0, 0, false, 0, 0, "test", "proto")
	if err != nil {
		return err
	}
	for start := time.Now(); time.Since(start) < 10*time.Second; {
		time.Sleep(time.Second)
		log.Println("sending message")
		msg, err := dc.SendMessage()
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(msg, "%v: hello world", time.Now())
		if err != nil {
			return err
		}
		if err = msg.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Setup a bare-bones TLS config for the server
func generateTLSConfig() *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"moq-00"},
	}
}
