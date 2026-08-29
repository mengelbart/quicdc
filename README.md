# quicdc

WebRTC-style data channels over QUIC streams. Channels are negotiated with
`DATA_CHANNEL_OPEN`/`DATA_CHANNEL_OPEN_ACK` messages modelled on RFC 8832, plus
a `DATA_CHANNEL_CLOSE` message. Every message is sent on its own QUIC stream,
so messages of different channels never head-of-line block each other.

## Channel types

| Type | `ordered` | `rxTime` |
| --- | --- | --- |
| reliable | true | 0 |
| reliable unordered | false | 0 |
| partially reliable, timed | true | > 0 |
| partially reliable, timed unordered | false | > 0 |

On a partially reliable channel the sender resets a message stream once
`rxTime` has elapsed, and an ordered receiver skips a message that has not
arrived `rxTime` after the gap appeared.

Not implemented: the retransmission-limited channel type
(`PARTIAL_RELIABLE_REXMIT`), which QUIC has no equivalent for.

## Usage

`quicdcquic` adapts a `*quic.Conn` from
[quic-go](https://github.com/quic-go/quic-go) to the `Connection` interface the
session needs. Stream priorities are forwarded if the QUIC stack supports them.

```go
session := quicdc.NewSession(quicdcquic.NewConnection(conn))
go session.Run(ctx)
defer session.Close()

// reliable ordered channel with ID 0 and priority 0
dc, err := session.OpenDataChannel(ctx, 0, 0, true, 0, "chat", "example")

msg, err := dc.SendMessage(ctx)
msg.Write([]byte("hello"))
msg.Close()
```

The peer receives it through the handler it registered before starting `Run`:

```go
session.OnIncomingDataChannel(func(dc *quicdc.DataChannel) {
	msg, err := dc.ReceiveMessage(ctx)
	body, err := io.ReadAll(msg)
	msg.Close()
})
```

See `example_test.go` for the full path, from a QUIC connection to an open data
channel.
