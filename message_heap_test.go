package quicdc

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageHeap(t *testing.T) {
	t.Run("peek", func(t *testing.T) {
		mh := &messageHeap{
			data: []*DataChannelReadMessage{
				{SequenceNumber: 0},
				{SequenceNumber: 1},
				{SequenceNumber: 2},
				{SequenceNumber: 3},
			},
		}
		heap.Init(mh)
		assert.Equal(t, &DataChannelReadMessage{SequenceNumber: 0}, mh.peek())
	})

	t.Run("pop", func(t *testing.T) {
		mh := &messageHeap{
			data: []*DataChannelReadMessage{
				{SequenceNumber: 0},
				{SequenceNumber: 1},
				{SequenceNumber: 2},
				{SequenceNumber: 3},
			},
		}
		heap.Init(mh)
		assert.Equal(t, &DataChannelReadMessage{SequenceNumber: 0}, heap.Pop(mh))
	})

	t.Run("empty", func(t *testing.T) {
		mh := &messageHeap{}
		assert.Nil(t, mh.peek())
		assert.Equal(t, 0, mh.size())
		assert.False(t, mh.contains(0))
		assert.Equal(t, "", mh.String())
	})

	t.Run("enqueue and dequeue", func(t *testing.T) {
		mh := &messageHeap{}
		for _, seqNr := range []uint64{3, 0, 4, 2, 1} {
			mh.enqueue(&DataChannelReadMessage{SequenceNumber: seqNr})
			assert.True(t, mh.contains(seqNr))
		}
		assert.Equal(t, 5, mh.size())
		assert.False(t, mh.contains(5))

		// Dequeue returns the messages in ascending sequence number order,
		// whatever the insert order was.
		for want := uint64(0); want < 5; want++ {
			assert.Equal(t, want, mh.peek().SequenceNumber)
			msg := mh.dequeue()
			assert.Equal(t, want, msg.SequenceNumber)
			assert.False(t, mh.contains(want))
		}
		assert.Equal(t, 0, mh.size())
		assert.Nil(t, mh.peek())
	})

	t.Run("string", func(t *testing.T) {
		mh := &messageHeap{}
		for _, seqNr := range []uint64{0, 1, 2} {
			mh.enqueue(&DataChannelReadMessage{SequenceNumber: seqNr})
		}
		assert.Equal(t, "0, 1, 2", mh.String())
	})
}
