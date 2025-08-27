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
}
