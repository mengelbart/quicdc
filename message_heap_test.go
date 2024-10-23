package quicdc

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageHeap(t *testing.T) {
	t.Run("peek", func(t *testing.T) {
		mh := &messageHeap{
			&DataChannelReadMessage{SequenceNumber: 0},
			&DataChannelReadMessage{SequenceNumber: 1},
			&DataChannelReadMessage{SequenceNumber: 2},
			&DataChannelReadMessage{SequenceNumber: 3},
		}
		heap.Init(mh)
		assert.Equal(t, &DataChannelReadMessage{SequenceNumber: 0}, mh.peek())
	})

	t.Run("pop", func(t *testing.T) {
		mh := &messageHeap{
			&DataChannelReadMessage{SequenceNumber: 0},
			&DataChannelReadMessage{SequenceNumber: 1},
			&DataChannelReadMessage{SequenceNumber: 2},
			&DataChannelReadMessage{SequenceNumber: 3},
		}
		heap.Init(mh)
		assert.Equal(t, &DataChannelReadMessage{SequenceNumber: 0}, heap.Pop(mh))
	})
}
