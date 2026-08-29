package quicdc

import (
	"container/heap"
	"fmt"
	"sync"
)

type messageHeap struct {
	lock sync.Mutex
	data []*DataChannelReadMessage
}

func (h *messageHeap) String() string {
	h.lock.Lock()
	defer h.lock.Unlock()
	res := ""
	for i, m := range h.data {
		res += fmt.Sprintf("%v", m.SequenceNumber)
		if i < len(h.data)-1 {
			res += ", "
		}
	}
	return res
}

func (h *messageHeap) Len() int {
	return len(h.data)
}

func (h *messageHeap) Less(i, j int) bool {
	return h.data[i].SequenceNumber < h.data[j].SequenceNumber
}

func (h *messageHeap) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *messageHeap) Push(m any) {
	h.data = append(h.data, m.(*DataChannelReadMessage))
}

func (h *messageHeap) Pop() any {
	old := h.data
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	h.data = old[0 : n-1]
	return x
}

func (h *messageHeap) peek() *DataChannelReadMessage {
	h.lock.Lock()
	defer h.lock.Unlock()
	if len(h.data) > 0 {
		return h.data[0]
	}
	return nil
}

// size returns the number of messages in the heap.
func (h *messageHeap) size() int {
	h.lock.Lock()
	defer h.lock.Unlock()
	return len(h.data)
}

// contains reports whether the heap holds a message with sequence number
// seqNr.
func (h *messageHeap) contains(seqNr uint64) bool {
	h.lock.Lock()
	defer h.lock.Unlock()
	for _, m := range h.data {
		if m.SequenceNumber == seqNr {
			return true
		}
	}
	return false
}

func (h *messageHeap) enqueue(msg *DataChannelReadMessage) {
	h.lock.Lock()
	defer h.lock.Unlock()
	heap.Push(h, msg)
}

func (h *messageHeap) dequeue() *DataChannelReadMessage {
	h.lock.Lock()
	defer h.lock.Unlock()
	return heap.Pop(h).(*DataChannelReadMessage)
}
