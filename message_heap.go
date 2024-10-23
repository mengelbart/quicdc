package quicdc

type messageHeap []*DataChannelReadMessage

func (h messageHeap) Len() int {
	return len(h)
}

func (h messageHeap) Less(i, j int) bool {
	return h[i].SequenceNumber < h[j].SequenceNumber
}

func (h messageHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *messageHeap) Push(m any) {
	*h = append(*h, m.(*DataChannelReadMessage))
}

func (h *messageHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x

}

func (h messageHeap) peek() *DataChannelReadMessage {
	if len(h) > 0 {
		return h[0]
	}
	return nil
}
