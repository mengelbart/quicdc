package quicdc

// sequenceWindow tracks which sequence numbers an unordered data channel
// received, so a repeat can be detected without buffering messages. It
// remembers the numbers within size of the highest number seen, in a ring
// indexed by sequence number.
type sequenceWindow struct {
	seen []bool
	// high is the highest sequence number recorded so far, valid once
	// started is true.
	high    uint64
	started bool
}

func newSequenceWindow(size int) *sequenceWindow {
	return &sequenceWindow{seen: make([]bool, size)}
}

// record marks seqNr as received. It reports whether seqNr repeats a number
// the window still holds, and whether it fell behind the window entirely, in
// which case the window cannot tell.
func (w *sequenceWindow) record(seqNr uint64) (repeat, tooOld bool) {
	size := uint64(len(w.seen))
	if !w.started {
		w.started = true
		w.high = seqNr
		w.seen[seqNr%size] = true
		return false, false
	}
	switch {
	case seqNr > w.high:
		if seqNr-w.high >= size {
			// The window slid past everything it held.
			clear(w.seen)
		} else {
			for n := w.high + 1; n < seqNr; n++ {
				w.seen[n%size] = false
			}
		}
		w.high = seqNr
	case w.high-seqNr >= size:
		return false, true
	case w.seen[seqNr%size]:
		return true, false
	}
	w.seen[seqNr%size] = true
	return false, false
}
