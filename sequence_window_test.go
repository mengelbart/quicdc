package quicdc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSequenceWindow(t *testing.T) {
	// record asserts that seqNr is accepted.
	record := func(t *testing.T, w *sequenceWindow, seqNr uint64) {
		t.Helper()
		repeat, tooOld := w.record(seqNr)
		assert.False(t, repeat, "sequence number %v reported as a repeat", seqNr)
		assert.False(t, tooOld, "sequence number %v reported as behind the window", seqNr)
	}

	t.Run("in order", func(t *testing.T) {
		w := newSequenceWindow(4)
		// Sequence numbers a multiple of the window size apart share a slot,
		// so this also covers the ring wrapping.
		for seqNr := uint64(0); seqNr < 100; seqNr++ {
			record(t, w, seqNr)
		}
	})

	t.Run("out of order", func(t *testing.T) {
		w := newSequenceWindow(4)
		for _, seqNr := range []uint64{2, 0, 3, 1} {
			record(t, w, seqNr)
		}
	})

	t.Run("repeat", func(t *testing.T) {
		w := newSequenceWindow(4)
		record(t, w, 1)
		record(t, w, 3)

		// The highest number seen and one behind it are both still held.
		for _, seqNr := range []uint64{3, 1} {
			repeat, tooOld := w.record(seqNr)
			assert.True(t, repeat)
			assert.False(t, tooOld)
		}
	})

	t.Run("behind the window", func(t *testing.T) {
		w := newSequenceWindow(4)
		record(t, w, 0)
		record(t, w, 4)

		repeat, tooOld := w.record(0)
		assert.False(t, repeat)
		assert.True(t, tooOld)

		// One later is the oldest number the window still holds.
		record(t, w, 1)
	})

	t.Run("forward jump clears skipped slots", func(t *testing.T) {
		w := newSequenceWindow(4)
		record(t, w, 2)
		record(t, w, 5)

		// 2 slid out of the window, and 3 and 4 were never received, so its
		// slot must not report them as repeats.
		record(t, w, 4)
		record(t, w, 3)
	})

	t.Run("forward jump past the whole window", func(t *testing.T) {
		w := newSequenceWindow(4)
		record(t, w, 0)
		record(t, w, 100)

		record(t, w, 97)
		repeat, tooOld := w.record(96)
		assert.False(t, repeat)
		assert.True(t, tooOld)
	})
}
