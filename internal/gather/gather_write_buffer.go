package gather

import (
	"io"
	"sync"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.GetContextLoggerFunc("gather")

// WriteBuffer is a write buffer for content of unknown size that manages
// data in a series of byte slices of uniform size.
type WriteBuffer struct {
	alloc *chunkAllocator
	mu    sync.Mutex
	inner Bytes
}

// Close releases all memory allocated by this buffer.
func (b *WriteBuffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.alloc != nil {
		for _, s := range b.inner.Slices {
			b.alloc.releaseChunk(s)
		}

		b.alloc = nil
	}

	b.inner.invalidate()
}

// CloneContiguous initializes the write buffer with a contiguous (single-slice) copy of the provided
// slices.
func (b *WriteBuffer) CloneContiguous(byt Bytes) []byte {
	contig := b.MakeContiguous(byt.Length())
	output := contig[:0]

	for _, s := range byt.Slices {
		output = append(output, s...)
	}

	return contig
}

// MakeContiguous ensures the write buffer consists of exactly one contiguous single slice of the provided length
// and returns the slice.
func (b *WriteBuffer) MakeContiguous(length int) []byte {
	b.Reset()

	b.mu.Lock()
	defer b.mu.Unlock()

	var v []byte

	if length > contiguousAllocator.chunkSize {
		v = make([]byte, length)
	} else {
		b.alloc = contiguousAllocator
		v = b.allocChunk()[0:length]
	}

	b.inner.Slices = [][]byte{v}

	return v
}

// Reset resets buffer back to empty.
func (b *WriteBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.alloc != nil {
		for _, s := range b.inner.Slices {
			b.alloc.releaseChunk(s)
		}
	}

	b.inner.invalidate()

	b.inner = Bytes{}
}

// Write implements io.Writer for appending to the buffer.
func (b *WriteBuffer) Write(data []byte) (n int, err error) {
	b.Append(data)
	return len(data), nil
}

// AppendSectionTo appends the section of the buffer to the provided slice and returns it.
func (b *WriteBuffer) AppendSectionTo(w io.Writer, offset, size int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.inner.AppendSectionTo(w, offset, size)
}

// Length returns the combined length of all slices.
func (b *WriteBuffer) Length() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.inner.Length()
}

// ToByteSlice appends all bytes to the provided slice and returns it.
func (b *WriteBuffer) ToByteSlice() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.inner.ToByteSlice()
}

// Bytes returns inner gather.Bytes.
func (b *WriteBuffer) Bytes() Bytes {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.inner
}

// Append appends the specified slice of bytes to the buffer.
func (b *WriteBuffer) Append(data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.inner.assertValid()

	if len(b.inner.Slices) == 0 {
		b.inner.sliceBuf[0] = b.allocChunk()
		b.inner.Slices = b.inner.sliceBuf[0:1]
	}

	for len(data) > 0 {
		ndx := len(b.inner.Slices) - 1
		remaining := cap(b.inner.Slices[ndx]) - len(b.inner.Slices[ndx])

		if remaining == 0 {
			b.inner.Slices = append(b.inner.Slices, b.allocChunk())
			ndx = len(b.inner.Slices) - 1
			remaining = cap(b.inner.Slices[ndx]) - len(b.inner.Slices[ndx])
		}

		chunkSize := remaining
		if chunkSize > len(data) {
			chunkSize = len(data)
		}

		b.inner.Slices[ndx] = append(b.inner.Slices[ndx], data[0:chunkSize]...)
		data = data[chunkSize:]
	}
}

func (b *WriteBuffer) allocChunk() []byte {
	if b.alloc == nil {
		b.alloc = defaultAllocator
	}

	return b.alloc.allocChunk()
}

// NewWriteBuffer creates new write buffer.
func NewWriteBuffer() *WriteBuffer {
	return &WriteBuffer{}
}
