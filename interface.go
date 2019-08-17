package bigbitvector

import (
	"errors"
	"io/ioutil"
)

// ErrClosedIterator is returned when Iterator.Close() is called multiple times
var ErrClosedIterator = errors.New("iterator is already closed")

// BigBitVector provides an interface for dealing with very large bitvectors that
// don't necessarily fit in memory.
type BigBitVector interface {
	// Frozen returns true if this bitvector is read-only.
	Frozen() bool

	// Len returns the number of bits in this bitvector.
	Len() uint64

	// BitAt returns the bit with the given index.
	//
	// On-disk bitvectors have very slow random access.  If your accesses are
	// sequential or roughly sequential, you should consider using an
	// Iterator.
	BitAt(uint64) (bool, error)

	// SetBitAt replaces the bit with the given index.
	//
	// On-disk bitvectors have very slow random access.  If your accesses are
	// sequential or roughly sequential, you should consider using an
	// Iterator.
	SetBitAt(uint64, bool) error

	// Iterate returns an Iterator that starts at index (i) and stops at
	// index (j-1).
	Iterate(uint64, uint64) Iterator

	// ReverseIterate returns an Iterator that starts at index (j-1) and
	// stops at index (i).
	ReverseIterate(uint64, uint64) Iterator

	// CopyFrom replaces this bitvector's bits with the bits from the
	// provided bitvector.  The bitvectors must have the same length.
	CopyFrom(BigBitVector) error

	// Truncate trims the bitvector to the given length.
	Truncate(uint64) error

	// Freeze makes the bitvector read-only.
	Freeze() error

	// Flush ensures that all pending writes have reached the OS.
	Flush() error

	// Close flushes any writes and frees the resources used by the bitvector.
	Close() error

	// Debug generates a human-friendly string representing the bits in
	// the bitvector.
	Debug() string
}

// Iterator provides an interface for fast sequential access to a BigBitVector.
//
// The basic usage pattern is:
//
//   iter := vec.Iterate(i, j)
//   for iter.Next() {
//     ... // call Index(), Bit(), and/or SetBit()
//   }
//   err := iter.Close()
//   if err != nil {
//     ... // handle error
//   }
//
// Iterators are created in an indeterminate state; the caller must invoke
// Next() to advance to the first item.
//
type Iterator interface {
	// Next advances the iterator to the next bit and returns true, or
	// returns false if the end of the iteration has been reached or if an
	// error has occurred.
	Next() bool

	// Skip(n) is equivalent to calling Next() n times, but faster.
	Skip(uint64) bool

	// Index returns the index of the current bit.
	Index() uint64

	// Bit returns the current bit.
	Bit() bool

	// SetBit replaces the current bit.
	SetBit(bool)

	// Err returns the error which caused Next() to return false.
	Err() error

	// Flush ensures that all pending writes have reached the OS.
	Flush() error

	// Close flushes writes and frees the resources used by the iterator.
	Close() error
}

// New constructs a BigBitVector instance.
//
// Constructing a BigBitVector is very similar to constructing a BigArray, except
// that BitVectors ignore BytesPerItem and MaxValue.
//
func New(opts ...Option) (BigBitVector, error) {
	var o options
	o.apply(opts...)
	o.populate()

	numBytes := (o.numValues + 7) / 8
	if o.backingFile == nil && numBytes < o.diskThreshold {
		ba := &inMemoryArray{
			data: make([]byte, numBytes),
			bits: o.numValues,
			ro:   o.isReadOnly,
		}
		return ba, nil
	}

	doc := false
	if o.backingFile == nil {
		var err error
		o.backingFile, err = ioutil.TempFile("", "tmp")
		if err != nil {
			return nil, err
		}
		err = o.backingFile.Truncate(int64(numBytes))
		if err != nil {
			removeFile(o.backingFile)
			return nil, err
		}
		doc = true
	}

	ba := &onDiskArray{
		f:     o.backingFile,
		p:     o.bufferPool,
		cache: make(map[uint64]*cachePage),
		num:   o.numValues,
		psz:   o.pageSize,
		ro:    o.isReadOnly,
		doc:   doc,
	}
	return ba, nil
}
