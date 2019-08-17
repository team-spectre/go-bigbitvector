package bigbitvector

import (
	"fmt"
	"io"
)

type inMemoryArray struct {
	data []byte
	bits uint64
	ro   bool
}

func (bv *inMemoryArray) Frozen() bool {
	return bv.ro
}

func (bv *inMemoryArray) Len() uint64 {
	return bv.bits
}

func (bv *inMemoryArray) BitAt(index uint64) (bool, error) {
	if index >= bv.Len() {
		return false, io.EOF
	}
	b, m := byteAndMask(index)
	return (bv.data[b] & m) != 0, nil
}

func (bv *inMemoryArray) SetBitAt(index uint64, bit bool) error {
	if bv.ro {
		panic("BigBitVector is read-only")
	}
	if index >= bv.Len() {
		return io.EOF
	}
	b, m := byteAndMask(index)
	if bit {
		bv.data[b] |= m
	} else {
		bv.data[b] &= ^m
	}
	return nil
}

func (bv *inMemoryArray) Iterate(i, j uint64) Iterator {
	if i > j {
		panic(fmt.Errorf("inMemoryArray.Iterate: i > j: i=%d j=%d", i, j))
	}
	return &inMemoryIterator{
		bv:   bv,
		base: i,
		num:  (j - i),
	}
}

func (bv *inMemoryArray) ReverseIterate(i, j uint64) Iterator {
	if i > j {
		panic(fmt.Errorf("inMemoryArray.ReverseIterate: i > j: i=%d j=%d", i, j))
	}
	return &inMemoryIterator{
		bv:   bv,
		base: i,
		num:  (j - i),
		down: true,
	}
}

func (bv *inMemoryArray) CopyFrom(src BigBitVector) error {
	if bv.ro {
		panic("BigBitVector is read-only")
	}
	if src.Len() != bv.Len() {
		panic("bit arrays are not equal in size")
	}
	if x, ok := src.(*inMemoryArray); ok {
		copy(bv.data, x.data)
		return nil
	}
	return copyFromImpl(bv, src)
}

func (bv *inMemoryArray) Truncate(n uint64) error {
	if bv.ro {
		panic("BigBitVector is read-only")
	}
	if n > bv.Len() {
		panic("cannot grow a bit array")
	}
	numBytes := (n + 7) / 8
	bv.data = bv.data[0:numBytes]
	bv.bits = n
	return nil
}

func (bv *inMemoryArray) Freeze() error {
	bv.ro = true
	return nil
}

func (bv *inMemoryArray) Flush() error {
	return nil
}

func (bv *inMemoryArray) Close() error {
	return nil
}

func (bv *inMemoryArray) Debug() string {
	return debugImpl(bv)
}

var _ BigBitVector = (*inMemoryArray)(nil)

type inMemoryIterator struct {
	bv     *inMemoryArray
	err    error
	base   uint64
	pos    uint64
	num    uint64
	val    bool
	primed bool
	down   bool
}

func (iter *inMemoryIterator) Err() error { return iter.err }
func (iter *inMemoryIterator) Next() bool { return iter.Skip(1) }

func (iter *inMemoryIterator) Index() uint64 {
	if !iter.primed {
		panic("must call Next() before Index()")
	}
	if iter.pos >= iter.num {
		panic("must not call Index() after Next() returns false")
	}
	if iter.down {
		return iter.base + (iter.num - iter.pos - 1)
	}
	return iter.base + iter.pos
}

func (iter *inMemoryIterator) Bit() bool {
	if !iter.primed {
		panic("must call Next() before Bit()")
	}
	if iter.pos >= iter.num {
		panic("must not call Bit() after Next() returns false")
	}
	return iter.val
}

func (iter *inMemoryIterator) SetBit(bit bool) {
	if !iter.primed {
		panic("must call Next() before SetBit()")
	}
	if iter.pos >= iter.num {
		panic("must not call SetBit() after Next() returns false")
	}
	if iter.err != nil {
		return
	}
	if iter.bv.ro {
		panic("BigBitVector is read-only")
	}
	iter.val = bit
	b, m := byteAndMask(iter.Index())
	ref := &iter.bv.data[b]
	if bit {
		*ref |= m
	} else {
		*ref &= ^m
	}
}

func (iter *inMemoryIterator) Skip(n uint64) bool {
	if iter.pos > iter.num {
		panic(fmt.Sprintf("iter.pos=%d iter.num=%d", iter.pos, iter.num))
	}
	if n == 0 && !iter.primed {
		panic("must call Next() before Skip(0)")
	}
	if iter.err != nil {
		return false
	}
	if !iter.primed {
		n--
		iter.primed = true
	}
	if n >= (iter.num - iter.pos) {
		iter.pos = iter.num
		iter.val = false
		return false
	}
	iter.pos += n
	b, m := byteAndMask(iter.Index())
	iter.val = (iter.bv.data[b] & m) != 0
	return true
}

func (iter *inMemoryIterator) Flush() error {
	return nil
}

func (iter *inMemoryIterator) Close() error {
	err := iter.err
	*iter = inMemoryIterator{err: ErrClosedIterator}
	return err
}

var _ Iterator = (*inMemoryIterator)(nil)
