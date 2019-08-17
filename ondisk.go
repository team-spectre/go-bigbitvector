package bigbitvector

import (
	"fmt"
	"io"
	"sync"
)

type cachePage struct {
	buf    []byte
	data   []byte
	off    uint64
	refcnt uint32
	dirty  bool
}

type onDiskArray struct {
	f     File
	p     *sync.Pool
	cache map[uint64]*cachePage
	num   uint64
	psz   uint
	ro    bool
	doc   bool
}

func (bv *onDiskArray) Frozen() bool {
	return bv.ro
}

func (bv *onDiskArray) Len() uint64 {
	return bv.num
}

func (bv *onDiskArray) BitAt(index uint64) (bool, error) {
	if index >= bv.Len() {
		return false, io.EOF
	}

	var tmp [1]byte
	b, m := byteAndMask(index)

	_, err := bv.f.ReadAt(tmp[:], int64(b))
	if err != nil {
		return false, err
	}
	bit := (tmp[0] & m) != 0
	return bit, nil
}

func (bv *onDiskArray) SetBitAt(index uint64, bit bool) error {
	if bv.ro {
		panic("BigBitVector is read-only")
	}
	if index >= bv.Len() {
		return io.EOF
	}

	var tmp [1]byte
	b, m := byteAndMask(index)

	_, err := bv.f.ReadAt(tmp[:], int64(b))
	if err != nil {
		return err
	}

	if bit {
		tmp[0] |= m
	} else {
		tmp[0] &= ^m
	}

	_, err = bv.f.WriteAt(tmp[:], int64(b))
	for _, page := range bv.cache {
		end := page.off + uint64(len(page.data))
		if b >= page.off && b < end {
			b -= page.off
			page.data[b] = tmp[0]
		}
	}
	return err
}

func (bv *onDiskArray) Iterate(i, j uint64) Iterator {
	if i > j {
		panic(fmt.Errorf("onDiskArray.Iterate: i > j: i=%d j=%d", i, j))
	}
	return &onDiskIterator{
		bv:  bv,
		pos: i - 1,
		end: j,
	}
}

func (bv *onDiskArray) ReverseIterate(i, j uint64) Iterator {
	if i > j {
		panic(fmt.Errorf("onDiskArray.ReverseIterate: i > j: i=%d j=%d", i, j))
	}
	return &onDiskIterator{
		bv:   bv,
		pos:  j + 1,
		end:  i,
		down: true,
	}
}

func (bv *onDiskArray) CopyFrom(src BigBitVector) error {
	if bv.ro {
		panic("BigBitVector is read-only")
	}
	if src.Len() != bv.Len() {
		panic("bit arrays are not equal in size")
	}
	return copyFromImpl(bv, src)
}

func (bv *onDiskArray) Truncate(length uint64) error {
	if bv.ro {
		panic("BigBitVector is read-only")
	}
	if length > bv.Len() {
		panic("cannot grow a bit array")
	}
	if len(bv.cache) != 0 {
		panic("Truncate() with live iterators is undefined behavior")
	}
	lengthBytes := (length + 7) / 8
	bv.num = length
	return bv.f.Truncate(int64(lengthBytes))
}

func (bv *onDiskArray) Freeze() error {
	bv.ro = true
	return bv.Flush()
}

func (bv *onDiskArray) Flush() error {
	type flusher interface{ Flush() error }

	var finalError error
	for _, page := range bv.cache {
		if err := flushPage(bv, page); err != nil && finalError == nil {
			finalError = err
		}
	}
	if f, ok := bv.f.(flusher); ok {
		if err := f.Flush(); finalError == nil {
			finalError = err
		}
	}
	return finalError
}

func (bv *onDiskArray) Sync() error {
	type syncer interface{ Sync() error }

	if err := bv.Flush(); err != nil {
		return err
	}
	if f, ok := bv.f.(syncer); ok {
		return f.Sync()
	}
	return &NotImplementedError{Op: "Sync"}
}

func (bv *onDiskArray) Close() error {
	needClose := true
	defer func() {
		if needClose && bv.doc {
			removeFile(bv.f)
		} else if needClose {
			bv.f.Close()
		}
	}()

	if len(bv.cache) != 0 {
		panic("BigBitVector.Close called with outstanding iterators")
	}

	if bv.doc {
		needClose = false
		return removeFile(bv.f)
	}

	needClose = false
	return bv.f.Close()
}

func (bv *onDiskArray) Debug() string {
	return debugImpl(bv)
}

func (bv *onDiskArray) acquirePage(off uint64) (*cachePage, error) {
	page, found := bv.cache[off]
	if found {
		page.refcnt++
		return page, nil
	}

	var bb []byte
	if bv.p != nil {
		bb = bv.p.Get().([]byte)
	}

	var b []byte
	if uint(cap(bb)) >= bv.psz {
		b = bb[0:bv.psz]
	} else {
		b = make([]byte, bv.psz)
	}

	n, err := bv.f.ReadAt(b, int64(off))
	if err != nil && err != io.EOF {
		return nil, err
	}
	b = b[0:n]

	page = &cachePage{
		buf:    bb,
		data:   b,
		off:    off,
		refcnt: 1,
		dirty:  false,
	}
	bv.cache[off] = page
	return page, nil
}

func (bv *onDiskArray) disposePage(page *cachePage) {
	if page == nil {
		return
	}
	if page.dirty {
		panic("cannot dispose of a dirty page")
	}
	page.refcnt--
	if page.refcnt > 0 {
		return
	}
	delete(bv.cache, page.off)
	if bv.p != nil && page.buf != nil {
		bv.p.Put(page.buf)
	}
	*page = cachePage{}
}

var _ BigBitVector = (*onDiskArray)(nil)

type onDiskIterator struct {
	bv   *onDiskArray
	page *cachePage
	err  error
	pos  uint64
	end  uint64
	val  bool
	down bool
}

func (iter *onDiskIterator) Err() error { return iter.err }
func (iter *onDiskIterator) Bit() bool  { return iter.val }
func (iter *onDiskIterator) Next() bool { return iter.Skip(1) }

func (iter *onDiskIterator) Index() uint64 {
	index := iter.pos
	if iter.down {
		index--
	}
	return index
}

func (iter *onDiskIterator) Skip(n uint64) bool {
	if iter.err != nil {
		return false
	}

	var index uint64
	if iter.down {
		if iter.down && iter.pos <= (iter.end+n) {
			iter.pos = iter.end
			iter.val = false
			return false
		}
		iter.pos -= n
		index = iter.pos - 1
	} else {
		iter.pos += n
		if iter.pos >= iter.end {
			iter.pos = iter.end
			iter.val = false
			return false
		}
		index = iter.pos
	}

	psz := uint64(iter.bv.psz)
	b, m := byteAndMask(index)
	pageOffset := (b / psz) * psz

	page := iter.page
	if page != nil && page.off != pageOffset {
		err := flushPage(iter.bv, page)
		if err != nil {
			iter.err = err
			iter.val = false
			return false
		}
		iter.bv.disposePage(page)
		iter.page = nil
		page = nil
	}
	if page == nil {
		var err error
		page, err = iter.bv.acquirePage(pageOffset)
		if err != nil {
			iter.err = err
			iter.val = false
			return false
		}
		iter.page = page
	}
	if b < page.off || b > (page.off+uint64(len(page.data))) {
		panic("BUG")
	}

	b -= pageOffset
	iter.val = (page.data[b] & m) != 0
	return true
}

func (iter *onDiskIterator) SetBit(bit bool) {
	if iter.bv.ro {
		panic("BigArray is read-only")
	}
	if iter.err != nil {
		return
	}

	index := iter.Index()
	b, m := byteAndMask(index)
	b -= iter.page.off
	ref := &iter.page.data[b]
	if bit {
		*ref |= m
	} else {
		*ref &= ^m
	}
	iter.page.dirty = true
}

func (iter *onDiskIterator) Flush() error {
	return flushPage(iter.bv, iter.page)
}

func (iter *onDiskIterator) Close() error {
	err := iter.Flush()
	if iter.err != nil {
		err = iter.err
	}
	iter.bv.disposePage(iter.page)
	*iter = onDiskIterator{err: ErrClosedIterator}
	return err
}

var _ Iterator = (*onDiskIterator)(nil)

func flushPage(bv *onDiskArray, page *cachePage) error {
	if page != nil && page.dirty {
		_, err := bv.f.WriteAt(page.data, int64(page.off))
		if err != nil {
			return err
		}
		page.dirty = false
	}
	return nil
}
