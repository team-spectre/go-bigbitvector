package bigbitvector

import (
	"fmt"
	"io"
	"sync"
)

const (
	defaultPageSize        = 16384     // 16 KiB
	defaultOnDiskThreshold = 268435456 // 256 MiB
)

type options struct {
	numValues          uint64
	diskThreshold      uint64
	backingFile        File
	bufferPool         *sync.Pool
	pageSize           uint
	diskThresholdIsSet bool
	isReadOnly         bool
}

func (o *options) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

func (o *options) populate() {
	if !o.diskThresholdIsSet {
		o.diskThreshold = defaultOnDiskThreshold
	}

	if o.pageSize == 0 {
		o.pageSize = defaultPageSize
	}
}

func (o options) debugString() string {
	hasFile := (o.backingFile != nil)
	hasPool := (o.bufferPool != nil)
	return fmt.Sprintf(
		"{num:%d odt:%d odtset:%v psz:%d file:%v pool:%v ro:%v}",
		o.numValues,
		o.diskThreshold,
		o.diskThresholdIsSet,
		o.pageSize,
		hasFile,
		hasPool,
		o.isReadOnly)
}

// Option is a behavior customization for New.
type Option func(*options)

// NumValues specifies the length of the array to create.
//
// NumValues must be specified for all arrays.
//
func NumValues(size uint64) Option {
	return func(o *options) { o.numValues = size }
}

// OnDiskThreshold specifies the maximum memory usage (bytes) for an in-memory
// BigArray.  Arrays larger than this will be backed automatically by a
// temporary file.  The default is 256 MiB.
//
func OnDiskThreshold(size uint64) Option {
	return func(o *options) {
		o.diskThreshold = size
		o.diskThresholdIsSet = true
	}
}

// PageSize specifies the page size for disk I/O.  The created array's
// Iterators will load data from disk in blocks of this size.
//
// Must be divisible by 8 and should be at least 4096, or 0 to use the default
// (which is 16 KiB).
//
func PageSize(size uint) Option {
	return func(o *options) { o.pageSize = size }
}

// WithPool specifies a buffer pool to use for disk I/O.  The pool must contain
// []byte slices with a capacity at least as large as the value for PageSize.
//
func WithPool(pool *sync.Pool) Option {
	return func(o *options) { o.bufferPool = pool }
}

// WithFile specifies the read-write file handle which will back the array.
func WithFile(file File) Option {
	return func(p *options) { p.backingFile = file }
}

// WithReadOnlyFile specifies the read-only file handle which will back the array.
func WithReadOnlyFile(file io.ReaderAt) Option {
	return func(p *options) {
		p.backingFile = wrappedReaderAt{file}
		p.isReadOnly = true
	}
}
