package bigbitvector

import (
	"bytes"
	"os"
)

func byteAndMask(index uint64) (uint64, byte) {
	i := index / 8
	j := index % 8
	return i, byte(1) << j
}

func removeFile(file File) error {
	type namer interface{ Name() string }

	name := file.(namer).Name()
	if err := os.Remove(name); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

func debugImpl(ba BigBitVector) string {
	var buf bytes.Buffer
	buf.WriteByte('[')
	ForEach(ba, func(_ uint64, bit bool) error {
		b := byte('0')
		if bit {
			b = '1'
		}
		buf.WriteByte(b)
		return nil
	})
	buf.WriteByte(']')
	return buf.String()
}

func copyFromImpl(dst, src BigBitVector) error {
	srcIter := src.Iterate(0, src.Len())
	needCloseSrc := true
	defer func() {
		if needCloseSrc {
			srcIter.Close()
		}
	}()

	dstIter := dst.Iterate(0, dst.Len())
	needCloseDst := true
	defer func() {
		if needCloseDst {
			dstIter.Close()
		}
	}()

	for srcIter.Next() && dstIter.Next() {
		dstIter.SetBit(srcIter.Bit())
	}

	needCloseDst = false
	err := dstIter.Close()
	if err != nil {
		return err
	}

	needCloseSrc = false
	return srcIter.Close()
}
