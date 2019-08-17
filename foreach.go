package bigbitvector

// ForEach is a convenience function that iterates over the entire vector in
// the forward direction.
func ForEach(ba BigBitVector, fn func(uint64, bool) error) error {
	iter := ba.Iterate(0, ba.Len())
	for iter.Next() {
		err := fn(iter.Index(), iter.Bit())
		if err != nil {
			iter.Close()
			return err
		}
	}
	return iter.Close()
}

// ReverseForEach is a convenience function that iterates over the entire
// vector in the reverse direction.
func ReverseForEach(ba BigBitVector, fn func(uint64, bool) error) error {
	iter := ba.ReverseIterate(0, ba.Len())
	for iter.Next() {
		err := fn(iter.Index(), iter.Bit())
		if err != nil {
			iter.Close()
			return err
		}
	}
	return iter.Close()
}
