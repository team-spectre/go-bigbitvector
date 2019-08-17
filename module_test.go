package bigbitvector

import (
	"sync"
	"testing"
)

func RunBitVectorBasicTests(t *testing.T, opts ...Option) {
	t.Helper()
	t.Log("basic tests")

	opts = append(opts,
		PageSize(32),
		NumValues(1024))

	ba, err := New(opts...)
	if err != nil {
		t.Errorf("New: error: %v", err)
		return
	}
	defer ba.Close()

	if 1024 != ba.Len() {
		t.Errorf("BigBitVector.Len: expected 1024, got %d", ba.Len())
	}

	bit, err := ba.BitAt(42)
	if err != nil {
		t.Errorf("BigBitVector.BitAt 42 [1/2]: error: %v", err)
	}
	if bit {
		t.Error("42: expected false, got true")
	}

	err = ba.SetBitAt(42, true)
	if err != nil {
		t.Errorf("BigBitVector.SetBitAt 42: error: %v", err)
	}

	bit, err = ba.BitAt(42)
	if err != nil {
		t.Errorf("BigBitVector.BitAt 42 [2/2]: error: %v", err)
	}
	if !bit {
		t.Error("42: expected true, got false")
	}

	bit, err = ba.BitAt(43)
	if err != nil {
		t.Errorf("BigBitVector.BitAt 43: error: %v", err)
	}
	if bit {
		t.Error("43: expected false, got true")
	}

	bit, err = ba.BitAt(41)
	if err != nil {
		t.Errorf("BigBitVector.BitAt 41: error: %v", err)
	}
	if bit {
		t.Error("41: expected false, got true")
	}

	_, err = ba.BitAt(0)
	if err != nil {
		t.Errorf("BigBitVector.BitAt 0: error: %v", err)
	}

	_, err = ba.BitAt(1023)
	if err != nil {
		t.Errorf("BigBitVector.BitAt 1023: error: %v", err)
	}
}

func TestBitVector_InMemory(t *testing.T) {
	RunBitVectorBasicTests(t,
		PageSize(32))
}

func TestBitVector_OnDisk_NoPool(t *testing.T) {
	RunBitVectorBasicTests(t,
		PageSize(32),
		OnDiskThreshold(0))
}

func TestBitVector_OnDisk_WithPool(t *testing.T) {
	pool := &sync.Pool{
		New: func() interface{} {
			return make([]byte, 32)
		},
	}
	RunBitVectorBasicTests(t,
		PageSize(32),
		OnDiskThreshold(0),
		WithPool(pool))
}
