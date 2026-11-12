package storage

import "context"

// Fake is a Store whose methods call the matching Fn field; a nil field returns
// zero values.
type Fake struct {
	GetRangeFn    func(key string, offset, length int64) ([]byte, error)
	GetSuffixFn   func(key string, n int64) ([]byte, error)
	PutFn         func(key string, body []byte) error
	PutIfAbsentFn func(key string, body []byte) error
	ListFn        func(prefix, after string) ([]string, error)
}

var _ Store = Fake{}

// Discard is a Store that drops every object, for a collector that has no object
// store to write to.
var Discard Store = Fake{}

func (f Fake) GetRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
	if f.GetRangeFn == nil {
		return nil, nil
	}
	return f.GetRangeFn(key, offset, length)
}

func (f Fake) GetSuffix(_ context.Context, key string, n int64) ([]byte, error) {
	if f.GetSuffixFn == nil {
		return nil, nil
	}
	return f.GetSuffixFn(key, n)
}

func (f Fake) Put(_ context.Context, key string, body []byte) error {
	if f.PutFn == nil {
		return nil
	}
	return f.PutFn(key, body)
}

func (f Fake) PutIfAbsent(_ context.Context, key string, body []byte) error {
	if f.PutIfAbsentFn == nil {
		return nil
	}
	return f.PutIfAbsentFn(key, body)
}

func (f Fake) List(_ context.Context, prefix, after string) ([]string, error) {
	if f.ListFn == nil {
		return nil, nil
	}
	return f.ListFn(prefix, after)
}
