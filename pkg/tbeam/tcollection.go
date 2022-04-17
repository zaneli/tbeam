package tbeam

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

type TCollection[T any] struct {
	underlying beam.PCollection
}

func Wrap[T any](underlying beam.PCollection) TCollection[T] {
	return TCollection[T]{
		underlying: underlying,
	}
}

func (t TCollection[T]) Unwrap() beam.PCollection {
	return t.underlying
}
