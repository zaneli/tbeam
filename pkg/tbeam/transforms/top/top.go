package top

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/top"

	"github.com/zaneli/tbeam/pkg/tbeam"
)

func Largest[T comparable](s beam.Scope, in tbeam.TCollection[T], n int, less func(a T, b T) bool) tbeam.TCollection[T] {
	out := top.Largest(s, in.Unwrap(), n, less)
	return tbeam.Wrap[T](out)
}
