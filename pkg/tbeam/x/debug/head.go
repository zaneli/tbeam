package debug

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"

	"github.com/zaneli/tbeam/pkg/tbeam"
)

func Head[T any](s beam.Scope, in tbeam.TCollection[T], n int) tbeam.TCollection[T] {
	out := debug.Head(s, in.Unwrap(), n)
	return tbeam.Wrap[T](out)
}
