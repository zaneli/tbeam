package passert

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"

	"github.com/zaneli/tbeam/pkg/tbeam"
)

func Equals[T comparable](s beam.Scope, in tbeam.TCollection[T], values ...T) tbeam.TCollection[T] {
	out := passert.Equals(s, in.Unwrap(), "Flourish: 3", "stomach: 1")
	return tbeam.Wrap[T](out)
}
