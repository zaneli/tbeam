package stats

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"

	"github.com/zaneli/tbeam/pkg/tbeam"
)

func init() {
	beam.RegisterFunction(toCountedFn[any])
}

type Counted[T any] struct {
	Element T
	Count   int
}

func Count[T any](s beam.Scope, col tbeam.TCollection[T]) tbeam.TCollection[Counted[T]] {
	pre := stats.Count(s, col.Unwrap())
	post := beam.ParDo(s, toCountedFn[T], pre)
	return tbeam.Wrap[Counted[T]](post)
}

func toCountedFn[T any](e T, c int) Counted[T] {
	return Counted[T]{Element: e, Count: c}
}
