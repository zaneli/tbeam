package stats

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"

	"github.com/zaneli/tbeam/pkg/tbeam"
)

func init() {
	beam.RegisterFunction(toCountedFn[any])
}

func Count[T any](s beam.Scope, col tbeam.TCollection[T]) tbeam.TCollection[tbeam.Counted[T]] {
	pre := stats.Count(s, col.Unwrap())
	post := beam.ParDo(s, toCountedFn[T], pre)
	return tbeam.Wrap[tbeam.Counted[T]](post)
}

func toCountedFn[T any](e T, c int) tbeam.Counted[T] {
	return tbeam.Counted[T]{Key: e, Value: c}
}
