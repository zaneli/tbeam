package debug

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"

	"github.com/zaneli/tbeam/pkg/tbeam"
)

func Print[T any](s beam.Scope, in tbeam.TCollection[T]) tbeam.TCollection[T] {
	out := debug.Print(s, in.Unwrap())
	return tbeam.Wrap[T](out)
}

func Printf[T any](s beam.Scope, format string, in tbeam.TCollection[T]) tbeam.TCollection[T] {
	out := debug.Printf(s, format, in.Unwrap())
	return tbeam.Wrap[T](out)
}
