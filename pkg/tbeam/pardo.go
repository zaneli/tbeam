package tbeam

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

type ParDoFunc[In any, Out any] interface {
	StartBundle(context.Context, func(Out)) error
	ProcessElement(context.Context, In, func(Out)) error
	FinishBundle(context.Context, func(Out)) error
}

func ParDo[In any, Out any](s beam.Scope, dofn ParDoFunc[In, Out], col TCollection[In], opts ...beam.Option) TCollection[Out] {
	out := beam.ParDo(s, dofn, col.Unwrap(), opts...)
	return Wrap[Out](out)
}

func ParDoF[In any, Out any](s beam.Scope, dofn func(in In) Out, col TCollection[In], opts ...beam.Option) TCollection[Out] {
	out := beam.ParDo(s, dofn, col.Unwrap(), opts...)
	return Wrap[Out](out)
}
