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

func ParDo[In any, Out any](s beam.Scope, dofn ParDoFunc[In, Out], in TCollection[In], opts ...beam.Option) TCollection[Out] {
	out := beam.ParDo(s, dofn, in.Unwrap(), opts...)
	return Wrap[Out](out)
}

func ParDoFn[In any, Out any](s beam.Scope, dofn func(in In) Out, in TCollection[In], opts ...beam.Option) TCollection[Out] {
	out := beam.ParDo(s, dofn, in.Unwrap(), opts...)
	return Wrap[Out](out)
}

func ParDoEmitFn[In any, Out any](s beam.Scope, dofn func(In, func(Out)) error, in TCollection[In], opts ...beam.Option) TCollection[Out] {
	out := beam.ParDo(s, dofn, in.Unwrap(), opts...)
	return Wrap[Out](out)
}
