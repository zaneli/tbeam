package tbeam

import (
	"github.com/zaneli/tbeam/pkg/tbeam/core/typex"
)

type Counted[T any] typex.KeyValue[T, int]

type KeyValue[K any, V any] typex.KeyValue[K, V]
