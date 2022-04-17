package textio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"

	"github.com/zaneli/tbeam/pkg/tbeam"
)

func Read(s beam.Scope, glob string) tbeam.TCollection[string] {
	col := textio.Read(s, glob)
	return tbeam.Wrap[string](col)
}

func Write(s beam.Scope, filename string, col tbeam.TCollection[string]) {
	textio.Write(s, filename, col.Unwrap())
}
