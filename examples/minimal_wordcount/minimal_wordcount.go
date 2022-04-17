// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This example code was copied from https://github.com/apache/beam/blob/cff9ccd86b390d8e5edfaa850fcf132da178330e/sdks/go/examples/minimal_wordcount/minimal_wordcount.go
package main

import (
	"context"
	"fmt"
	"regexp"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct"

	"github.com/zaneli/tbeam/pkg/tbeam"
	"github.com/zaneli/tbeam/pkg/tbeam/io/textio"
	"github.com/zaneli/tbeam/pkg/tbeam/transforms/stats"

	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
)

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func init() {
	stats.RegisterCountFunction[string]()
}

func main() {
	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	lines := textio.Read(s, "gs://apache-beam-samples/shakespeare/kinglear.txt")

	words := tbeam.ParDoEmitFn(s, func(line string, emit func(string)) error {
		for _, word := range wordRE.FindAllString(line, -1) {
			emit(word)
		}
		return nil
	}, lines)

	counted := stats.Count(s, words)

	formatted := tbeam.ParDoFn(s, func(counted tbeam.Counted[string]) string {
		return fmt.Sprintf("%s: %v", counted.Key, counted.Value)
	}, counted)

	textio.Write(s, "wordcounts.txt", formatted)

	direct.Execute(context.Background(), p)
}
