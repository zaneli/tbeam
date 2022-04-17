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

// This example code was copied from https://github.com/apache/beam/blob/cff9ccd86b390d8e5edfaa850fcf132da178330e/sdks/go/examples/complete/autocomplete/autocomplete.go
package main

import (
	"context"
	"flag"
	"os"
	"regexp"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"

	"github.com/zaneli/tbeam/pkg/tbeam"
	"github.com/zaneli/tbeam/pkg/tbeam/io/textio"
	"github.com/zaneli/tbeam/pkg/tbeam/transforms/top"
	"github.com/zaneli/tbeam/pkg/tbeam/x/debug"
)

var (
	input = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/haiku/old_pond.txt"), "Files to read.")
	n     = flag.Int("top", 3, "Number of completions")
)

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func init() {
	beam.RegisterFunction(extractFn)
	beam.RegisterFunction(less)
}

func extractFn(line string, emit func(string)) error {
	for _, word := range wordRE.FindAllString(line, -1) {
		emit(word)
	}
	return nil
}

func less(a, b string) bool {
	return len(a) < len(b)
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	log.Info(ctx, "Running autocomplete")

	p := beam.NewPipeline()
	s := p.Root()
	lines, err := textio.Immediate(s, *input)
	if err != nil {
		log.Exitf(ctx, "Failed to read %v: %v", *input, err)
	}
	words := tbeam.ParDoEmitFn(s, extractFn, lines)

	hits := top.Largest[string](s, words, *n, less)
	debug.Print(s, hits)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
