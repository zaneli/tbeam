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

// This example code was copied from https://github.com/apache/beam/blob/cff9ccd86b390d8e5edfaa850fcf132da178330e/sdks/go/examples/contains/contains.go
package main

import (
	"context"
	"flag"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"

	"github.com/zaneli/tbeam/pkg/tbeam"
	"github.com/zaneli/tbeam/pkg/tbeam/io/textio"
	"github.com/zaneli/tbeam/pkg/tbeam/transforms/stats"
	"github.com/zaneli/tbeam/pkg/tbeam/x/debug"
)

var (
	input  = flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "File(s) to read.")
	search = flag.String("search", "", "Only return words that contain this substring.")
)

func init() {
	beam.RegisterFunction(extractFn)
	beam.RegisterFunction(formatFn)
	beam.RegisterType(reflect.TypeOf((*includeFn)(nil)).Elem())
	stats.RegisterCountFunction[string]()
}

func FilterWords(s beam.Scope, lines tbeam.TCollection[string]) tbeam.TCollection[tbeam.Counted[string]] {
	s = s.Scope("FilterWords")
	words := tbeam.ParDoEmitFn(s, extractFn, lines)
	filtered := tbeam.ParDo[string, string](s, &includeFn{Search: *search}, words)
	counted := stats.Count(s, filtered)
	return debug.Head[tbeam.Counted[string]](s, counted, 10)
}

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func extractFn(line string, emit func(string)) error {
	for _, w := range wordRE.FindAllString(line, -1) {
		emit(w)
	}
	return nil
}

type includeFn struct {
	Search string `json:"search"`
}

func (*includeFn) StartBundle(_ context.Context, _ func(string)) error {
	return nil
}

func (f *includeFn) ProcessElement(_ context.Context, s string, emit func(string)) error {
	if strings.Contains(s, f.Search) {
		emit(s)
	}
	return nil
}

func (*includeFn) FinishBundle(_ context.Context, _ func(string)) error {
	return nil
}

func formatFn(counted tbeam.Counted[string]) string {
	return fmt.Sprintf("%s: %v", counted.Key, counted.Value)
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	if *search == "" {
		log.Exit(ctx, "No search string provided. Use --search=foo")
	}

	log.Info(ctx, "Running contains")

	p := beam.NewPipeline()
	s := p.Root()
	lines := textio.Read(s, *input)
	filtered := FilterWords(s, lines)
	formatted := tbeam.ParDoFn(s, formatFn, filtered)
	debug.Print(s, formatted)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
