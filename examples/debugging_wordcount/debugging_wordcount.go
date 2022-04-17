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

// This example code was copied from https://github.com/apache/beam/blob/cff9ccd86b390d8e5edfaa850fcf132da178330e/sdks/go/examples/debugging_wordcount/debugging_wordcount.go
package main

import (
	"context"
	"flag"
	"fmt"
	"reflect"
	"regexp"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"

	"github.com/zaneli/tbeam/pkg/tbeam"
	"github.com/zaneli/tbeam/pkg/tbeam/io/textio"
	"github.com/zaneli/tbeam/pkg/tbeam/testing/passert"
	"github.com/zaneli/tbeam/pkg/tbeam/transforms/stats"
)

var (
	input  = flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "File(s) to read.")
	filter = flag.String("filter", "Flourish|stomach", "Regex filter pattern to use. Only words matching this pattern will be included.")
	output = flag.String("output", "", "Output file (required).")
)

func init() {
	beam.RegisterFunction(extractFn)
	beam.RegisterFunction(formatFn)
	beam.RegisterType(reflect.TypeOf((*filterFn)(nil)).Elem())
}

type filterFn struct {
	Filter string `json:"filter"`

	re *regexp.Regexp
}

func (f *filterFn) Setup() {
	f.re = regexp.MustCompile(f.Filter)
}

func (*filterFn) StartBundle(_ context.Context, _ func(tbeam.Counted[string])) error {
	return nil
}

func (f *filterFn) ProcessElement(ctx context.Context, counted tbeam.Counted[string], emit func(tbeam.Counted[string])) error {
	if f.re.MatchString(counted.Key) {
		log.Infof(ctx, "Matched: %v", counted.Key)
		emit(counted)
	} else {
		log.Debugf(ctx, "Did not match: %v", counted.Key)
	}
	return nil
}

func (*filterFn) FinishBundle(_ context.Context, _ func(tbeam.Counted[string])) error {
	return nil
}

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func extractFn(line string, emit func(string)) error {
	for _, word := range wordRE.FindAllString(line, -1) {
		emit(word)
	}
	return nil
}

func formatFn(counted tbeam.Counted[string]) string {
	return fmt.Sprintf("%s: %v", counted.Key, counted.Value)
}

func CountWords(s beam.Scope, lines tbeam.TCollection[string]) tbeam.TCollection[tbeam.Counted[string]] {
	s = s.Scope("CountWords")
	col := tbeam.ParDoEmitFn(s, extractFn, lines)
	return stats.Count(s, col)
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	if *output == "" {
		log.Exit(ctx, "No output provided")
	}
	if _, err := regexp.Compile(*filter); err != nil {
		log.Exitf(ctx, "Invalid filter: %v", err)
	}

	p := beam.NewPipeline()
	s := p.Root()

	lines := textio.Read(s, *input)
	counted := CountWords(s, lines)
	filtered := tbeam.ParDo[tbeam.Counted[string], tbeam.Counted[string]](s, &filterFn{Filter: *filter}, counted)
	formatted := tbeam.ParDoFn(s, formatFn, filtered)

	passert.Equals(s, formatted, "Flourish: 3", "stomach: 1")

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
