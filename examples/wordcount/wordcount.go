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

// This example code was copied from https://github.com/apache/beam/blob/cff9ccd86b390d8e5edfaa850fcf132da178330e/sdks/go/examples/wordcount/wordcount.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"

	"github.com/zaneli/tbeam/pkg/tbeam"
	"github.com/zaneli/tbeam/pkg/tbeam/io/textio"
	"github.com/zaneli/tbeam/pkg/tbeam/transforms/stats"
)

var (
	input  = flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "File(s) to read.")
	output = flag.String("output", "", "Output file (required).")
)

func init() {
	beam.RegisterFunction(formatFn)
	beam.RegisterType(reflect.TypeOf((*extractFn)(nil)))
}

var (
	wordRE          = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
	empty           = beam.NewCounter("extract", "emptyLines")
	smallWordLength = flag.Int("small_word_length", 9, "length of small words (default: 9)")
	smallWords      = beam.NewCounter("extract", "smallWords")
	lineLen         = beam.NewDistribution("extract", "lineLenDistro")
)

type extractFn struct {
	SmallWordLength int `json:"smallWordLength"`
}

func (f *extractFn) StartBundle(_ context.Context, _ func(string)) error {
	return nil
}

func (f *extractFn) ProcessElement(ctx context.Context, line string, emit func(string)) error {
	lineLen.Update(ctx, int64(len(line)))
	if len(strings.TrimSpace(line)) == 0 {
		empty.Inc(ctx, 1)
	}
	for _, word := range wordRE.FindAllString(line, -1) {
		if len(word) < f.SmallWordLength {
			smallWords.Inc(ctx, 1)
		}
		emit(word)
	}
	return nil
}

func (f *extractFn) FinishBundle(_ context.Context, _ func(string)) error {
	return nil
}

func formatFn(counted stats.Counted[string]) string {
	return fmt.Sprintf("%s: %v", counted.Element, counted.Count)
}

func CountWords(s beam.Scope, lines tbeam.TCollection[string]) tbeam.TCollection[stats.Counted[string]] {
	s = s.Scope("CountWords")
	col := tbeam.ParDo[string, string](s, &extractFn{SmallWordLength: *smallWordLength}, lines)
	return stats.Count(s, col)
}

func main() {
	flag.Parse()
	beam.Init()

	if *output == "" {
		log.Fatal("No output provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	lines := textio.Read(s, *input)
	counted := CountWords(s, lines)
	formatted := tbeam.ParDoFn[stats.Counted[string], string](s, formatFn, counted)
	textio.Write(s, *output, formatted)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
