// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// profcheck is a tool that verifies that a ProfilesData proto conforms with
// the signal schema requirements and spec.
package main

import (
	"flag"
	"fmt"
	"os"

	profiles "go.opentelemetry.io/proto/otlp/profiles/v1development"
	"google.golang.org/protobuf/proto"
)

var checkDupes = flag.Bool("check-dupes", false, "Enable check for duplicates in the dictionary")
var checkSampleShapes = flag.Bool("check-sample-shapes", true, "Enable check for sample shapes")

func main() {
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		fmt.Println("Usage: profcheck [-check-dupes] <file>")
		os.Exit(1)
	}

	inputPath := args[0]
	contents, err := os.ReadFile(inputPath)
	if err != nil {
		fmt.Printf("Error reading file: %s\n", err)
		os.Exit(1)
	}

	var data profiles.ProfilesData
	if err := proto.Unmarshal(contents, &data); err != nil {
		fmt.Printf("Failed to read file %s as ProfilesData: %s\n", inputPath, err)
		os.Exit(1)
	}

	if err := (ConformanceChecker{CheckDictionaryDuplicates: *checkDupes, CheckSampleTimestampShape: *checkSampleShapes}).Check(&data); err != nil {
		fmt.Printf("%s: conformance checks failed: %v\n", inputPath, err)
	}
	fmt.Printf("%s: conformance checks passed\n", inputPath)
}
