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

package main

import (
	"errors"
	"fmt"

	profiles "go.opentelemetry.io/proto/otlp/profiles/v1development"
	"google.golang.org/protobuf/proto"
)

func CheckConformance(data *profiles.ProfilesData) error {
	dict := data.Dictionary
	if len(data.ResourceProfiles) == 0 {
		return errors.New("resource profiles are empty")
	}
	for _, resourceProfiles := range data.ResourceProfiles {
		// TODO: Check attributes?
		for _, scopeProfiles := range resourceProfiles.ScopeProfiles {
			// TODO: Check attributes?
			for i, profile := range scopeProfiles.Profiles {
				if err := checkProfile(profile, dict); err != nil {
					return fmt.Errorf("profile %d: %v", i, err)
				}
			}
		}
	}
	return checkDictionary(dict)
}

func checkProfile(prof *profiles.Profile, dict *profiles.ProfilesDictionary) error {
	var errs error
	if err := checkAttributeIndices(prof.AttributeIndices, dict); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "attribute_indices"))
	}
	if err := checkValueType(prof.SampleType, dict); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "sample_type"))
	}
	if err := checkValueType(prof.PeriodType, dict); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "period_type"))
	}
	for i, s := range prof.Sample {
		if err := checkSample(s, prof.TimeUnixNano, prof.TimeUnixNano+prof.DurationNano, dict); err != nil {
			errs = errors.Join(errs, prefixErrorf(err, "sample[%d]", i))
		}
		// TODO: Check uniqueness of samples?
		// Key: {stack_index, sorted(attribute_indices), link_index}
		// Related: https://github.com/open-telemetry/opentelemetry-proto/issues/706.
	}
	for i, strIdx := range prof.CommentStrindices {
		if err := checkIndex(len(dict.StringTable), strIdx); err != nil {
			errs = errors.Join(errs, prefixErrorf(err, "comment_strindices[%d]", i))
		}
	}
	return errs
}

func checkSample(s *profiles.Sample, startUnixNano uint64, endUnixNano uint64, dict *profiles.ProfilesDictionary) error {
	var errs error
	if err := checkIndex(len(dict.StackTable), s.StackIndex); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "stack_index"))
	}
	if err := checkAttributeIndices(s.AttributeIndices, dict); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "attribute_indices"))
	}
	if err := checkIndex(len(dict.LinkTable), s.LinkIndex); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "link_index"))
	}
	for i, tsUnixNano := range s.TimestampsUnixNano {
		if tsUnixNano < startUnixNano || tsUnixNano > endUnixNano {
			errs = errors.Join(errs, fmt.Errorf("timestamps_unix_nano[%d]=%d is outside profile time range [%d, %d]", i, tsUnixNano, startUnixNano, endUnixNano))
		}
	}
	// TODO: Add a check for the value vs timestamp shapes.
	return errs
}

func checkDictionary(dict *profiles.ProfilesDictionary) error {
	var errs error

	if err := checkMappingTable(dict.GetMappingTable(), dict); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "mapping_table"))
	}

	if err := checkLocationTable(dict.GetLocationTable(), dict); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "location_table"))
	}

	if err := checkFunctionTable(dict.GetFunctionTable(), dict); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "function_table"))
	}

	if err := checkLinkTable(dict.GetLinkTable()); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "link_table"))
	}

	if err := checkStringTable(dict.GetStringTable()); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "string_table"))
	}

	if err := checkAttributeTable(dict.GetAttributeTable(), len(dict.GetStringTable())); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "attribute_table"))
	}

	if err := checkStackTable(dict.GetStackTable(), len(dict.GetLocationTable())); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "stack_table"))
	}

	return errs
}

func checkValueType(valueType *profiles.ValueType, dict *profiles.ProfilesDictionary) error {
	var errs error
	if err := checkIndex(len(dict.StringTable), valueType.UnitStrindex); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "unit_strindex"))
	}
	if err := checkIndex(len(dict.StringTable), valueType.TypeStrindex); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "type_strindex"))
	}
	return nil
}

func checkMappingTable(mappingTable []*profiles.Mapping, dict *profiles.ProfilesDictionary) error {
	if err := checkZeroVal(mappingTable); err != nil {
		return err
	}
	var errs error
	for idx, m := range mappingTable {
		if err := checkIndex(len(dict.StringTable), m.FilenameStrindex); err != nil {
			errs = errors.Join(errs, prefixErrorf(err, "[%d].filename_strindex", idx))
		}
		if err := checkAttributeIndices(m.AttributeIndices, dict); err != nil {
			errs = errors.Join(errs, prefixErrorf(err, "[%d].attribute_indices", idx))
		}
		if !(m.MemoryStart == 0 && m.MemoryLimit == 0) && !(m.MemoryStart < m.MemoryLimit) {
			errs = errors.Join(errs, fmt.Errorf("[%d]: memory_start=%016x, memory_limit=%016x: must be both zero or start < limit", idx, m.MemoryStart, m.MemoryLimit))
		}
	}
	// TODO: Add optional uniqueness check.
	// TODO: Add optional unreferenced entries check.
	return errs
}

func checkLocationTable(locTable []*profiles.Location, dict *profiles.ProfilesDictionary) error {
	if err := checkZeroVal(locTable); err != nil {
		return err
	}
	var errs error
	for locIdx, loc := range locTable {
		if err := checkIndex(len(dict.MappingTable), loc.MappingIndex); err != nil {
			errs = errors.Join(errs, prefixErrorf(err, "[%d].mapping_index", locIdx))
		}
		if err := checkAttributeIndices(loc.AttributeIndices, dict); err != nil {
			errs = errors.Join(errs, prefixErrorf(err, "[%d].attribute_indices", locIdx))
		}
		for lineIdx, line := range loc.Line {
			if err := checkLine(line, dict); err != nil {
				errs = errors.Join(errs, prefixErrorf(err, "[%d].line[%d]", locIdx, lineIdx))
			}
		}
	}
	// TODO: Add optional uniqueness check.
	// TODO: Add optional unreferenced entries check.
	return errs
}

func checkLine(line *profiles.Line, dict *profiles.ProfilesDictionary) error {
	var errs error
	if err := checkIndex(len(dict.FunctionTable), line.FunctionIndex); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "function_index"))
	}
	if err := checkNonNegative(line.Line); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "line"))
	}
	if err := checkNonNegative(line.Column); err != nil {
		errs = errors.Join(errs, prefixErrorf(err, "column"))
	}
	return errs
}

func checkFunctionTable(funcTable []*profiles.Function, dict *profiles.ProfilesDictionary) error {
	if err := checkZeroVal(funcTable); err != nil {
		return err
	}
	var errs error
	for idx, fnc := range funcTable {
		if err := checkIndex(len(dict.StringTable), fnc.NameStrindex); err != nil {
			errs = errors.Join(errs, prefixErrorf(err, "[%d].name_strindex", idx))
		}
		if err := checkIndex(len(dict.StringTable), fnc.SystemNameStrindex); err != nil {
			errs = errors.Join(errs, prefixErrorf(err, "[%d].system_name_strindex", idx))
		}
		if err := checkIndex(len(dict.StringTable), fnc.FilenameStrindex); err != nil {
			errs = errors.Join(errs, prefixErrorf(err, "[%d].filename_strindex", idx))
		}
		if err := checkNonNegative(fnc.StartLine); err != nil {
			errs = errors.Join(errs, prefixErrorf(err, "[%d].start_line", idx))
		}
	}
	// TODO: Add optional uniqueness check.
	// TODO: Add optional unreferenced entries check.
	return errs
}

func checkLinkTable(linkTable []*profiles.Link) error {
	if err := checkZeroVal(linkTable); err != nil {
		return err
	}
	var errs error
	for idx, link := range linkTable[1:] {
		if gotLen, wantLen := len(link.TraceId), 16; gotLen != wantLen {
			errs = errors.Join(errs, fmt.Errorf("len([%d].trace_id) == %d, want %d", idx, gotLen, wantLen))
		}
		if gotLen, wantLen := len(link.SpanId), 8; gotLen != wantLen {
			errs = errors.Join(errs, fmt.Errorf("len([%d].span_id) == %d, want %d", idx, gotLen, wantLen))
		}
	}
	// TODO: Add optional uniqueness check.
	// TODO: Add optional unreferenced entries check.
	return errs
}

func checkStringTable(strTable []string) error {
	if len(strTable) == 0 {
		return errors.New("empty string table, must have at least empty string")
	}
	if strTable[0] != "" {
		return fmt.Errorf("must have empty string at index 0, got %q", strTable[0])
	}
	var errs error
	strIdxs := map[string]int{}
	for idx, s := range strTable {
		if origIdx, ok := strIdxs[s]; ok {
			errs = errors.Join(errs, fmt.Errorf("duplicate string at index %d, orig index %d: %s", idx, origIdx, s))
			continue
		}
		strIdxs[s] = idx
	}
	return errs
}

func checkAttributeTable(attrTable []*profiles.KeyValueAndUnit, lenStrTable int) error {
	if err := checkZeroVal(attrTable); err != nil {
		return err
	}
	var errs error
	for pos, kvu := range attrTable {
		if err := checkIndex(lenStrTable, kvu.KeyStrindex); err != nil {
			errs = errors.Join(errs, prefixErrorf(err, "[%d].key_strindex", pos))
		}
		if err := checkIndex(lenStrTable, kvu.UnitStrindex); err != nil {
			errs = errors.Join(errs, prefixErrorf(err, "[%d].unit_strindex", pos))
		}
	}
	// TODO: Add optional uniqueness check.
	// TODO: Add optional unreferenced entries check.
	return errs
}

func checkStackTable(stackTable []*profiles.Stack, lenLocTable int) error {
	if err := checkZeroVal(stackTable); err != nil {
		return err
	}
	var errs error
	for i, stack := range stackTable {
		for j, locIndex := range stack.LocationIndices {
			if err := checkIndex(lenLocTable, locIndex); err != nil {
				errs = errors.Join(errs, prefixErrorf(err, "[%d].location_indices[%d]", i, j))
			}
		}
	}
	// TODO: Add optional uniqueness check.
	// TODO: Add optional unreferenced entries check.
	return errs
}

// checkZeroVal verifies that the given slice meets Profiles dictionary
// conventions: the slice is not empty and has zero value at index zero.
func checkZeroVal[T any, P interface {
	*T
	proto.Message
}](table []P) error {
	if len(table) == 0 {
		return errors.New("empty table, must have at least zero value entry")
	}
	var zeroVal P = new(T)
	if !proto.Equal(table[0], zeroVal) {
		return fmt.Errorf("must have zero value %#v at index 0, got %#v", zeroVal, table[0])
	}
	return nil
}

func checkAttributeIndices(attrIndices []int32, dict *profiles.ProfilesDictionary) error {
	var errs error
	keys := map[string]int{}
	for pos, attrIdx := range attrIndices {
		if err := checkIndex(len(dict.AttributeTable), attrIdx); err != nil {
			errs = errors.Join(errs, prefixErrorf(err, "[%d]", pos))
			continue
		}
		attr := dict.AttributeTable[attrIdx]
		if err := checkIndex(len(dict.StringTable), attr.KeyStrindex); err != nil {
			errs = errors.Join(errs, prefixErrorf(err, "[%d].key_strindex", pos))
			continue
		}
		key := dict.StringTable[attr.KeyStrindex]
		if prevPos, ok := keys[key]; ok {
			errs = errors.Join(errs, fmt.Errorf("[%d].key_strindex: duplicate key %q, previously seen at [%d].key_strindex", pos, key, prevPos))
		} else {
			keys[key] = pos
		}
	}
	return errs
}

func checkIndices(length int, indices []int32) error {
	var errs error
	for i, idx := range indices {
		if err := checkIndex(length, idx); err != nil {
			errs = errors.Join(errs, prefixErrorf(err, "[%d]", i))
		}
	}
	return errs
}

func checkIndex(length int, idx int32) error {
	if idx < 0 || int(idx) >= length {
		return fmt.Errorf("index %d is out of range [0..%d)", idx, length)
	}
	return nil
}

func checkNonNegative(n int64) error {
	if n < 0 {
		return fmt.Errorf("%d < 0, must be non-negative", n)
	}
	return nil
}

func prefixErrorf(err error, format string, args ...any) error {
	prefix := fmt.Sprintf(format, args...)
	if merr, ok := err.(interface{ Unwrap() []error }); ok {
		errs := merr.Unwrap()
		for i, e := range errs {
			errs[i] = fmt.Errorf("%s: %w", prefix, e)
		}
		return errors.Join(errs...)
	}
	return fmt.Errorf("%s: %w", prefix, err)
}
