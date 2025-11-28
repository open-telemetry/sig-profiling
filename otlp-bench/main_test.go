package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	cprofiles "github.com/open-telemetry/sig-profiling/otlp-bench/internal/otlpversions/gh733/opentelemetry/proto/collector/profiles/v1development"
	common "github.com/open-telemetry/sig-profiling/otlp-bench/internal/otlpversions/gh733/opentelemetry/proto/common/v1"
	profiles "github.com/open-telemetry/sig-profiling/otlp-bench/internal/otlpversions/gh733/opentelemetry/proto/profiles/v1development"
	resource "github.com/open-telemetry/sig-profiling/otlp-bench/internal/otlpversions/gh733/opentelemetry/proto/resource/v1"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestApp(t *testing.T) {
	outDir := t.TempDir()
	_, _, err := runTestApp(t, []string{"--out", outDir, filepath.Join("testdata", "k8s.otlp")})
	if err != nil {
		t.Fatal(err)
	}
	results, err := os.ReadFile(filepath.Join(outDir, "summary.csv"))
	if err != nil {
		t.Fatalf("read results: %v", err)
	}
	csvReader := csv.NewReader(strings.NewReader(string(results)))
	records, err := csvReader.ReadAll()
	if err != nil {
		t.Fatalf("read csv: %v\n%s\n", err, string(results))
	}
	assertEqual(t, records[0], []string{"file", "encoding", "payloads", "uncompressed_bytes", "gzip_6_bytes"})
	assertEqual(t, len(records), 4)
}

type testSample struct {
	processAttrs map[string]string
	otherAttrs   map[string]string
}

func createTestProfilesData(samples []testSample) *cprofiles.ExportProfilesServiceRequest {
	dict := &profiles.ProfilesDictionary{
		StringTable: []string{""}, // Start with empty string at index 0
		AttributeTable: []*profiles.KeyValueAndUnit{
			{}, // Zero value at index 0
		},
	}

	// Add strings to dictionary
	addString := func(s string) int32 {
		for i, str := range dict.StringTable {
			if str == s {
				return int32(i)
			}
		}
		dict.StringTable = append(dict.StringTable, s)
		return int32(len(dict.StringTable) - 1)
	}

	// Add attribute to dictionary
	addAttribute := func(key, value string) int32 {
		keyIdx := addString(key)
		attr := &profiles.KeyValueAndUnit{
			KeyStrindex: keyIdx,
			Value: &common.AnyValue{
				Value: &common.AnyValue_StringValue{StringValue: value},
			},
		}
		dict.AttributeTable = append(dict.AttributeTable, attr)
		return int32(len(dict.AttributeTable) - 1)
	}

	resourceProfile := &profiles.ResourceProfiles{
		Resource: &resource.Resource{
			Attributes: []*common.KeyValue{
				{Key: "service.name", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "test-service"}}},
			},
		},
		ScopeProfiles: []*profiles.ScopeProfiles{
			{
				Scope: &common.InstrumentationScope{
					Name: "test-scope",
				},
				Profiles: []*profiles.Profile{
					{
						SampleType: &profiles.ValueType{
							TypeStrindex: addString("samples"),
							UnitStrindex: addString("count"),
						},
						Samples: nil, // Will be populated below
					},
				},
			},
		},
	}

	// Create samples
	for _, sample := range samples {
		var attrIndices []int32
		for key, value := range sample.processAttrs {
			attrIndices = append(attrIndices, addAttribute(key, value))
		}
		for key, value := range sample.otherAttrs {
			attrIndices = append(attrIndices, addAttribute(key, value))
		}

		sample := &profiles.Sample{
			StackIndex:         0,
			Values:             []int64{1},
			AttributeIndices:   attrIndices,
			TimestampsUnixNano: []uint64{1234567890000000000},
		}
		resourceProfile.ScopeProfiles[0].Profiles[0].Samples = append(
			resourceProfile.ScopeProfiles[0].Profiles[0].Samples, sample)
	}

	return &cprofiles.ExportProfilesServiceRequest{
		ResourceProfiles: []*profiles.ResourceProfiles{resourceProfile},
		Dictionary:       dict,
	}
}

func createTestProfilesDataWithUnit(samples []testSample) *cprofiles.ExportProfilesServiceRequest {
	data := createTestProfilesData(samples)
	// Add unit to first process attribute
	for _, attr := range data.Dictionary.AttributeTable {
		if attr.KeyStrindex != 0 {
			key := data.Dictionary.StringTable[attr.KeyStrindex]
			if _, ok := processAttributes[key]; ok {
				attr.UnitStrindex = int32(len(data.Dictionary.StringTable))
				data.Dictionary.StringTable = append(data.Dictionary.StringTable, "test-unit")
				break
			}
		}
	}
	return data
}

func createTestProfilesDataWithOriginalPayload(samples []testSample) *cprofiles.ExportProfilesServiceRequest {
	data := createTestProfilesData(samples)
	// Add original payload to profile
	data.ResourceProfiles[0].ScopeProfiles[0].Profiles[0].OriginalPayload = []byte("test payload")
	return data
}

type resourceAttrs struct {
	attrs map[string]any
}

func createTestProfilesDataWithResourceAttrs(resourceAttrsList []resourceAttrs) *cprofiles.ExportProfilesServiceRequest {
	if len(resourceAttrsList) == 0 {
		resourceAttrsList = []resourceAttrs{{attrs: map[string]any{"service.name": "test-service"}}}
	}

	dict := &profiles.ProfilesDictionary{
		StringTable: []string{""}, // Start with empty string at index 0
		AttributeTable: []*profiles.KeyValueAndUnit{
			{}, // Zero value at index 0
		},
	}

	// Add strings to dictionary
	addString := func(s string) int32 {
		for i, str := range dict.StringTable {
			if str == s {
				return int32(i)
			}
		}
		dict.StringTable = append(dict.StringTable, s)
		return int32(len(dict.StringTable) - 1)
	}

	var resourceProfiles []*profiles.ResourceProfiles
	for _, ra := range resourceAttrsList {
		var attrs []*common.KeyValue
		for key, value := range ra.attrs {
			if strVal, ok := value.(string); ok {
				attrs = append(attrs, &common.KeyValue{
					Key: key,
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: strVal},
					},
				})
			}
		}

		resourceProfile := &profiles.ResourceProfiles{
			Resource: &resource.Resource{
				Attributes: attrs,
			},
			ScopeProfiles: []*profiles.ScopeProfiles{
				{
					Scope: &common.InstrumentationScope{
						Name: "test-scope",
					},
					Profiles: []*profiles.Profile{
						{
							SampleType: &profiles.ValueType{
								TypeStrindex: addString("samples"),
								UnitStrindex: addString("count"),
							},
							Samples: []*profiles.Sample{
								{
									StackIndex:         0,
									Values:             []int64{1},
									AttributeIndices:   []int32{},
									TimestampsUnixNano: []uint64{1234567890000000000},
								},
							},
						},
					},
				},
			},
		}
		resourceProfiles = append(resourceProfiles, resourceProfile)
	}

	return &cprofiles.ExportProfilesServiceRequest{
		ResourceProfiles: resourceProfiles,
		Dictionary:       dict,
	}
}

func createTestProfilesDataWithMixedResourceAttrs(resourceAttrsList []resourceAttrs) *cprofiles.ExportProfilesServiceRequest {
	if len(resourceAttrsList) == 0 {
		resourceAttrsList = []resourceAttrs{{attrs: map[string]any{"service.name": "test-service", "port": 8080, "enabled": true}}}
	}

	dict := &profiles.ProfilesDictionary{
		StringTable: []string{""}, // Start with empty string at index 0
		AttributeTable: []*profiles.KeyValueAndUnit{
			{}, // Zero value at index 0
		},
	}

	// Add strings to dictionary
	addString := func(s string) int32 {
		for i, str := range dict.StringTable {
			if str == s {
				return int32(i)
			}
		}
		dict.StringTable = append(dict.StringTable, s)
		return int32(len(dict.StringTable) - 1)
	}

	var resourceProfiles []*profiles.ResourceProfiles
	for _, ra := range resourceAttrsList {
		var attrs []*common.KeyValue
		for key, value := range ra.attrs {
			switch v := value.(type) {
			case string:
				attrs = append(attrs, &common.KeyValue{
					Key: key,
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: v},
					},
				})
			case int:
				attrs = append(attrs, &common.KeyValue{
					Key: key,
					Value: &common.AnyValue{
						Value: &common.AnyValue_IntValue{IntValue: int64(v)},
					},
				})
			case bool:
				attrs = append(attrs, &common.KeyValue{
					Key: key,
					Value: &common.AnyValue{
						Value: &common.AnyValue_BoolValue{BoolValue: v},
					},
				})
			}
		}

		resourceProfile := &profiles.ResourceProfiles{
			Resource: &resource.Resource{
				Attributes: attrs,
			},
			ScopeProfiles: []*profiles.ScopeProfiles{
				{
					Scope: &common.InstrumentationScope{
						Name: "test-scope",
					},
					Profiles: []*profiles.Profile{
						{
							SampleType: &profiles.ValueType{
								TypeStrindex: addString("samples"),
								UnitStrindex: addString("count"),
							},
							Samples: []*profiles.Sample{
								{
									StackIndex:         0,
									Values:             []int64{1},
									AttributeIndices:   []int32{},
									TimestampsUnixNano: []uint64{1234567890000000000},
								},
							},
						},
					},
				},
			},
		}
		resourceProfiles = append(resourceProfiles, resourceProfile)
	}

	return &cprofiles.ExportProfilesServiceRequest{
		ResourceProfiles: resourceProfiles,
		Dictionary:       dict,
	}
}

func createTestProfilesDataWithPreDictifiedAttrs(resourceAttrsList []resourceAttrs) *cprofiles.ExportProfilesServiceRequest {
	data := createTestProfilesDataWithResourceAttrs(resourceAttrsList)
	dict := data.Dictionary

	// Pre-dictify the first attribute
	if len(data.ResourceProfiles) > 0 && len(data.ResourceProfiles[0].Resource.Attributes) > 0 {
		attr := data.ResourceProfiles[0].Resource.Attributes[0]
		if attr.Key != "" {
			attr.KeyRef = dictStrIndex(attr.Key, dict)
			attr.Key = ""
		}
		if attr.Value.GetStringValue() != "" {
			attr.Value = &common.AnyValue{
				Value: &common.AnyValue_StringRef{
					StringRef: dictStrIndex(attr.Value.GetStringValue(), dict),
				},
			}
		}
	}

	return data
}

func runTestApp(t *testing.T, args []string) (stdout, stderr string, err error) {
	var outBuf bytes.Buffer
	var errBuf bytes.Buffer
	a := &App{
		Stdout: &outBuf,
		Stderr: &errBuf,
	}
	err = a.Run(t.Context(), append([]string{"otlp-bench"}, args...)...)
	return outBuf.String(), errBuf.String(), err
}

func assertEqual(t *testing.T, got, want any) {
	t.Helper()
	if diff := cmp.Diff(got, want, protocmp.Transform()); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestSplitByProcess(t *testing.T) {
	// Test with manually constructed data to achieve higher coverage
	testCases := []struct {
		name        string
		input       *cprofiles.ExportProfilesServiceRequest
		expectPanic bool
		panicMsg    string
	}{
		{
			name: "basic split by process",
			input: createTestProfilesData([]testSample{
				{processAttrs: map[string]string{"process.pid": "123"}, otherAttrs: map[string]string{"thread.id": "456"}},
				{processAttrs: map[string]string{"process.pid": "789"}, otherAttrs: map[string]string{"thread.id": "101"}},
			}),
		},
		{
			name: "process attribute with unit (should panic)",
			input: createTestProfilesDataWithUnit([]testSample{
				{processAttrs: map[string]string{"process.pid": "123"}, otherAttrs: map[string]string{"thread.id": "456"}},
			}),
			expectPanic: true,
			panicMsg:    "process attribute with unit is not supported",
		},
		{
			name: "profile with original payload (should panic)",
			input: createTestProfilesDataWithOriginalPayload([]testSample{
				{processAttrs: map[string]string{"process.pid": "123"}, otherAttrs: map[string]string{"thread.id": "456"}},
			}),
			expectPanic: true,
			panicMsg:    "splitting a profile with an original payload is not supported",
		},
		{
			name: "multiple processes with same resource attributes",
			input: createTestProfilesData([]testSample{
				{processAttrs: map[string]string{"process.pid": "123", "process.executable.name": "app1"}, otherAttrs: map[string]string{"thread.id": "456"}},
				{processAttrs: map[string]string{"process.pid": "789", "process.executable.name": "app2"}, otherAttrs: map[string]string{"thread.id": "101"}},
				{processAttrs: map[string]string{"process.pid": "123", "process.executable.name": "app1"}, otherAttrs: map[string]string{"thread.id": "789"}}, // Same process as first
			}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectPanic {
				defer func() {
					if r := recover(); r != nil {
						if panicMsg, ok := r.(string); ok && panicMsg == tc.panicMsg {
							// Expected panic
							return
						}
						t.Errorf("unexpected panic: %v", r)
					} else {
						t.Errorf("expected panic with message %q but no panic occurred", tc.panicMsg)
					}
				}()
			}

			// Count total samples before splitting
			originalSampleCount := countSamples(tc.input)

			result := splitByProcess(tc.input)
			if result == nil {
				if !tc.expectPanic {
					t.Fatal("splitByProcess returned nil")
				}
				return
			}

			// Verify dictionary is preserved
			if result.Dictionary == nil {
				t.Error("result dictionary should not be nil")
				return // Can't continue without dictionary
			}

			// Verify ResourceProfiles exist
			if len(result.ResourceProfiles) == 0 {
				t.Error("result should have at least one ResourceProfile")
			}

			// Count total samples after splitting - should be preserved
			resultSampleCount := countSamples(result)
			if resultSampleCount != originalSampleCount {
				t.Errorf("sample count mismatch: got %d, want %d", resultSampleCount, originalSampleCount)
			}

			// Verify process attributes are moved from samples to resources
			// and non-process attributes remain in samples
			verifyProcessAttributesMoved(t, tc.input, result)

			// Verify that samples with different process attributes are split into different ResourceProfiles
			verifySamplesSplitByProcess(t, tc.input, result)
		})
	}

	// Also test with real data from file to ensure backward compatibility
	t.Run("with real test data", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join("testdata", "k8s.otlp"))
		if err != nil {
			t.Fatal(err)
		}
		profiles, err := unmarshalOTLP(data)
		if err != nil {
			t.Fatal(err)
		}
		if len(profiles) == 0 {
			t.Fatal("unmarshalOTLP returned no profiles")
		}
		gh733Profile := profiles[0]
		// Ensure we have at least one resource profile in the input
		if len(gh733Profile.ResourceProfiles) == 0 {
			t.Fatal("test data should have at least one resource profile")
		}

		// Count total samples before splitting
		originalSampleCount := countSamples(gh733Profile)

		result := splitByProcess(gh733Profile)
		if result == nil {
			t.Fatal("splitByProcess returned nil")
		}

		// Verify dictionary is preserved
		if result.Dictionary == nil {
			t.Error("result dictionary should not be nil")
			return // Can't continue without dictionary
		}
		if gh733Profile.Dictionary != nil && result.Dictionary != gh733Profile.Dictionary {
			// Dictionary should be the same reference or at least have the same content
			if len(result.Dictionary.StringTable) != len(gh733Profile.Dictionary.StringTable) {
				t.Errorf("dictionary string table length mismatch: got %d, want %d",
					len(result.Dictionary.StringTable), len(gh733Profile.Dictionary.StringTable))
			}
		}

		// Verify ResourceProfiles exist
		if len(result.ResourceProfiles) == 0 {
			t.Error("result should have at least one ResourceProfile")
		}

		// Count total samples after splitting - should be preserved
		resultSampleCount := countSamples(result)
		if resultSampleCount != originalSampleCount {
			t.Errorf("sample count mismatch: got %d, want %d", resultSampleCount, originalSampleCount)
		}

		// Verify process attributes are moved from samples to resources
		// and non-process attributes remain in samples
		verifyProcessAttributesMoved(t, gh733Profile, result)

		// Verify that samples with different process attributes are split into different ResourceProfiles
		verifySamplesSplitByProcess(t, gh733Profile, result)
	})
}

func countSamples(profile *cprofiles.ExportProfilesServiceRequest) int {
	count := 0
	for _, rp := range profile.ResourceProfiles {
		for _, sp := range rp.ScopeProfiles {
			for _, p := range sp.Profiles {
				count += len(p.Samples)
			}
		}
	}
	return count
}

func verifyProcessAttributesMoved(t *testing.T, original, result *cprofiles.ExportProfilesServiceRequest) {
	t.Helper()

	// Collect all process attribute keys from original samples
	originalProcessAttrsInSamples := make(map[string]bool)
	for _, rp := range original.ResourceProfiles {
		for _, sp := range rp.ScopeProfiles {
			for _, p := range sp.Profiles {
				for _, s := range p.Samples {
					for _, ai := range s.AttributeIndices {
						attr := original.Dictionary.AttributeTable[ai]
						key := original.Dictionary.StringTable[attr.KeyStrindex]
						if _, ok := processAttributes[key]; ok {
							originalProcessAttrsInSamples[key] = true
						}
					}
				}
			}
		}
	}

	// If there were no process attributes in samples, skip this check
	if len(originalProcessAttrsInSamples) == 0 {
		return
	}

	// Verify process attributes are now in resources, not in samples
	for _, rp := range result.ResourceProfiles {
		// Check that samples don't have process attributes
		for _, sp := range rp.ScopeProfiles {
			for _, p := range sp.Profiles {
				for _, s := range p.Samples {
					for _, ai := range s.AttributeIndices {
						attr := result.Dictionary.AttributeTable[ai]
						key := result.Dictionary.StringTable[attr.KeyStrindex]
						if _, ok := processAttributes[key]; ok {
							t.Errorf("sample still contains process attribute %q, should be moved to resource", key)
						}
					}
				}
			}
		}
	}
}

func verifySamplesSplitByProcess(t *testing.T, original, result *cprofiles.ExportProfilesServiceRequest) {
	t.Helper()

	// Group original samples by their process attributes
	originalGroups := make(map[string]int) // hash -> sample count
	for _, rp := range original.ResourceProfiles {
		for _, sp := range rp.ScopeProfiles {
			for _, p := range sp.Profiles {
				for _, s := range p.Samples {
					processAttrs := []*profiles.KeyValueAndUnit{}
					for _, ai := range s.AttributeIndices {
						attr := original.Dictionary.AttributeTable[ai]
						key := original.Dictionary.StringTable[attr.KeyStrindex]
						if _, ok := processAttributes[key]; ok {
							processAttrs = append(processAttrs, attr)
						}
					}
					// Create a hash of process attributes for grouping
					hash := hashProcessAttrs(processAttrs, original.Dictionary)
					originalGroups[string(hash)]++
				}
			}
		}
	}

	// If there are no process attributes, we can't verify splitting
	if len(originalGroups) == 0 {
		return
	}

	// Verify that result has at least as many ResourceProfiles as distinct process attribute groups
	// (it could have more if resource attributes also differ)
	if len(result.ResourceProfiles) < len(originalGroups) {
		t.Errorf("expected at least %d ResourceProfiles (one per process attribute group), got %d",
			len(originalGroups), len(result.ResourceProfiles))
	}
}

func hashProcessAttrs(attrs []*profiles.KeyValueAndUnit, dict *profiles.ProfilesDictionary) []byte {
	// Simple hash based on sorted attribute keys
	keys := make([]string, 0, len(attrs))
	for _, attr := range attrs {
		keys = append(keys, dict.StringTable[attr.KeyStrindex])
	}
	slices.Sort(keys)
	h := sha256.New()
	for _, key := range keys {
		h.Write([]byte(key))
	}
	return h.Sum(nil)
}

func TestScaleSamples(t *testing.T) {
	testCases := []struct {
		name     string
		input    *cprofiles.ExportProfilesServiceRequest
		factor   int
		expected int // expected sample count after scaling
	}{
		{
			name:     "scale by 1 (no change)",
			input:    createTestProfilesData([]testSample{{processAttrs: map[string]string{"process.pid": "123"}}}),
			factor:   1,
			expected: 1,
		},
		{
			name: "scale by 3",
			input: createTestProfilesData([]testSample{
				{processAttrs: map[string]string{"process.pid": "123"}},
				{processAttrs: map[string]string{"process.pid": "456"}},
			}),
			factor:   3,
			expected: 6, // 2 original samples * 3 = 6
		},
		{
			name:     "scale by 5 with multiple profiles",
			input:    createTestProfilesDataWithResourceAttrs([]resourceAttrs{{}, {}}), // Creates 2 resource profiles, each with 1 sample
			factor:   5,
			expected: 10, // 2 original samples * 5 = 10
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Count original samples
			originalCount := countSamples(tc.input)

			// Scale samples
			scaleSamples(tc.input, tc.factor)

			// Verify sample count
			resultCount := countSamples(tc.input)
			if resultCount != tc.expected {
				t.Errorf("expected %d samples after scaling by %d, got %d", tc.expected, tc.factor, resultCount)
			}

			// Verify the scaling factor matches expectation
			if tc.factor > 1 && resultCount != originalCount*tc.factor {
				t.Errorf("sample count should be %d * %d = %d, got %d", originalCount, tc.factor, originalCount*tc.factor, resultCount)
			}
		})
	}
}

func TestUseResourceAttrDict(t *testing.T) {
	// Test with manually constructed data to achieve higher coverage
	testCases := []struct {
		name  string
		input *cprofiles.ExportProfilesServiceRequest
	}{
		{
			name: "basic resource attributes dictification",
			input: createTestProfilesDataWithResourceAttrs([]resourceAttrs{
				{attrs: map[string]any{"service.name": "test-service", "service.version": "1.0.0"}},
			}),
		},
		{
			name: "multiple resource profiles with different attributes",
			input: createTestProfilesDataWithResourceAttrs([]resourceAttrs{
				{attrs: map[string]any{"service.name": "service1", "host.name": "host1"}},
				{attrs: map[string]any{"service.name": "service2", "host.name": "host2"}},
			}),
		},
		{
			name: "resource attributes with mixed types",
			input: createTestProfilesDataWithMixedResourceAttrs([]resourceAttrs{
				{attrs: map[string]any{"service.name": "test-service", "port": 8080, "enabled": true}},
			}),
		},
		{
			name: "already dictified attributes (should be preserved)",
			input: createTestProfilesDataWithPreDictifiedAttrs([]resourceAttrs{
				{attrs: map[string]any{"service.name": "test-service"}},
			}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Count original dictionary size
			originalDictSize := len(tc.input.Dictionary.StringTable)

			result := useResourceAttrDict(tc.input)
			if result == nil {
				t.Fatal("useResourceAttrDict returned nil")
			}

			// Verify dictionary exists and has grown or stayed the same
			if result.Dictionary == nil {
				t.Error("result dictionary should not be nil")
				return
			}

			// Dictionary should have at least as many strings as original
			if len(result.Dictionary.StringTable) < originalDictSize {
				t.Errorf("result dictionary should have at least %d strings, got %d",
					originalDictSize, len(result.Dictionary.StringTable))
			}

			// Verify ResourceProfiles exist and attributes are dictified
			if len(result.ResourceProfiles) != len(tc.input.ResourceProfiles) {
				t.Errorf("expected %d ResourceProfiles, got %d",
					len(tc.input.ResourceProfiles), len(result.ResourceProfiles))
			}

			// Verify each resource profile's attributes are dictified
			for i, rp := range result.ResourceProfiles {
				originalRp := tc.input.ResourceProfiles[i]

				// Attributes should be dictified
				if len(rp.Resource.Attributes) != len(originalRp.Resource.Attributes) {
					t.Errorf("ResourceProfile %d: expected %d attributes, got %d",
						i, len(originalRp.Resource.Attributes), len(rp.Resource.Attributes))
					continue
				}

				// Check each attribute is dictified
				for j, attr := range rp.Resource.Attributes {
					originalAttr := originalRp.Resource.Attributes[j]

					// Key should be converted to KeyRef (unless it already was)
					if originalAttr.KeyRef == 0 && attr.KeyRef == 0 {
						t.Errorf("ResourceProfile %d, Attribute %d: key should have been converted to KeyRef", i, j)
					}

					// If original had Key, result should have KeyRef
					if originalAttr.Key != "" && attr.KeyRef == 0 {
						t.Errorf("ResourceProfile %d, Attribute %d: expected KeyRef for attribute with key %q", i, j, originalAttr.Key)
					}

					// String values should be converted to StringRef
					if originalStr := originalAttr.Value.GetStringValue(); originalStr != "" {
						if attr.Value.GetStringRef() == 0 {
							t.Errorf("ResourceProfile %d, Attribute %d: string value should have been converted to StringRef", i, j)
						} else {
							// Verify the string reference points to the correct string
							if attr.Value.GetStringRef() >= int32(len(result.Dictionary.StringTable)) {
								t.Errorf("ResourceProfile %d, Attribute %d: StringRef %d out of bounds", i, j, attr.Value.GetStringRef())
							} else {
								dictStr := result.Dictionary.StringTable[attr.Value.GetStringRef()]
								if dictStr != originalStr {
									t.Errorf("ResourceProfile %d, Attribute %d: StringRef points to %q, expected %q",
										i, j, dictStr, originalStr)
								}
							}
						}
					}

					// Non-string values should remain unchanged
					if _, isString := originalAttr.Value.Value.(*common.AnyValue_StringValue); !isString {
						if diff := cmp.Diff(attr.Value, originalAttr.Value, protocmp.Transform()); diff != "" {
							t.Errorf("ResourceProfile %d, Attribute %d: non-string value changed (-want +got):\n%s", i, j, diff)
						}
					}
				}

				// Other resource fields should be preserved
				if rp.Resource.DroppedAttributesCount != originalRp.Resource.DroppedAttributesCount {
					t.Errorf("ResourceProfile %d: DroppedAttributesCount changed from %d to %d",
						i, originalRp.Resource.DroppedAttributesCount, rp.Resource.DroppedAttributesCount)
				}

				if rp.SchemaUrl != originalRp.SchemaUrl {
					t.Errorf("ResourceProfile %d: SchemaUrl changed from %q to %q",
						i, originalRp.SchemaUrl, rp.SchemaUrl)
				}
			}

			// Verify sample count is preserved
			originalSampleCount := countSamples(tc.input)
			resultSampleCount := countSamples(result)
			if resultSampleCount != originalSampleCount {
				t.Errorf("sample count mismatch: got %d, want %d", resultSampleCount, originalSampleCount)
			}
		})
	}

	// Also test with real data from file to ensure backward compatibility
	t.Run("with real test data", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join("testdata", "k8s.otlp"))
		if err != nil {
			t.Fatal(err)
		}
		profiles, err := unmarshalOTLP(data)
		if err != nil {
			t.Fatal(err)
		}
		if len(profiles) == 0 {
			t.Fatal("unmarshalOTLP returned no profiles")
		}
		originalProfile := profiles[0]

		// Ensure we have at least one resource profile with attributes
		if len(originalProfile.ResourceProfiles) == 0 {
			t.Fatal("test data should have at least one resource profile")
		}

		// Count original dictionary size
		originalDictSize := len(originalProfile.Dictionary.StringTable)

		result := useResourceAttrDict(originalProfile)
		if result == nil {
			t.Fatal("useResourceAttrDict returned nil")
		}

		// Verify dictionary exists and has grown or stayed the same
		if result.Dictionary == nil {
			t.Error("result dictionary should not be nil")
			return
		}

		// Dictionary should have at least as many strings as original
		if len(result.Dictionary.StringTable) < originalDictSize {
			t.Errorf("result dictionary should have at least %d strings, got %d",
				originalDictSize, len(result.Dictionary.StringTable))
		}

		// Verify ResourceProfiles exist and attributes are dictified
		if len(result.ResourceProfiles) != len(originalProfile.ResourceProfiles) {
			t.Errorf("expected %d ResourceProfiles, got %d",
				len(originalProfile.ResourceProfiles), len(result.ResourceProfiles))
		}

		// Verify each resource profile's attributes are dictified
		for i, rp := range result.ResourceProfiles {
			originalRp := originalProfile.ResourceProfiles[i]

			// Attributes should be dictified
			if len(rp.Resource.Attributes) != len(originalRp.Resource.Attributes) {
				t.Errorf("ResourceProfile %d: expected %d attributes, got %d",
					i, len(originalRp.Resource.Attributes), len(rp.Resource.Attributes))
				continue
			}

			// Check each attribute is dictified
			for j, attr := range rp.Resource.Attributes {
				originalAttr := originalRp.Resource.Attributes[j]

				// Key should be converted to KeyRef (unless it already was)
				if originalAttr.KeyRef == 0 && attr.KeyRef == 0 {
					t.Errorf("ResourceProfile %d, Attribute %d: key should have been converted to KeyRef", i, j)
				}

				// If original had Key, result should have KeyRef
				if originalAttr.Key != "" && attr.KeyRef == 0 {
					t.Errorf("ResourceProfile %d, Attribute %d: expected KeyRef for attribute with key %q", i, j, originalAttr.Key)
				}

				// String values should be converted to StringRef
				if originalStr := originalAttr.Value.GetStringValue(); originalStr != "" {
					if attr.Value.GetStringRef() == 0 {
						t.Errorf("ResourceProfile %d, Attribute %d: string value should have been converted to StringRef", i, j)
					} else {
						// Verify the string reference points to the correct string
						if attr.Value.GetStringRef() >= int32(len(result.Dictionary.StringTable)) {
							t.Errorf("ResourceProfile %d, Attribute %d: StringRef %d out of bounds", i, j, attr.Value.GetStringRef())
						} else {
							dictStr := result.Dictionary.StringTable[attr.Value.GetStringRef()]
							if dictStr != originalStr {
								t.Errorf("ResourceProfile %d, Attribute %d: StringRef points to %q, expected %q",
									i, j, dictStr, originalStr)
							}
						}
					}
				}

				// Non-string values should remain unchanged
				if _, isString := originalAttr.Value.Value.(*common.AnyValue_StringValue); !isString {
					if diff := cmp.Diff(attr.Value, originalAttr.Value); diff != "" {
						t.Errorf("ResourceProfile %d, Attribute %d: non-string value changed (-want +got):\n%s", i, j, diff)
					}
				}
			}

			// Other resource fields should be preserved
			if rp.Resource.DroppedAttributesCount != originalRp.Resource.DroppedAttributesCount {
				t.Errorf("ResourceProfile %d: DroppedAttributesCount changed from %d to %d",
					i, originalRp.Resource.DroppedAttributesCount, rp.Resource.DroppedAttributesCount)
			}

			if rp.SchemaUrl != originalRp.SchemaUrl {
				t.Errorf("ResourceProfile %d: SchemaUrl changed from %q to %q",
					i, originalRp.SchemaUrl, rp.SchemaUrl)
			}
		}

		// Verify sample count is preserved
		originalSampleCount := countSamples(originalProfile)
		resultSampleCount := countSamples(result)
		if resultSampleCount != originalSampleCount {
			t.Errorf("sample count mismatch: got %d, want %d", resultSampleCount, originalSampleCount)
		}
	})
}
