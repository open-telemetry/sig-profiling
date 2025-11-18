package main

import (
	"errors"
	"strings"
	"testing"

	common "go.opentelemetry.io/proto/otlp/common/v1"
	profiles "go.opentelemetry.io/proto/otlp/profiles/v1development"
)

func TestCheckConformance(t *testing.T) {
	for _, tc := range []struct {
		desc    string
		data    *profiles.ProfilesData
		wantErr string
	}{{
		desc:    "no profiles",
		data:    &profiles.ProfilesData{},
		wantErr: "resource profiles are empty",
	}, {
		desc: "minimal valid profile",
		data: &profiles.ProfilesData{
			Dictionary: &profiles.ProfilesDictionary{
				MappingTable:   []*profiles.Mapping{{}},
				LocationTable:  []*profiles.Location{{}},
				FunctionTable:  []*profiles.Function{{}},
				LinkTable:      []*profiles.Link{{}},
				StringTable:    []string{""},
				AttributeTable: []*profiles.KeyValueAndUnit{{}},
				StackTable:     []*profiles.Stack{{}},
			},
			ResourceProfiles: []*profiles.ResourceProfiles{{}},
		},
		wantErr: "",
	}, {
		desc: "no empty string at pos 0",
		data: &profiles.ProfilesData{
			Dictionary: &profiles.ProfilesDictionary{
				MappingTable:   []*profiles.Mapping{{}},
				LocationTable:  []*profiles.Location{{}},
				FunctionTable:  []*profiles.Function{{}},
				LinkTable:      []*profiles.Link{{}},
				StringTable:    []string{"a"},
				AttributeTable: []*profiles.KeyValueAndUnit{{}},
				StackTable:     []*profiles.Stack{{}},
			},
			ResourceProfiles: []*profiles.ResourceProfiles{{}},
		},
		wantErr: "must have empty string at index 0",
	}, {
		desc: "duplicate string",
		data: &profiles.ProfilesData{
			Dictionary: &profiles.ProfilesDictionary{
				MappingTable:   []*profiles.Mapping{{}},
				LocationTable:  []*profiles.Location{{}},
				FunctionTable:  []*profiles.Function{{}},
				LinkTable:      []*profiles.Link{{}},
				StringTable:    []string{"", "a", "b", "a"},
				AttributeTable: []*profiles.KeyValueAndUnit{{}},
				StackTable:     []*profiles.Stack{{}},
			},
			ResourceProfiles: []*profiles.ResourceProfiles{{}},
		},
		wantErr: "duplicate string",
	}, {
		desc: "duplicate attribute key in location",
		data: &profiles.ProfilesData{
			Dictionary: &profiles.ProfilesDictionary{
				MappingTable: []*profiles.Mapping{{}},
				LocationTable: []*profiles.Location{
					{},
					{AttributeIndices: []int32{1, 2}},
				},
				FunctionTable: []*profiles.Function{{}},
				LinkTable:     []*profiles.Link{{}},
				StringTable:   []string{"", "k1"},
				AttributeTable: []*profiles.KeyValueAndUnit{
					{},
					{KeyStrindex: 1, Value: makeAnyValue("v1")},
					{KeyStrindex: 1, Value: makeAnyValue("v2")},
				},
				StackTable: []*profiles.Stack{{}},
			},
			ResourceProfiles: []*profiles.ResourceProfiles{{}},
		},
		wantErr: `duplicate key "k1"`,
	}} {
		t.Run(tc.desc, func(t *testing.T) {
			err := CheckConformance(tc.data)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Errorf("CheckConformance(): got error %q, want no error", err)
			case tc.wantErr == "" && err == nil:
				break
			case err == nil:
				t.Errorf("CheckConformance(): got no error, want error containing %q", tc.wantErr)
			case !strings.Contains(err.Error(), tc.wantErr):
				t.Errorf("CheckConformance(): got error %q, want error containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestPrefixErrorf(t *testing.T) {
	for _, tc := range []struct {
		desc string
		err  error
		want string
	}{{
		desc: "single error",
		err:  errors.New("error 1"),
		want: "prefix: error 1",
	}, {
		desc: "multiple errors",
		err:  errors.Join(errors.New("error 1"), errors.New("error 2")),
		want: "prefix: error 1\nprefix: error 2",
	}} {
		t.Run(tc.desc, func(t *testing.T) {
			got := prefixErrorf(tc.err, "prefix").Error()
			if got != tc.want {
				t.Errorf("prefixErrorf(): got %q, want %q", got, tc.want)
			}
		})
	}
}

func makeAnyValue[T string | int64](v T) *common.AnyValue {
	switch val := any(v).(type) {
	case string:
		return &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: val}}
	case int64:
		return &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: val}}
	}
	return nil
}
