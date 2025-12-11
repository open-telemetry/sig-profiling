package main

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	common "go.opentelemetry.io/proto/otlp/common/v1"
	profiles "go.opentelemetry.io/proto/otlp/profiles/v1development"
)

func TestCheckConformance(t *testing.T) {
	zeroDictionary := &profiles.ProfilesDictionary{
		MappingTable:   []*profiles.Mapping{{}},
		LocationTable:  []*profiles.Location{{}},
		FunctionTable:  []*profiles.Function{{}},
		LinkTable:      []*profiles.Link{{}},
		StringTable:    []string{""},
		AttributeTable: []*profiles.KeyValueAndUnit{{}},
		StackTable:     []*profiles.Stack{{}},
	}
	zeroDictWithStringTable := func(strTable []string) *profiles.ProfilesDictionary {
		ret := proto.CloneOf(zeroDictionary)
		ret.StringTable = strTable
		return ret
	}

	for _, tc := range []struct {
		desc              string
		data              *profiles.ProfilesData
		disableDupesCheck bool
		checkSampleShapes bool
		wantErr           string
	}{{
		desc:    "no profiles",
		data:    &profiles.ProfilesData{},
		wantErr: "resource profiles are empty",
	}, {
		desc: "minimal valid profile",
		data: &profiles.ProfilesData{
			Dictionary: zeroDictionary,
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{
					Profiles: []*profiles.Profile{{}},
				}},
			}},
		},
		wantErr: "",
	}, {
		desc: "no scope profiles",
		data: &profiles.ProfilesData{
			Dictionary:       zeroDictionary,
			ResourceProfiles: []*profiles.ResourceProfiles{{}},
		},
		wantErr: "resource profiles has no scope profiles",
	}, {
		desc: "no profiles in scope",
		data: &profiles.ProfilesData{
			Dictionary: zeroDictionary,
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{}},
			}},
		},
		wantErr: "scope profiles has no profiles",
	}, {
		desc: "no empty string at pos 0",
		data: &profiles.ProfilesData{
			Dictionary: zeroDictWithStringTable([]string{"a"}),
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{
					Profiles: []*profiles.Profile{{}},
				}},
			}},
		},
		wantErr: "must have empty string at index 0",
	}, {
		desc: "duplicate string",
		data: &profiles.ProfilesData{
			Dictionary: zeroDictWithStringTable([]string{"", "a", "b", "a"}),
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{
					Profiles: []*profiles.Profile{{}},
				}},
			}},
		},
		wantErr: "duplicate string",
	}, {
		desc: "duplicate string (disabled check)",
		data: &profiles.ProfilesData{
			Dictionary: zeroDictWithStringTable([]string{"", "a", "b", "a"}),
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{
					Profiles: []*profiles.Profile{{}},
				}},
			}},
		},
		disableDupesCheck: true,
		wantErr:           "",
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
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{
					Profiles: []*profiles.Profile{{}},
				}},
			}},
		},
		wantErr: `duplicate key "k1"`,
	}, {
		desc: "timestamp before start",
		data: &profiles.ProfilesData{
			Dictionary: zeroDictionary,
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{
					Profiles: []*profiles.Profile{{
						TimeUnixNano: 100,
						DurationNano: 10,
						Sample: []*profiles.Sample{{
							TimestampsUnixNano: []uint64{99},
						}},
					}},
				}},
			}},
		},
		wantErr: "timestamps_unix_nano[0]=99 is outside profile time range [100, 110)",
	}, {
		desc: "timestamp at start",
		data: &profiles.ProfilesData{
			Dictionary: zeroDictionary,
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{
					Profiles: []*profiles.Profile{{
						TimeUnixNano: 100,
						DurationNano: 10,
						Sample: []*profiles.Sample{{
							TimestampsUnixNano: []uint64{100},
						}},
					}},
				}},
			}},
		},
		wantErr: "",
	}, {
		desc: "timestamp at end (exclusive)",
		data: &profiles.ProfilesData{
			Dictionary: zeroDictionary,
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{
					Profiles: []*profiles.Profile{{
						TimeUnixNano: 100,
						DurationNano: 10,
						Sample: []*profiles.Sample{{
							TimestampsUnixNano: []uint64{110},
						}},
					}},
				}},
			}},
		},
		wantErr: "timestamps_unix_nano[0]=110 is outside profile time range [100, 110)",
	}, {
		desc: "timestamp after end",
		data: &profiles.ProfilesData{
			Dictionary: zeroDictionary,
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{
					Profiles: []*profiles.Profile{{
						TimeUnixNano: 100,
						DurationNano: 10,
						Sample: []*profiles.Sample{{
							TimestampsUnixNano: []uint64{111},
						}},
					}},
				}},
			}},
		},
		wantErr: "timestamps_unix_nano[0]=111 is outside profile time range [100, 110)",
	}, {
		desc: "sample with no values and no timestamps",
		data: &profiles.ProfilesData{
			Dictionary: zeroDictionary,
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{
					Profiles: []*profiles.Profile{{
						Sample: []*profiles.Sample{{}},
					}},
				}},
			}},
		},
		wantErr: "sample must have at least one values or timestamps_unix_nano entry",
	}, {
		desc: "sample with values and timestamps length mismatch",
		data: &profiles.ProfilesData{
			Dictionary: zeroDictionary,
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{
					Profiles: []*profiles.Profile{{
						TimeUnixNano: 100,
						DurationNano: 10,
						Sample: []*profiles.Sample{{
							Values:             []int64{1},
							TimestampsUnixNano: []uint64{100, 101},
						}},
					}},
				}},
			}},
		},
		wantErr: "values (len=1) and timestamps_unix_nano (len=2) must contain the same number of elements",
	}, {
		desc: "sample with values only",
		data: &profiles.ProfilesData{
			Dictionary: zeroDictionary,
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{
					Profiles: []*profiles.Profile{{
						Sample: []*profiles.Sample{{
							Values: []int64{1},
						}},
					}},
				}},
			}},
		},
		wantErr: "",
	}, {
		desc: "sample with timestamps only",
		data: &profiles.ProfilesData{
			Dictionary: zeroDictionary,
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{
					Profiles: []*profiles.Profile{{
						TimeUnixNano: 100,
						DurationNano: 10,
						Sample: []*profiles.Sample{{
							TimestampsUnixNano: []uint64{100},
						}},
					}},
				}},
			}},
		},
		wantErr: "",
	}, {
		desc: "sample with values and timestamps matching",
		data: &profiles.ProfilesData{
			Dictionary: zeroDictionary,
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{
					Profiles: []*profiles.Profile{{
						TimeUnixNano: 100,
						DurationNano: 10,
						Sample: []*profiles.Sample{{
							Values:             []int64{1},
							TimestampsUnixNano: []uint64{100},
						}},
					}},
				}},
			}},
		},
		wantErr: "",
	}, {
		desc: "mixed sample types",
		data: &profiles.ProfilesData{
			Dictionary: zeroDictionary,
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{
					Profiles: []*profiles.Profile{{
						TimeUnixNano: 100,
						DurationNano: 10,
						Sample: []*profiles.Sample{{
							Values: []int64{1},
						}, {
							TimestampsUnixNano: []uint64{100},
						}},
					}},
				}},
			}},
		},
		checkSampleShapes: true,
		wantErr:           "does not match expected sample shape",
	}, {
		desc: "mixed sample types (disabled check)",
		data: &profiles.ProfilesData{
			Dictionary: zeroDictionary,
			ResourceProfiles: []*profiles.ResourceProfiles{{
				ScopeProfiles: []*profiles.ScopeProfiles{{
					Profiles: []*profiles.Profile{{
						TimeUnixNano: 100,
						DurationNano: 10,
						Sample: []*profiles.Sample{{
							Values: []int64{1},
						}, {
							TimestampsUnixNano: []uint64{100},
						}},
					}},
				}},
			}},
		},
		checkSampleShapes: false,
		wantErr:           "",
	}} {
		t.Run(tc.desc, func(t *testing.T) {
			c := ConformanceChecker{CheckDictionaryDuplicates: !tc.disableDupesCheck, CheckSampleTimestampShape: tc.checkSampleShapes}
			err := c.Check(tc.data)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Errorf("Check(): got error %q, want no error", err)
			case tc.wantErr == "" && err == nil:
				break
			case err == nil:
				t.Errorf("Check(): got no error, want error containing %q", tc.wantErr)
			case !strings.Contains(err.Error(), tc.wantErr):
				t.Errorf("Check(): got error %q, want error containing %q", err, tc.wantErr)
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
