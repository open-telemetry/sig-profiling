// Command otlp-analyze prints a CSV summary of one or more OTLP profile
// payloads (each file must contain a serialized
// ExportProfilesServiceRequest proto).
//
// Usage:
//
//	otlp-analyze <file-or-dir>
//
// If the argument is a directory, all entries (non-recursive) are processed
// in ascending lexicographic order. One CSV row is emitted per file.
//
// Note: this is clanker code, but it has been human reviewed. Clanker PRs
// that extract more columns against the original OTLP data are welcome.
package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	profilespb "go.opentelemetry.io/proto/otlp/collector/profiles/v1development"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	profilepb "go.opentelemetry.io/proto/otlp/profiles/v1development"
	"google.golang.org/protobuf/proto"
)

// otlpProfile holds one CSV row: the final, fully-computed column values
// for one file. Each exported field maps to one CSV column: the column
// name is the snake_case form of the field name (see camelToSnake), and
// the column description is the `desc` struct tag. The order of fields
// below defines the order of CSV columns.
//
// time.Time fields are zero and DurationSec is NaN when no profile in
// the file carried a timestamp; toString renders both as empty cells.
type otlpProfile struct {
	ID     int `desc:"sequence number of the file (1-based, in sorted order)"`
	HostID int `desc:"numeric ID assigned to datadog.host.name; first host seen is 1, repeated hosts keep the same ID"`

	StartTS time.Time `desc:"RFC3339Nano timestamp of the earliest profile in the file (min of Profile.time_unix_nano)"`
	EndTS   time.Time `desc:"RFC3339Nano timestamp where the last profile ends (max of Profile.time_unix_nano + Profile.duration_nano)"`

	DurationSec       float64 `desc:"end_ts - start_ts in seconds"`
	UncompressedBytes int     `desc:"size of the file in bytes (the serialized ExportProfilesServiceRequest)"`

	ResourceProfilesCount      int `desc:"total number of ResourceProfiles entries"`
	ResourceProfilesAttrsCount int `desc:"total number of attributes on ResourceProfiles.resource"`
	ScopeProfilesCount         int `desc:"total number of ScopeProfiles entries"`
	ScopeProfilesAttrsCount    int `desc:"total number of attributes on ScopeProfiles.scope"`
	ProfilesCount              int `desc:"total number of Profile entries"`
	ProfilesAttrsCount         int `desc:"total number of attribute_indices on Profile entries"`
	SamplesCount               int `desc:"total number of Sample entries across all profiles"`
	ValuesCount                int `desc:"total number of values across all samples"`
	TimestampsCount            int `desc:"total number of timestamps_unix_nano across all samples"`
	SampleAttrsCount           int `desc:"total number of attribute_indices on Sample entries"`
	OriginalPayloadsSumBytes   int `desc:"sum of len(Profile.original_payload) across all profiles"`

	ResourceProfilesProtoBytes int `desc:"size of a request containing only ResourceProfiles entries"`
	DictionaryProtoBytes       int `desc:"size of a request containing only ProfilesDictionary"`
	ResourceMessagesProtoBytes int `desc:"sum of proto.Size(Resource) across ResourceProfiles"`
	ScopeMessagesProtoBytes    int `desc:"sum of proto.Size(InstrumentationScope) across ScopeProfiles"`
	ProfileMessagesProtoBytes  int `desc:"sum of proto.Size(Profile) across profiles"`
	SampleMessagesProtoBytes   int `desc:"sum of proto.Size(Sample) across samples"`

	DictStringsCount               int     `desc:"len(ProfilesDictionary.string_table)"`
	DictMappingsCount              int     `desc:"len(ProfilesDictionary.mapping_table)"`
	DictLocationsCount             int     `desc:"len(ProfilesDictionary.location_table)"`
	DictFunctionsCount             int     `desc:"len(ProfilesDictionary.function_table)"`
	DictLinksCount                 int     `desc:"len(ProfilesDictionary.link_table)"`
	DictAttributesCount            int     `desc:"len(ProfilesDictionary.attribute_table)"`
	DictStacksCount                int     `desc:"len(ProfilesDictionary.stack_table)"`
	DictStringTableProtoBytes      int     `desc:"proto.Size of a ProfilesDictionary containing only string_table"`
	DictMappingTableProtoBytes     int     `desc:"proto.Size of a ProfilesDictionary containing only mapping_table"`
	DictLocationTableProtoBytes    int     `desc:"proto.Size of a ProfilesDictionary containing only location_table"`
	DictFunctionTableProtoBytes    int     `desc:"proto.Size of a ProfilesDictionary containing only function_table"`
	DictLinkTableProtoBytes        int     `desc:"proto.Size of a ProfilesDictionary containing only link_table"`
	DictAttributeTableProtoBytes   int     `desc:"proto.Size of a ProfilesDictionary containing only attribute_table"`
	DictStackTableProtoBytes       int     `desc:"proto.Size of a ProfilesDictionary containing only stack_table"`
	DictStringBytes                int     `desc:"sum of len(s) for ProfilesDictionary.string_table"`
	DictStringMaxBytes             int     `desc:"max len(s) for ProfilesDictionary.string_table"`
	FunctionNameBytes              int     `desc:"sum of len(string_table[Function.name_strindex]) across function_table"`
	FunctionNameMaxBytes           int     `desc:"max len(string_table[Function.name_strindex]) across function_table"`
	FunctionSystemNameBytes        int     `desc:"sum of len(string_table[Function.system_name_strindex]) across function_table"`
	FunctionFilenameBytes          int     `desc:"sum of len(string_table[Function.filename_strindex]) across function_table"`
	MappingFilenameBytes           int     `desc:"sum of len(string_table[Mapping.filename_strindex]) across mapping_table"`
	DictAttributeKeyBytes          int     `desc:"sum of len(string_table[KeyValueAndUnit.key_strindex]) across attribute_table"`
	DictAttributeUnitBytes         int     `desc:"sum of len(string_table[KeyValueAndUnit.unit_strindex]) across attribute_table"`
	DictAttributeValueBytes        int     `desc:"sum of direct string/bytes payload lengths in attribute_table values"`
	ResourceAttributeKeyBytes      int     `desc:"sum of len(KeyValue.key) across Resource.attributes"`
	ResourceAttributeValueBytes    int     `desc:"sum of direct string/bytes payload lengths in Resource.attributes values"`
	StackLocationsCount            int     `desc:"total number of location_indices across stack_table"`
	MaxStackDepth                  int     `desc:"max len(Stack.location_indices) across stack_table"`
	LocationLinesCount             int     `desc:"total number of Line entries across location_table"`
	LocationAttributeIndicesCount  int     `desc:"total number of attribute_indices across location_table"`
	MappingAttributeIndicesCount   int     `desc:"total number of attribute_indices across mapping_table"`
	MeanStackDepth                 float64 `desc:"average len(Stack.location_indices) across ProfilesDictionary.stack_table entries"`
	MeanLinesPerLocation           float64 `desc:"average len(Location.lines) across ProfilesDictionary.location_table entries"`
	MeanSampleAttrs                float64 `desc:"average len(Sample.attribute_indices) across samples"`
	MeanTimestampsPerSample        float64 `desc:"average len(Sample.timestamps_unix_nano) across samples"`
	MeanValuesPerSample            float64 `desc:"average len(Sample.values) across samples"`
	SampledStackFramesCount        int     `desc:"total stack frame occurrences referenced by samples, weighted by len(timestamps_unix_nano), len(values), or 1"`
	GoFraction                     float64 `desc:"fraction of sampled stack frames inferred as Go: profile.frame.type=native and Mapping has process.executable.build_id.go"`
	NativeFraction                 float64 `desc:"fraction of sampled stack frames with profile.frame.type=native, excluding frames counted as Go"`
	KernelFraction                 float64 `desc:"fraction of sampled stack frames with profile.frame.type=kernel"`
	JavaFraction                   float64 `desc:"fraction of sampled stack frames with profile.frame.type=jvm"`
	PythonFraction                 float64 `desc:"fraction of sampled stack frames with profile.frame.type=cpython"`
	JavascriptFraction             float64 `desc:"fraction of sampled stack frames with profile.frame.type=v8js"`
	DotnetFraction                 float64 `desc:"fraction of sampled stack frames with profile.frame.type=dotnet"`
	RubyFraction                   float64 `desc:"fraction of sampled stack frames with profile.frame.type=ruby"`
	PHPFraction                    float64 `desc:"fraction of sampled stack frames with profile.frame.type=php"`
	BeamFraction                   float64 `desc:"fraction of sampled stack frames with profile.frame.type=beam"`
	PerlFraction                   float64 `desc:"fraction of sampled stack frames with profile.frame.type=perl"`
	OtherFrameTypeFraction         float64 `desc:"fraction of sampled stack frames with another profile.frame.type value"`
	UnknownFrameTypeFraction       float64 `desc:"fraction of sampled stack frames without a profile.frame.type attribute"`
	GoSymbolFraction               float64 `desc:"fraction of Go dictionary locations that have a function name"`
	NativeSymbolFraction           float64 `desc:"fraction of native dictionary locations, excluding Go, that have a function name"`
	KernelSymbolFraction           float64 `desc:"fraction of kernel dictionary locations that have a function name"`
	JavaSymbolFraction             float64 `desc:"fraction of Java dictionary locations that have a function name"`
	PythonSymbolFraction           float64 `desc:"fraction of Python dictionary locations that have a function name"`
	JavascriptSymbolFraction       float64 `desc:"fraction of JavaScript dictionary locations that have a function name"`
	DotnetSymbolFraction           float64 `desc:"fraction of .NET dictionary locations that have a function name"`
	RubySymbolFraction             float64 `desc:"fraction of Ruby dictionary locations that have a function name"`
	PHPSymbolFraction              float64 `desc:"fraction of PHP dictionary locations that have a function name"`
	BeamSymbolFraction             float64 `desc:"fraction of BEAM dictionary locations that have a function name"`
	PerlSymbolFraction             float64 `desc:"fraction of Perl dictionary locations that have a function name"`
	OtherFrameTypeSymbolFraction   float64 `desc:"fraction of dictionary locations with another profile.frame.type value that have a function name"`
	UnknownFrameTypeSymbolFraction float64 `desc:"fraction of dictionary locations without a profile.frame.type attribute that have a function name"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: %s <file-or-dir>", filepath.Base(os.Args[0]))
	}
	files, err := listFiles(os.Args[1])
	if err != nil {
		return err
	}

	w := csv.NewWriter(os.Stdout)
	if err := writeAndFlush(w, headerRow()); err != nil {
		return err
	}
	hostIDs := map[string]int{}
	for i, path := range files {
		m, err := analyze(i+1, path, hostIDs)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if err := writeAndFlush(w, dataRow(m)); err != nil {
			return err
		}
	}
	return nil
}

// listFiles returns the file to process. If path is a directory, it returns
// all entries in it (non-recursive) sorted ascending. Otherwise it returns
// the path itself.
func listFiles(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []string{path}, nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		files = append(files, filepath.Join(path, e.Name()))
	}
	sort.Strings(files)
	return files, nil
}

// headerRow returns the CSV header derived from the otlpProfile field names.
func headerRow() []string {
	r := make([]string, profileType.NumField())
	for i := range r {
		r[i] = camelToSnake(profileType.Field(i).Name)
	}
	return r
}

// profileType is the cached reflect.Type of otlpProfile.
var profileType = reflect.TypeOf(otlpProfile{})

// camelToSnake converts a Go-idiomatic CamelCase identifier to snake_case.
// It handles runs of capitals correctly: "ID" -> "id", "StartTS" ->
// "start_ts", "OriginalPayloadsSumBytes" -> "original_payloads_sum_bytes".
//
// Rule: insert "_" before an uppercase letter that is either preceded by
// a lowercase letter / digit, or starts a new word inside a run of caps
// (i.e. is followed by a lowercase letter).
func camelToSnake(s string) string {
	rs := []rune(s)
	var b strings.Builder
	b.Grow(len(rs) + 4)
	for i, r := range rs {
		if i > 0 && unicode.IsUpper(r) {
			prev := rs[i-1]
			next := rune(0)
			if i+1 < len(rs) {
				next = rs[i+1]
			}
			if unicode.IsLower(prev) || unicode.IsDigit(prev) ||
				(unicode.IsUpper(prev) && unicode.IsLower(next)) {
				b.WriteByte('_')
			}
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

func writeAndFlush(w *csv.Writer, row []string) error {
	if err := w.Write(row); err != nil {
		return err
	}
	w.Flush()
	return w.Error()
}

func analyze(id int, path string, hostIDs map[string]int) (*otlpProfile, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var req profilespb.ExportProfilesServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("unmarshal OTLP ExportProfilesServiceRequest: %w", err)
	}

	m := &otlpProfile{
		ID: id, UncompressedBytes: len(body), DurationSec: math.NaN(),
		MeanStackDepth: math.NaN(), MeanLinesPerLocation: math.NaN(),
		MeanSampleAttrs: math.NaN(), MeanTimestampsPerSample: math.NaN(), MeanValuesPerSample: math.NaN(),
		GoFraction: math.NaN(), NativeFraction: math.NaN(), KernelFraction: math.NaN(), JavaFraction: math.NaN(),
		PythonFraction: math.NaN(), JavascriptFraction: math.NaN(), DotnetFraction: math.NaN(), RubyFraction: math.NaN(),
		PHPFraction: math.NaN(), BeamFraction: math.NaN(), PerlFraction: math.NaN(),
		OtherFrameTypeFraction: math.NaN(), UnknownFrameTypeFraction: math.NaN(),
		GoSymbolFraction: math.NaN(), NativeSymbolFraction: math.NaN(), KernelSymbolFraction: math.NaN(), JavaSymbolFraction: math.NaN(),
		PythonSymbolFraction: math.NaN(), JavascriptSymbolFraction: math.NaN(), DotnetSymbolFraction: math.NaN(), RubySymbolFraction: math.NaN(),
		PHPSymbolFraction: math.NaN(), BeamSymbolFraction: math.NaN(), PerlSymbolFraction: math.NaN(),
		OtherFrameTypeSymbolFraction: math.NaN(), UnknownFrameTypeSymbolFraction: math.NaN(),
	}

	dict := req.GetDictionary()
	st := dict.GetStringTable()
	m.ResourceProfilesProtoBytes = proto.Size(&profilespb.ExportProfilesServiceRequest{ResourceProfiles: req.GetResourceProfiles()})
	m.DictionaryProtoBytes = proto.Size(&profilespb.ExportProfilesServiceRequest{Dictionary: dict})
	m.DictStringsCount = len(st)
	m.DictMappingsCount = len(dict.GetMappingTable())
	m.DictLocationsCount = len(dict.GetLocationTable())
	m.DictFunctionsCount = len(dict.GetFunctionTable())
	m.DictLinksCount = len(dict.GetLinkTable())
	m.DictAttributesCount = len(dict.GetAttributeTable())
	m.DictStacksCount = len(dict.GetStackTable())
	m.DictStringTableProtoBytes = proto.Size(&profilepb.ProfilesDictionary{StringTable: st})
	m.DictMappingTableProtoBytes = proto.Size(&profilepb.ProfilesDictionary{MappingTable: dict.GetMappingTable()})
	m.DictLocationTableProtoBytes = proto.Size(&profilepb.ProfilesDictionary{LocationTable: dict.GetLocationTable()})
	m.DictFunctionTableProtoBytes = proto.Size(&profilepb.ProfilesDictionary{FunctionTable: dict.GetFunctionTable()})
	m.DictLinkTableProtoBytes = proto.Size(&profilepb.ProfilesDictionary{LinkTable: dict.GetLinkTable()})
	m.DictAttributeTableProtoBytes = proto.Size(&profilepb.ProfilesDictionary{AttributeTable: dict.GetAttributeTable()})
	m.DictStackTableProtoBytes = proto.Size(&profilepb.ProfilesDictionary{StackTable: dict.GetStackTable()})
	for _, s := range st {
		m.DictStringBytes += len(s)
		if len(s) > m.DictStringMaxBytes {
			m.DictStringMaxBytes = len(s)
		}
	}
	for _, mapping := range dict.GetMappingTable() {
		m.MappingFilenameBytes += stringTablePayloadLen(st, mapping.GetFilenameStrindex())
		m.MappingAttributeIndicesCount += len(mapping.GetAttributeIndices())
	}
	for _, location := range dict.GetLocationTable() {
		m.LocationLinesCount += len(location.GetLines())
		m.LocationAttributeIndicesCount += len(location.GetAttributeIndices())
	}
	if m.DictLocationsCount > 0 {
		m.MeanLinesPerLocation = float64(m.LocationLinesCount) / float64(m.DictLocationsCount)
	}
	for _, function := range dict.GetFunctionTable() {
		nameLen := stringTablePayloadLen(st, function.GetNameStrindex())
		m.FunctionNameBytes += nameLen
		if nameLen > m.FunctionNameMaxBytes {
			m.FunctionNameMaxBytes = nameLen
		}
		m.FunctionSystemNameBytes += stringTablePayloadLen(st, function.GetSystemNameStrindex())
		m.FunctionFilenameBytes += stringTablePayloadLen(st, function.GetFilenameStrindex())
	}
	frameTypeByAttrIndex := map[int]string{}
	goBuildIDAttrIndices := map[int]bool{}
	for i, attr := range dict.GetAttributeTable() {
		key := stringTableValue(st, attr.GetKeyStrindex())
		m.DictAttributeKeyBytes += len(key)
		m.DictAttributeUnitBytes += stringTablePayloadLen(st, attr.GetUnitStrindex())
		m.DictAttributeValueBytes += anyValuePayloadBytes(attr.GetValue(), st)
		switch key {
		case "profile.frame.type":
			frameTypeByAttrIndex[i] = anyValueString(attr.GetValue(), st)
		case "process.executable.build_id.go":
			goBuildIDAttrIndices[i] = true
		}
	}
	mappingIsGo := make([]bool, len(dict.GetMappingTable()))
	for i, mapping := range dict.GetMappingTable() {
		for _, attrIndex := range mapping.GetAttributeIndices() {
			if goBuildIDAttrIndices[int(attrIndex)] {
				mappingIsGo[i] = true
				break
			}
		}
	}
	locationFrameTypes := make([]string, len(dict.GetLocationTable()))
	locationIsGo := make([]bool, len(dict.GetLocationTable()))
	locationLanguageCounts := map[string]int{}
	locationLanguageSymbolCounts := map[string]int{}
	for i, location := range dict.GetLocationTable() {
		for _, attrIndex := range location.GetAttributeIndices() {
			if frameType, ok := frameTypeByAttrIndex[int(attrIndex)]; ok {
				locationFrameTypes[i] = frameType
				break
			}
		}
		mappingIndex := int(location.GetMappingIndex())
		locationIsGo[i] = mappingIndex >= 0 && mappingIndex < len(mappingIsGo) && mappingIsGo[mappingIndex]
		language := frameLanguage(int32(i), locationFrameTypes, locationIsGo)
		locationLanguageCounts[language]++
		if locationHasFunctionName(location, dict.GetFunctionTable(), st) {
			locationLanguageSymbolCounts[language]++
		}
	}
	for _, stack := range dict.GetStackTable() {
		depth := len(stack.GetLocationIndices())
		m.StackLocationsCount += depth
		if depth > m.MaxStackDepth {
			m.MaxStackDepth = depth
		}
	}
	if m.DictStacksCount > 0 {
		m.MeanStackDepth = float64(m.StackLocationsCount) / float64(m.DictStacksCount)
	}

	frameLanguageCounts := map[string]int{}

	var minTime, maxTime uint64 // minTime == 0 means no profile carried a timestamp
	for _, rp := range req.GetResourceProfiles() {
		m.ResourceProfilesCount++
		if r := rp.GetResource(); r != nil {
			m.ResourceMessagesProtoBytes += proto.Size(r)
			m.ResourceProfilesAttrsCount += len(r.GetAttributes())
			for _, attr := range r.GetAttributes() {
				m.ResourceAttributeKeyBytes += len(attr.GetKey())
				m.ResourceAttributeValueBytes += anyValuePayloadBytes(attr.GetValue(), st)
				if attr.GetKey() == "datadog.host.name" && m.HostID == 0 {
					hostName := anyValueString(attr.GetValue(), st)
					if hostName != "" {
						if hostID := hostIDs[hostName]; hostID != 0 {
							m.HostID = hostID
						} else {
							m.HostID = len(hostIDs) + 1
							hostIDs[hostName] = m.HostID
						}
					}
				}
			}
		}
		for _, sp := range rp.GetScopeProfiles() {
			m.ScopeProfilesCount++
			if sc := sp.GetScope(); sc != nil {
				m.ScopeMessagesProtoBytes += proto.Size(sc)
				m.ScopeProfilesAttrsCount += len(sc.GetAttributes())
			}
			for _, p := range sp.GetProfiles() {
				m.ProfilesCount++
				m.ProfileMessagesProtoBytes += proto.Size(p)
				m.ProfilesAttrsCount += len(p.GetAttributeIndices())
				m.OriginalPayloadsSumBytes += len(p.GetOriginalPayload())
				if t := p.GetTimeUnixNano(); t != 0 {
					end := t + p.GetDurationNano()
					if minTime == 0 || t < minTime {
						minTime = t
					}
					if end > maxTime {
						maxTime = end
					}
				}
				for _, sm := range p.GetSamples() {
					m.SamplesCount++
					m.SampleMessagesProtoBytes += proto.Size(sm)
					m.SampleAttrsCount += len(sm.GetAttributeIndices())
					m.ValuesCount += len(sm.GetValues())
					m.TimestampsCount += len(sm.GetTimestampsUnixNano())
					frameWeight := sampleObservationWeight(sm)
					stackIndex := int(sm.GetStackIndex())
					if stackIndex >= 0 && stackIndex < len(dict.GetStackTable()) {
						for _, locationIndex := range dict.GetStackTable()[stackIndex].GetLocationIndices() {
							m.SampledStackFramesCount += frameWeight
							frameLanguageCounts[frameLanguage(locationIndex, locationFrameTypes, locationIsGo)] += frameWeight
						}
					}
				}
			}
		}
	}
	if m.SamplesCount > 0 {
		m.MeanSampleAttrs = float64(m.SampleAttrsCount) / float64(m.SamplesCount)
		m.MeanTimestampsPerSample = float64(m.TimestampsCount) / float64(m.SamplesCount)
		m.MeanValuesPerSample = float64(m.ValuesCount) / float64(m.SamplesCount)
	}
	if m.SampledStackFramesCount > 0 {
		denom := float64(m.SampledStackFramesCount)
		m.GoFraction = float64(frameLanguageCounts["go"]) / denom
		m.NativeFraction = float64(frameLanguageCounts["native"]) / denom
		m.KernelFraction = float64(frameLanguageCounts["kernel"]) / denom
		m.JavaFraction = float64(frameLanguageCounts["java"]) / denom
		m.PythonFraction = float64(frameLanguageCounts["python"]) / denom
		m.JavascriptFraction = float64(frameLanguageCounts["javascript"]) / denom
		m.DotnetFraction = float64(frameLanguageCounts["dotnet"]) / denom
		m.RubyFraction = float64(frameLanguageCounts["ruby"]) / denom
		m.PHPFraction = float64(frameLanguageCounts["php"]) / denom
		m.BeamFraction = float64(frameLanguageCounts["beam"]) / denom
		m.PerlFraction = float64(frameLanguageCounts["perl"]) / denom
		m.OtherFrameTypeFraction = float64(frameLanguageCounts["other"]) / denom
		m.UnknownFrameTypeFraction = float64(frameLanguageCounts["unknown"]) / denom
	}
	m.GoSymbolFraction = symbolFraction(locationLanguageSymbolCounts, locationLanguageCounts, "go")
	m.NativeSymbolFraction = symbolFraction(locationLanguageSymbolCounts, locationLanguageCounts, "native")
	m.KernelSymbolFraction = symbolFraction(locationLanguageSymbolCounts, locationLanguageCounts, "kernel")
	m.JavaSymbolFraction = symbolFraction(locationLanguageSymbolCounts, locationLanguageCounts, "java")
	m.PythonSymbolFraction = symbolFraction(locationLanguageSymbolCounts, locationLanguageCounts, "python")
	m.JavascriptSymbolFraction = symbolFraction(locationLanguageSymbolCounts, locationLanguageCounts, "javascript")
	m.DotnetSymbolFraction = symbolFraction(locationLanguageSymbolCounts, locationLanguageCounts, "dotnet")
	m.RubySymbolFraction = symbolFraction(locationLanguageSymbolCounts, locationLanguageCounts, "ruby")
	m.PHPSymbolFraction = symbolFraction(locationLanguageSymbolCounts, locationLanguageCounts, "php")
	m.BeamSymbolFraction = symbolFraction(locationLanguageSymbolCounts, locationLanguageCounts, "beam")
	m.PerlSymbolFraction = symbolFraction(locationLanguageSymbolCounts, locationLanguageCounts, "perl")
	m.OtherFrameTypeSymbolFraction = symbolFraction(locationLanguageSymbolCounts, locationLanguageCounts, "other")
	m.UnknownFrameTypeSymbolFraction = symbolFraction(locationLanguageSymbolCounts, locationLanguageCounts, "unknown")
	if minTime != 0 && maxTime != 0 {
		m.StartTS = time.Unix(0, int64(minTime)).UTC()
		m.EndTS = time.Unix(0, int64(maxTime)).UTC()
		m.DurationSec = time.Duration(maxTime - minTime).Seconds()
	}
	return m, nil
}

func stringTablePayloadLen(table []string, idx int32) int {
	return len(stringTableValue(table, idx))
}

func stringTableValue(table []string, idx int32) string {
	i := int(idx)
	if i < 0 || i >= len(table) {
		return ""
	}
	return table[i]
}

func anyValueString(v *commonpb.AnyValue, stringTable []string) string {
	if v == nil {
		return ""
	}
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_StringValueStrindex:
		return stringTableValue(stringTable, x.StringValueStrindex)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'f', -1, 64)
	default:
		return ""
	}
}

func anyValuePayloadBytes(v *commonpb.AnyValue, stringTable []string) int {
	if v == nil {
		return 0
	}
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return len(x.StringValue)
	case *commonpb.AnyValue_BytesValue:
		return len(x.BytesValue)
	case *commonpb.AnyValue_StringValueStrindex:
		return stringTablePayloadLen(stringTable, x.StringValueStrindex)
	case *commonpb.AnyValue_ArrayValue:
		var n int
		for _, item := range x.ArrayValue.GetValues() {
			n += anyValuePayloadBytes(item, stringTable)
		}
		return n
	case *commonpb.AnyValue_KvlistValue:
		var n int
		for _, kv := range x.KvlistValue.GetValues() {
			n += len(kv.GetKey()) + anyValuePayloadBytes(kv.GetValue(), stringTable)
		}
		return n
	default:
		return 0
	}
}

func sampleObservationWeight(sample *profilepb.Sample) int {
	if n := len(sample.GetTimestampsUnixNano()); n > 0 {
		return n
	}
	if n := len(sample.GetValues()); n > 0 {
		return n
	}
	return 1
}

func locationHasFunctionName(location *profilepb.Location, functions []*profilepb.Function, stringTable []string) bool {
	for _, line := range location.GetLines() {
		functionIndex := int(line.GetFunctionIndex())
		if functionIndex >= 0 && functionIndex < len(functions) &&
			stringTableValue(stringTable, functions[functionIndex].GetNameStrindex()) != "" {
			return true
		}
	}
	return false
}

func symbolFraction(symbolCounts, totalCounts map[string]int, language string) float64 {
	total := totalCounts[language]
	if total == 0 {
		return math.NaN()
	}
	return float64(symbolCounts[language]) / float64(total)
}

func frameLanguage(locationIndex int32, locationFrameTypes []string, locationIsGo []bool) string {
	i := int(locationIndex)
	if i < 0 || i >= len(locationFrameTypes) {
		return "unknown"
	}
	switch locationFrameTypes[i] {
	case "native":
		if i < len(locationIsGo) && locationIsGo[i] {
			return "go"
		}
		return "native"
	case "kernel":
		return "kernel"
	case "jvm":
		return "java"
	case "cpython":
		return "python"
	case "v8js":
		return "javascript"
	case "dotnet":
		return "dotnet"
	case "ruby":
		return "ruby"
	case "php":
		return "php"
	case "beam":
		return "beam"
	case "perl":
		return "perl"
	case "":
		return "unknown"
	default:
		return "other"
	}
}

// dataRow returns one CSV row by reflecting over m's fields in declaration
// order and rendering each via toString.
func dataRow(m *otlpProfile) []string {
	v := reflect.ValueOf(*m)
	r := make([]string, v.NumField())
	for i := range r {
		r[i] = toString(v.Field(i).Interface())
	}
	return r
}

// toString renders a column value to its CSV cell representation. NaN
// floats and zero time.Time values become empty cells; ints and float64
// use their natural decimal form (no scientific notation, no trailing
// zeros for floats); time.Time uses RFC3339Nano.
func toString(v any) string {
	switch v := v.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case float64:
		if math.IsNaN(v) {
			return ""
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case time.Time:
		if v.IsZero() {
			return ""
		}
		return v.Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(v)
	}
}
