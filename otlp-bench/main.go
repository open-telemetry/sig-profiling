package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	cprofiles "github.com/open-telemetry/sig-profiling/otlp-bench/internal/otlpversions/gh733/opentelemetry/proto/collector/profiles/v1development"
	common "github.com/open-telemetry/sig-profiling/otlp-bench/internal/otlpversions/gh733/opentelemetry/proto/common/v1"
	profiles "github.com/open-telemetry/sig-profiling/otlp-bench/internal/otlpversions/gh733/opentelemetry/proto/profiles/v1development"
	resource "github.com/open-telemetry/sig-profiling/otlp-bench/internal/otlpversions/gh733/opentelemetry/proto/resource/v1"
	"github.com/urfave/cli/v3"
	"google.golang.org/protobuf/proto"
)

func main() {
	app := NewApp()
	if err := app.Run(context.Background(), os.Args...); err != nil {
		fmt.Fprintf(app.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func NewApp() *App {
	return &App{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

type App struct {
	Stdout io.Writer
	Stderr io.Writer
}

func (a *App) Run(ctx context.Context, args ...string) error {
	cmd := &cli.Command{
		Writer:    a.Stdout,
		ErrWriter: a.Stderr,
		ArgsUsage: "file [file ...]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "out",
				Usage:   "directory to write results",
				Aliases: []string{"o"},
				Value:   "otlp-bench-results",
			},
			&cli.IntFlag{
				Name:    "samples",
				Usage:   "scale samples in baseline profile by duplicating them this many times",
				Aliases: []string{"s"},
				Value:   1,
			},
		},
		Arguments: []cli.Argument{
			&cli.StringArgs{
				// Add print sub command (or verbose one)
				Name:      "file",
				UsageText: "OTLP profile file to read",
				Min:       1,
				Max:       -1,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			samples := cmd.Int("samples")
			outDir := cmd.String("out")
			files := cmd.StringArgs("file")
			return a.run(ctx, samples, outDir, files...)
		},
	}
	return cmd.Run(ctx, args)
}

func (a *App) run(_ context.Context, samples int, outDir string, files ...string) error {
	if outDir == "" {
		return fmt.Errorf("output directory must not be empty")
	}

	os.RemoveAll(outDir)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("create output directory %q: %w", outDir, err)
	}

	resultsPath := filepath.Join(outDir, "summary.csv")
	outFile, err := os.Create(resultsPath)
	if err != nil {
		return fmt.Errorf("create results file %q: %w", resultsPath, err)
	}
	defer outFile.Close()

	csvWriter := csv.NewWriter(outFile)

	if err := csvWriter.Write([]string{"file", "encoding", "payloads", "uncompressed_bytes", "gzip_6_bytes", "zstd_bytes"}); err != nil {
		return fmt.Errorf("write header row: %w", err)
	}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read file: %w", err)
		}

		// Copy input file to output directory
		copyPath := filepath.Join(outDir, filepath.Base(file))
		if err := os.WriteFile(copyPath, data, 0644); err != nil {
			return fmt.Errorf("copy input file to %q: %w", copyPath, err)
		}

		baselinePayloads, err := unmarshalOTLP(data)
		if err != nil {
			return fmt.Errorf("unmarshal gh733 profile: %w", err)
		}

		var stats struct {
			baseline         profileSize
			splitByProcess   profileSize
			resourceAttrDict profileSize
		}
		for _, baseline := range baselinePayloads {
			if samples > 1 {
				scaleSamples(baseline, samples)
			}

			baseFilename := filepath.Base(file)
			if err := appendTextProfileToFile(outDir, baseFilename, "baseline", baseline); err != nil {
				return fmt.Errorf("write baseline profile: %w", err)
			}
			baselineSizes, err := profileSizes(baseline)
			if err != nil {
				return fmt.Errorf("calculate baseline sizes: %w", err)
			}
			stats.baseline = stats.baseline.Add(baselineSizes)

			byProcess := splitByProcess(baseline)
			if err := appendTextProfileToFile(outDir, baseFilename, "split-by-process", byProcess); err != nil {
				return fmt.Errorf("write split-by-process profile: %w", err)
			}
			byProcessSizes, err := profileSizes(byProcess)
			if err != nil {
				return fmt.Errorf("calculate split-by-process sizes: %w", err)
			}
			stats.splitByProcess = stats.splitByProcess.Add(byProcessSizes)

			resourceAttrDict := useResourceAttrDict(byProcess)
			if err := appendTextProfileToFile(outDir, baseFilename, "resource-attr-dict", resourceAttrDict); err != nil {
				return fmt.Errorf("write resource-attr-dict profile: %w", err)
			}
			resourceAttrDictSizes, err := profileSizes(resourceAttrDict)
			if err != nil {
				return fmt.Errorf("calculate resource-attr-dict sizes: %w", err)
			}
			stats.resourceAttrDict = stats.resourceAttrDict.Add(resourceAttrDictSizes)
		}
		payloadCount := len(baselinePayloads)
		writeRow(csvWriter, file, "baseline", payloadCount, stats.baseline)
		writeRow(csvWriter, file, "split-by-process", payloadCount, stats.splitByProcess)
		writeRow(csvWriter, file, "resource-attr-dict", payloadCount, stats.resourceAttrDict)
		csvWriter.Flush()
	}
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return fmt.Errorf("flush csv: %w", err)
	}
	return nil
}

type profileSize struct {
	uncompressed int
	gzip6        int
	zstd         int
}

func (p profileSize) Add(other profileSize) profileSize {
	return profileSize{
		uncompressed: p.uncompressed + other.uncompressed,
		gzip6:        p.gzip6 + other.gzip6,
		zstd:         p.zstd + other.zstd,
	}
}

func profileSizes(profile *cprofiles.ExportProfilesServiceRequest) (profileSize, error) {
	uncompressed, err := proto.Marshal(profile)
	if err != nil {
		return profileSize{}, fmt.Errorf("marshal profile: %w", err)
	}

	var gzipComp bytes.Buffer
	gw, err := gzip.NewWriterLevel(&gzipComp, gzip.DefaultCompression)
	if err != nil {
		return profileSize{}, fmt.Errorf("create gzip writer: %w", err)
	}
	if _, err := gw.Write(uncompressed); err != nil {
		return profileSize{}, fmt.Errorf("write compressed data into gzip: %w", err)
	}
	if err := gw.Close(); err != nil {
		return profileSize{}, fmt.Errorf("close gzip writer: %w", err)
	}

	zstdDst, err := os.CreateTemp("", "zstd-profileSizes")
	if err != nil {
		return profileSize{}, fmt.Errorf("failed to create temporary zstd destination file: %w", err)
	}
	defer os.Remove(zstdDst.Name())

	zstdEncoder, err := zstd.NewWriter(zstdDst)
	if err != nil {
		return profileSize{}, fmt.Errorf("failed to create zstd encoder: %w", err)
	}
	if _, err := zstdEncoder.Write(uncompressed); err != nil {
		return profileSize{}, fmt.Errorf("write compressed data into zstd: %w", err)
	}
	if err := zstdEncoder.Close(); err != nil {
		return profileSize{}, fmt.Errorf("close zstd encoder: %w", err)
	}

	zstdInfo, err := zstdDst.Stat()
	if err != nil {
		return profileSize{}, fmt.Errorf("stat zstd file: %w", err)
	}
	zstdDst.Close()

	return profileSize{
		uncompressed: len(uncompressed),
		gzip6:        gzipComp.Len(),
		zstd:         int(zstdInfo.Size()),
	}, nil
}

func writeRow(csvWriter *csv.Writer, file, encoding string, payloads int, sizes profileSize) error {
	return csvWriter.Write([]string{
		file,
		encoding,
		fmt.Sprintf("%d", payloads),
		fmt.Sprintf("%d", sizes.uncompressed),
		fmt.Sprintf("%d", sizes.gzip6),
		fmt.Sprintf("%d", sizes.zstd),
	})
}

func unmarshalOTLP(data []byte) ([]*cprofiles.ExportProfilesServiceRequest, error) {
	// First try direct unmarshaling
	var msg cprofiles.ExportProfilesServiceRequest
	if err := proto.Unmarshal(data, &msg); err == nil {
		return []*cprofiles.ExportProfilesServiceRequest{&msg}, nil
	}

	// If direct unmarshaling fails, try length-prefixed format
	// The first 4 bytes contain the size as a big-endian uint32.
	// See https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/fileexporter/README.md#file-format
	var msgs []*cprofiles.ExportProfilesServiceRequest
	for len(data) > 0 {
		if len(data) < 4 {
			return nil, fmt.Errorf("data too short for length-prefixed format")
		}

		size := binary.BigEndian.Uint32(data[:4])
		if len(data) < int(4+size) {
			return nil, fmt.Errorf("data length %d does not match expected size %d", len(data), 4+size)
		}

		data = data[4:]
		var msg cprofiles.ExportProfilesServiceRequest
		if err := proto.Unmarshal(data[:size], &msg); err != nil {
			return nil, fmt.Errorf("unmarshal length-prefixed message: %w", err)
		}
		msgs = append(msgs, &msg)
		data = data[size:]
	}
	return msgs, nil
}

func scaleSamples(data *cprofiles.ExportProfilesServiceRequest, factor int) {
	for _, rp := range data.ResourceProfiles {
		for _, sp := range rp.ScopeProfiles {
			for _, p := range sp.Profiles {
				originalSamples := make([]*profiles.Sample, len(p.Samples))
				copy(originalSamples, p.Samples)
				p.Samples = make([]*profiles.Sample, 0, len(originalSamples)*factor)
				for range factor {
					p.Samples = append(p.Samples, originalSamples...)
				}
			}
		}
	}
}

var processAttributes = map[string]struct{}{
	"process.pid":             {},
	"process.executable.name": {},
	"process.executable.path": {},
}

func splitByProcess(data *cprofiles.ExportProfilesServiceRequest) *cprofiles.ExportProfilesServiceRequest {
	newProfile := &cprofiles.ExportProfilesServiceRequest{
		Dictionary: proto.Clone(data.Dictionary).(*profiles.ProfilesDictionary),
	}
	resourceProfilesIdx := map[string]*profiles.ResourceProfiles{}
	for _, rp := range data.ResourceProfiles {
		resourceAttrsStr := hash(keyValuesString(rp.Resource.Attributes, data.Dictionary))
		for si, sp := range rp.ScopeProfiles {
			for pi, p := range sp.Profiles {
				for _, s := range p.Samples {
					newS := &profiles.Sample{
						StackIndex:         s.StackIndex,
						Values:             s.Values,
						AttributeIndices:   nil,
						LinkIndex:          s.LinkIndex,
						TimestampsUnixNano: s.TimestampsUnixNano,
					}
					processAttrs := []*profiles.KeyValueAndUnit{}
					for _, ai := range s.AttributeIndices {
						attr := data.Dictionary.AttributeTable[ai]
						key := data.Dictionary.StringTable[attr.KeyStrindex]
						if _, ok := processAttributes[key]; ok {
							processAttrs = append(processAttrs, attr)
						} else {
							newS.AttributeIndices = append(newS.AttributeIndices, ai)
						}
					}
					processAttrsStr := keyValueAndUnitsString(processAttrs, data.Dictionary)
					combinedHash := hash(resourceAttrsStr, processAttrsStr)
					newRp, ok := resourceProfilesIdx[string(combinedHash)]
					if !ok {
						newRpAttrs := make([]*common.KeyValue, len(rp.Resource.Attributes))
						copy(newRpAttrs, rp.Resource.Attributes)
						for _, pa := range processAttrs {
							if pa.UnitStrindex != 0 {
								panic("process attribute with unit is not supported")
							}
							newRpAttrs = append(newRpAttrs, &common.KeyValue{
								Key:   data.Dictionary.StringTable[pa.KeyStrindex],
								Value: pa.Value,
							})
						}

						newRp = &profiles.ResourceProfiles{
							Resource: &resource.Resource{
								Attributes:             newRpAttrs,
								DroppedAttributesCount: rp.Resource.DroppedAttributesCount,
								EntityRefs:             rp.Resource.EntityRefs,
							},
							ScopeProfiles: make([]*profiles.ScopeProfiles, len(rp.ScopeProfiles)),
							SchemaUrl:     rp.SchemaUrl,
						}
						resourceProfilesIdx[string(combinedHash)] = newRp
						newProfile.ResourceProfiles = append(newProfile.ResourceProfiles, newRp)
					}
					newSp := newRp.ScopeProfiles[si]
					if newSp == nil {
						newSp = &profiles.ScopeProfiles{
							Scope:     sp.Scope,
							Profiles:  make([]*profiles.Profile, len(sp.Profiles)),
							SchemaUrl: sp.SchemaUrl,
						}
						newRp.ScopeProfiles[si] = newSp
					}
					newP := newSp.Profiles[pi]
					if newP == nil {
						if p.OriginalPayload != nil {
							panic("splitting a profile with an original payload is not supported")
						}
						newP = &profiles.Profile{
							SampleType:             p.SampleType,
							Samples:                nil,
							TimeUnixNano:           p.TimeUnixNano,
							DurationNano:           p.DurationNano,
							PeriodType:             p.PeriodType,
							Period:                 p.Period,
							ProfileId:              p.ProfileId,
							DroppedAttributesCount: p.DroppedAttributesCount,
							OriginalPayloadFormat:  p.OriginalPayloadFormat,
							OriginalPayload:        p.OriginalPayload,
							AttributeIndices:       p.AttributeIndices,
						}
						newSp.Profiles[pi] = newP
					}
					newP.Samples = append(newP.Samples, newS)
				}
			}
		}
	}
	return newProfile
}

func hash(values ...string) string {
	h := sha256.New()
	for _, value := range values {
		h.Write([]byte(value))
	}
	return string(h.Sum(nil))
}

func keyValueAndUnitsString(attrs []*profiles.KeyValueAndUnit, dict *profiles.ProfilesDictionary) string {
	attrsCopy := make([]*profiles.KeyValueAndUnit, len(attrs))
	copy(attrsCopy, attrs)
	slices.SortFunc(attrsCopy, func(a, b *profiles.KeyValueAndUnit) int {
		return strings.Compare(dict.StringTable[a.KeyStrindex], dict.StringTable[b.KeyStrindex])
	})
	var parts []string
	for _, attr := range attrsCopy {
		unit := ""
		if attr.UnitStrindex != 0 {
			unit = fmt.Sprintf(" &%s", dict.StringTable[attr.UnitStrindex])
		}
		parts = append(parts, fmt.Sprintf("&%s=%s%s", dict.StringTable[attr.KeyStrindex], anyValueString(attr.Value, dict), unit))
	}
	return strings.Join(parts, ", ")
}

func appendTextProfileToFile(outDir, baseFilename, suffix string, data *cprofiles.ExportProfilesServiceRequest) error {
	outPath := filepath.Join(outDir, baseFilename+"."+suffix+".txt")
	f, err := os.OpenFile(outPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file %q: %w", outPath, err)
	}
	defer f.Close()
	printProfile(f, data)
	return nil
}

func printProfile(out io.Writer, data *cprofiles.ExportProfilesServiceRequest) {
	for _, rp := range data.ResourceProfiles {
		fmt.Fprintf(out, "Resource: %s\n", keyValuesString(rp.Resource.Attributes, data.Dictionary))
		for _, sp := range rp.ScopeProfiles {
			fmt.Fprintf(out, "  Scope: %s: %s\n", sp.Scope.Name, keyValuesString(sp.Scope.Attributes, data.Dictionary))
			for _, p := range sp.Profiles {
				typeStr, unitStr := data.Dictionary.StringTable[p.SampleType.TypeStrindex], data.Dictionary.StringTable[p.SampleType.UnitStrindex]
				end := time.Unix(int64(p.TimeUnixNano/1e9), int64(p.TimeUnixNano%1e9))
				start := end.Add(-time.Duration(p.DurationNano))
				fmt.Fprintf(out, "    Profile: %s=%s (%s - %s)\n", typeStr, unitStr, start.String(), end.String())
				for _, s := range p.Samples {
					attrs := []*profiles.KeyValueAndUnit{}
					for _, ai := range s.AttributeIndices {
						attr := data.Dictionary.AttributeTable[ai]
						attrs = append(attrs, attr)
					}
					fmt.Fprintf(out, "      Sample: %s\n", keyValueAndUnitsString(attrs, data.Dictionary))
				}
			}
		}
	}
}

func keyValuesString(attrs []*common.KeyValue, dict *profiles.ProfilesDictionary) string {
	attrsCopy := make([]*common.KeyValue, len(attrs))
	copy(attrsCopy, attrs)
	slices.SortFunc(attrsCopy, func(a, b *common.KeyValue) int {
		return strings.Compare(a.Key, b.Key)
	})
	parts := []string{}
	for _, attr := range attrsCopy {
		key := attr.Key
		if attr.KeyRef != 0 {
			key = "&" + dict.StringTable[attr.KeyRef]
		}
		parts = append(parts, fmt.Sprintf("%s=%s", key, anyValueString(attr.Value, dict)))
	}
	return strings.Join(parts, ", ")
}

func anyValueString(av *common.AnyValue, dict *profiles.ProfilesDictionary) string {
	switch av.Value.(type) {
	case *common.AnyValue_StringValue:
		return fmt.Sprintf("%q", av.GetStringValue())
	case *common.AnyValue_StringRef:
		str := dict.StringTable[av.GetStringRef()]
		return fmt.Sprintf("&%q", str)
	case *common.AnyValue_IntValue:
		return fmt.Sprintf("%d", av.GetIntValue())
	default:
		return av.String()
	}
}

func useResourceAttrDict(data *cprofiles.ExportProfilesServiceRequest) *cprofiles.ExportProfilesServiceRequest {
	newProfile := &cprofiles.ExportProfilesServiceRequest{
		Dictionary: proto.Clone(data.Dictionary).(*profiles.ProfilesDictionary),
	}

	for _, rp := range data.ResourceProfiles {
		newRp := &profiles.ResourceProfiles{
			Resource: &resource.Resource{
				Attributes:             dictifyKeyValues(rp.Resource.Attributes, newProfile.Dictionary),
				DroppedAttributesCount: rp.Resource.DroppedAttributesCount,
				EntityRefs:             rp.Resource.EntityRefs,
			},
			ScopeProfiles: rp.ScopeProfiles,
			SchemaUrl:     rp.SchemaUrl,
		}
		newProfile.ResourceProfiles = append(newProfile.ResourceProfiles, newRp)
	}

	return newProfile
}

func dictifyKeyValues(attrs []*common.KeyValue, dict *profiles.ProfilesDictionary) []*common.KeyValue {
	newAttrs := make([]*common.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		if attr.KeyRef != 0 {
			newAttrs = append(newAttrs, attr)
			continue
		}

		value := dictAnyValue(attr.Value, dict)
		newAttr := &common.KeyValue{
			KeyRef: dictStrIndex(attr.Key, dict),
			Value:  value,
		}
		newAttrs = append(newAttrs, newAttr)
	}
	return newAttrs
}

func dictAnyValue(av *common.AnyValue, dict *profiles.ProfilesDictionary) *common.AnyValue {
	if _, ok := av.Value.(*common.AnyValue_StringValue); ok {
		return &common.AnyValue{
			Value: &common.AnyValue_StringRef{
				StringRef: dictStrIndex(av.GetStringValue(), dict),
			},
		}
	}
	return av
}

// dictStrIndex returns the index of the string in the dictionary. If the string
// is not found, it is added to the dictionary.
func dictStrIndex(str string, dict *profiles.ProfilesDictionary) int32 {
	for i, s := range dict.StringTable {
		if s == str {
			return int32(i)
		}
	}
	dict.StringTable = append(dict.StringTable, str)
	return int32(len(dict.StringTable) - 1)
}
