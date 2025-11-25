package main

import (
	"context"
	"fmt"
	"os"

	"github.com/open-telemetry/sig-profiling/otlp-bench/versions/v1.9.0/slim/otlp/profiles/v1development"
	"github.com/urfave/cli/v3"
	"google.golang.org/protobuf/proto"
)

// Flag and argument identifiers
const (
	benchOutFlag      = "out"
	benchTruncateFlag = "truncate"
	benchFilesArg     = "files"
)

// newBenchCommand creates the bench command for benchmarking OTLP profile files
func (a *App) newBenchCommand() *cli.Command {
	return &cli.Command{
		Name:  "bench",
		Usage: "Benchmark OTLP encoded profile files",
		Description: `Analyzes one or more OTLP encoded profile files and outputs
benchmark metrics to a CSV file.

The command takes one or more OTLP profile files as input and generates
benchmark results in CSV format. Use the --out flag to specify the output
file path (defaults to otlp-bench.csv) and --truncate to overwrite an
existing output file.`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    benchOutFlag,
				Aliases: []string{"o"},
				Usage:   "Output CSV file path",
				Value:   "otlp-bench.csv",
			},
			&cli.BoolFlag{
				Name:    benchTruncateFlag,
				Aliases: []string{"t"},
				Usage:   "Truncate output file if it exists. By default existing records are updated as needed.",
			},
		},
		Arguments: []cli.Argument{
			&cli.StringArgs{
				Name:      benchFilesArg,
				UsageText: "<file1> [file2] ...",
				Min:       1,
				Max:       -1, // Unlimited
			},
		},
		Action: a.benchAction,
	}
}

// benchAction is the main action handler for the bench command
func (a *App) benchAction(ctx context.Context, cmd *cli.Command) error {
	args, err := a.parseBenchArgs(cmd)
	if err != nil {
		return err
	}

	a.Log.Info("bench command invoked",
		"files", args.Files,
		"out", args.Out,
		"truncate", args.Truncate,
	)

	for _, file := range args.Files {
		if err := a.benchFile(file); err != nil {
			return err
		}
	}

	return nil
}

func (a *App) benchFile(file string) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	profilesData := &v1development.ProfilesData{}
	if err := proto.Unmarshal(data, profilesData); err != nil {
		return fmt.Errorf("unmarshal profiles data: %w", err)
	}

	fmt.Printf("len(profilesData.Dictionary.StringTable): %v\n", len(profilesData.Dictionary.StringTable))

	return nil
}

// benchArgs holds the parsed command arguments and flags
type benchArgs struct {
	Files    []string
	Out      string
	Truncate bool
}

// parseBenchArgs parses and validates the bench command arguments
func (a *App) parseBenchArgs(cmd *cli.Command) (benchArgs, error) {
	args := benchArgs{
		Files:    cmd.StringArgs(benchFilesArg),
		Out:      cmd.String(benchOutFlag),
		Truncate: cmd.Bool(benchTruncateFlag),
	}

	if len(args.Files) == 0 {
		return benchArgs{}, cli.Exit("at least one input file required", 1)
	}

	return args, nil
}
