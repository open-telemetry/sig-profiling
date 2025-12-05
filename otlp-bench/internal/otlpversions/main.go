package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
)

//go:generate go run .

func main() {
	if err := run(context.Background(), os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, stdout, stderr io.Writer) error {
	versions := []struct {
		Repo     string
		Revision string
		Name     string
	}{
		// https://github.com/open-telemetry/opentelemetry-proto/pull/733
		{
			Repo:     "https://github.com/florianl/opentelemetry-proto.git",
			Revision: "0ae008f9dafa0939410169b8296eef86ebad8f49",
			Name:     "gh733",
		},
	}

	for _, version := range versions {
		pkgPrefix := "github.com/open-telemetry/sig-profiling/otlp-bench/internal/otlpversions"
		cmd := exec.CommandContext(ctx, "go", "run", "../otlpgen", version.Repo, version.Revision, version.Name, pkgPrefix)
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		if err := cmd.Run(); err != nil {
			return err
		}
	}
	return nil
}
