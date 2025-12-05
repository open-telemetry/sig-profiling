package main

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/open-telemetry/sig-profiling/otlp-bench/internal/otlpbuild"
)

func main() {
	if err := run(context.Background(), os.Stdout, os.Stderr, os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, stdout, stderr io.Writer, args []string) (err error) {
	args = args[1:]
	if len(args) != 4 {
		return fmt.Errorf("expected 4 arguments, got %d", len(args))
	}
	remote, revision, dst, pkgPrefix := args[0], args[1], args[2], args[3]

	dstAbs, err := filepath.Abs(dst)
	if err != nil {
		return fmt.Errorf("resolve destination directory: %w", err)
	}

	// N.b. we are creating the temporary direcory in the dst directory because
	// /tmp is often mounted on a different filesystem than the dst directory
	// which causes issues with the docker volume sharing.
	tmpDir := filepath.Join(filepath.Dir(dst), "tmp")
	if err := os.RemoveAll(tmpDir); err != nil {
		return fmt.Errorf("remove temporary directory: %w", err)
	}
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	defer func() {
		if os.Getenv("KEEP_TMP_DIR") != "" || err != nil {
			fmt.Fprintf(stderr, "keeping temporary directory: %s\n", tmpDir)
			return
		}
		err = cmp.Or(err, os.RemoveAll(tmpDir))
	}()

	cloneDir := filepath.Join(tmpDir, "clone")
	if err = clone(ctx, remote, revision, cloneDir); err != nil {
		return fmt.Errorf("clone: %w", err)
	}

	buildDir := filepath.Join(tmpDir, "build")
	if err := otlpbuild.Build(ctx, otlpbuild.Config{
		SrcDir:        filepath.Join(cloneDir, "opentelemetry"),
		TmpDir:        buildDir,
		DstDir:        dstAbs,
		PackagePrefix: pkgPrefix,
	}); err != nil {
		return fmt.Errorf("build: %w", err)
	}

	fmt.Fprintf(stdout, "built %s\n", dst)
	return nil
}

func clone(ctx context.Context, remote, revision, cloneDir string) error {
	if err := os.RemoveAll(cloneDir); err != nil {
		return fmt.Errorf("remove clone directory: %w", err)
	}

	var buf bytes.Buffer
	cmd := exec.CommandContext(ctx, "git", "clone", remote, "--revision", revision, cloneDir)
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git clone: %w: %s", err, buf.String())
	}
	return nil
}
