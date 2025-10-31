package main

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/lmittmann/tint"
	"github.com/urfave/cli/v3"
)

func main() {
	var app App
	app.Run(context.Background(), os.Args)
}

type App struct {
	Writer         io.Writer
	ErrWriter      io.Writer
	Log            *slog.Logger
	VersionDir     string
	ExitErrHandler func(ctx context.Context, c *cli.Command, err error)
}

func (b *App) Run(ctx context.Context, args []string) error {
	cmd := b.newRootCommand()
	return cmd.Run(ctx, args)
}

func (a *App) newRootCommand() *cli.Command {
	// Start app initialization
	a.Writer = cmp.Or(a.Writer, io.Writer(os.Stdout))
	a.ErrWriter = cmp.Or(a.ErrWriter, io.Writer(os.Stderr))
	if a.ExitErrHandler == nil {
		a.ExitErrHandler = func(ctx context.Context, c *cli.Command, err error) {
			a.Log.Error("command failed", "error", err)
			os.Exit(1)
		}
	}

	root := &cli.Command{
		Name:  "otlp-bench",
		Usage: "Manage OTLP profiling benchmarks",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "verbose",
				Usage: "Enable verbose command output",
			},
		},
		Before:         a.before,
		Writer:         a.Writer,
		ErrWriter:      a.ErrWriter,
		ExitErrHandler: a.ExitErrHandler,
		Commands: []*cli.Command{
			a.newBenchCommand(),
			{
				Name:  "version",
				Usage: "Manage OTLP proto versions",
				Commands: []*cli.Command{
					a.newVersionAddCommand(),
				},
			},
		},
	}
	return root
}

const (
	rootVerboseFlag = "verbose"
)

func (a *App) before(ctx context.Context, cmd *cli.Command) (context.Context, error) {
	// Init logger
	if a.Log == nil {
		handlerOpts := &tint.Options{Level: slog.LevelDebug}
		if cmd.Bool(rootVerboseFlag) {
			handlerOpts.Level = slog.LevelDebug
		}
		tint.NewHandler(a.ErrWriter, handlerOpts)
		handler := tint.NewHandler(a.Writer, handlerOpts)
		logger := slog.New(handler)
		a.Log = logger
	}

	// Resolve versions directory
	if a.VersionDir == "" {
		a.VersionDir = "versions"
	}
	var err error
	a.VersionDir, err = filepath.Abs(a.VersionDir)
	if err != nil {
		return ctx, fmt.Errorf("resolve versions dir: %w", err)
	}
	if !strings.HasPrefix(a.VersionDir, goModRoot()) {
		return ctx, fmt.Errorf("versions dir must be a subdirectory of the go mod root: %s is not a subdirectory of %s", a.VersionDir, goModRoot())
	}

	return ctx, nil
}

func (a *App) exec(ctx context.Context, dir string, name string, args ...string) error {
	command := fmt.Sprintf("%s %s", name, strings.Join(args, " "))
	a.Log.Info("executing command", "command", command, "dir", dir)
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Stdout = a.Writer
	cmd.Stderr = a.ErrWriter
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("execute command %s: %w", command, err)
	}
	return nil
}

func goModRoot() string {
	_, file, _, ok := runtime.Caller(1)
	if !ok {
		return ""
	}
	return filepath.Dir(file)
}
