package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v3"
)

func (a *App) newVersionAddCommand() *cli.Command {
	return &cli.Command{
		Name:  "add",
		Usage: "Add a new OTLP proto version",
		Description: strings.TrimSpace(`
Clones opentelemetry-proto-go, points its opentelemetry-proto submodule
to the requested commit (and optional remote), runs code generation, and
adds the new version to the versions directory. If the version already exists,
it will be updated.`),
		Arguments: a.makeVersionAddArgs(),
		Action:    a.versionAddAction,
	}
}

const (
	versionAddNameArg   = "name"
	versionAddRefArg    = "git-ref"
	versionAddRemoteArg = "git-remote"
)

func (a *App) makeVersionAddArgs() []cli.Argument {
	return []cli.Argument{
		&cli.StringArg{
			Name:      versionAddNameArg,
			UsageText: "<" + versionAddNameArg + ">",
			Config:    cli.StringConfig{TrimSpace: true},
		},
		&cli.StringArg{
			Name:      versionAddRefArg,
			UsageText: "<" + versionAddRefArg + ">",
			Config:    cli.StringConfig{TrimSpace: true},
		},
		&cli.StringArgs{
			Name:      versionAddRemoteArg,
			UsageText: "[" + versionAddRemoteArg + "]",
			Max:       1,
			Config:    cli.StringConfig{TrimSpace: true},
		},
	}
}

func (a *App) versionAddAction(ctx context.Context, cmd *cli.Command) error {
	args, err := a.parseVersionAddArgs(cmd)
	if err != nil {
		return err
	}
	a.Log.Info("version add", "name", args.Name, "commit", args.Ref, "remote", args.Remote, "dir", a.VersionDir)

	protoGoDir := filepath.Join(a.VersionDir, ".opentelemetry-proto-go")
	if _, err := os.Stat(filepath.Join(protoGoDir, ".git")); errors.Is(err, os.ErrNotExist) {
		// TODO make this atomic by using a temporary directory and moving it
		// after all steps are successful
		if err := os.MkdirAll(a.VersionDir, 0o755); err != nil {
			return fmt.Errorf("ensure proto-go directory: %w", err)
		}

		if err := a.exec(ctx, a.VersionDir, "git", "clone", "https://github.com/open-telemetry/opentelemetry-proto-go.git", protoGoDir); err != nil {
			return fmt.Errorf("git clone proto-go: %w", err)
		}
	}

	if err := a.exec(ctx, protoGoDir, "git", "pull"); err != nil {
		return fmt.Errorf("git pull: %w", err)
	}

	if err := a.exec(ctx, protoGoDir, "git", "submodule", "update", "--init"); err != nil {
		return fmt.Errorf("update proto submodule: %w", err)
	}

	protoDir := filepath.Join(protoGoDir, "opentelemetry-proto")
	if err := a.exec(ctx, protoDir, "git", "fetch", args.Remote, args.Ref); err != nil {
		return fmt.Errorf("fetch proto ref: %w", err)
	}

	relDir := filepath.Join(strings.TrimPrefix(a.VersionDir, goModRoot()), args.Name)

	makeArgs := []string{
		"submodule-version",
		"clean",
		"gen-otlp-protobuf-slim",
		"VERSION=" + args.Ref,
		"GO_MOD_ROOT=github.com/open-telemetry/sig-profiling/otlp-bench" + relDir,
	}
	if err := a.exec(ctx, protoGoDir, "make", makeArgs...); err != nil {
		return fmt.Errorf("update proto submodule: %w", err)
	}

	srcDir := filepath.Join(protoGoDir, "gen", "go", "github.com", "open-telemetry", "sig-profiling", "otlp-bench", relDir)
	dstDir := filepath.Join(a.VersionDir, args.Name)
	if err := os.RemoveAll(dstDir); err != nil {
		return fmt.Errorf("remove destination directory: %w", err)
	}
	if err := a.exec(ctx, a.VersionDir, "cp", "-r", srcDir, dstDir); err != nil {
		return fmt.Errorf("copy generated code: %w", err)
	}

	// Create version.json file with version metadata
	versionJSON, err := json.MarshalIndent(args, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal version metadata: %w", err)
	}

	versionJSONPath := filepath.Join(dstDir, "version.json")
	if err := os.WriteFile(versionJSONPath, versionJSON, 0644); err != nil {
		return fmt.Errorf("write version.json: %w", err)
	}

	fmt.Fprintf(a.Writer, "version added: %s at %s\n", args.Name, args.Ref)

	return nil
}

type versionAddArgs struct {
	Name   string `json:"name"`
	Ref    string `json:"ref"`
	Remote string `json:"remote"`
}

func (a *App) parseVersionAddArgs(cmd *cli.Command) (versionAddArgs, error) {
	var args versionAddArgs
	args.Name = cmd.StringArg(versionAddNameArg)
	if args.Name == "" {
		return versionAddArgs{}, cli.Exit("name cannot be empty", 1)
	}

	args.Ref = cmd.StringArg(versionAddRefArg)
	if args.Ref == "" {
		return versionAddArgs{}, cli.Exit("commit or tag cannot be empty", 1)
	}

	if remotes := cmd.StringArgs(versionAddRemoteArg); len(remotes) > 0 {
		args.Remote = remotes[0]
		if args.Remote == "" {
			return versionAddArgs{}, cli.Exit("remote URL cannot be empty", 1)
		}
	} else {
		args.Remote = "https://github.com/open-telemetry/opentelemetry-proto.git"
	}

	return args, nil
}
