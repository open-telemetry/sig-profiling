Implementation Plan
===================

- Establish CLI entry point in `main.go` that constructs a root `cli.Command` and calls `Run(ctx, os.Args)`; wire global options (for example workspace directory or verbosity) so they are accessible to subcommands via `cmd.Context()`.
- Define a `version` subcommand (`&cli.Command{Name: "version", Commands: []*cli.Command{...}}`) and nest an `add` command whose `Usage` matches existing README guidance; implement its `Action` as `func(ctx context.Context, cmd *cli.Command) error` to align with the urfave/cli v3 API.
- Inside the `Action`, validate positional arguments with `cmd.Args()`: require `<module>` and `<commit>`; accept optional `<remote>`; surface friendly usage errors via `cli.Exit` to ensure non-zero exit codes when inputs are invalid.
- Prepare workspace configuration discovery (for example determine locations for `opentelemetry-proto-go`, staging directories, or Go workspace files); support environment variables or flags so the command works both locally and in CI.
- Implement git workflow helpers (wrappers around `exec.CommandContext`) to clone or fetch `opentelemetry-proto-go`, retarget its submodule to the requested commit or remote, and record new version metadata before any code generation steps.
- Orchestrate generation and linkage steps: invoke proto generators, rewrite module paths if required, and update Go workspace state (`go.work` or similar) without disturbing existing entries; write concise progress output via `cmd.Writer`.
- Add automated coverage: unit tests for argument validation and helper logic (using temporary directories and mocked exec calls), plus an integration-style test gated by `testing.Short()` that exercises the happy path with local fixtures; document manual verification commands in the README.
- Integrate the command into existing build/test workflows (`go test ./...`, `go vet`, lint) and consider follow-up enhancements such as configurable cache directories or a dry-run flag to keep the CLI flexible.
