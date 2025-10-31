# otlp-bench

`otlp-bench` is a tool for comparing different variants for encoding profiling data as OTLP.

A variant is a combination of a specific version (commit) of the opentelemetry-proto repository as well as code for encoding profiling data as OTLP.

## Managing Versions

### Example Usage

Adding new versions of the OTLP profiling protocol is done via the `version add` command.

```bash
# Add a specific version from upstream
./otlp-bench version add <name> <git-ref>

# Add a version from a fork
./otlp-bench version add <name> <git-ref> <git-remote-url>
```

### Flags

- `--verbose` prints every shell command that the tool launches.

Generated assets are stored in `./versions` using the system `git`, `make`, and `go` binaries, and cloning from the upstream `opentelemetry-proto-go` repository. Each invocation writes metadata to `./versions/versions.json` and initialises/updates `./versions/go.work` so that the generated modules can be consumed immediately.

### How It Works

1. Clones the latest `opentelemetry-proto-go` (build system)
2. Points its submodule at the specified commit/fork
3. Generates Go code with custom module paths
4. Makes versions available via Go workspace
