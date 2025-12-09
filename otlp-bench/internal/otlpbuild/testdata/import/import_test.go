//go:build import_test

package import_test

import (
	_ "github.com/open-telemetry/sig-profiling/otlp-bench/internal/otlpbuild/testdata/dst/foo/opentelemetry/proto/profiles/v1development"
	_ "github.com/open-telemetry/sig-profiling/otlp-bench/internal/otlpbuild/testdata/dst/v1.9.0-bar-baz/opentelemetry/proto/profiles/v1development"
)
