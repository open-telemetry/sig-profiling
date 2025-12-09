package otlpbuild

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestBuild(t *testing.T) {
	tmpDir := testSetupTmpDir(t)
	var wg sync.WaitGroup
	for _, namespace := range []string{"foo", "v1.9.0-bar-baz"} {
		wg.Go(func() {
			testBuild(t, tmpDir, namespace)
		})
	}
	wg.Wait()
	testImport(t)
}

func testSetupTmpDir(t *testing.T) string {
	tmpDir := filepath.Join("testdata", "tmp")
	if err := os.RemoveAll(tmpDir); err != nil {
		t.Fatalf("failed to remove temporary directory: %v", err)
	}
	return tmpDir
}

func testBuild(t *testing.T, tmpDir, namespace string) {
	t.Helper()
	config := Config{
		SrcDir:        filepath.Join("testdata", "src", "opentelemetry"),
		TmpDir:        tmpDir,
		DstDir:        filepath.Join("testdata", "dst", namespace),
		PackagePrefix: "github.com/open-telemetry/sig-profiling/otlp-bench/internal/otlpbuild/testdata/dst",
	}
	if err := Build(t.Context(), config); err != nil {
		t.Fatalf("failed to build OTLP: %v", err)
	}
}

func testImport(t *testing.T) {
	t.Helper()
	var buf bytes.Buffer
	cmd := exec.CommandContext(t.Context(), "go", "test", "-tags", "import_test", "./testdata/import")
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to test otlpimport: %v\n\n%s\n", err, buf.String())
	}
}

func TestRewriteProtoFile(t *testing.T) {
	in := bytes.TrimSpace([]byte(`
syntax = "proto3";

package opentelemetry.proto.logs.v1;

import "foo/opentelemetry/proto/common/v1/common.proto";
import "foo/opentelemetry/proto/resource/v1/resource.proto";

option csharp_namespace = "OpenTelemetry.Proto.Logs.V1";
option java_multiple_files = true;
option java_package = "io.opentelemetry.proto.logs.v1";
option java_outer_classname = "LogsProto";
option go_package = "go.opentelemetry.io/proto/otlp/logs/v1";
`))
	want := bytes.TrimSpace([]byte(`
syntax = "proto3";

package aloomhlfokdpapnlmjfnannehpdmflmchfnkikdd.opentelemetry.proto.logs.v1;

import "foo/opentelemetry/proto/common/v1/common.proto";
import "foo/opentelemetry/proto/resource/v1/resource.proto";

option csharp_namespace = "OpenTelemetry.Proto.Logs.V1";
option java_multiple_files = true;
option java_package = "io.opentelemetry.proto.logs.v1";
option java_outer_classname = "LogsProto";
option go_package = "github.com/a/b/foo/opentelemetry/proto/logs/v1";
`))

	got := rewriteProtoFileData(in, "foo", "github.com/a/b")
	if diff := cmp.Diff(string(want), string(got)); diff != "" {
		t.Errorf("rewriteProtoFileData mismatch (-want +got):\n%s", diff)
	}

}
