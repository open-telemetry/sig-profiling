package otlpbuild

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type Config struct {
	// SrcDir is the path to opentelemetry directory, typically inside a copy of
	// the opentelemetry-proto repository.
	SrcDir string
	// TmpDir is the path to a temporary directory that will be used to build a
	// version of the OTLP proto files.
	TmpDir string
	// DstDir is the path to the directory where the built OTLP Go files will be
	// stored.
	DstDir string
	// PackagePrefix is the prefix to use for the Go package names.
	PackagePrefix string
}

// Build builds the OTLP Go bindings and uses the base name of the DstDir as a
// namespace to allow importing multiple versions of the same proto files into
// the same program.
func Build(ctx context.Context, c Config) error {
	// derive srcDir
	srcDir, err := filepath.Abs(filepath.Join(c.TmpDir, "src"))
	if err != nil {
		return fmt.Errorf("get absolute path: %w", err)
	}

	// derive namespace directory
	namespace := filepath.Base(c.DstDir)
	namespaceDir := filepath.Join(srcDir, namespace)

	// derive dstDir
	dstDir, err := filepath.Abs(c.DstDir)
	if err != nil {
		return fmt.Errorf("get absolute path: %w", err)
	}

	// copy srcDir to nameSpaceDir
	if err := os.CopyFS(filepath.Join(namespaceDir, "opentelemetry"), os.DirFS(c.SrcDir)); err != nil {
		return fmt.Errorf("copy source directory: %w", err)
	}

	// find proto files
	protoFiles, err := findProtoFiles(ctx, namespaceDir)
	if err != nil {
		return fmt.Errorf("find proto files: %w", err)
	}

	// rewrite proto files
	for _, protoFile := range protoFiles {
		if err := rewriteProtoFile(protoFile, namespace, c.PackagePrefix); err != nil {
			return fmt.Errorf("rewrite proto file: %w", err)
		}
	}

	// compile proto files
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return fmt.Errorf("create destination directory: %w", err)
	}
	if err := compileProtoFiles(ctx, c.TmpDir, srcDir, namespace, dstDir, protoFiles); err != nil {
		return fmt.Errorf("compile proto files: %w", err)
	}

	return nil

}

func findProtoFiles(ctx context.Context, protoRootDir string) ([]string, error) {
	if _, err := os.Stat(protoRootDir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("find proto files: %w", err)
	}

	var protoFiles []string
	walkErr := filepath.WalkDir(protoRootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if d.IsDir() {
			return nil
		}
		if filepath.Ext(d.Name()) == ".proto" {
			protoFiles = append(protoFiles, path)
		}
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("find proto files: %w", walkErr)
	}

	sort.Strings(protoFiles)
	return protoFiles, nil

}

func rewriteProtoFile(protoFile, namespace, pkgPrefix string) error {
	content, err := os.ReadFile(protoFile)
	if err != nil {
		return fmt.Errorf("read proto file: %w", err)
	}
	content = rewriteProtoFileData(content, namespace, pkgPrefix)
	if err := os.WriteFile(protoFile, content, 0o644); err != nil {
		return fmt.Errorf("write proto file: %w", err)
	}
	return nil
}

var packageRe = regexp.MustCompile(`(?m)^package\s+(opentelemetry\.proto\.)`)
var importRe = regexp.MustCompile(`(?m)^import\s+"(opentelemetry/proto)`)
var goPackageRe = regexp.MustCompile(`(?m)^option go_package = "go\.opentelemetry\.io/proto/otlp`)

func rewriteProtoFileData(data []byte, namespace, pkgPrefix string) []byte {
	pkgNamespace := namespaceHash(namespace)
	data = packageRe.ReplaceAll(data, []byte(`package `+pkgNamespace+`.$1`))
	data = importRe.ReplaceAll(data, []byte(`import "`+namespace+`/$1`))
	data = goPackageRe.ReplaceAll(data, []byte(`option go_package = "`+pkgPrefix+`/`+namespace+"/opentelemetry/proto"))
	return data
}

func namespaceHash(namespace string) string {
	hash := sha1.Sum([]byte(namespace))
	encoded := make([]byte, len(hash)*2)
	for i, b := range hash {
		encoded[i*2] = 'a' + (b >> 4)
		encoded[i*2+1] = 'a' + (b & 0x0f)
	}
	return string(encoded)
}

func compileProtoFiles(ctx context.Context, tmpDir, protoDir, namespace, dstDir string, protoFiles []string) error {
	uid := os.Getuid()

	absTmpDir, err := filepath.Abs(tmpDir)
	if err != nil {
		return fmt.Errorf("get absolute temporary directory: %w", err)
	}

	tmpDstDir := filepath.Join(absTmpDir, "dst")
	if err := os.MkdirAll(tmpDstDir, 0o755); err != nil {
		return fmt.Errorf("create destination directory: %w", err)
	}

	cmdArgs := []string{
		"docker",
		"run",
		"--rm",
		"-u", fmt.Sprintf("%d", uid),
		"-v", fmt.Sprintf("%s:%s", absTmpDir, absTmpDir),
		"-w", absTmpDir,
		"otel/build-protobuf:0.9.0",
		"--proto_path=" + protoDir,
		"--go_opt=paths=source_relative",
		"--go_out=" + tmpDstDir,
	}
	cmdArgs = append(cmdArgs, protoFiles...)

	var buf bytes.Buffer
	cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
	cmd.Dir = absTmpDir
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %s: %s", strings.Join(cmdArgs, " "), err, buf.String())
	}

	// copy to dstDir
	if err := os.RemoveAll(dstDir); err != nil {
		return fmt.Errorf("remove destination directory: %w", err)
	}
	if err := os.CopyFS(dstDir, os.DirFS(filepath.Join(tmpDstDir, namespace))); err != nil {
		return fmt.Errorf("copy tmp dst to final dst directory: %w", err)
	}

	return nil
}
