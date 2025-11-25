package main

import (
	"context"
	"os"
	"testing"

	"github.com/urfave/cli/v3"
)

func TestVersionAdd(t *testing.T) {
	for _, removeBefore := range []bool{true, false} {
		builder := testAppBuilder{RemoveDirBefore: removeBefore, RemoveDirAfter: !removeBefore}
		app := builder.Build(t)
		err := app.Run(t.Context(), []string{"", "version", "add", "dict", "bcbee5a324cae805e296be1bf0e4edc8d6da6585", "https://github.com/florianl/opentelemetry-proto.git", "--verbose"})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestBench(t *testing.T) {
	builder := testAppBuilder{}
	app := builder.Build(t)
	err := app.Run(t.Context(), []string{"", "bench", "testdata/profile.otlp", "--verbose"})
	if err != nil {
		t.Fatal(err)
	}
}

type testAppBuilder struct {
	RemoveDirBefore bool
	RemoveDirAfter  bool
}

func (b testAppBuilder) Build(t *testing.T) *App {
	t.Helper()
	a := &App{
		Writer:         t.Output(),
		ErrWriter:      t.Output(),
		VersionDir:     "testdata/versions",
		ExitErrHandler: func(ctx context.Context, c *cli.Command, err error) {},
	}
	removeDir := func() {
		if err := os.RemoveAll(a.VersionDir); err != nil {
			t.Fatal(err)
		}

	}
	if b.RemoveDirBefore {
		removeDir()
	}
	if b.RemoveDirAfter {
		t.Cleanup(func() {
			if err := os.RemoveAll(a.VersionDir); err != nil {
				t.Fatal(err)
			}
		})
	}
	return a
}
