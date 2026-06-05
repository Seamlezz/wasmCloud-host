package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"

	"dagger/wasmcloud-host/internal/dagger"
)

type cargoWorkspace struct {
	Workspace struct {
		Package struct {
			Version string `toml:"version"`
		} `toml:"package"`
	} `toml:"workspace"`
}

func workspaceVersion(ctx context.Context, cargo *dagger.File) (string, error) {
	raw, err := cargo.Contents(ctx)
	if err != nil {
		return "", fmt.Errorf("read Cargo.toml: %w", err)
	}
	var doc cargoWorkspace
	if _, err := toml.Decode(raw, &doc); err != nil {
		return "", fmt.Errorf("parse Cargo.toml: %w", err)
	}
	v := doc.Workspace.Package.Version
	if v == "" {
		return "", fmt.Errorf("missing [workspace.package].version in Cargo.toml")
	}
	return v, nil
}

func imageTagExists(
	ctx context.Context,
	registry string,
	image string,
	tag string,
	username string,
	password *dagger.Secret,
) (bool, error) {
	ref := fmt.Sprintf("%s/%s:%s", registry, image, tag)
	auth := dag.Container().WithRegistryAuth(registry, username, password)
	_, err := auth.From(ref).Sync(ctx)
	if err == nil {
		return true, nil
	}
	if isManifestNotFound(err) {
		return false, nil
	}
	return false, err
}

func isManifestNotFound(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not found") ||
		strings.Contains(msg, "manifest unknown") ||
		strings.Contains(msg, "404")
}
