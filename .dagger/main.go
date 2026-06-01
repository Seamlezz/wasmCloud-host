// Build and publish the wasmcloud-host runtime container image.
package main

import (
	"context"
	"fmt"

	"dagger/wasmcloud-host/internal/dagger"
)

type WasmcloudHost struct {
	Source *dagger.Directory `json:"source"`
}

func New(
	// +defaultPath="/"
	// +ignore=["target", ".git", ".dagger", "docs"]
	source *dagger.Directory,
) *WasmcloudHost {
	return &WasmcloudHost{Source: source}
}

// +check
func (m *WasmcloudHost) Check(ctx context.Context) (*dagger.Container, error) {
	platform, err := dag.DefaultPlatform(ctx)
	if err != nil {
		return nil, err
	}
	return m.withChecks(platform), nil
}

func (m *WasmcloudHost) Build(
	// +optional
	// +default="linux/amd64"
	platform dagger.Platform,
) *dagger.Container {
	return m.runtimeImageForPlatform(platform)
}

func (m *WasmcloudHost) RuntimeVersion(ctx context.Context) (string, error) {
	return workspaceVersion(ctx, m.Source.File("Cargo.toml"))
}

func (m *WasmcloudHost) PublishIfNeeded(
	ctx context.Context,
	registry string,
	image string,
	username string,
	password *dagger.Secret,
	// +optional
	force bool,
	// +optional
	dryRun bool,
	// +optional
	// +default=true
	includeLatest bool,
) (string, error) {
	version, err := m.RuntimeVersion(ctx)
	if err != nil {
		return "", err
	}

	if !force {
		exists, err := imageTagExists(ctx, registry, image, version, username, password)
		if err != nil {
			return "", err
		}
		if exists {
			return fmt.Sprintf("skipped: %s/%s:%s already exists", registry, image, version), nil
		}
	}

	if dryRun {
		return fmt.Sprintf("dry-run: would publish %s/%s:%s", registry, image, version), nil
	}

	return m.Publish(ctx, registry, image, version, username, password, includeLatest)
}

func (m *WasmcloudHost) Publish(
	ctx context.Context,
	registry string,
	image string,
	tag string,
	username string,
	password *dagger.Secret,
	// +optional
	// +default=true
	includeLatest bool,
) (string, error) {
	variants := m.labeledVariants(tag)
	auth := dag.Container().WithRegistryAuth(registry, username, password)

	versionRef, err := auth.Publish(ctx, fmt.Sprintf("%s/%s:%s", registry, image, tag), dagger.ContainerPublishOpts{
		PlatformVariants: variants,
	})
	if err != nil {
		return "", err
	}

	if !includeLatest {
		return versionRef, nil
	}

	_, err = auth.Publish(ctx, fmt.Sprintf("%s/%s:latest", registry, image), dagger.ContainerPublishOpts{
		PlatformVariants: variants,
	})
	if err != nil {
		return "", err
	}

	return versionRef, nil
}
