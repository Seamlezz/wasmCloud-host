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

func (m *WasmcloudHost) NeedsPublish(
	ctx context.Context,
	registry string,
	image string,
	username string,
	password *dagger.Secret,
	// +optional
	force bool,
) (bool, error) {
	if force {
		return true, nil
	}

	version, err := m.RuntimeVersion(ctx)
	if err != nil {
		return false, err
	}

	exists, err := imageTagExists(ctx, registry, image, version, username, password)
	if err != nil {
		return false, err
	}
	return !exists, nil
}

func (m *WasmcloudHost) Build(
	ctx context.Context,
	// +optional
	platform dagger.Platform,
) (*dagger.Container, error) {
	if platform == "" {
		var err error
		platform, err = dag.DefaultPlatform(ctx)
		if err != nil {
			return nil, err
		}
	}
	return m.runtimeImageForPlatform(platform), nil
}

func (m *WasmcloudHost) RuntimeVersion(ctx context.Context) (string, error) {
	return workspaceVersion(ctx, m.Source.File("Cargo.toml"))
}

func platformTagSuffix(platform dagger.Platform) (string, error) {
	switch platform {
	case "linux/amd64":
		return "amd64", nil
	case "linux/arm64":
		return "arm64", nil
	default:
		return "", fmt.Errorf("unsupported publish platform %q", platform)
	}
}

func (m *WasmcloudHost) PublishPlatform(
	ctx context.Context,
	registry string,
	image string,
	platform dagger.Platform,
	username string,
	password *dagger.Secret,
) (string, error) {
	version, err := m.RuntimeVersion(ctx)
	if err != nil {
		return "", err
	}
	suffix, err := platformTagSuffix(platform)
	if err != nil {
		return "", err
	}
	tag := fmt.Sprintf("%s-%s", version, suffix)

	return m.runtimeImageForPlatform(platform).
		WithLabel("org.opencontainers.image.version", version).
		WithLabel("org.opencontainers.image.source", "https://github.com/Seamlezz/wasmCloud-host").
		WithRegistryAuth(registry, username, password).
		Publish(ctx, fmt.Sprintf("%s/%s:%s", registry, image, tag))
}

func (m *WasmcloudHost) Publish(
	ctx context.Context,
	registry string,
	image string,
	// +optional
	tag string,
	username string,
	password *dagger.Secret,
	// +optional
	// +default=true
	includeLatest bool,
) (string, error) {
	if tag == "" {
		var err error
		tag, err = m.RuntimeVersion(ctx)
		if err != nil {
			return "", err
		}
	}

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
