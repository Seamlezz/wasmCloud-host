package main

import (
	"dagger/wasmcloud-host/internal/dagger"
	"fmt"
)

const (
	workspace    = "/workspace"
	builderImage = "cgr.dev/chainguard/rust:latest-dev"
	runtimeImage = "cgr.dev/chainguard/wolfi-base"
	binaryPath   = workspace + "/target/release/wasmcloud-host"
	stagedBinary = "/out/wasmcloud-host"
	installPath  = "/usr/local/bin/wasmcloud-host"
)

var publishPlatforms = []dagger.Platform{
	"linux/amd64",
	"linux/arm64",
}

func (m *WasmcloudHost) container(platform dagger.Platform) *dagger.Container {
	cargoRegistry := dag.CacheVolume("wasmcloud-host-cargo-registry")
	targetCache := dag.CacheVolume(fmt.Sprintf("wasmcloud-host-target-%s", platform))

	return dag.Container(dagger.ContainerOpts{Platform: platform}).
		From(builderImage).
		WithUser("0").
		WithExec([]string{"apk", "add", "--no-cache", "protoc", "protobuf", "protobuf-dev"}).
		WithDirectory(workspace, m.Source).
		WithWorkdir(workspace).
		WithMountedCache("/usr/local/cargo/registry", cargoRegistry).
		WithMountedCache(workspace+"/target", targetCache).
		WithEnvVariable("CARGO_HOME", "/usr/local/cargo")
}

func (m *WasmcloudHost) withChecks(platform dagger.Platform) *dagger.Container {
	return m.container(platform).
		WithExec([]string{"cargo", "clippy", "--all-targets", "--", "-D", "warnings"}).
		WithExec([]string{"cargo", "test"})
}

func (m *WasmcloudHost) buildBinary(platform dagger.Platform) *dagger.Container {
	return m.container(platform).
		WithExec([]string{
			"cargo", "build", "--release",
			"-p", "wasmcloud-host-runtime",
			"--bin", "wasmcloud-host",
		}).
		WithDirectory("/out", dag.Directory()).
		WithExec([]string{"cp", binaryPath, stagedBinary})
}

func (m *WasmcloudHost) runtimeImageForPlatform(platform dagger.Platform) *dagger.Container {
	builder := m.buildBinary(platform)
	perms := 0o755

	return dag.Container(dagger.ContainerOpts{Platform: platform}).
		From(runtimeImage).
		WithFile(installPath, builder.File(stagedBinary), dagger.ContainerWithFileOpts{
			Permissions: perms,
		}).
		WithEnvVariable("RUST_LOG", "info").
		WithEntrypoint([]string{installPath})
}

func (m *WasmcloudHost) platformVariants() []*dagger.Container {
	variants := make([]*dagger.Container, 0, len(publishPlatforms))
	for _, platform := range publishPlatforms {
		variants = append(variants, m.runtimeImageForPlatform(platform))
	}
	return variants
}

func (m *WasmcloudHost) labeledVariants(tag string) []*dagger.Container {
	repoURL := "https://github.com/Seamlezz/wasmCloud-host"
	variants := m.platformVariants()
	labeled := make([]*dagger.Container, len(variants))
	for i, ctr := range variants {
		labeled[i] = ctr.
			WithLabel("org.opencontainers.image.version", tag).
			WithLabel("org.opencontainers.image.source", repoURL)
	}
	return labeled
}
