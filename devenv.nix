# Script names are shared with the other repos of this stack. They avoid `test` and `fmt`,
# which lose to the shell builtin and coreutils.
# `devenv shell` gives Rust, protoc and the scripts below. Image scripts use the host's Docker.
# `devenv test` runs the full local check.
{ pkgs, ... }:

let
  image = "ghcr.io/seamlezz/wasmcloud-host";
in
{
  languages.rust = {
    enable = true;
    channel = "stable";
    version = "1.95.0"; # workspace rust-version
    components = [ "rustc" "cargo" "clippy" "rustfmt" "rust-analyzer" ];
  };

  packages = [ pkgs.git pkgs.jq pkgs.protobuf ];

  scripts = {
    format = {
      description = "Format all Rust code";
      exec = ''cd "$DEVENV_ROOT" && cargo fmt --all'';
    };
    format-check = {
      description = "Fail when Rust code is not formatted";
      exec = ''cd "$DEVENV_ROOT" && cargo fmt --all --check'';
    };
    lint = {
      description = "Run clippy";
      exec = ''cd "$DEVENV_ROOT" && cargo clippy --all-targets -- -D warnings'';
    };
    tests = {
      description = "Run all tests";
      exec = ''cd "$DEVENV_ROOT" && cargo test'';
    };
    build = {
      description = "Build the release host binary";
      exec = ''cd "$DEVENV_ROOT" && cargo build --release -p wasmcloud-host-runtime --bin wasmcloud-host'';
    };
    check = {
      description = "Format check, lint and test everything";
      exec = ''
        set -euo pipefail
        format-check
        lint
        tests
      '';
    };
    runtime-version = {
      description = "Print the workspace version, which is also the image tag";
      exec = ''
        cd "$DEVENV_ROOT"
        cargo metadata --no-deps --format-version 1 | jq -re '.packages[] | select(.name == "wasmcloud-host-runtime") | .version'
      '';
    };
    ghcr-login = {
      description = "Log Docker in to ghcr.io with GHCR_USERNAME and GHCR_TOKEN";
      exec = ''printf '%s' "$GHCR_TOKEN" | docker login ghcr.io --username "$GHCR_USERNAME" --password-stdin'';
    };
    needs-publish = {
      description = "Print true when the current version has no image yet; --force always prints true";
      exec = ''
        set -euo pipefail
        if [ "''${1:-}" = "--force" ]; then
          echo true
          exit 0
        fi
        version="$(runtime-version)"
        # Only a missing tag means publish; any other registry failure stops the run.
        if output="$(docker buildx imagetools inspect "${image}:$version" 2>&1)"; then
          echo false
        elif grep -qiE 'not found|manifest unknown|404' <<<"$output"; then
          echo true
        else
          echo "$output" >&2
          exit 1
        fi
      '';
    };
    build-image = {
      description = "Build the image for one platform (default: this machine's) and load it into Docker as wasmcloud-host:dev";
      exec = ''
        set -euo pipefail
        cd "$DEVENV_ROOT"
        docker buildx build ''${1:+--platform "$1"} --load --tag wasmcloud-host:dev .
      '';
    };
    publish-platform = {
      description = "Build and push <version>-<arch> for one platform, for example linux/amd64";
      exec = ''
        set -euo pipefail
        cd "$DEVENV_ROOT"
        platform="$1"
        case "$platform" in
          linux/amd64 | linux/arm64) ;;
          *)
            echo "publish-platform: unsupported platform $platform" >&2
            exit 1
            ;;
        esac
        version="$(runtime-version)"
        docker buildx build --push --provenance=false \
          --platform "$platform" \
          --label "org.opencontainers.image.version=$version" \
          --label "org.opencontainers.image.source=https://github.com/Seamlezz/wasmCloud-host" \
          --tag "${image}:$version-''${platform#linux/}" \
          .
      '';
    };
    publish-manifest = {
      description = "Combine the published platform images into <version> and latest";
      exec = ''
        set -euo pipefail
        version="$(runtime-version)"
        docker buildx imagetools create \
          --tag "${image}:$version" \
          --tag "${image}:latest" \
          "${image}:$version-amd64" \
          "${image}:$version-arm64"
      '';
    };
  };

  enterTest = "check";
}
