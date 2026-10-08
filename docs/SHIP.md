# Shipping images (downstream fork)

This fork publishes container images from a **tag-driven** GitHub Actions pipeline
(`.github/workflows/ship.yml`) that runs on **github.com-hosted** `ubuntu-latest`
runners. It is downstream-only — upstream `go.etcd.io/etcd-operator` uses Prow and
has no in-repo GitHub Actions.

## Registries

Every release is pushed, with the **git tag verbatim**, to both:

- `ghcr.io/xrl/etcd-operator`
- `docker.io/xrlx/etcd-operator`

Each registry gets per-arch images (`:<tag>-amd64`, `:<tag>-arm64`) stitched into a
single multi-arch manifest `:<tag>`. **No `latest` tag is ever published**, and there
are no branch-push triggers.

## Tag convention (idiomatic semver)

| Kind        | Pattern                              | Examples                       |
|-------------|--------------------------------------|--------------------------------|
| Release     | `v<major>.<minor>.<patch>`           | `v0.1.0`, `v1.4.2`             |
| Pre-release | `v<major>.<minor>.<patch>-<id>`      | `v0.1.0-pre.0`, `v0.1.0-rc.1` |

Both forms fire the workflow (`v[0-9]+.[0-9]+.[0-9]+` and `v[0-9]+.[0-9]+.[0-9]+-*`).
Never tag `latest`; pin consumers by digest (see below).

## One-time setup (repo secrets on `xrl/etcd-operator`)

- **`DOCKER_HUB_PAT`** — a Docker Hub access token for user `xrlx` with **write**
  to `docker.io/xrlx/etcd-operator`. **You must add this** under
  Settings → Secrets and variables → Actions. Without it the Docker Hub login step
  fails and the run errors out.
- **`GITHUB_TOKEN`** — automatic; the workflow uses it (with `packages: write`) to
  push to ghcr.io. No setup needed.

## Triggering a build

Push a tag:

```sh
git tag v0.1.0-pre.0
git push fork v0.1.0-pre.0
```

(or run the workflow manually via **workflow_dispatch**). The job:

1. cross-compiles `cmd/main.go` natively per arch (amd64, arm64) — no QEMU;
2. packages each binary with `Dockerfile.ship` (COPY-not-compile);
3. pushes per-arch images and a stitched manifest to both registries;
4. cosign keyless-signs each digest (best-effort; a registry that rejects OCI
   signatures will not fail the publish);
5. runs `make build-installer IMG=ghcr.io/xrl/etcd-operator@<digest>` and uploads
   `dist/install*.yaml` as a workflow artifact, pinned by **digest** not tag.

The operator keeps its real `healthz`/`readyz` probes (`cmd/main.go`); nothing about
health is faked in the image.

## Corporate pull-through path

Some environments do not pull from Docker Hub directly; they go through an
Artifactory (or similar) Docker Hub pull-through cache. Once
`docker.io/xrlx/etcd-operator:<tag>` is published, the pull is:

```
<dockerhub-cache-host>/xrlx/etcd-operator:<tag>
```

Read the exact pull-through hostname from the registry's own setup page; do not
guess it from another team's registry host. GitOps manifests should reference
the image **by digest** (`<dockerhub-cache-host>/xrlx/etcd-operator@sha256:...`),
not by floating tag.
