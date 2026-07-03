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

## Viasat pull-through path (GKE / ndp-us-dev)

Viasat does **not** pull from Docker Hub directly. Docker Hub images are mirrored
into the Viasat ecosystem through **Artifactory's Docker Hub pull-through cache**, and
GKE / `ndp-us-dev` pulls from that Artifactory host.

Confirmed Artifactory patterns (from the ENTSR wiki audit):

- Viasat Artifactory docker repos resolve as
  `<repo>.docker.artifactory.viasat.com/<image>:<tag>`.
- The KVS team's GKE registry is `kvs-docker-prod.docker.artifactory.viasat.com`,
  and it already remaps Docker Hub images. Wiki precedent:
  `docker.io/bitnamilegacy/etcd` → `kvs-docker-prod.docker.artifactory.viasat.com/etcd:3.5.29-bitnami`.

So once `docker.io/xrlx/etcd-operator:<tag>` is published, the Viasat-side pull is:

```
<artifactory-dockerhub-cache-host>/xrlx/etcd-operator:<tag>
```

> **CONFIRM THE EXACT HOSTNAME.** The specific `docker-hub-remote` (pull-through)
> hostname was **not** present in the ENTSR wiki audit. Read it from Artifactory's
> **"Set Me Up"** UI for the Docker Hub remote/virtual repo, or ask the platform /
> KVS team, before wiring it into any manifest. Do not guess it from the
> `kvs-docker-prod` example above — that is the KVS GKE registry, not necessarily the
> Docker Hub pull-through endpoint.

The `ndp-argo` ApplicationSet should reference that pull-through path **by digest**
(LAWS-4), e.g. `<artifactory-dockerhub-cache-host>/xrlx/etcd-operator@sha256:...`,
not by floating tag.
