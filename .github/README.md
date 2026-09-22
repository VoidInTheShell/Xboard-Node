# Versioned releases

This repository publishes xboard-node from its own source. Release configuration is in
release-config.json; next_version is currently 1.14.0. The formal v1.14.0 is
already published, so this value must be advanced before the next formal
release; the requested version must equal the advanced next_version.

## Publish

- A dev push publishes the final commit as vNEXT-dev.RUN_ID.RUN_ATTEMPT.
  A push containing multiple commits produces one version from that event's tip,
  not one version per commit. Later branch changes do not change its source.
- Run the existing workflow on dev with release_version=v1.14.0 to publish a formal
  version. The input must match next_version. No merge or deployment is performed
  by this formal publishing operation.
- A tag alone does not trigger publishing. The workflow prepares the tag and draft,
  builds from that exact tag, then publishes only after all required artifacts pass.
- A public version cannot be overwritten. Failed-job reruns reuse their prepared
  plan; a new development run attempt creates a new development version.

## Transitional deployment behavior

This workflow tests, builds and publishes only; it has no server deployment job.
Installation and updates are explicit operations through the installer or updater.
Non-dev builds retain run-specific build tags and do not become installable Releases.
No floating branch/latest image tags are published.

## Artifact contract (schema version 1)

Every public release contains release-manifest.json:

- component, repository, version, channel (stable/dev), source_commit;
- image: ghcr.io/voidintheshell/xboard-node:VERSION;
- platforms: linux/amd64 and linux/arm64;
- compatibility.panel_contract=1 and compatibility.update_protocol=1;
- update_capability=external-executor-required: enroll the independent
  `xbctl updater` service using updater.sample.json before submitting tasks.
- binaries: architecture-specific xboard-node/xbctl asset URLs, plus installer URL.
- Release assets include install.sh, compose.sample.yaml and config.yml.example.
  The installer asset defaults to its exact release version. Use the installation
  instructions in the root README; old rolling dev releases do not have this contract.

The updater must reject drafts, missing/incompatible manifests, wrong repository
namespaces and incomplete platforms. The Git tag, image tag and runtime version
must agree. Releases are visible only after image publication and runtime checks
on both architectures and successful binary builds. No artifact hash comparison is required.

Published image versions are never overwritten on retries. Authentication or
registry failures stop publication instead of assuming a version does not exist.
A failed release stays draft and must not be listed as an available update.
