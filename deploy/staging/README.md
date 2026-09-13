# Xboard-Node deployment

GitHub Actions tests, builds and publishes the node. Images use `build-<run_id>-<run_attempt>` version tags, with branch and release aliases. CI does not deploy or replace a server's ingress. Create and bind machines through the panel administrator API/MCP; install or upgrade their runtime over SSH using the selected published version.

## Existing machines

Keep the machine token, panel binding, runtime configuration, certificate storage and existing reverse proxy across upgrades. Back up configuration first. Update only the node runtime, check its health and panel heartbeat, and test an actual user subscription before accepting the release. Restore the previous image and configuration if acceptance fails.

Use `/home/beihai/docker/xboard-node` for container runtime data. When a server already has Caddy, Traefik or another ingress, reuse its business domain and add only the required route on standard 80/443. Keep internal node listeners on loopback and preserve unrelated services. Server addresses and credentials belong in private operational configuration, outside this repository.

## Optional isolated deployment example

The files in this directory retain the earlier standalone node plus Caddy example. They are not invoked automatically by CI and must not be applied to an existing machine-managed deployment. The script refuses native installations and refuses a fresh installation when standard ingress ports are occupied. Existing credentials, edge configuration, Caddy state and cover content persist across upgrades. The historical Compose project name remains stable for existing volume identity.

The example's node ID, WebSocket transport and cover site are not a template to overwrite live panel records. Configure an isolated target explicitly before using it. For an existing machine, use its actual panel configuration and ingress instead.
