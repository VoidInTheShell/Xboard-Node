# Xboard-Node staging deployment

Every pushed branch tests and builds `ghcr.io/voidintheshell/xboard-node`. Pushes to `main`, `master`, `dev`, and `new-dev`, plus opt-in `staging/**` branches, deploy automatically; other branches deploy only when `workflow_dispatch` is run on that branch. A deployment always uses the immutable image digest and never compiles on US2.

## Runtime layout

- Target directory: `/home/beihai/docker/xboard-node`
- Compose project: `us2-xboard-node`
- Containers: `xboard-node` and `xboard-node-edge`
- Network: host mode; the node owns internal listener 30080 and the isolated Caddy edge owns public TCP 443
- Health endpoint: `http://127.0.0.1:65530/healthz`
- Panel: the Xboard staging URL from the GitHub Environment
- Node binding: fresh Xboard staging node ID `1`, type `vless`
- Public edge: `https://cdn-p1.sacbridge.dpdns.org`; the exact VLESS WebSocket path is routed to `127.0.0.1:30080`, while every other request is served by a Hugo static cover site

The edge uses the Caddy 2.10.2 binary copied from a pinned official image into the same immutable Xboard-Node image. It takes over standard TCP 80/443 and requires the existing DNS-only `cdn-p1.sacbridge.dpdns.org` A record to keep pointing at US2. The first deployment removes the superseded US2 test containers `trojan-panel-caddy`, `trojan-panel-core`, `uegsub-web`, and `subconverter-metacubex`, their `/tpdata` tree, plus the exact legacy `xboard-node.service`, its two known binaries and `/etc/xboard-node` if that systemd unit exists. The unrelated Nezha monitoring deployment is preserved. It never targets production node hosts.

## GitHub environment

Environment secrets:

- `STAGING_SSH_PRIVATE_KEY`: dedicated US2 deployment key
- `STAGING_SSH_KNOWN_HOSTS`: pinned US2 SSH host-key line
- `STAGING_API_KEY`: must equal Xboard's `STAGING_SERVER_TOKEN`
- `STAGING_NODE_WS_PATH`: must equal the VLESS WS path seeded by the Xboard staging workflow

Environment variables:

- `STAGING_SSH_HOST`
- `STAGING_SSH_PORT`
- `STAGING_SSH_USER` (normally `beihai`)
- `STAGING_PANEL_URL`
- `STAGING_NODE_DOMAIN` (currently `cdn-p1.sacbridge.dpdns.org`; workflow validation rejects domains outside `sacbridge.dpdns.org`)
- `STAGING_NODE_ID` (normally `1`)
- `STAGING_NODE_TYPE` (normally `vless`)

GitHub Actions builds the Hugo cover from `deploy/staging/cover` before uploading the deployment bundle. The remote script validates `/home/beihai/docker/xboard-node`, serializes deployments with `.deploy.lock`, pulls the exact image digest, validates Caddy before starting, and requires node health, authenticated panel configuration application, TLS cover HTTP 200, custom 404, and public WebSocket 101 evidence before reporting success. The shared staging node is not branch-isolated; the latest successful deployment becomes current. After branch testing, rerun the default `dev` branch workflow to return the node to its repository mainline.
