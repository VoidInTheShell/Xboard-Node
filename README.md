# xboard-node

Node backend for [Xboard](https://github.com/VoidInTheShell/Xboard). Supports `sing-box` / `xray-core` dual kernels.

> **Disclaimer**: This project is for educational and learning purposes only.

## Features

- Protocols: V2Ray family, Trojan, Shadowsocks, Hysteria2, TUIC, AnyTLS
- Sync: WebSocket push + REST polling dual channel
- User controls: speed limit, device limit, alive-IP tracking, hot update
- Deploy modes: node mode, machine mode, standalone mode
- Multi-instance: single process binding multiple panels / nodes

## Install

### Docker

```bash
docker run -d --restart=always --network=host \
  -e apiHost=https://panel.com -e apiKey=TOKEN -e nodeID=1 \
  ghcr.io/voidintheshell/xboard-node:${XBOARD_NODE_VERSION:?Set a published version tag}
```

### Docker Compose

```bash
export XBOARD_NODE_VERSION=v1.14.0 # replace with an actually published version
curl -fLO "https://github.com/VoidInTheShell/Xboard-Node/releases/download/${XBOARD_NODE_VERSION}/compose.sample.yaml"
curl -fLO "https://github.com/VoidInTheShell/Xboard-Node/releases/download/${XBOARD_NODE_VERSION}/config.yml.example"
cp config.yml.example config.yml
vim config.yml # configure the panel and node/machine binding
docker compose -f compose.sample.yaml up -d
```

### Installer (Linux systemd)

```bash
# Node mode
curl -fsSL https://github.com/VoidInTheShell/Xboard-Node/releases/latest/download/install.sh | \
  sudo bash -s -- --mode node --panel https://panel.example.com --token TOKEN --node-id 1

# Machine mode
curl -fsSL https://github.com/VoidInTheShell/Xboard-Node/releases/latest/download/install.sh | \
  sudo bash -s -- --mode machine --panel https://panel.example.com --token TOKEN --machine-id 1

```

The release installer pins its own version for both executables. For development
versions, replace `latest/download` with `download/<exact-dev-version>`. The first
versioned release must exist before these commands can be used; historical rolling
`dev` releases are not compatible with this installer contract.

## xbctl

Run `xbctl` after installation for help. Common commands:

```bash
xbctl list                          # list all instances
xbctl status                        # running status
xbctl bind add-node --panel URL --token TOKEN --node-id 1
xbctl bind add-machine --panel URL --token TOKEN --machine-id 1
xbctl bind remove-node --panel URL --node-id 1
xbctl service restart
```

## Configuration

Legacy single-panel config is fully compatible. Appending bindings auto-migrates to `instances` format. See `config.yml.example`.

## Extensions

- Custom routes: [docs-custom-routes.md](docs-custom-routes.md)
- Custom outbounds: [docs-custom-outbounds.md](docs-custom-outbounds.md)
- DNS providers (ACME DNS-01): [docs-dns-providers.md](docs-dns-providers.md)

## Usage observations

Updated panels can accept an independent observation stream at
`/api/v2/server/usage` and `/api/v2/server/machine/usage`. Existing billing reports
are unchanged. Xray and sing-box track raw upload/download per user/source IP;
machine mode reports per-interface cumulative NIC RX/TX separately. The agent
never guesses a device platform from an IP address.

Reports use process epochs and increasing sequence numbers. Source counters have
their own random generations, so short-lived/recreated sources cannot reuse an
old cumulative counter. Unacknowledged source samples are retained in bounded
memory; acknowledged inactive sources expire after five minutes. At most 2,000
sources are tracked per instance, with explicit incomplete-collection reporting
on overflow. Large user counter sets rotate through bounded batches; failures
back off and retry cumulative values. Unsent data is not durable across a process
crash. Initial NIC/process readings establish a baseline at the panel.

### Host network counters in Docker

For machine mode in a Linux container, mount the host network counters and
network metadata read-only. This also applies when the proxy uses bridge
networking behind an existing reverse proxy; published ports stay unchanged.
Add these entries to the existing Node service:

```yaml
environment:
  XBOARD_HOST_NET_DEV: /run/xboard-host/net-dev
  XBOARD_HOST_SYS: /run/xboard-host/sys
volumes:
  - /proc/1/net/dev:/run/xboard-host/net-dev:ro
  - /sys:/run/xboard-host/sys:ro
```

Use `/proc/1/net/dev`, not `/proc/net/dev`: the latter can resolve to the
container's network namespace. No privileged mode, host PID namespace, or
network-mode change is required. Historical NIC usage and current network
speed use the same selected interfaces; loopback, common virtual interfaces,
and bridge/bond members are excluded to avoid duplicate counts.

New panels expose each NIC's `collectionScope` (`host`, `container`, or
`unknown` for older records) through the infrastructure API and MCP. A scope
change starts a separate baseline. Old history remains intact and is not
relabelled as host traffic. Without host mounts, containers report their own
network counters. If an explicitly configured host source is unreadable,
collection reports an error instead of falling back to container counters.

The nominal source sampling/report interval is about 20 seconds (the service's
10-second tracker cadence with a 15-second minimum). Per-IP speed is an interval
average in bytes/second, not an instantaneous packet rate. Xray preserves the
native inbound reader required by mux/XUDP; observations are separate from its
built-in billing statistics. Both direct and zero-copy sing-box traffic paths
use the same upload/download direction.

## Panel-managed updates

An independent Linux host service can apply the exact version selected in Admin or through MCP to a single Node installation. See [UPDATER.md](UPDATER.md) for enrollment, systemd/Docker/Compose targets, persisted version selections, and recovery. Publishing a release does not automatically update enrolled hosts.

## License

MPL-2.0.
