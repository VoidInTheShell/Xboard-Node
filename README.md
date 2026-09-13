# xboard-node

Node backend for [Xboard](https://github.com/cedar2025/Xboard). Supports `sing-box` / `xray-core` dual kernels.

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
  ghcr.io/cedar2025/xboard-node:latest
```

### Docker Compose

```bash
git clone -b compose --depth 1 https://github.com/cedar2025/xboard-node.git
cd xboard-node
vim config/config.yml   # set panel.url / token / node_id
docker compose up -d
```

### Installer (Linux systemd)

```bash
# Node mode
curl -fsSL https://raw.githubusercontent.com/cedar2025/xboard-node/dev/install.sh | \
  sudo bash -s -- --mode node --panel https://panel.example.com --token TOKEN --node-id 1

# Machine mode
curl -fsSL https://raw.githubusercontent.com/cedar2025/xboard-node/dev/install.sh | \
  sudo bash -s -- --mode machine --panel https://panel.example.com --token TOKEN --machine-id 1

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

The nominal source sampling/report interval is about 20 seconds (the service's
10-second tracker cadence with a 15-second minimum). Per-IP speed is an interval
average in bytes/second, not an instantaneous packet rate. Xray preserves the
native inbound reader required by mux/XUDP; observations are separate from its
built-in billing statistics. Both direct and zero-copy sing-box traffic paths
use the same upload/download direction.

## License

MPL-2.0.
