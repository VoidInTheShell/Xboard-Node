#!/usr/bin/env bash
set -Eeuo pipefail

TARGET_DIR="/home/beihai/docker/xboard-node"
EXPECTED_TARGET="/home/beihai/docker/xboard-node"
BUNDLE_DIR="${1:-}"
REGISTRY_USER="${2:-}"

log() {
    printf '[node-deploy] %s\n' "$*"
}

fail() {
    printf '[node-deploy] ERROR: %s\n' "$*" >&2
    exit 1
}

require_file() {
    [ -f "$1" ] || fail "required file is missing: $1"
}

[ -n "$BUNDLE_DIR" ] || fail "bundle directory argument is required"
[ -n "$REGISTRY_USER" ] || fail "registry user argument is required"
[ "$(realpath -m "$TARGET_DIR")" = "$EXPECTED_TARGET" ] || fail "unexpected target directory"

RESOLVED_BUNDLE=$(realpath -e "$BUNDLE_DIR")
case "$RESOLVED_BUNDLE" in
    /home/beihai/docker/.incoming/xboard-node-*) ;;
    *) fail "refusing unexpected bundle path: $RESOLVED_BUNDLE" ;;
esac

require_file "$RESOLVED_BUNDLE/compose.yaml"
require_file "$RESOLVED_BUNDLE/config.yml"
require_file "$RESOLVED_BUNDLE/Caddyfile.template"
require_file "$RESOLVED_BUNDLE/runtime.env"
require_file "$RESOLVED_BUNDLE/deploy.env"
require_file "$RESOLVED_BUNDLE/edge.env"
require_file "$RESOLVED_BUNDLE/cover-public/index.html"
require_file "$RESOLVED_BUNDLE/cover-public/404.html"

IFS= read -r REGISTRY_TOKEN || true
[ -n "${REGISTRY_TOKEN:-}" ] || fail "registry token was not provided on stdin"

install -d -m 750 "$TARGET_DIR"
exec 9>"$TARGET_DIR/.deploy.lock"
flock -x 9
log "acquired deployment lock"

if [ -f "$TARGET_DIR/compose.yaml" ]; then
    if [ -f "$TARGET_DIR/.deploy.env" ]; then
        sudo -n docker compose --env-file "$TARGET_DIR/.deploy.env" -f "$TARGET_DIR/compose.yaml" down --remove-orphans || true
    else
        sudo -n docker compose -f "$TARGET_DIR/compose.yaml" down --remove-orphans || true
    fi
fi

if systemctl list-unit-files --type=service 2>/dev/null | grep -q '^xboard-node\.service'; then
    log "removing the legacy systemd test-node installation"
    sudo -n systemctl disable --now xboard-node.service || true
    sudo -n rm -f -- /etc/systemd/system/xboard-node.service /usr/local/bin/xboard-node /usr/local/bin/xbctl
    LEGACY_CONFIG=$(realpath -m /etc/xboard-node)
    [ "$LEGACY_CONFIG" = "/etc/xboard-node" ] || fail "unexpected legacy config path"
    sudo -n rm -rf -- "$LEGACY_CONFIG"
    sudo -n systemctl daemon-reload
fi

sudo -n chown -R beihai:beihai "$TARGET_DIR"
install -m 644 "$RESOLVED_BUNDLE/compose.yaml" "$TARGET_DIR/compose.yaml"
install -m 644 "$RESOLVED_BUNDLE/config.yml" "$TARGET_DIR/config.yml"
install -m 600 "$RESOLVED_BUNDLE/runtime.env" "$TARGET_DIR/runtime.env"
install -m 600 "$RESOLVED_BUNDLE/deploy.env" "$TARGET_DIR/.deploy.env"
install -m 600 "$RESOLVED_BUNDLE/edge.env" "$TARGET_DIR/edge.env"
install -d -m 750 "$TARGET_DIR/cover-public" "$TARGET_DIR/caddy-data" "$TARGET_DIR/caddy-config"
find "$TARGET_DIR/cover-public" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
cp -a "$RESOLVED_BUNDLE/cover-public/." "$TARGET_DIR/cover-public/"
find "$TARGET_DIR/cover-public" -type d -exec chmod 750 {} +
find "$TARGET_DIR/cover-public" -type f -exec chmod 640 {} +

XBOARD_NODE_IMAGE=$(sed -n 's/^XBOARD_NODE_IMAGE=//p' "$TARGET_DIR/.deploy.env" | tail -1)
[ -n "$XBOARD_NODE_IMAGE" ] || fail "XBOARD_NODE_IMAGE is missing from deploy.env"
NODE_DOMAIN=$(sed -n 's/^NODE_DOMAIN=//p' "$TARGET_DIR/edge.env" | tail -1)
NODE_WS_PATH=$(sed -n 's/^NODE_WS_PATH=//p' "$TARGET_DIR/edge.env" | tail -1)
[[ "$NODE_DOMAIN" =~ ^[a-z0-9.-]+$ ]] || fail "NODE_DOMAIN is invalid"
[[ "$NODE_DOMAIN" == *.sacbridge.dpdns.org ]] || fail "NODE_DOMAIN must use sacbridge.dpdns.org"
[[ "$NODE_WS_PATH" =~ ^/[A-Za-z0-9._~/-]{8,160}$ ]] || fail "NODE_WS_PATH is invalid"
case "$NODE_WS_PATH" in *'|'*|*'&'*) fail "NODE_WS_PATH contains an unsupported character" ;; esac
sed -e "s|__NODE_DOMAIN__|$NODE_DOMAIN|g" -e "s|__NODE_WS_PATH__|$NODE_WS_PATH|g" \
    "$RESOLVED_BUNDLE/Caddyfile.template" > "$TARGET_DIR/Caddyfile"
chmod 600 "$TARGET_DIR/Caddyfile"

AUTH_DIR=$(mktemp -d "/tmp/xboard-node-docker-auth.XXXXXX")
cleanup() {
    sudo -n rm -rf -- "$AUTH_DIR"
    unset REGISTRY_TOKEN
}
trap cleanup EXIT

printf '%s\n' "$REGISTRY_TOKEN" | sudo -n docker --config "$AUTH_DIR" login ghcr.io --username "$REGISTRY_USER" --password-stdin >/dev/null
unset REGISTRY_TOKEN
sudo -n docker --config "$AUTH_DIR" pull "$XBOARD_NODE_IMAGE"

compose() {
    sudo -n docker compose --env-file "$TARGET_DIR/.deploy.env" -f "$TARGET_DIR/compose.yaml" "$@"
}

compose run --rm --no-deps --entrypoint /usr/bin/caddy xboard-node-edge \
    validate --config /etc/caddy/Caddyfile --adapter caddyfile

LEGACY_CONTAINERS=(trojan-panel-caddy trojan-panel-core uegsub-web subconverter-metacubex)
for container in "${LEGACY_CONTAINERS[@]}"; do
    if sudo -n docker container inspect "$container" >/dev/null 2>&1; then
        log "removing superseded US2 test container: $container"
        sudo -n docker rm --force "$container"
    fi
done

if [ -d /tpdata ]; then
    LEGACY_DATA=$(realpath -m /tpdata)
    [ "$LEGACY_DATA" = "/tpdata" ] || fail "unexpected legacy data path"
    log "removing superseded US2 test data: /tpdata"
    sudo -n rm -rf -- "$LEGACY_DATA"
fi

compose up -d xboard-node xboard-node-edge

for _ in $(seq 1 60); do
    status=$(sudo -n docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' xboard-node 2>/dev/null || true)
    if [ "$status" = "healthy" ] || [ "$status" = "running" ]; then
        sudo -n docker exec xboard-node wget -q -O /dev/null http://127.0.0.1:65530/healthz
        for _ in $(seq 1 60); do
            node_logs=$(sudo -n docker logs xboard-node 2>&1 || true)
            if printf '%s\n' "$node_logs" | grep -q 'config updated, [0-9][0-9]* users'; then
                edge_status=$(sudo -n docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' xboard-node-edge 2>/dev/null || true)
                if [ "$edge_status" != "healthy" ]; then
                    sleep 2
                    continue
                fi
                curl --fail --silent --show-error --resolve "$NODE_DOMAIN:443:127.0.0.1" "https://$NODE_DOMAIN/" | grep -q 'Pacific Atlas'
                missing_status=$(curl --silent --show-error --output /tmp/xboard-node-edge-404.html --write-out '%{http_code}' \
                    --resolve "$NODE_DOMAIN:443:127.0.0.1" "https://$NODE_DOMAIN/not-a-real-field-note")
                [ "$missing_status" = "404" ] || fail "cover site unknown path did not return 404"
                grep -q 'This trail ends here' /tmp/xboard-node-edge-404.html
                rm -f -- /tmp/xboard-node-edge-404.html
                sudo -n docker image prune -f >/dev/null
                log "node authenticated to the panel, applied its configuration, and passed TLS cover checks"
                log "node deployment complete: $XBOARD_NODE_IMAGE"
                compose ps
                exit 0
            fi
            if printf '%s\n' "$node_logs" | grep -Eq 'auth failed|handshake status (401|403)|invalid token'; then
                break
            fi
            sleep 2
        done
        break
    fi
    if [ "$status" = "unhealthy" ] || [ "$status" = "exited" ] || [ "$status" = "dead" ]; then
        break
    fi
    sleep 2
done

compose ps || true
compose logs --tail 180 xboard-node xboard-node-edge || true
fail "xboard-node did not become healthy and complete an authenticated config sync"
