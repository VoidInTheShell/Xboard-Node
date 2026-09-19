#!/usr/bin/env bash
set -Eeuo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

APP_NAME="xboard-node"
INSTALL_ROOT="/etc/xboard-node"
BACKUP_DIR="${INSTALL_ROOT}/backups"
INSTALL_META="${INSTALL_ROOT}/install-meta.json"
CONFIG_FILE="${INSTALL_ROOT}/config.yml"
CREDENTIALS_FILE="${INSTALL_ROOT}/credentials.env"
BINARY_PATH="/usr/local/bin/xboard-node"
UPDATER_BINARY_PATH="/usr/local/libexec/xboard-updater"
UPDATER_CONFIG_DIR="/etc/xboard-updater"
UPDATER_CONFIG_PATH="${UPDATER_CONFIG_DIR}/config.json"
UPDATER_TOKEN_PATH="${UPDATER_CONFIG_DIR}/token"
UPDATER_STATE_DIR="/var/lib/xboard-updater"
UPDATER_SERVICE_PATH="/etc/systemd/system/xboard-updater.service"
SERVICE_NAME="xboard-node.service"
SERVICE_PATH="/etc/systemd/system/${SERVICE_NAME}"
CLI_PATH="/usr/local/bin/xbctl"
INSTALLER_COPY_PATH="${INSTALL_ROOT}/install.sh"
CLI_BINARY_SOURCE=""
DEFAULT_HEALTH_PORT=65530
DEFAULT_KERNEL="singbox"
DEFAULT_MODE="node"
DEFAULT_ACTION="install"
DEFAULT_RELEASE_VERSION="latest"
DEFAULT_LOG_LEVEL="info"
DEFAULT_KERNEL_LOG_LEVEL="warn"
DEFAULT_DOWNLOAD_BASE="https://github.com/VoidInTheShell/Xboard-Node/releases"
DEFAULT_UPDATER_DOWNLOAD_BASE="https://github.com/VoidInTheShell/xboard-admin/releases"

ACTION="${DEFAULT_ACTION}"
MODE=""
PANEL_URL=""
TOKEN=""
ENROLLMENT_TOKEN=""
UPDATER_EXECUTOR_ID=""
UPDATER_EXECUTOR_SECRET=""
NODE_ID=""
NODE_TYPE=""
MACHINE_ID=""
KERNEL_TYPE="${DEFAULT_KERNEL}"
RELEASE_VERSION="${DEFAULT_RELEASE_VERSION}"
UPDATER_VERSION="${UPDATER_VERSION:-}"
INSTALLATION_METHOD="${INSTALLATION_METHOD:-systemd}"
HEALTH_PORT="${DEFAULT_HEALTH_PORT}"
HEALTH_ENABLED=1
RUNTIME_GOMEMLIMIT=""
RUNTIME_GOGC=""
BINARY_SOURCE=""
CLI_BINARY_SOURCE=""
UPDATER_BINARY_SOURCE="${UPDATER_BINARY_SOURCE:-}"
FORCE_RECONFIGURE=0
PURGE=0
YES=0
ARCH=""
OS=""
DOWNLOAD_URL=""
UPDATER_DOWNLOAD_URL=""
UPDATER_IMAGE=""
NODE_CONTAINER_NAME=""
UPDATER_CONTAINER_NAME=""
COMPOSE_PROJECT=""
COMPOSE_FILE=""
CURRENT_STATE="fresh"
TMP_DIR=""
BACKUP_PATH=""
SERVICE_EXISTED=0
CLEANUP_DONE=0

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step()  { echo -e "${CYAN}[STEP]${NC} ${BOLD}$1${NC}"; }

cleanup_tmp() {
    if [ "$CLEANUP_DONE" -eq 1 ]; then
        return
    fi
    CLEANUP_DONE=1
    if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ]; then
        rm -rf "$TMP_DIR"
    fi
}

load_health_port_from_config() {
    local cfg_path="$1"
    if [ ! -f "$cfg_path" ]; then
        return
    fi
    local parsed
    if [ -x "$CLI_PATH" ]; then
        parsed=$("$CLI_PATH" config health-port --config "$cfg_path" 2>/dev/null)
    else
        parsed=$(grep -m1 'health_port:' "$cfg_path" 2>/dev/null | sed 's/.*health_port:[[:space:]]*//' | tr -cd '0-9')
    fi
    if [ -n "$parsed" ] && [ "$parsed" -ge 0 ] 2>/dev/null; then
        HEALTH_PORT="$parsed"
        if [ "$HEALTH_PORT" -eq 0 ]; then
            HEALTH_ENABLED=0
        else
            HEALTH_ENABLED=1
        fi
    fi
}

rollback_install() {
    log_warn "Rolling back installation"
    if [ -n "$BACKUP_PATH" ] && [ -d "$BACKUP_PATH" ]; then
        if [ -f "$BACKUP_PATH/xboard-node" ]; then
            install -m 755 "$BACKUP_PATH/xboard-node" "$BINARY_PATH"
        else
            rm -f "$BINARY_PATH"
        fi
        if [ -f "$BACKUP_PATH/config.yml" ]; then
            install -m 600 "$BACKUP_PATH/config.yml" "$CONFIG_FILE"
        else
            rm -f "$CONFIG_FILE"
        fi
        if [ -f "$BACKUP_PATH/credentials.env" ]; then
            install -m 600 "$BACKUP_PATH/credentials.env" "$CREDENTIALS_FILE"
        else
            rm -f "$CREDENTIALS_FILE"
        fi
        if [ -f "$BACKUP_PATH/install-meta.json" ]; then
            install -m 644 "$BACKUP_PATH/install-meta.json" "$INSTALL_META"
        else
            rm -f "$INSTALL_META"
        fi
        if [ -f "$BACKUP_PATH/xbctl" ]; then
            install -m 755 "$BACKUP_PATH/xbctl" "$CLI_PATH"
        else
            rm -f "$CLI_PATH"
        fi
        if [ -f "$BACKUP_PATH/xboard-updater" ]; then
            install -m 755 "$BACKUP_PATH/xboard-updater" "$UPDATER_BINARY_PATH"
        elif [ -n "$UPDATER_BINARY_SOURCE" ] || [ -n "$UPDATER_VERSION" ]; then
            rm -f "$UPDATER_BINARY_PATH"
        fi
        if [ -f "$BACKUP_PATH/updater-config.json" ]; then
            install -d -m 700 "$UPDATER_CONFIG_DIR"
            install -m 600 "$BACKUP_PATH/updater-config.json" "$UPDATER_CONFIG_PATH"
        elif [ -n "$UPDATER_EXECUTOR_SECRET" ] || [ -f "$UPDATER_CONFIG_PATH" ]; then
            rm -f "$UPDATER_CONFIG_PATH"
        fi
        if [ -f "$BACKUP_PATH/updater-token" ]; then
            install -d -m 700 "$UPDATER_CONFIG_DIR"
            install -m 600 "$BACKUP_PATH/updater-token" "$UPDATER_TOKEN_PATH"
        elif [ -n "$UPDATER_EXECUTOR_SECRET" ] || [ -f "$UPDATER_TOKEN_PATH" ]; then
            rm -f "$UPDATER_TOKEN_PATH"
        fi
        if [ -f "$BACKUP_PATH/xboard-updater.service" ]; then
            install -m 644 "$BACKUP_PATH/xboard-updater.service" "$UPDATER_SERVICE_PATH"
        elif [ -n "$UPDATER_EXECUTOR_SECRET" ]; then
            rm -f "$UPDATER_SERVICE_PATH"
        fi
        if [ -f "$BACKUP_PATH/compose.yaml" ]; then
            install -m 600 "$BACKUP_PATH/compose.yaml" "$COMPOSE_FILE"
        elif [ "$INSTALLATION_METHOD" = "compose" ]; then
            rm -f "$COMPOSE_FILE"
        fi
        if [ -f "$BACKUP_PATH/${SERVICE_NAME}" ]; then
            install -m 644 "$BACKUP_PATH/${SERVICE_NAME}" "$SERVICE_PATH"
        else
            rm -f "$SERVICE_PATH"
        fi
    fi
    if [ "$INSTALLATION_METHOD" != "systemd" ]; then
        docker rm -f "$UPDATER_CONTAINER_NAME" "$NODE_CONTAINER_NAME" >/dev/null 2>&1 || true
        if [ -f "$COMPOSE_FILE" ] && [ "$INSTALLATION_METHOD" = "compose" ]; then
            start_container_deployment || true
        fi
        log_warn "Container deployment rollback complete; verify the restored health state"
        return 0
    fi
    load_health_port_from_config "$CONFIG_FILE"
    systemctl daemon-reload || true
    if [ "$SERVICE_EXISTED" -eq 1 ] || [ -f "$SERVICE_PATH" ]; then
        systemctl reset-failed "$SERVICE_NAME" >/dev/null 2>&1 || true
        systemctl restart "$SERVICE_NAME" >/dev/null 2>&1 || true
        if ! wait_for_health; then
            log_error "Rollback completed but restored service did not become healthy"
            show_recent_logs
            return 1
        fi
    else
        systemctl disable "$SERVICE_NAME" >/dev/null 2>&1 || true
    fi
    log_warn "Rollback complete"
}

on_error() {
    local exit_code=$?
    local line_no=${1:-unknown}
    if [ "$exit_code" -ne 0 ]; then
        log_error "Install failed at line ${line_no} (exit=${exit_code})"
        if [ -n "$BACKUP_PATH" ]; then
            rollback_install || true
        fi
    fi
    cleanup_tmp
    exit "$exit_code"
}
trap 'on_error $LINENO' ERR
trap cleanup_tmp EXIT

usage() {
    cat <<'HELP'

  xboard-node Installer

  ACTIONS:
    install      Install or reconcile the configured deployment (default)
    upgrade      Upgrade binary and restart service
    uninstall    Remove installed service and binary (config kept unless --purge)
    status       Show current installation status
    help         Show this help

  MODES (auto-detected from --node-id or --machine-id if omitted):
    --mode node      Panel single-node mode (default)
    --mode machine   Panel machine mode

  REQUIRED FOR NODE MODE:
    --panel, -a      Panel URL
    --token, -t      Panel server token
    --node-id, -n    Node ID

  REQUIRED FOR MACHINE MODE:
    --panel, -a       Panel URL
    --token, -t       Existing machine token (legacy/manual mode)
    --machine-id      Machine ID

  OPTIONAL:
    --node-type, -T     Explicit node type for node mode
    --kernel, -k        singbox or xray (default: singbox)
    --version           Release version or latest (default: latest)
    --enrollment-token  Short-lived, one-time machine enrollment token
    --updater-version   Exact Admin Release version for the standalone updater
    --installation-method systemd, docker or compose (enrollment mode)
    --binary            Use a local xboard-node binary path instead of downloading
    --xbctl-binary      Use a local xbctl binary path instead of downloading
    --updater-binary    Use a local xboard-updater binary path instead of downloading
    --health-port       Local health port (default: 65530, use 0 to disable)
    --gomemlimit        Runtime GOMEMLIMIT value, e.g. 256MiB
    --gogc              Runtime GOGC value, e.g. 50
    --force-reconfigure Overwrite an existing install even if mode/target changed
    --purge             With uninstall, delete /etc/xboard-node too
    --yes, -y           Non-interactive confirmation for destructive operations

  EXAMPLES:
    sudo bash install.sh --panel https://panel.example.com --token TOKEN --node-id 1
    sudo bash install.sh --panel https://panel.example.com --token TOKEN --machine-id 1
    sudo bash install.sh upgrade
    sudo bash install.sh uninstall --purge --yes

HELP
}

parse_args() {
    local positional=()
    while [ $# -gt 0 ]; do
        case "$1" in
            install|upgrade|uninstall|status|help)
                ACTION="$1"
                shift
                ;;
            --mode)
                MODE="$2"
                shift 2
                ;;
            --panel|-a|--api)
                PANEL_URL="$2"
                shift 2
                ;;
            --token|-t)
                TOKEN="$2"
                shift 2
                ;;
            --enrollment-token)
                ENROLLMENT_TOKEN="$2"
                shift 2
                ;;
            --node-id|-n)
                NODE_ID="$2"
                shift 2
                ;;
            --node-type|-T)
                NODE_TYPE="$2"
                shift 2
                ;;
            --machine-id)
                MACHINE_ID="$2"
                shift 2
                ;;
            --kernel|-k)
                KERNEL_TYPE="$2"
                shift 2
                ;;
            --version)
                RELEASE_VERSION="$2"
                shift 2
                ;;
            --updater-version)
                UPDATER_VERSION="$2"
                shift 2
                ;;
            --installation-method)
                INSTALLATION_METHOD="$2"
                shift 2
                ;;
            --binary)
                BINARY_SOURCE="$2"
                shift 2
                ;;
            --xbctl-binary)
                CLI_BINARY_SOURCE="$2"
                shift 2
                ;;
            --updater-binary)
                UPDATER_BINARY_SOURCE="$2"
                shift 2
                ;;
            --health-port)
                HEALTH_PORT="$2"
                shift 2
                ;;
            --gomemlimit)
                RUNTIME_GOMEMLIMIT="$2"
                shift 2
                ;;
            --gogc)
                RUNTIME_GOGC="$2"
                shift 2
                ;;
            --force-reconfigure)
                FORCE_RECONFIGURE=1
                shift
                ;;
            --purge)
                PURGE=1
                shift
                ;;
            --yes|-y)
                YES=1
                shift
                ;;
            --help|-h)
                ACTION="help"
                shift
                ;;
            *)
                positional+=("$1")
                shift
                ;;
        esac
    done

    if [ ${#positional[@]} -gt 0 ] && [ "$ACTION" = "install" ]; then
        ACTION="${positional[0]}"
    fi

    case "$KERNEL_TYPE" in
        singbox|SingBox|SINGBOX) KERNEL_TYPE="singbox" ;;
        xray|Xray|XRAY) KERNEL_TYPE="xray" ;;
        *) ;;
    esac

    # Auto-detect mode from arguments when --mode is not specified.
    if [ -z "$MODE" ]; then
        if [ -n "$MACHINE_ID" ]; then
            MODE="machine"
        else
            MODE="node"
        fi
    fi

    case "$MODE" in
        node|machine) ;;
        *)
            log_error "Unsupported mode: $MODE"
            usage
            exit 1
            ;;
    esac
}

check_root() {
    if [ "$(id -u)" -ne 0 ]; then
        log_error "Please run as root or with sudo"
        exit 1
    fi
}

detect_arch() {
    local raw
    raw=$(uname -m)
    case "$raw" in
        x86_64|amd64) ARCH="amd64" ;;
        aarch64|arm64) ARCH="arm64" ;;
        *)
            log_error "Unsupported architecture: $raw"
            exit 1
            ;;
    esac
}

detect_os() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS="$ID"
    else
        OS="unknown"
    fi
}

ensure_systemd() {
    if ! command -v systemctl >/dev/null 2>&1; then
        log_error "systemd is required for this installer"
        exit 1
    fi
    if [ ! -d /run/systemd/system ]; then
        log_error "This host does not appear to be running systemd"
        exit 1
    fi
}

run_with_retry() {
    local attempts="$1"
    local delay="$2"
    shift 2
    local i=1
    while [ "$i" -le "$attempts" ]; do
        if "$@"; then
            return 0
        fi
        if [ "$i" -lt "$attempts" ]; then
            log_warn "Command failed, retrying in ${delay}s: $*"
            sleep "$delay"
        fi
        i=$((i + 1))
    done
    return 1
}

install_dependencies() {
    case "$OS" in
        ubuntu|debian)
            DEBIAN_FRONTEND=noninteractive run_with_retry 10 3 apt-get update -qq
            DEBIAN_FRONTEND=noninteractive run_with_retry 10 3 apt-get install -y -qq curl wget ca-certificates >/dev/null 2>&1
            ;;
        centos|rhel|rocky|almalinux|fedora)
            if command -v dnf >/dev/null 2>&1; then
                run_with_retry 5 3 dnf install -y -q curl wget ca-certificates >/dev/null 2>&1
            else
                run_with_retry 5 3 yum install -y -q curl wget ca-certificates >/dev/null 2>&1
            fi
            ;;
        *)
            log_warn "OS ${OS} is not in the official support set; continuing best-effort"
            ;;
    esac
}

ensure_dirs() {
    mkdir -p "$INSTALL_ROOT" "$BACKUP_DIR" "${INSTALL_ROOT}/certs" "${INSTALL_ROOT}/state" "${INSTALL_ROOT}/instances"
    chmod 700 "$INSTALL_ROOT"
}

validate_positive_int() {
    local label="$1"
    local value="$2"
    if ! [[ "$value" =~ ^[0-9]+$ ]] || [ "$value" -le 0 ]; then
        log_error "${label} must be a positive integer, got: ${value}"
        exit 1
    fi
}

validate_https_panel_url() {
    local value="$1"
    local label="${2:-Panel URL}"
    case "$value" in
        *\"*|*\'*|*[[:space:]]*)
            log_error "${label} contains unsupported characters"
            exit 1
            ;;
    esac
    case "$value" in
        https://?*) ;;
        *)
            log_error "${label} must use HTTPS"
            exit 1
            ;;
    esac
}

validate_install_request() {
    if [ -z "$PANEL_URL" ]; then
        log_error "Panel URL is required"
        exit 1
    fi
    if [ -z "$TOKEN" ] && { [ "$MODE" != "machine" ] || [ -z "$ENROLLMENT_TOKEN" ]; }; then
        log_error "Token is required"
        exit 1
    fi
    validate_https_panel_url "$PANEL_URL"
    if [ -n "$UPDATER_VERSION" ] && ! [[ "$UPDATER_VERSION" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-dev\.[1-9][0-9]*\.[1-9][0-9]*)?$ ]]; then
        log_error "updater-version must be an exact vX.Y.Z or vX.Y.Z-dev.RUN.ATTEMPT release"
        exit 1
    fi
    if ! [[ "$HEALTH_PORT" =~ ^[0-9]+$ ]]; then
        log_error "health-port must be a non-negative integer"
        exit 1
    fi
    if [ "$HEALTH_PORT" -eq 0 ]; then
        HEALTH_ENABLED=0
    fi
    case "$KERNEL_TYPE" in
        singbox|xray) ;;
        *)
            log_error "Kernel must be singbox or xray"
            exit 1
            ;;
    esac
    case "$MODE" in
        node)
            validate_positive_int "Node ID" "$NODE_ID"
            ;;
        machine)
            validate_positive_int "Machine ID" "$MACHINE_ID"
            if [ -n "$ENROLLMENT_TOKEN" ]; then
                if [ -z "$UPDATER_VERSION" ]; then
                    log_error "Enrollment mode requires an exact updater-version"
                    exit 1
                fi
                case "$INSTALLATION_METHOD" in
                    systemd|docker|compose) ;;
                    *)
                        log_error "installation-method must be systemd, docker or compose"
                        exit 1
                        ;;
                esac
            fi
            ;;
    esac
    if [ "$MODE" != "machine" ] && [ "$INSTALLATION_METHOD" != "systemd" ]; then
        log_error "Docker and Compose installation methods require machine mode enrollment"
        exit 1
    fi
    case "$INSTALLATION_METHOD" in
        systemd|docker|compose) ;;
        *)
            log_error "installation-method must be systemd, docker or compose"
            exit 1
            ;;
    esac
    if [ "$MODE" = "machine" ]; then
        NODE_CONTAINER_NAME="xboard-node-machine-${MACHINE_ID}"
        UPDATER_CONTAINER_NAME="xboard-updater-machine-${MACHINE_ID}"
        COMPOSE_PROJECT="xboard-node-machine-${MACHINE_ID}"
        COMPOSE_FILE="${INSTALL_ROOT}/compose.yaml"
    fi
}

ensure_docker() {
    if ! command -v docker >/dev/null 2>&1; then
        log_error "Docker is required for ${INSTALLATION_METHOD} installation"
        exit 1
    fi
    if ! docker info >/dev/null 2>&1; then
        log_error "Docker daemon is not available"
        exit 1
    fi
}

json_read_string_at() {
    local json="$1" pos="$2" length="${#1}" ch escape hex decoded out=""
    if [ "${json:pos:1}" != '"' ]; then
        return 1
    fi
    pos=$((pos + 1))
    while [ "$pos" -lt "$length" ]; do
        ch="${json:pos:1}"
        pos=$((pos + 1))
        if [ "$ch" = '"' ]; then
            JSON_STRING_VALUE="$out"
            JSON_STRING_NEXT="$pos"
            return 0
        fi
        if [ "$ch" != '\' ]; then
            out="${out}${ch}"
            continue
        fi
        if [ "$pos" -ge "$length" ]; then
            return 1
        fi
        escape="${json:pos:1}"
        pos=$((pos + 1))
        if [ "$escape" = '"' ]; then
            out="${out}"'"'
        elif [ "$escape" = '\' ]; then
            out="${out}"'\'
        elif [ "$escape" = '/' ]; then
            out="${out}/"
        elif [ "$escape" = 'b' ]; then
            out="${out}"$'\b'
        elif [ "$escape" = 'f' ]; then
            out="${out}"$'\f'
        elif [ "$escape" = 'n' ]; then
            out="${out}"$'\n'
        elif [ "$escape" = 'r' ]; then
            out="${out}"$'\r'
        elif [ "$escape" = 't' ]; then
            out="${out}"$'\t'
        elif [ "$escape" = 'u' ]; then
            hex="${json:pos:4}"
            if ! [[ "$hex" =~ ^[0-9A-Fa-f]{4}$ ]]; then
                return 1
            fi
            printf -v decoded '%b' "\\u${hex}"
            out="${out}${decoded}"
            pos=$((pos + 4))
        else
            return 1
        fi
    done
    return 1
}

json_field() {
    local key="$1" json
    json=$(cat)
    if command -v jq >/dev/null 2>&1; then
        printf '%s' "$json" | jq -er --arg key "$key" 'first(.. | objects | .[$key]? | select(type == "string" or type == "number" or type == "boolean") | if type == "string" then . else tostring end)'
        return
    fi
    if command -v python3 >/dev/null 2>&1; then
        printf '%s' "$json" | python3 -c '
import json, sys
key = sys.argv[1]
value = json.load(sys.stdin)
def find(item):
    if isinstance(item, dict):
        if key in item and isinstance(item[key], (str, int, float, bool)):
            print(item[key])
            return True
        return any(find(child) for child in item.values())
    if isinstance(item, list):
        return any(find(child) for child in item)
    return False
if not find(value):
    raise SystemExit(1)
' "$key"
        return
    fi

    local pos=0 length="${#json}" ch token next probe value start
    while [ "$pos" -lt "$length" ]; do
        ch="${json:pos:1}"
        if [ "$ch" != '"' ]; then
            pos=$((pos + 1))
            continue
        fi
        if ! json_read_string_at "$json" "$pos"; then
            return 1
        fi
        token="$JSON_STRING_VALUE"
        next="$JSON_STRING_NEXT"
        probe="$next"
        while [ "$probe" -lt "$length" ]; do
            case "${json:probe:1}" in
                ' '|$'\t'|$'\r'|$'\n') probe=$((probe + 1)) ;;
                *) break ;;
            esac
        done
        if [ "$token" = "$key" ] && [ "${json:probe:1}" = ':' ]; then
            probe=$((probe + 1))
            while [ "$probe" -lt "$length" ]; do
                case "${json:probe:1}" in
                    ' '|$'\t'|$'\r'|$'\n') probe=$((probe + 1)) ;;
                    *) break ;;
                esac
            done
            if [ "${json:probe:1}" = '"' ]; then
                if json_read_string_at "$json" "$probe"; then
                    printf '%s\n' "$JSON_STRING_VALUE"
                    return 0
                fi
                return 1
            fi
            start="$probe"
            while [ "$probe" -lt "$length" ]; do
                ch="${json:probe:1}"
                case "$ch" in
                    ','|'}'|']') break ;;
                esac
                probe=$((probe + 1))
            done
            value="${json:start:probe-start}"
            value="${value#"${value%%[![:space:]]*}"}"
            value="${value%"${value##*[![:space:]]}"}"
            [ -n "$value" ] || return 1
            printf '%s\n' "$value"
            return 0
        fi
        pos="$next"
    done
    return 1
}

json_string_field() {
    json_field "$1"
}

enroll_machine() {
    if [ "$MODE" != "machine" ] || [ -z "$ENROLLMENT_TOKEN" ]; then
        return 0
    fi
    log_step "Exchanging one-time machine enrollment"
    local payload response machine_id panel_url machine_token executor_id executor_secret
    payload=$(printf '{"enrollment_token":"%s","architecture":"linux/%s","installation_method":"%s","node_version":"%s","updater_version":"%s","node_instance_id":"machine-%s"}' \
        "$ENROLLMENT_TOKEN" "$ARCH" "$INSTALLATION_METHOD" "$RELEASE_VERSION" "$UPDATER_VERSION" "$MACHINE_ID")
    # This exchange consumes a one-time token. Never retry it automatically:
    # a transport timeout must be diagnosed and a fresh token issued instead.
    response=$(curl -fsSL \
        -H 'Content-Type: application/json' -H 'Accept: application/json' \
        --data "$payload" "${PANEL_URL%/}/api/v2/server/machine/enroll") || {
        log_error "Machine enrollment failed; generate a new installation command"
        exit 1
    }
    machine_id=$(printf '%s' "$response" | json_field machine_id) || machine_id=""
    panel_url=$(printf '%s' "$response" | json_string_field panel_url) || panel_url=""
    machine_token=$(printf '%s' "$response" | json_string_field machine_token) || machine_token=""
    executor_id=$(printf '%s' "$response" | json_string_field executor_id) || executor_id=""
    executor_secret=$(printf '%s' "$response" | json_string_field executor_secret) || executor_secret=""
    if [ "$machine_id" != "$MACHINE_ID" ] || [ -z "$panel_url" ] || [ -z "$machine_token" ] || [ -z "$executor_id" ] || [ -z "$executor_secret" ]; then
        log_error "Machine enrollment returned an invalid registration"
        exit 1
    fi
    validate_https_panel_url "$panel_url" "Machine enrollment panel URL"
    PANEL_URL="$panel_url"
    TOKEN="$machine_token"
    UPDATER_EXECUTOR_ID="$executor_id"
    UPDATER_EXECUTOR_SECRET="$executor_secret"
}

detect_current_state() {
    local has_binary=0 has_config=0 has_service=0
    [ -x "$BINARY_PATH" ] && has_binary=1
    [ -f "$CONFIG_FILE" ] && has_config=1
    [ -f "$SERVICE_PATH" ] && has_service=1

    if [ "$has_binary" -eq 0 ] && [ "$has_config" -eq 0 ] && [ "$has_service" -eq 0 ]; then
        CURRENT_STATE="fresh"
    elif [ "$has_binary" -eq 1 ] && [ "$has_config" -eq 1 ] && [ "$has_service" -eq 1 ]; then
        CURRENT_STATE="installed"
    else
        CURRENT_STATE="partial"
    fi
}

require_reconfigure_confirmation() {
    return
}

select_binary_source() {
    if [ -n "$BINARY_SOURCE" ]; then
        if [ ! -f "$BINARY_SOURCE" ]; then
            log_error "Binary source not found: $BINARY_SOURCE"
            exit 1
        fi
        echo "$BINARY_SOURCE"
        return
    fi
    if [ -f "./xboard-node" ]; then
        echo "./xboard-node"
        return
    fi
    if [ -f "./xboard-node-linux-${ARCH}" ]; then
        echo "./xboard-node-linux-${ARCH}"
        return
    fi
    echo ""
}

resolve_release_version() {
    if [ "$RELEASE_VERSION" = "latest" ]; then
        local metadata
        metadata=$(curl -fsSL --retry 3 'https://api.github.com/repos/VoidInTheShell/Xboard-Node/releases/latest') || {
            log_error "Cannot resolve an owned stable release; choose an available version with --version"
            return 1
        }
        RELEASE_VERSION=$(printf '%s' "$metadata" | sed -nE 's/.*"tag_name"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p' | head -1)
        if [[ "$RELEASE_VERSION" == *-dev.* ]]; then
            log_error "latest must resolve to a stable release; select a development version explicitly"
            return 1
        fi
    fi
    if ! [[ "$RELEASE_VERSION" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-dev\.[1-9][0-9]*\.[1-9][0-9]*)?$ ]]; then
        log_error "Expected an exact vX.Y.Z or vX.Y.Z-dev.RUN.ATTEMPT release"
        return 1
    fi
}

resolve_download_url() {
    local artifact="$1"
    if [ "$RELEASE_VERSION" = "latest" ]; then
        DOWNLOAD_URL="${DEFAULT_DOWNLOAD_BASE}/latest/download/${artifact}"
    else
        DOWNLOAD_URL="${DEFAULT_DOWNLOAD_BASE}/download/${RELEASE_VERSION}/${artifact}"
    fi
}

resolve_updater_download_url() {
    if [ -z "$UPDATER_VERSION" ]; then
        UPDATER_DOWNLOAD_URL=""
        return
    fi
    UPDATER_DOWNLOAD_URL="${DEFAULT_UPDATER_DOWNLOAD_BASE}/download/${UPDATER_VERSION}/xboard-updater-linux-${ARCH}"
}

stage_binary() {
    if [ "$INSTALLATION_METHOD" != "systemd" ]; then
        return 0
    fi
    local staged="$TMP_DIR/xboard-node"
    local local_src
    local_src=$(select_binary_source)
    if [ -n "$local_src" ]; then
        log_step "Using local binary: ${local_src}"
        cp "$local_src" "$staged"
    else
        resolve_download_url "xboard-node-linux-${ARCH}"
        log_step "Downloading binary: ${DOWNLOAD_URL}"
        if ! curl -fsSL "$DOWNLOAD_URL" -o "$staged"; then
            log_error "Failed to download binary from ${DOWNLOAD_URL}"
            exit 1
        fi
    fi
    chmod +x "$staged"
    if ! "$staged" -v >/dev/null 2>&1; then
        log_error "Downloaded binary failed version check"
        exit 1
    fi
    if [ -z "$local_src" ]; then
        local reported
        reported=$("$staged" -v | awk '{print $2}')
        if [ "$reported" != "$RELEASE_VERSION" ]; then
            log_error "Downloaded binary does not report the selected release"
            exit 1
        fi
    fi
}

stage_xbctl() {
    local staged="$TMP_DIR/xbctl"
    local local_src=""
    if [ -n "$CLI_BINARY_SOURCE" ]; then
        if [ ! -f "$CLI_BINARY_SOURCE" ]; then
            log_error "xbctl binary source not found: $CLI_BINARY_SOURCE"
            exit 1
        fi
        local_src="$CLI_BINARY_SOURCE"
    elif [ -f "./xbctl" ]; then
        local_src="./xbctl"
    elif [ -f "./xbctl-linux-${ARCH}" ]; then
        local_src="./xbctl-linux-${ARCH}"
    fi
    if [ -n "$local_src" ]; then
        log_step "Using local xbctl binary: ${local_src}"
        cp "$local_src" "$staged"
    else
        resolve_download_url "xbctl-linux-${ARCH}"
        log_step "Downloading xbctl: ${DOWNLOAD_URL}"
        if ! curl -fsSL "$DOWNLOAD_URL" -o "$staged"; then
            log_error "Failed to download xbctl from ${DOWNLOAD_URL}"
            exit 1
        fi
    fi
    chmod +x "$staged"
    if ! "$staged" version > /dev/null 2>&1; then
        log_error "Downloaded xbctl failed version check"
        exit 1
    fi
    if [ -z "$local_src" ]; then
        local reported
        reported=$("$staged" version | awk '{print $2}')
        if [ "$reported" != "$RELEASE_VERSION" ]; then
            log_error "Downloaded xbctl does not report the selected release"
            exit 1
        fi
    fi
}

stage_updater() {
    local staged="$TMP_DIR/xboard-updater"
    local local_src="$UPDATER_BINARY_SOURCE"
    if [ "$INSTALLATION_METHOD" != "systemd" ]; then
        if [ -n "$local_src" ]; then
            log_error "--updater-binary is only supported for systemd installation"
            exit 1
        fi
        if [ -z "$UPDATER_VERSION" ]; then
            log_error "Docker and Compose installation require an exact updater-version"
            exit 1
        fi
        UPDATER_IMAGE="ghcr.io/voidintheshell/xboard-admin-updater:${UPDATER_VERSION}"
        log_step "Pulling standalone updater image: ${UPDATER_IMAGE}"
        docker pull "$UPDATER_IMAGE" >/dev/null
        return 0
    fi
    if [ -z "$local_src" ] && [ -z "$UPDATER_VERSION" ]; then
        return 0
    fi
    if [ -n "$local_src" ]; then
        if [ ! -f "$local_src" ]; then
            log_error "updater binary source not found: $local_src"
            exit 1
        fi
        log_step "Using local xboard-updater binary: ${local_src}"
        cp "$local_src" "$staged"
    else
        resolve_updater_download_url
        log_step "Downloading standalone updater: ${UPDATER_DOWNLOAD_URL}"
        if ! curl -fsSL "$UPDATER_DOWNLOAD_URL" -o "$staged"; then
            log_error "Failed to download standalone updater from ${UPDATER_DOWNLOAD_URL}"
            exit 1
        fi
    fi
    chmod +x "$staged"
    if ! "$staged" version >/dev/null 2>&1; then
        log_error "Standalone updater failed version check"
        exit 1
    fi
    if [ -n "$UPDATER_VERSION" ]; then
        local reported
        reported=$("$staged" version | awk '{print $1}')
        if [ "$reported" != "$UPDATER_VERSION" ]; then
            log_error "Standalone updater does not report the selected Admin Release"
            exit 1
        fi
    fi
}

render_config() {
    local init_args=(
        config init
        --mode "$MODE"
        --panel-url "$PANEL_URL"
        --kernel "${KERNEL_TYPE:-singbox}"
        --health-port "${HEALTH_PORT:-0}"
        --token "$TOKEN"
        --version "$RELEASE_VERSION"
        --output "$TMP_DIR/config.yml"
        --credentials-out "$TMP_DIR/credentials.env"
        --meta "$TMP_DIR/install-meta.json"
        --install-root "$INSTALL_ROOT"
    )
    if [ -f "$CONFIG_FILE" ]; then
        init_args+=(--config "$CONFIG_FILE")
    fi
    if [ -f "$CREDENTIALS_FILE" ]; then
        init_args+=(--credentials-in "$CREDENTIALS_FILE")
    fi
    if [ "$MODE" = "machine" ]; then
        init_args+=(--machine-id "$MACHINE_ID")
    else
        init_args+=(--node-id "$NODE_ID")
        if [ -n "$NODE_TYPE" ]; then
            init_args+=(--node-type "$NODE_TYPE")
        fi
    fi
    if [ -n "$RUNTIME_GOMEMLIMIT" ]; then
        init_args+=(--gomemlimit "$RUNTIME_GOMEMLIMIT")
    fi
    if [ -n "$RUNTIME_GOGC" ] && [ "$RUNTIME_GOGC" -gt 0 ] 2>/dev/null; then
        init_args+=(--gogc "$RUNTIME_GOGC")
    fi

    local output
    output=$("$TMP_DIR/xbctl" "${init_args[@]}") || {
        log_error "xbctl config init failed"
        exit 1
    }

    INSTANCE_ID=$(echo "$output" | grep '^INSTANCE_ID=' | cut -d= -f2-)
    chmod 600 "$TMP_DIR/credentials.env"
}

render_updater_config() {
    if [ "$MODE" != "machine" ] || [ -z "$UPDATER_EXECUTOR_SECRET" ]; then
        return 0
    fi
    if [ -z "${INSTANCE_ID:-}" ] || ! [[ "$INSTANCE_ID" =~ ^[A-Za-z0-9_.-]+$ ]]; then
        log_error "xbctl returned an invalid machine instance id"
        exit 1
    fi
    mkdir -p "$UPDATER_STATE_DIR"
    chmod 700 "$UPDATER_STATE_DIR"
    mkdir -p "$TMP_DIR/updater"
    printf '%s\n' "$UPDATER_EXECUTOR_SECRET" > "$TMP_DIR/updater/token"
    chmod 600 "$TMP_DIR/updater/token"
    local target_json
    case "$INSTALLATION_METHOD" in
        systemd)
            target_json=$(printf '      "id": "%s",\n      "name": "xboard-node-machine-%s",\n      "component": "xboard-node",\n      "method": "systemd",\n      "binary": "%s",\n      "service": "%s",\n      "health_url": "http://127.0.0.1:%s/healthz"' \
                "$INSTANCE_ID" "$MACHINE_ID" "$BINARY_PATH" "$SERVICE_NAME" "$HEALTH_PORT")
            ;;
        docker)
            target_json=$(printf '      "id": "%s",\n      "name": "xboard-node-machine-%s",\n      "component": "xboard-node",\n      "method": "docker",\n      "container": "%s",\n      "health_url": "http://127.0.0.1:%s/healthz"' \
                "$INSTANCE_ID" "$MACHINE_ID" "$NODE_CONTAINER_NAME" "$HEALTH_PORT")
            ;;
        compose)
            target_json=$(printf '      "id": "%s",\n      "name": "xboard-node-machine-%s",\n      "component": "xboard-node",\n      "method": "compose",\n      "compose_file": "%s",\n      "compose_project": "%s",\n      "compose_service": "xboard-node",\n      "health_url": "http://127.0.0.1:%s/healthz"' \
                "$INSTANCE_ID" "$MACHINE_ID" "$COMPOSE_FILE" "$COMPOSE_PROJECT" "$HEALTH_PORT")
            ;;
    esac
    cat >"$TMP_DIR/updater/config.json" <<EOF_UPDATER
{
  "panel_url": "${PANEL_URL}",
  "token_file": "${UPDATER_TOKEN_PATH}",
  "state_dir": "${UPDATER_STATE_DIR}",
  "executor_id": "${UPDATER_EXECUTOR_ID}",
  "installation_method": "${INSTALLATION_METHOD}",
  "handoff_path": "${UPDATER_STATE_DIR}/handoff.json",
  "targets": [
    {
${target_json}
    }
  ]
}
EOF_UPDATER
    chmod 600 "$TMP_DIR/updater/config.json"
}

render_compose_file() {
    if [ "$INSTALLATION_METHOD" != "compose" ]; then
        return 0
    fi
    mkdir -p "$TMP_DIR/deployment"
    cat >"$TMP_DIR/deployment/compose.yaml" <<EOF_COMPOSE
services:
  xboard-node:
    image: ghcr.io/voidintheshell/xboard-node:${RELEASE_VERSION}
    container_name: ${NODE_CONTAINER_NAME}
    restart: unless-stopped
    init: true
    network_mode: host
    env_file:
      - ${CREDENTIALS_FILE}
    volumes:
      - ${CONFIG_FILE}:/etc/xboard-node/config.yml:ro
      - ${INSTALL_ROOT}/certs:/etc/xboard-node/certs
      - ${INSTALL_ROOT}/state:/etc/xboard-node/state
      - ${INSTALL_ROOT}/instances:/etc/xboard-node/instances

  xboard-updater:
    image: ${UPDATER_IMAGE}
    container_name: ${UPDATER_CONTAINER_NAME}
    restart: unless-stopped
    depends_on:
      xboard-node:
        condition: service_started
    network_mode: host
    read_only: true
    tmpfs:
      - /tmp:rw,noexec,nosuid,size=64m
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ${INSTALL_ROOT}:${INSTALL_ROOT}:ro
      - ${UPDATER_CONFIG_PATH}:${UPDATER_CONFIG_PATH}:ro
      - ${UPDATER_TOKEN_PATH}:${UPDATER_TOKEN_PATH}:ro
      - ${UPDATER_STATE_DIR}:${UPDATER_STATE_DIR}
    command: ["updater", "run", "--config", "${UPDATER_CONFIG_PATH}"]
EOF_COMPOSE
    chmod 600 "$TMP_DIR/deployment/compose.yaml"
}

render_service() {
    cat >"$TMP_DIR/${SERVICE_NAME}" <<EOF_UNIT
[Unit]
Description=Xboard Node Backend
Documentation=https://github.com/VoidInTheShell/Xboard-Node
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${INSTALL_ROOT}
EnvironmentFile=-${CREDENTIALS_FILE}
ExecStart=${BINARY_PATH} -c ${CONFIG_FILE}
Restart=always
RestartSec=5
LimitNOFILE=1048576
NoNewPrivileges=true
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF_UNIT
}

backup_existing_state() {
    BACKUP_PATH="${BACKUP_DIR}/$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$BACKUP_PATH"
    if [ -x "$BINARY_PATH" ]; then
        cp "$BINARY_PATH" "$BACKUP_PATH/xboard-node"
    fi
    if [ -x "$CLI_PATH" ]; then
        cp "$CLI_PATH" "$BACKUP_PATH/xbctl"
    fi
    if [ -x "$UPDATER_BINARY_PATH" ]; then
        cp "$UPDATER_BINARY_PATH" "$BACKUP_PATH/xboard-updater"
    fi
    if [ -f "$CONFIG_FILE" ]; then
        cp "$CONFIG_FILE" "$BACKUP_PATH/config.yml"
    fi
    if [ -f "$CREDENTIALS_FILE" ]; then
        cp "$CREDENTIALS_FILE" "$BACKUP_PATH/credentials.env"
    fi
    if [ -f "$INSTALL_META" ]; then
        cp "$INSTALL_META" "$BACKUP_PATH/install-meta.json"
    fi
    if [ -f "$UPDATER_CONFIG_PATH" ]; then
        cp "$UPDATER_CONFIG_PATH" "$BACKUP_PATH/updater-config.json"
    fi
    if [ -f "$UPDATER_TOKEN_PATH" ]; then
        cp "$UPDATER_TOKEN_PATH" "$BACKUP_PATH/updater-token"
    fi
    if [ -f "$UPDATER_SERVICE_PATH" ]; then
        cp "$UPDATER_SERVICE_PATH" "$BACKUP_PATH/xboard-updater.service"
    fi
    if [ -f "$COMPOSE_FILE" ]; then
        cp "$COMPOSE_FILE" "$BACKUP_PATH/compose.yaml"
    fi
    if [ -f "$SERVICE_PATH" ]; then
        cp "$SERVICE_PATH" "$BACKUP_PATH/${SERVICE_NAME}"
        SERVICE_EXISTED=1
    else
        SERVICE_EXISTED=0
    fi
}

stop_existing_service() {
    if [ "$INSTALLATION_METHOD" = "compose" ] && [ -f "$COMPOSE_FILE" ]; then
        docker compose --project-name "$COMPOSE_PROJECT" --file "$COMPOSE_FILE" down >/dev/null 2>&1 || true
        return
    fi
    if [ "$INSTALLATION_METHOD" = "docker" ]; then
        docker rm -f "$UPDATER_CONTAINER_NAME" "$NODE_CONTAINER_NAME" >/dev/null 2>&1 || true
        return
    fi
    if [ -f "$SERVICE_PATH" ] || systemctl is-active "$SERVICE_NAME" >/dev/null 2>&1; then
        systemctl stop "$SERVICE_NAME" >/dev/null 2>&1 || true
    fi
}

install_staged_files() {
    stop_existing_service
    if [ "$INSTALLATION_METHOD" = "systemd" ]; then
        install -m 755 "$TMP_DIR/xboard-node" "$BINARY_PATH"
    fi
    install -m 600 "$TMP_DIR/config.yml" "$CONFIG_FILE"
    install -m 600 "$TMP_DIR/credentials.env" "$CREDENTIALS_FILE"
    install -m 644 "$TMP_DIR/install-meta.json" "$INSTALL_META"
    if [ -f "$0" ] && [ "$(realpath "$0")" != "$(realpath "$INSTALLER_COPY_PATH" 2>/dev/null || echo "$INSTALLER_COPY_PATH")" ]; then
        install -m 755 "$0" "$INSTALLER_COPY_PATH"
    fi
    install -m 755 "$TMP_DIR/xbctl" "$CLI_PATH"
    ln -sf "$CLI_PATH" /usr/bin/xbctl 2>/dev/null || true
    if [ -f "$TMP_DIR/xboard-updater" ]; then
        install -d -m 755 "$(dirname "$UPDATER_BINARY_PATH")"
        install -m 755 "$TMP_DIR/xboard-updater" "$UPDATER_BINARY_PATH"
    fi
    if [ -f "$TMP_DIR/updater/config.json" ]; then
        install -d -m 700 "$UPDATER_CONFIG_DIR"
        install -m 600 "$TMP_DIR/updater/config.json" "$UPDATER_CONFIG_PATH"
        install -m 600 "$TMP_DIR/updater/token" "$UPDATER_TOKEN_PATH"
    fi
    if [ -f "$TMP_DIR/deployment/compose.yaml" ]; then
        install -m 600 "$TMP_DIR/deployment/compose.yaml" "$COMPOSE_FILE"
    fi
    if [ "$INSTALLATION_METHOD" = "systemd" ]; then
        install -m 644 "$TMP_DIR/${SERVICE_NAME}" "$SERVICE_PATH"
        systemctl daemon-reload
        systemctl enable "$SERVICE_NAME" > /dev/null 2>&1
    fi
}

start_container_deployment() {
    if [ "$INSTALLATION_METHOD" = "docker" ]; then
        docker rm -f "$UPDATER_CONTAINER_NAME" "$NODE_CONTAINER_NAME" >/dev/null 2>&1 || true
        docker run -d --name "$NODE_CONTAINER_NAME" --restart unless-stopped --init --network host \
            --env-file "$CREDENTIALS_FILE" \
            -v "${CONFIG_FILE}:/etc/xboard-node/config.yml:ro" \
            -v "${INSTALL_ROOT}/certs:/etc/xboard-node/certs" \
            -v "${INSTALL_ROOT}/state:/etc/xboard-node/state" \
            -v "${INSTALL_ROOT}/instances:/etc/xboard-node/instances" \
            "ghcr.io/voidintheshell/xboard-node:${RELEASE_VERSION}" >/dev/null
        docker run -d --name "$UPDATER_CONTAINER_NAME" --restart unless-stopped --network host \
            --read-only --tmpfs /tmp:rw,noexec,nosuid,size=64m \
            --security-opt no-new-privileges --cap-drop ALL \
            -v /var/run/docker.sock:/var/run/docker.sock \
            -v "${UPDATER_CONFIG_PATH}:${UPDATER_CONFIG_PATH}:ro" \
            -v "${UPDATER_TOKEN_PATH}:${UPDATER_TOKEN_PATH}:ro" \
            -v "${UPDATER_STATE_DIR}:${UPDATER_STATE_DIR}" \
            "$UPDATER_IMAGE" updater run --config "$UPDATER_CONFIG_PATH" >/dev/null
        return
    fi
    docker compose --project-name "$COMPOSE_PROJECT" --file "$COMPOSE_FILE" up -d xboard-node xboard-updater >/dev/null
}

wait_for_health() {
    if [ "$INSTALLATION_METHOD" != "systemd" ]; then
        local attempt=0
        while [ "$attempt" -lt 30 ]; do
            if docker inspect --format '{{.State.Running}}' "$NODE_CONTAINER_NAME" 2>/dev/null | grep -qx true \
                && curl -fsS "http://127.0.0.1:${HEALTH_PORT}/healthz" >/dev/null 2>&1; then
                return 0
            fi
            sleep 1
            attempt=$((attempt + 1))
        done
        return 1
    fi
    if ! systemctl is-active "$SERVICE_NAME" >/dev/null 2>&1; then
        return 1
    fi
    if [ "$HEALTH_ENABLED" -eq 0 ]; then
        return 0
    fi
    local attempt=0
    local max_attempts=30
    while [ "$attempt" -lt "$max_attempts" ]; do
        if ! systemctl is-active "$SERVICE_NAME" >/dev/null 2>&1; then
            return 1
        fi
        if curl -fsS "http://127.0.0.1:${HEALTH_PORT}/healthz" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        attempt=$((attempt + 1))
    done
    return 1
}

show_recent_logs() {
    if command -v journalctl >/dev/null 2>&1; then
        journalctl -u "$SERVICE_NAME" -n 30 --no-pager || true
    fi
}

start_service() {
    if [ "$INSTALLATION_METHOD" != "systemd" ]; then
        start_container_deployment
        if ! wait_for_health; then
            log_error "Container deployment failed health check"
            return 1
        fi
        return 0
    fi
    if systemctl is-enabled "$SERVICE_NAME" >/dev/null 2>&1; then
        systemctl restart "$SERVICE_NAME"
    else
        systemctl start "$SERVICE_NAME"
    fi
    if ! wait_for_health; then
        log_error "Service failed health check"
        show_recent_logs
        return 1
    fi
}

perform_install() {
    validate_install_request
    detect_current_state
    require_reconfigure_confirmation
    TMP_DIR=$(mktemp -d)
    ensure_dirs
    # Complete release download/version preflight before exchanging the
    # one-time enrollment token. A bad artifact must not consume a usable
    # installation credential.
    stage_binary
    stage_xbctl
    stage_updater
    enroll_machine
    render_config
    render_updater_config
    render_compose_file
    if [ "$INSTALLATION_METHOD" = "systemd" ]; then
        render_service
    fi
    backup_existing_state
    install_staged_files
    start_service

    log_info "Installation succeeded"
    log_info "Service: ${SERVICE_NAME}"
    log_info "Config: ${CONFIG_FILE}"
    log_info "Credentials: ${CREDENTIALS_FILE}"
    if [ "$HEALTH_ENABLED" -eq 1 ]; then
        log_info "Health: http://127.0.0.1:${HEALTH_PORT}/healthz"
    fi
    log_info "CLI: ${CLI_PATH}  (run '${CLI_PATH} list' if xbctl is not in PATH)"
    if [ -f /etc/xboard-updater/config.json ] && { [ -x "$UPDATER_BINARY_PATH" ] || [ -f "$TMP_DIR/xboard-updater" ]; }; then
        "$CLI_PATH" updater install --config /etc/xboard-updater/config.json
    elif [ -f /etc/xboard-updater/config.json ]; then
        log_warn "Updater config exists, but no standalone xboard-updater is installed; rerun with --updater-version or --updater-binary"
    else
        log_info "Standalone updater is independent from xbctl. Provide --updater-version from an Admin Release, then enroll with 'xbctl updater install'."
    fi
}

perform_upgrade() {
    if [ "$INSTALLATION_METHOD" != "systemd" ]; then
        log_error "Use the standalone Updater to upgrade a Docker or Compose installation"
        return 1
    fi
    detect_current_state
    if [ "$CURRENT_STATE" = "fresh" ]; then
        log_warn "No existing install found; falling back to install"
        perform_install
        return
    fi
    TMP_DIR=$(mktemp -d)
    ensure_dirs
    stage_binary
    stage_xbctl
    stage_updater
    render_service
    backup_existing_state
    install -m 755 "$TMP_DIR/xboard-node" "$BINARY_PATH"
    install -m 755 "$TMP_DIR/xbctl" "$CLI_PATH"
    if [ -f "$TMP_DIR/xboard-updater" ]; then
        install -d -m 755 "$(dirname "$UPDATER_BINARY_PATH")"
        install -m 755 "$TMP_DIR/xboard-updater" "$UPDATER_BINARY_PATH"
    fi
    ln -sf "$CLI_PATH" /usr/bin/xbctl 2>/dev/null || true
    install -m 644 "$TMP_DIR/${SERVICE_NAME}" "$SERVICE_PATH"
    systemctl daemon-reload
    systemctl restart "$SERVICE_NAME"
    if ! wait_for_health; then
        log_error "Upgrade health check failed"
        show_recent_logs
        return 1
    fi
    log_info "Upgrade succeeded"
}

confirm_uninstall() {
    if [ "$YES" -eq 1 ]; then
        return
    fi
    echo
    read -r -p "Proceed with uninstall? [y/N]: " answer
    if ! [[ "$answer" =~ ^[Yy]$ ]]; then
        log_warn "Uninstall cancelled"
        exit 0
    fi
}

perform_uninstall() {
    confirm_uninstall
    if [ -f "$SERVICE_PATH" ]; then
        systemctl stop "$SERVICE_NAME" >/dev/null 2>&1 || true
        systemctl disable "$SERVICE_NAME" >/dev/null 2>&1 || true
        rm -f "$SERVICE_PATH"
        systemctl daemon-reload || true
    fi
    rm -f "$BINARY_PATH"
    rm -f "$CLI_PATH"
    rm -f "$UPDATER_BINARY_PATH"
    rm -f "$UPDATER_CONFIG_PATH" "$UPDATER_TOKEN_PATH" "$UPDATER_SERVICE_PATH"
    systemctl daemon-reload >/dev/null 2>&1 || true
    rm -f /usr/bin/xbctl 2>/dev/null || true
    if [ "$PURGE" -eq 1 ]; then
        rm -rf "$INSTALL_ROOT"
        log_info "Removed ${INSTALL_ROOT}"
    else
        rm -f "$INSTALL_META"
        log_info "Config preserved under ${INSTALL_ROOT}"
    fi
    log_info "Uninstall complete"
}

perform_status() {
    detect_current_state
    echo
    echo -e "${BOLD}xboard-node install status${NC}"
    echo "  state:   ${CURRENT_STATE}"
    if [ -f "$INSTALL_META" ]; then
        echo "  meta:    ${INSTALL_META}"
        if [ -x "$CLI_PATH" ]; then
            "$CLI_PATH" list 2>/dev/null || true
        else
            # Simple key extraction from JSON (no Python needed)
            local val
            for key in config_mode version latest_instance_id instance_count updated_at; do
                val=$(sed -n "s/.*\"${key}\": *\"\{0,1\}\([^\"]*\)\"\{0,1\}.*/\1/p" "$INSTALL_META" | head -1)
                val="${val%,}"  # strip trailing comma from numeric JSON values
                [ -n "$val" ] && echo "  ${key}: ${val}"
            done
        fi
    fi
    if [ -f "$SERVICE_PATH" ]; then
        echo "  service: ${SERVICE_NAME}"
        systemctl status "$SERVICE_NAME" --no-pager || true
    fi
}

main() {
    parse_args "$@"
    case "$ACTION" in
        help)
            usage
            exit 0
            ;;
        status)
            ensure_systemd
            perform_status
            exit 0
            ;;
    esac

    check_root
    detect_arch
    detect_os
    if [ "$INSTALLATION_METHOD" = "systemd" ]; then
        ensure_systemd
    else
        ensure_docker
    fi
    install_dependencies

    # Resolve once so Node and xbctl cannot download two different latest releases.
    if [ "$ACTION" = "install" ] || [ "$ACTION" = "upgrade" ]; then
        if [ -n "$ENROLLMENT_TOKEN" ] || [ -z "$BINARY_SOURCE" ] || [ -z "$CLI_BINARY_SOURCE" ]; then
            resolve_release_version
        fi
    fi

    case "$ACTION" in
        install)
            perform_install
            ;;
        upgrade)
            perform_upgrade
            ;;
        uninstall)
            perform_uninstall
            ;;
        *)
            log_error "Unknown action: $ACTION"
            usage
            exit 1
            ;;
    esac
}

main "$@"
