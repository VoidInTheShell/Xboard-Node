# 从面板更新节点实例

`xbctl updater` 在 Linux 宿主机提供独立更新服务，支持 systemd 二进制、Docker、Docker Compose。节点进程被停止或替换时，更新服务仍然运行；同一宿主机上的实例串行更新，配置、证书和持久化数据保留。

首次接入需安装含 `updater` 命令的自有准确版本 `xbctl`，在 Xboard 后端执行 `php artisan update:executor node --machine-id=<服务器ID>` 签发该主机的专用凭据。将返回的 token 写入宿主机 `/etc/xboard-updater/token`，不要与节点连接 Token 混用。以 `updater.sample.json` 创建 `/etc/xboard-updater/config.json`，填写真实面板 URL、安装路径、服务名及健康地址。

配置、凭据由 root 所有，权限 `0600`；配置目录和 `/var/lib/xboard-updater` 由 root 所有，权限 `0700`，父目录不可被其他用户写入。然后执行：

```sh
xbctl updater check --config /etc/xboard-updater/config.json
xbctl updater install --config /etc/xboard-updater/config.json
systemctl status xboard-updater.service
```

`install.sh` 发现已有 `/etc/xboard-updater/config.json` 时会安装更新服务；没有配置时不会创建凭据或假装已接入。`check` 仅校验配置，不运行任务。更新器只读取本机登记的目标，不接受面板下发的 shell 命令或任意下载源。

一个 node/machine/standalone 进程以及其所有入站是一个安装实例；不能把共享同一二进制、systemd 服务或容器的入站登记为多个更新实例。添加独立安装时，在配置的 `targets` 数组增加单独的 ID、实际服务和健康地址。

Docker 实例设置 `method: "docker"` 和 `container`；Compose 实例设置 `method: "compose"`、绝对 `compose_file`、`compose_project`、`compose_service`。配置/证书必须挂载持久化；不支持自动删除容器，手工静态 IP 容器请使用 Compose。Compose 后续手工启动需加载状态目录内的 `compose-<project>.json` 版本覆盖文件，避免原 `.env` 将已选版本覆盖回去。

Admin“版本更新 → 节点客户端更新”展示实例当前版本及当前分支检测到的版本。点击单行或批量升级后，在弹窗选择主线/Dev 和准确版本；同机串行、每实例独立记录结果。版本下载失败不替换原实例；替换后验证准确版本和配置的健康地址，失败尝试恢复旧二进制/容器。业务侧的面板重连与入站恢复仍应在测试环境中验收，HTTP 健康检查不能替代真实流量测试。

排障使用 `journalctl -u xboard-updater.service`，任务日志和快照在 `/var/lib/xboard-updater`。不要删除执行中的 `active.json`，不要同时手工替换实例。恢复失败会锁定后续任务，人工恢复并验证后在面板后端使用 `update:executor node --machine-id=<ID> --resume` 解锁。快照可能含凭据，禁止上传公开仓库。版本发布不会自动升级已接入主机。

面板自身更新、MCP 参数和数据库恢复规则见 Xboard 仓库 `deploy/updater/README.md`。
