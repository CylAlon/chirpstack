# Alinkwise ChirpStack 扩展说明

这个目录用于集中放置 Alinkwise 对 ChirpStack 的自定义扩展，目标是减少对 ChirpStack 上游源码的分散修改，后续拉取上游新版本时便于溯源和迁移。

## 当前新增能力

- 新增 `AlinkwiseService.ListTenantDevices`。
- 提供租户维度的终端列表查询，避免前端为了展示“终端管理”而循环查询每个 Application。
- 支持服务端分页、搜索、状态过滤、应用过滤、设备配置过滤、标签过滤和排序。
- 返回终端所属应用、设备配置、最后在线时间、运行状态、电量/信号状态、JoinEUI、Class 等列表展示字段。
- 新增 `AlinkwiseService.ClearGatewayFrameLog`，用于清除单个网关 Redis 实时帧缓存 `gw:{gateway_id}:stream:frame`。
- 新增 `AlinkwiseService.ClearDeviceFrameLog`，用于清除单个终端 Redis 实时帧缓存 `device:{dev_eui}:stream:frame`。
- 新增 `AlinkwiseService.ClearDeviceMetrics`，用于清除单个终端 Redis 指标缓存 `metrics:{device:{dev_eui}}*`。

## 主要文件

- `chirpstack/src/alinkwise/mod.rs`
  - Alinkwise 扩展模块入口。
- `chirpstack/src/alinkwise/api.rs`
  - gRPC 服务实现，负责权限校验、请求参数转换和响应组装。
- `chirpstack/src/alinkwise/device_query.rs`
  - 终端列表的高效查询 SQL / Diesel 实现。
- `chirpstack/src/alinkwise/README.md`
  - 当前说明文档。

## 必要接入点

这些文件是为了让自定义服务接入 ChirpStack 主程序和 API 生成流程，不可完全避免：

- `chirpstack/src/main.rs`
  - 增加 `mod alinkwise;`。
- `chirpstack/src/api/mod.rs`
  - 注册 `AlinkwiseServiceServer`。
- `api/proto/api/alinkwise.proto`
  - grpc-web / HTTP 网关使用的 API proto。
- `api/rust/proto/chirpstack/api/alinkwise.proto`
  - Rust API crate 使用的 API proto。
- `api/rust/build.rs`
  - 把 `alinkwise.proto` 加入 Rust proto 生成列表。
- `api/grpc-web/Makefile`
  - 把 `alinkwise.proto` 加入前端 grpc-web 生成列表。

## 前端接入

- `alinkwise-ui/src/lib/chirpstack-admin.ts`
  - 新增 `listTenantDevices()` 封装。
- `alinkwise-ui/src/components/admin/device-page.tsx`
  - 终端管理页面改为调用 `ListTenantDevices`，支持租户、应用、状态和搜索过滤。
- `alinkwise-ui/src/data/admin.ts`
  - 恢复 `/device/mote` 终端管理菜单入口。
- `alinkwise-ui/src/pages/admin-page.tsx`
  - 接入 `DevicePage` 页面和终端详情面包屑。

## 更新上游时的检查清单

1. 优先保留整个 `chirpstack/src/alinkwise/` 目录。
2. 对比并重新应用上面列出的必要接入点。
3. 确认 `api/proto/api/alinkwise.proto` 和 `api/rust/proto/chirpstack/api/alinkwise.proto` 仍然一致。
4. 运行 `cargo check -p chirpstack` 确认 Rust 服务仍可编译。
5. 如前端需要调用新 proto，进入 `chirpstack/api/grpc-web` 运行 `make api` 重新生成本地 grpc-web 文件。
