# quick2api

> [English](README.md) | **中文**

quick2api 是一个非官方协议适配器：把你 **Amazon Quick** 桌面端账号背后
实际驱动的模型，以常见的 `/v1` API（OpenAI `chat/completions` 与
Anthropic `messages`）的形式暴露出来——任何标准客户端（OpenAI SDK、
Claude Code、各种聊天客户端）都能像调普通端点一样调它。

> **免责声明**：本项目与 Amazon 无任何关联、未获其背书。它使用你自己
> Quick 账号的本地登录凭据（桌面应用已经存好的那一份）访问其内部
> WebSocket 接口。仅供个人账号自用；请自行确认符合 Amazon Quick 的
> 服务条款。

## 做什么

```
你的客户端 ── /v1/chat/completions ──▶ quick2api ──AIDP WSS──▶ Amazon Quick 云端 agent
           ── /v1/messages ─────────┘        ▲
                                 本地自起的 quickwork-agent outpost
                                 → /auth/sncs-session → id_token
```

- `POST /v1/chat/completions` —— OpenAI 接口，SSE 流式 + 非流式。
- `POST /v1/messages` —— Anthropic Messages API（Claude Code 与
  Anthropic SDK 走这条）；`/v1/messages/count_tokens` 作别名接受。
- `GET /v1/models` —— 对外的 model 名 → 上游 `agent_mode` 映射。
- `GET /healthz` —— 会话源状态（无密钥）。
- **大 prompt 走带外通道**：超过 ~55 KB 的 `user_message` 先上传到
  Quick 附件服务，再由一条短消息引用——一次模型回合，不切片。
  附件不可用时回退到分片回合。
- 用量透传：`turn_usage`（token、cache_read/write、`cost_usd`、
  `resolved_model_id`）落在 OpenAI `usage` 字段上。
- **跑在你 Quick 登录的那台机器上**——目前是 Windows（outpost 要读桌面
  应用的凭据库）；HTTP API 监听地址随你绑。
- `-probe` 一次性探针：起服务前先验证整条链路。
- 单个静态 Go 二进制；依赖仅 `gorilla/websocket` 与 `gopkg.in/yaml.v3`。

## 上游链路（为什么离不开桌面端）

1. `quick2api` 自己 spawn
   `quickwork-agent --outpost-only --manifest desktop --port <p>`，stdin
   第一行写自己生成的 bearer——与桌面应用完全同一条路径，但用独立
   端口，绝不碰应用自己的 8780 实例。
2. `GET /auth/sncs-session` 返回 `id_token`、`ws_host`、`provider`
   （`social`）、`actor_arn`、`aws_account_id`。
3. 聊天走
   `wss://<ws_host>/streaming/quickwork-chat-bidirectional-stream`：
   上行 `rawInputEvent` JSON、下行 AWS event-stream；完整协议细节见
   [docs/protocol.md](docs/protocol.md)（逆向实录，值得读）。

`agent_mode:"smart"` 目前在上游解析为 `us.anthropic.claude-opus-5-5`
（据 `turn_usage`），走的是你 Quick 订阅额度。

## 快速开始

前置条件：本机装有 **并登录过** Amazon Quick 桌面版（要反代的就是这个
账号），Go 1.22+（或直接用预编译二进制）。

```bash
go build -o quick2api ./cmd/quick2api

# 一键链路验证——起 outpost、取 session、发探针消息、打印回复与用量：
./quick2api -probe

# 起服务：
cp config.example.yaml config.yaml    # 可选——本机默认就能跑
./quick2api
```

然后把任何 OpenAI 客户端指到 `http://127.0.0.1:8377/v1`：

```bash
curl http://127.0.0.1:8377/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"quick","messages":[{"role":"user","content":"Reply with exactly: PONG"}],"stream":true}'
```

或者把 **Claude Code** 指过来（Anthropic 接口面）：

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:8377
export ANTHROPIC_AUTH_TOKEN=<config.yaml 里的 api.api_key>
claude -p "say PONG"
```

要从另一台机器访问（比如 dev 服务器），保持 `server.listen` 回环，
用 SSH 隧道：

```bash
ssh -N -R 8377:127.0.0.1:8377 user@dev-host
```

## 配置

见 [config.example.yaml](config.example.yaml)，每个键都有注释。配置查找
顺序：`-config` → `$QUICK2API_CONFIG` → `./config.yaml` → 平台用户配置目
录。状态/调试日志在 `$QUICK2API_STATE_DIR` 或平台 state 目录下。

最常用的几个键：

- `api.api_key` —— 下游 Bearer 校验（空=不校验，仅回环监听时可接受）。
- `quick.agent_modes` —— 客户端 model 名 → 上游 `agent_mode`。
- `quick.outpost_url`/`outpost_token` —— 挂到你已经跑着的 outpost 上。
- `quick.proxy` —— 上行 WSS 代理（默认走系统 `HTTPS_PROXY`）。

## 限制

- **凭据来自桌面端。** quick2api 继承 Quick 登录的账号——它自己无法
  独立建会话。登出/吊销 → 上游回 401、`/healthz` 报 `degraded`，直到
  应用里重登为止。
- **会话复用是可选的。** 不带 `X-Conversation-Id`/`X-Session-Id` 时
  每个请求仍开新的上游 turn；带上则复用严格追加的历史。历史分歧或
  重连时建立新的上游 thread；同一客户端会话的请求串行执行。
- **服务端执行器是可选的。** config 里的 `tools:` 可启用本地
  executor（`bash`/`files`），`tools.max_tool_rounds` 限轮。
  Quick 也有自己的原生工具，与这些执行器相互独立。
- **多账号要不同 outpost。** `quick.accounts` 在 lane 间轮询，但真
  多账号得一个 lane 一个 outpost（分开的 Quick 安装或挂别的
  `outpost_url`）。`max_concurrency` 是进程级，lane 间仍共享 CPU/内存。
- **客户端工具通过文本契约兼容。** 透传模式把 schema 写进 prompt，
  将完整的 `{type:"tool_use",...}` 回复转换为结构化 API 工具调用，
  交由客户端执行。这不会向 Quick 注册原生工具，上游仍可能拒绝。
  修复、验证结果和限制见 [客户端工具兼容性说明](docs/client-tool-compatibility.md)。

## v1.1 新加

- `image_url`/`file`/`image`/`document` 块走附件通道（与应用粘贴
  文件同一条）。
- `X-Conversation-Id`/`X-Session-Id` 按 id 复用上游 thread——多轮
  是真 thread 而不是每次重拼历史。
- `quick.accounts` 把 bridge 变成 lane 池（轮询 + auth/5xx 时每
  lane 冷却）。
- `tools.bash`/`tools.files` 开起来后跑服务端工具循环。
- `/v1/chat/completions` 现在接受 `tools`（按 `tools:` 配置决定是否
  执行）；`/v1/messages` 一直接受。

## 目录

```
cmd/quick2api/      入口（-config/-state-dir/-version/-probe）
cmd/wsdial/         WSS 握手状态码探针（诊断工具）
internal/app/       HTTP 路由 + 鉴权
internal/api/openai OpenAI 适配层（/v1/chat/completions、/v1/models）
internal/api/anthropic Anthropic 适配层（/v1/messages）
internal/bridge/    会话源 → 聊天客户端
internal/chat/      AIDP 双向流客户端（核心）
internal/eventstream AWS event-stream 解码器
internal/session/   session 模型 + outpost 生命周期
internal/config/    YAML 配置 + 路径解析
internal/debuglog/  按请求一个目录的调试记录
docs/protocol.md    线上验证过的协议实录（值得一读）
```

## 许可证

MIT —— 见 [LICENSE](LICENSE)。
