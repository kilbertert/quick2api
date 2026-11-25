# Windows 源码、测试和生产环境

本文记录 2026-11-25 经用户审阅通过的目录方案，以及清理旧安装前核对的功能覆盖。它是重建契约，不表示下列脚本、独立 profile 或新安装已经实现和验收。

## 固定目录与工作流程

| 用途 | 根目录 | API / 自有 outpost |
|---|---|---|
| 源码 | `D:\codex-workspace\github opencode\quick2api` | 不运行常驻服务 |
| 测试 | `D:\codex-workspace\github opencode\quick2api-test` | `127.0.0.1:20377` / `20378` |
| 唯一生产安装 | `D:\UserProgram\quick2api` | `127.0.0.1:19377` / `19378` |

源码修复 → 生成 release 候选包 → 测试区安装和验证 → 提交部署差异供用户批准 → 用同一个已验证的包替换生产安装。无需另设发布流程或 releases 目录。

测试根下使用 `packages/<build-id>`、`service`、`runs/<run-id>`、`fixtures`、`evidence`、`rollback` 和 `cache`。测试 service 与生产安装都使用 `bin`、`config`、`state`、`logs`、`run`、`scripts`，生产另有 `package-info.json`。测试失败不得回退连接生产。

首次整理只保留源码，按批准清单丢弃旧产物和历史证据，不建立历史迁移目录。生产根暂时留空，首次部署另行批准。以后正常升级才在测试区 rollback 保存上一个已验收程序包；它没有密钥、运行入口和监听端口。

## 构建和配置契约

从明确源码版本构建 `./cmd/quick2api` 和 `./cmd/quick2api-mcp`，程序进入测试 packages。正式候选要求干净提交；包清单记录提交、Go 版本、构建参数及每个二进制 SHA256，测试结果绑定包哈希，部署时不得重新构建另一份。

从 `config.example.yaml` 生成环境配置。测试和生产分别使用 key、配置、客户端设置、state、日志及工具等待队列；密钥不得写入源码、命令行、可分享包或 MCP 注册 JSON。MCP 只通过显式 `-config` 路径读取本机配置。

测试 MCP 必须指向测试 bin/config，生产 MCP 指向生产 bin/config。通过 Quick 支持的流程登记、选择和批准 profile；独立 profile 的可用性尚未验证，不复制凭据或批准记录，不借用生产注册宣称测试隔离通过。实际工具名称从各环境目录核实。官方 Quick 的 8780 实例不附着、不接管。

## 启停和 Claude 入口契约

启动指定安装内的二进制、配置、state 和工作目录，参数正确处理空格。后台启动使用隐藏窗口，stdout/stderr 写入安装 logs；有界检查进程提前退出和服务就绪。`healthz` 只能说明接口响应，不能证明实例归属或工具链可用。

run 记录 PID、创建时间、程序绝对路径和自有子进程归属。停止前重新核实身份，只停止该安装拥有的进程；禁止按名称批量停止官方 agent 或接管端口。启动失败不自动连接另一环境。

`claude-quick` 为生产入口，`claude-quick-test` 为测试入口。包装只给 Claude 子进程设置对应 endpoint/key/settings，清除冲突的凭据环境变量，不改变用户全局设置。PowerShell 使用 `@args` 完整透传参数并返回实际退出码；`--resume`、`--continue`、`--dangerously-skip-permissions` 等仍由 Claude 自身解释。旧安装硬编码的 key 不迁移。

包、配置和注册准备好后，测试服务的前台启动形式为：

```powershell
$q2aServiceRoot = 'D:\codex-workspace\github opencode\quick2api-test\service'
$q2aExecutable = Join-Path $q2aServiceRoot 'bin\quick2api.exe'
$q2aConfig = Join-Path $q2aServiceRoot 'config\test.yaml'
$q2aState = Join-Path $q2aServiceRoot 'state'
& $q2aExecutable -config $q2aConfig -state-dir $q2aState
```

这只是显式参数示例，不能替代后台启停脚本、注册和验收。

## 旧脚本与源码覆盖核对

清理清单含 50 个本地脚本/探针，并单列旧 CMD 包装。没有逐字节相同的已跟踪副本；以下是目的级覆盖核对，不表示旧脚本可以原样重建，也不表示此次已重新运行线上回归。

| 旧脚本的目的 | 当前仓库中的覆盖 |
|---|---|
| MCP 初始化、工具列表、调用、取消、EOF 和预算 | `internal/mcprelay/stdio_test.go` |
| 历史副本重建、完整性和缺失数据检查 | `scripts/native-regression/trace.cjs` 及 trace/private 测试 |
| Read、ENOENT 后续读取、AskUserQuestion、无工具请求 | `scripts/native-regression/run.cjs` |
| 并发、warm append、fork、目录变化、tool_choice none、取消后新轮次 | `scripts/native-regression/extended.cjs`；Anthropic native 测试 |
| 缓存表示变化、提问结果续接、尾部系统上下文、语义重复拒绝 | `scripts/native-regression/contract.cjs`、`verify-contract.cjs`；`internal/api/anthropic/native_contract_test.go` |
| 附件和工具历史、工具形状、文本不能伪造成可执行调用 | `internal/api/anthropic/messages_test.go`、native contract/native 测试 |
| 历史 Unknown tool 与真实本轮失败的诊断区分 | `internal/api/anthropic/native_diagnostics_test.go` |
| 流、叙述、checkpoint、fork/目录/过期、HTTP 断开 | `internal/api/anthropic/native_performance_test.go`、`native_disconnect_test.go` |
| body budget 和 token 计数 | `internal/app/app_test.go` |
| 精确 relay/schema、连通性错误和握手门禁 | `internal/chat/native_test.go`、`handshake_test.go`、`handshake_gate_test.go`；`scripts/native-regression/handshake/main.go` |
| 原生调用与协议说明、结果汇总 | `docs/native-client-relay.md`、`docs/protocol.md`、`scripts/native-regression/summarize.cjs` |
| 旧部署、启动、停止和 Claude 包装 | 本文保留必要行为契约；新布局的可执行脚本仍需实现和测试 |

一次性源码插桩、构建对比、REA 抽取和私有原始会话不是运行依赖，不迁入源码。删除原始证据后无法再声称对其原样回放；历史会话中的模型拒绝和上游过滤也不会因清理目录而得到修复。

**待实现的隔离门禁：** `run.cjs`、`extended.cjs`、`contract.cjs` 目前仍硬编码生产端口 19377。在改为显式验证测试端点和目录之前，不运行这三份线上脚本。独立 Quick profile、环境启停和包安装同样需要后续验证，不能把本次源码覆盖核对写成全链路验收通过。
