# CCB 被动捕获兜底方案

> 提交: `39c9879b` | 日期: 2026-05-18 | 参与者: Claude, Codex, Droid, Kimi

---

## 1. 问题背景

### 现象

通过 `warp-ask` 发送消息给 Agent 后，Agent 能理解消息并生成回复，但不走约定的 `warp-reply` 或 CCB 标记格式回复，导致回复丢失。

**典型案例**: Kimi 收到问候消息后生成了回复文本，但直接输出到终端而非使用 `warp-reply` 或 `[CCB_START/END]` 标记，bus 端无法识别，回复被当作普通输出丢弃。

### 根因

不是系统 bug，而是 **LLM Agent 指令遵循的概率性**。

- 当前回复机制依赖 Agent 主动配合（调用 `warp-reply` 或输出 CCB 标记）
- 不同 Agent 的指令遵循能力参差不齐
- LLM 的行为是非确定性的，无法用确定性协议强制约束
- 用确定性协议思维约束概率性 LLM 行为是范畴错误

---

## 2. 方案设计

### 2.1 核心原则

- **可靠性先于质量**：先确保回复不丢失，再逐步提升质量
- **绝不丢弃任何回复**：不符合格式的输出降级而非丢弃
- **零侵入**：不改动现有标记匹配逻辑，只在所有策略失败时触发兜底

### 2.2 三层回复通道

| 优先级 | 通道 | confidence | 说明 |
|--------|------|-----------|------|
| 1 | `warp-reply` / CCB_START/END 标记 | 0.9 - 1.0 | Agent 主动配合，回复最干净（现有逻辑不变） |
| 2 | 被动捕获 raw_output | 0.7 | Agent 零配合，bus 自动截取终端输出（**新增**） |
| 3 | timeout partial | 0.3 | 超时兜底，有总比没有好（**新增**） |

### 2.3 被动捕获机制

利用 `RawOutputCapture::register_request()` 的调用时机建立时间线切分：

```
T1 = register_request(req_id, pane)  ──→ 从此刻起，该 pane 所有 PTY 输出关联到 req_id
T2 = 输出稳定超时 或 下一个 register_request  ──→ T1~T2 之间的输出即为完整回复
```

**架构保障**：
- 每个 pane 同时只有一个活跃请求（`has_active_for_terminal` 串行化约束），天然防止多请求输出干扰
- `register_request` 在 `inject_prompt` 之前调用，确保捕获完整

### 2.4 六步清洗管线

被动捕获的 raw_output 混杂了大量噪声，需要清洗：

| 步骤 | 操作 | 说明 |
|------|------|------|
| 1 | 去除提示词回显 | 找到 `[CCB_REQ_ID:xxx]` 行，切掉它及之前的内容 |
| 2 | 去除指令文本 | 移除 `After completing...`、`warp-reply` 等 11 个 instruction patterns |
| 3 | 去除残留 CCB 标记 | Regex 移除未闭合的 `[CCB_START/END/DONE]` 标记文本 |
| 4 | 压缩空行 | 连续空行最多保留 2 个 |
| 5 | Provider chrome 清洗 | Kimi: `functions.Shell:N`、`<system>...</system>`；Codex: `›` 状态行 |
| 6 | Trim + thinking 检测 | 首尾空白清理，检测"让我思考"等关键词加入 warnings |

清洗后内容 < 50 字符视为无效回复，不触发 finalize。

### 2.5 自适应完成判定

替代固定超时，按 Agent 类型区分等待策略：

| Agent 类型 | 策略 | 输出稳定时间 | 最小运行时间 |
|-----------|------|------------|------------|
| 有 session listener | `SessionAware` | >= 2 秒 | >= 3 秒 |
| 无 session listener | `RawOutputOnly` | >= 8 秒 | >= 10 秒 |
| 全局兜底 | `HardTimeout` | 120 秒 | - |

判定逻辑：`should_finalize_passive(req_id, output_stable_for)` 综合策略、运行时间和输出稳定性。

### 2.6 元数据体系

所有回复（无论来源）携带结构化元数据：

```rust
struct ReplyCaptureMeta {
    source: CaptureSource,       // 来源枚举
    confidence: f64,             // 0.0 ~ 1.0 可信度
    warnings: Vec<String>,       // 警告列表
    raw_len: usize,              // 原始输出长度
    filtered_len: usize,         // 清洗后长度
    truncated: bool,             // 是否截断
}
```

CaptureSource 枚举及默认 confidence：

| 变体 | confidence | 含义 |
|------|-----------|------|
| `ExplicitReply` | 1.0 | Agent 主动调用 warp-reply |
| `BlockCompleted` | 0.95 | block 完成时捕获 |
| `RawOutputScan` | 0.95 | 终端输出扫描 |
| `SessionCompletion` | 0.95 | session 完成回调 |
| `PassiveRawOutput` | 0.7 | 被动捕获 raw_output |
| `PassiveGridOutput` | 0.55 | 被动捕获 grid fallback |
| `PassiveTimeoutPartial` | 0.3 | 超时部分捕获 |

### 2.7 ResponseStore v2

`StoredResponse` 升级到 schema v2，向后兼容：

```json
{
  "schema_version": 2,
  "req_id": "...",
  "provider": "kimi",
  "status": "success",
  "content": "...",
  "source": "passive_raw_output",
  "confidence": 0.7,
  "warnings": ["filtered_instruction_echo"],
  "raw_len": 10000,
  "filtered_len": 2800,
  "truncated": false,
  "finalized_at_ms": 456,
  "created_at_ms": 123,
  "updated_at_ms": 456
}
```

---

## 3. 代码改动

### 改动文件

| 文件 | 改动行数 | 内容 |
|------|---------|------|
| `raw_output.rs` | +11 | `RawRequestCapture` 加 `started_at` 字段 |
| `completion.rs` | +96 | `CompletionStrategy` 枚举、`should_finalize_passive` 自适应判定 |
| `mod.rs` | +551 | `CaptureSource` 扩展、`ReplyCaptureMeta`、`clean_passive_output` 清洗管线、`build_passive_candidate` 候选构建、`scan_terminal_outputs` 被动捕获集成、`finalize_request_with_reply` 元数据感知 |
| `protocol.rs` | +12 | `ReplyEntry` + `WaitResult` 加 `source/confidence/warnings` |
| `registry.rs` | +33 | `set_reply_metadata` 注册表扩展 |
| `store.rs` | +108 | `StoredResponse` v2 schema + 元数据持久化 |

**合计**: 6 files changed, 785 insertions(+), 25 deletions(-)

### 关键代码路径

```
scan_terminal_outputs()
  ├── marker scan (优先) → select_reply_capture_for_scan → extract_reply
  │     └── 找到标记 → finalize_request_with_reply (high confidence)
  │
  └── 被动捕获 fallback (兜底，仅当 marker scan 失败)
        ├── 检查 status == Running
        ├── output_stable_duration() 获取稳定时长
        ├── should_finalize_passive() 自适应判定
        ├── build_passive_candidate() 清洗 + 构造候选
        └── finalize_request_with_reply (PassiveRawOutput, confidence=0.7)
```

---

## 4. 实施过程

### 多 AI 协作

| 阶段 | 负责人 | 轮次 | 贡献 |
|------|--------|------|------|
| 技术讨论 | Claude (主持) | 15轮 | 追问、综合、推动共识 |
| 讨论参与 | Codex | 3轮 | CaptureSource 元数据体系、三层清洗策略、ResponseStore v2 |
| 讨论参与 | Droid | 5轮 | 被动捕获概念、register_request 时间线切分、分级质量保证框架 |
| 讨论参与 | Kimi | 4轮 | 多信号优先级队列、MVP 分阶段方案 |
| Phase 1 实现 | Codex | - | 元数据基础设施 |
| Phase 2 实现 | Droid | - | 被动捕获 + 清洗管线 |
| Phase 3 实现 | Kimi | - | 自适应完成判定 |
| Phase 4 验证 | Droid | - | 集成验证 + bug 修复 |

---

## 5. 未来规划

| 阶段 | 内容 | 时间 |
|------|------|------|
| 短期 | 编译 Warp 二进制，实际测试 Kimi 场景验证被动捕获效果 | 1-2天 |
| 中期 | Codex 编写的单元测试落地（`cargo test` 通过） | 2-3天 |
| 中期 | Prompt 优化层：改进 per-agent 回复指令模板，提升层级1命中率 | 3-5天 |
| 长期 | SharedMemoryLayer (SQLite) 接管持久化，支持跨会话查询和审计 | 1-2周 |
