# warp-ccb 多 Agent 通信修复全记录

> 日期: 2026-05-26
> 状态: 已完成并验证通过

---

## 一、问题总览

warp-ccb 多 Agent 通信系统存在以下核心问题：

| # | 问题 | 影响 |
|---|------|------|
| 1 | Python `pend()` 查询方向错误 | 任务发起方查不到回复 |
| 2 | Rust `deliver_callback` 找不到 caller pane | 回复无法主动注入 |
| 3 | Rust `alias_to_view_id` 未使用 | 别名 agent 无法被发现 |
| 4 | `extract_callback` 不支持别名 | 别名 provider 导致回调链断裂 |
| 5 | Python CLI 工具错误改写别名 | 消息路由到错误的目标 |
| 6 | `warp-ask` 阻塞等待 | 发起方无谓等待，应 fire-and-forget |
| 7 | Warp 崩溃重启 | `find_session` 在 model update 路径中造成重入 |
| 8 | agent 注册超时失败 | `SessionInfo` 缺少 `alias` 字段导致编译错误 |

---

## 二、架构背景

### 通信链路

```
任务发起方 (Claude/qa)                任务接收方 (Droid/reviewer)
     │                                      │
     ├─ warp-ask ──────────────────────────►│
     │   (SQLite bus_messages + TCP JSONL)   │
     │   to_agent="reviewer"                 │
     │                                       │
     │                              ccb-worker 监听通知
     │                              执行任务，生成回复
     │                                       │
     │◄── deliver_callback ─────────────────┤
     │   inject_prompt 到 caller pane        │
     │   (Rust 侧自动注入)                    │
```

### 别名配置 (`.warp-ccb/warp-ccb.config`)

```ini
[agents]
writer = kimi      # alias → provider
reviewer = droid
qa = claude
planner = droid
```

### 关键数据结构

- **`bus_messages` 表**: `from_agent`(发送方), `to_agent`(接收方), `msg_type`('ask'|'reply')
- **`bus_agents` 表**: `agent_name`(别名或 provider 名), `real_provider`(真实 provider)
- **`RequestEntry` (Rust)**: `caller`, `provider`, `callback_provider`, `caller_terminal_view_id`
- **`alias_to_view_id` (Rust HashMap)**: 别名 → pane EntityId 映射

---

## 三、详细修复记录

### 修复 1: Python `pend()` 查询方向

**文件**: `ccb-bridge/lib/bus_sqlite.py`

**根因**: `pend(agent_name)` 查询 `WHERE from_agent=?`。但 reply 记录中 `from_agent` 是回复方（如 "reviewer"），`to_agent` 才是发起方（如 "claude"）。当 Claude 调用 `pend("claude")` 时，查 `from_agent="claude"` 永远匹配不到 reply。

**修复**:
```python
# 旧: WHERE from_agent=? AND msg_type='reply'
# 新: WHERE to_agent=? AND msg_type='reply'
```

**验证**: 旧查询返回 0 行，新查询返回 5 行。

---

### 修复 2: Rust `alias_to_view_id` 完整支持

**文件**: `warp/app/src/ai/local_agent_bus/mod.rs`

**根因**: `alias_to_view_id` HashMap 已声明但从未被使用。`find_session("reviewer")` 调用 `resolve_agent("reviewer")` → `None` → 直接返回 `NotFound`。

**修复** (10 处改动):
1. `find_session` — 先查 `alias_to_view_id`，找不到再走 `resolve_agent`
2. `handle_launch` — 注册别名到 `alias_to_view_id`
3. `PendingLaunch` 结构体 — 增加 `alias` 字段
4. `inject_launch_to_terminal` — 传递 alias
5. `handle_ping` — 别名查询
6. `handle_close_pane/close_session` — 别名清理
7. `deregister_terminal` — 别名清理
8. `list_sessions` — 返回别名信息
9. `SessionInfo` 初始化 — 添加 `alias: None`
10. `BusCommand::Launch` match — 添加 `..` 忽略新字段

---

### 修复 3: `deliver_callback` 增强

**文件**: `warp/app/src/ai/local_agent_bus/mod.rs`

**根因**: 当 `caller_terminal_view_id = None`（用户手动开的 pane 没设 `WARP_CCB_TERMINAL_VIEW_ID` 环境变量），`find_session("claude")` 找不到 → 回复无法注入。

**修复**: `deliver_callback` 增加多级 fallback：
1. 精确 pane id → 2. `find_session(callback_provider)` → 3. `alias_to_view_id.get(raw_caller)` → 4. 写 debug 文件 + 日志

---

### 修复 4: `inject_and_register` 自动解析 caller pane

**文件**: `warp/app/src/ai/local_agent_bus/mod.rs`

**根因**: `caller_terminal_view_id` 为 None 时，reply 完成后 `deliver_callback` 无法定位 caller。

**修复**: 在 `inject_and_register` 和 Queued 分支中，当 `caller_terminal_view_id` 为 None 时，自动从 `alias_to_view_id` 和 `bus_launched_sessions` 中查找 caller 的 pane。

**重要约束**: 只做纯 HashMap 查找，**不调用 `find_session()`**，因为它内部的 `refresh_bus_launched_sessions_from_terminal_outputs` 会触发 `weak_handle.upgrade(ctx)` → `handle.update(ctx)`，在 model update 路径中造成重入 panic（导致 Warp 崩溃）。

```rust
// 安全的轻量级查找
let caller_terminal_view_id = match caller_terminal_view_id {
    Some(id) => Some(id),
    None => {
        // 1. alias_to_view_id HashMap 查找
        self.alias_to_view_id.get(&caller)
            .filter(|id| self.terminal_handles.contains_key(id))
            .copied()
        // 2. bus_launched_sessions 按 agent 类型查找
            .or_else(|| { ... })
    }
};
```

---

### 修复 5: `extract_callback` 支持别名

**文件**: `warp/app/src/ai/local_agent_bus/mod.rs`

**根因**: `extract_callback("claude", "reviewer")` 中 `normalize_provider_name("reviewer")` 返回 `None` → 整个函数返回 `None` → `callback_provider = None` → `deliver_callback` 直接 `return`，不执行回调注入。

**修复**:
```rust
// 旧: let target_provider = normalize_provider_name(provider)?;  // 别名直接返回 None
// 新:
fn extract_callback(caller: &str, provider: &str) -> Option<String> {
    let caller_provider = match normalize_provider_name(caller) {
        Some(p) => p,
        None => return None,
    };
    // 别名永远不等于 caller_provider，所以总是返回 Some(caller_provider)
    match normalize_provider_name(provider) {
        Some(target) if caller_provider == target => None,  // self-ask
        _ => Some(caller_provider),  // 不同 provider 或别名 → 返回 caller
    }
}
```

---

### 修复 6: Python CLI 工具不做别名解析

**文件**: `ccb-bridge/bin/warp-ask`, `warp-pend`, `warp-ping`

**根因**: 之前给 Python CLI 工具加了 `resolve_alias()`，把 `"reviewer"` 改成 `"droid"`。这导致：
- `warp-ask reviewer "任务"` → 消息发给 `to_agent="droid"`，但 reviewer 的 ccb-worker 监听的是 `agent_name="reviewer"`
- SQLite 通知发到 `"droid"`，但 `"droid"` 没有 agent 监听

**修复**: 撤掉所有 Python CLI 工具中的别名解析，保持原始名字传递。

**核心原则**: **别名解析只发生在 Rust `find_session` 中**（根据别名查 `alias_to_view_id` 找 pane）。Python CLI 工具传递原始名字，不做任何改写。

---

### 修复 7: `warp-ask` 改为 fire-and-forget

**文件**: `ccb-bridge/bin/warp-ask`

**根因**: `warp-ask` 默认 `do_wait=True`，会阻塞等待回复。但现在 Rust 侧 `deliver_callback` 会主动 inject 回复到发起方 pane，不需要阻塞等待。

**修复**: `do_wait = False`（fire-and-forget）。需要同步等待时可用 `--wait` 参数。

---

### 修复 8: Warp 崩溃（重入 panic）

**根因**: `inject_and_register` 中调用 `find_session(&caller, None, None, ctx)`，而 `find_session` 内部调用 `refresh_bus_launched_sessions_from_terminal_outputs`，后者执行 `weak_handle.upgrade(ctx)` → `handle.update(ctx, ...)` 触发 model 更新回调。由于 `inject_and_register` 本身已在 model update 路径中，造成重入，导致 panic/Warp 崩溃。

**修复**: 改为纯 HashMap 查找（不触发 ctx 回调），见修复 4。

---

### 修复 9: 编译错误修复

**文件**: `warp/app/src/ai/local_agent_bus/mod.rs`

| 错误 | 修复 |
|------|------|
| `SessionInfo` 缺少 `alias` 字段 (4处) | 添加 `alias: None` |
| `BusCommand::Launch` match 缺少新字段 | 添加 `..` |
| `ClosePane`/`CloseSession` 缺少 handler | 新增 match arm + 方法 |
| `EntityId::from(u64)` 不存在 | 使用 `entity_id_from_u64()` |

---

### 附加修复: `warp-pend` 消费通知

**文件**: `ccb-bridge/bin/warp-pend`

**修复**: 查询 reply 后消费 `reply_ready` 通知，防止通知堆积。

---

### 附加修复: codex-dual `pend` 脚本支持别名

**文件**: `C:\Users\Administrator\AppData\Local\codex-dual\bin\pend`

**修复**: 添加从 `.warp-ccb/warp-ccb.config` 读取别名映射，`/pend reviewer` 不再报 "Unknown provider"。

---

### 附加修复: Claude `pend` skill

**文件**: `C:\Users\Administrator\.claude\skills\pend\SKILL.md`

**修复**: 更新 skill 描述，添加别名列表（writer/reviewer/qa/planner）。

---

## 四、修改文件清单

| 文件 | 侧 | 修改内容 |
|------|-----|---------|
| `ccb-bridge/lib/bus_sqlite.py` | Python | `pend()` to_agent 修复 + `resolve_alias()` 工具函数 |
| `ccb-bridge/bin/warp-ask` | Python | 撤掉别名解析 + `do_wait=False` |
| `ccb-bridge/bin/warp-pend` | Python | 撤掉别名解析 + 消费通知 |
| `ccb-bridge/bin/warp-ping` | Python | 撤掉别名解析 |
| `ccb-bridge/test_bus_sqlite.py` | Python | 测试更新（asker-perspective 语义） |
| `warp/app/src/ai/local_agent_bus/mod.rs` | Rust | alias_to_view_id + find_session 别名 + extract_callback + deliver_callback + inject_and_register + 编译修复 |
| `codex-dual/bin/pend` | Python | 别名支持 |
| `.claude/skills/pend/SKILL.md` | Config | 别名描述 |

---

## 五、设计原则（经验教训）

1. **别名解析只在 Rust `find_session` 中发生** — Python CLI 工具传递原始名字，不做任何改写
2. **不在 model update 路径中调用 `find_session(ctx)`** — 它内部的 `refresh_bus_launched_sessions_from_terminal_outputs` 会触发重入 panic
3. **`extract_callback` 必须容忍别名** — `normalize_provider_name` 对别名返回 None 不应中断回调链
4. **fire-and-forget 优先** — Rust 侧主动 inject 回复，Python 侧不需要阻塞等待
5. **`bus_messages.to_agent` 使用原始名字** — 别名或 provider 名都可以，但必须与 `bus_agents.agent_name` 一致

---

## 六、验证方式

```bash
# 1. 编译 Rust
cd warp && cargo check -p warp  # 0 errors

# 2. 运行 Python 测试
python ccb-bridge/test_bus_sqlite.py  # 18/18 passed

# 3. 启动 agents
warp-ccb kill && warp-ccb  # 4/4 online

# 4. 测试通信
warp-ask reviewer "你好"           # 立即返回，不阻塞
# reviewer 的回复会自动注入到发起方 pane

# 5. 查看数据库状态
python -c "
from ccb-bridge.lib.bus_sqlite import BusDB
db = BusDB()
for r in db._conn.execute('SELECT * FROM bus_messages ORDER BY created_at DESC LIMIT 5').fetchall():
    print(dict(r))
db.close()
"
```
