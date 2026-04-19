# Feishu Codex Bridge (Long Connection)

通过飞书长连接订阅事件来控制本地 `codex`，不需要公网回调地址。

## 1. 功能

- 使用飞书官方 `lark-oapi` 长连接收消息（WebSocket）。
- 默认把消息当作 `codex exec` prompt 执行。
- 内置多模型 API：`OpenAI / Gemini / DeepSeek / Qwen`（可按会话切换后端）。
- 默认先发送“已收到”状态，再持续编辑同一条消息展示输出。
- 流式消息默认用 `post + md` 渲染（飞书 Markdown 子集）。
- 每个会话独立维护工作目录，可 `/setwd`。
- 若 `codex exec` 仅返回整段结果，桥接会自动按小块连续发送（伪流式）。

## 2. 前置条件

- 本机可用 `codex` CLI（`codex --version` 正常）。
- Python 3.9+
- 安装依赖：

```bash
python3 -m pip install --user lark-oapi
```

## 3. 配置环境变量

```bash
cd /Users/linxiaoyi/feishu-codex-bridge
cp .env.example .env
# 编辑 .env，填入 FEISHU_APP_ID / FEISHU_APP_SECRET
```

## 4. 飞书后台配置（长连接）

在飞书开放平台应用中：

- 启用事件订阅。
- 订阅事件：`im.message.receive_v1`
- 订阅方式选择“长连接”（不是请求地址回调）。
- 发布应用版本并确保机器人可被目标用户/群聊使用。

## 5. 启动

```bash
cd /Users/linxiaoyi/feishu-codex-bridge
python3 bridge.py
```

看到日志 `long connection started` 即表示已连上。

## 6. 飞书内可用指令

- `/codex <prompt>`: 执行 `codex exec --json`
- `/llm <prompt>`: 用当前激活后端执行（`codex` 或云模型）
- `/backend`: 查看后端状态与当前激活后端
- `/backend use <codex|openai|gemini|deepseek|qwen>`: 切换当前会话后端
- `/openai <prompt>`: 直连 OpenAI
- `/gemini <prompt>`: 直连 Gemini
- `/deepseek <prompt>`: 直连 DeepSeek
- `/qwen <prompt>`: 直连 Qwen
- `/cmd <args>`: 执行受限版 `codex <args>`（支持 `exec/review/apply/resume/fork/features`）
- `/setwd <path>`: 设置当前会话工作目录
- `/status`: 查看当前状态
- `/session`: 直接列出最近的本地 Codex 历史会话
- `/session current`: 查看当前绑定的 `codex_session_id`
- `/session list [n]`: 列出最近 `n` 条本地 Codex 历史会话
- `/session search <query>`: 按标题或 session id 搜索本地历史会话
- `/session use <n|id>`: 把当前飞书会话切到某个本地历史 `codex_session_id`
- `/new`: 新建一轮对话（清空当前上下文、队列和绑定）
- `/session set <id>`: 手动绑定到指定 `codex_session_id`
- `/session clear`: 清空当前绑定 `codex_session_id`
- `/reset`: 清空当前上下文和队列
- `/whoami`: 查看当前 `chat_id` 和 `sender_open_id`（用于配置白名单）
- `/security`: 查看当前安全策略生效值
- `/stop`: 终止当前任务
- `/help`: 查看帮助

不带前缀的消息会默认按当前激活后端处理（默认 `codex`）。

## 7. 说明

- 长连接模式下，不需要公网 URL、ngrok、cloudflared。
- 如果飞书里发消息没有回流，先检查应用是否发布、机器人是否在会话里、事件订阅是否已启用。
- 可调流式参数：`STREAM_CHUNK_CHARS`、`STREAM_FLUSH_SEC`、`STREAM_SEND_INTERVAL_SEC`、`STREAM_PSEUDO_CHUNK_CHARS`。
- 消息编辑参数：`STREAM_EDIT_IN_PLACE`、`STREAM_USE_MARKDOWN`、`STREAM_UPDATE_INTERVAL_SEC`、`STREAM_MAX_UPDATES_PER_MESSAGE`、`STREAM_MESSAGE_MAX_CHARS`、`STATUS_RECEIVED_TEXT`、`STREAM_MARKDOWN_TITLE`。
- 消息合并窗口：`MERGE_WINDOW_SEC`（同会话内该时间窗口内的连续消息会合并后再触发一次 Codex，默认 `0.3` 秒）。
- 本地 Codex 数据目录：`CODEX_HOME`（默认 `~/.codex`，用于读取 `session_index.jsonl` 和历史会话文件）。
- 会话持久化文件：`SESSION_STATE_FILE`（默认 `.feishu_session_map.json`，用于保存“飞书会话 -> codex_session_id”映射，重启后继续上下文）。
- 项目记忆目录：`PROJECT_STATE_DIR/memory/<project_slug>/`。桥接会把 `MEMORY.md`、`active-task.json` 和当天 `daily/YYYY-MM-DD.md` 注入到每次请求里。
- 自动记忆回写：`PROJECT_MEMORY_AUTO_UPDATE=1` 时，每轮完成后会从“用户请求 + 结果首段”提炼一条去重后的短结论追加到 `MEMORY.md`；长度由 `PROJECT_MEMORY_ENTRY_MAX_CHARS` 控制。
- 记忆注入裁剪：`PROJECT_MEMORY_MAX_CHARS` 控制 `MEMORY.md` 注入上限，`PROJECT_DAILY_TAIL_LINES` 控制当天日志尾部注入行数。
- 模型后端参数：`DEFAULT_BACKEND`、`LLM_REQUEST_TIMEOUT_SEC`、`LLM_TEMPERATURE`、`LLM_MAX_TOKENS`。
- 各家模型参数：`OPENAI_*`、`GEMINI_*`、`DEEPSEEK_*`、`QWEN_*`（`*_API_KEY` 为空时对应后端不可用）。
- `/session use <n|id>` 会优先从历史会话元信息里恢复对应 `cwd`，减少切回旧 session 后上下文和工作目录不一致的问题。
- 关键安全参数：`REQUIRE_P2P`、`ALLOWED_OPEN_IDS`、`ALLOWED_CHAT_IDS`、`COMMAND_TOKEN`、`ENABLE_RAW_CMD`、`RATE_LIMIT_PER_MINUTE`。

## 8. 安全建议（推荐）

- `REQUIRE_P2P=0`：默认允许私聊和群聊控制；如需只允许私聊，改成 `1`。
- 填写 `ALLOWED_CHAT_IDS` 或 `ALLOWED_OPEN_IDS`：只允许白名单会话/用户。
- 设置 `COMMAND_TOKEN`：每条指令前带口令，例如 `mytoken /codex 你好`。
- 保持 `ENABLE_RAW_CMD=0`：禁用 `/cmd` 透传命令。
- 设置合理限流：`RATE_LIMIT_PER_MINUTE` 防刷。
