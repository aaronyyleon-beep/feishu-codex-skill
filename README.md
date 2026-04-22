# Feishu Codex Bridge Skill

Codex skill for building, debugging, and operating a Feishu bot that controls a local Codex CLI.

## What This Skill Covers

- Feishu long connection setup and event flow
- Plain text, post, and forwarded message parsing
- Streamed replies through in-place Feishu message edits
- Per-chat session continuity with `session_key -> codex_session_id`
- Switching a Feishu chat back to saved local Codex sessions
- Browsing saved sessions with `/session`, checking the active binding with `/session current`, and starting fresh with `/new`
- Merge windows, per-chat queues, and workdir synchronization
- ACLs, rate limits, and raw-command hardening

## Repository Layout

- `SKILL.md`: main skill instructions and trigger description
- `agents/openai.yaml`: UI metadata for skill pickers
- `references/bridge-playbook.md`: operational details, commands, config, and troubleshooting
- `assets/feishu-codex-bridge/`: bundled runnable reference implementation (`bridge.py`, `.env.example`, `README.md`)

## Install Manually

This repository contains one skill whose internal skill name is `feishu-codex-bridge`.

```bash
git clone https://github.com/xllinbupt/feishu-codex-skill.git
mkdir -p ~/.codex/skills/feishu-codex-bridge
cp -R feishu-codex-skill/SKILL.md feishu-codex-skill/agents feishu-codex-skill/references feishu-codex-skill/assets ~/.codex/skills/feishu-codex-bridge/
```

After that, invoke it in Codex with prompts such as:

```text
$feishu-codex-bridge Help me debug why Feishu replies are not streaming.
$feishu-codex-bridge Add support for switching a Feishu chat to a saved Codex session.
```

## Bundled Code

This repository also includes a runnable reference implementation under `assets/feishu-codex-bridge/`.

- `bridge.py`: Feishu long-connection bridge implementation
- `.env.example`: safe example configuration
- `README.md`: usage notes for the bridge itself

## Host Scripts

This repository also includes a minimal set of host-side scripts for managing the local OpenClaw gateway from your own terminal:

- `scripts/restart-openclaw-gateway`
- `scripts/status-openclaw-gateway`
- `scripts/logs-openclaw-gateway`

Examples:

```bash
./scripts/restart-openclaw-gateway
./scripts/status-openclaw-gateway
./scripts/logs-openclaw-gateway
```

All three default to the launchd service `ai.openclaw.gateway` and accept an optional first argument to override the service name.

Sensitive runtime files are intentionally not included, such as `.env`, `.feishu_session_map.json`, or generated tokens.

## Notes

- The skill content currently points at the author's local bridge project path as the default implementation example.
- If your bridge project lives somewhere else, update those paths in `SKILL.md` and `references/bridge-playbook.md`.
