# astrbot_plugin_grok

把当前消息的图文写成一条 Job。三条命令走不同工人。

## 命令

```
/grok              建 Job 目录并立刻排队调研；插件预写入 Workbench 后 spawn CLI
/grok 附加需求原文
/grokbot           建 Job、直接入库，并通过 Workbench webhook 唤醒资源工人
/grokbot 附加需求原文
/grok暂存          建 Job 目录后，插件直接 upsert_job（pending），不启动工人
/grok暂存 附加需求原文
```

可附带图片，或引用一张带图的消息。余下文字写入 `message.txt`。

## 三种发图方式（1.2.7）

1. **同条图文**：命令与图片在同一条消息 → 立刻建 Job（并丢弃该 key 的旧 pending）。
2. **图先命令后**：先发纯图片（累加 pending，TTL 可配）再发命令 → 立刻消费 pending 建 Job。
3. **命令先发再等图**（更快 QQ 流）：先发 `/grok*` 且当前无图、也无 pending 时，回复 `等图中（N秒）…`，在 `await_image_sec`（默认 12）内把后续纯图片收进 staging；末张图后再等 `await_image_debounce_ms`（默认 1500ms）或到达硬超时后建 Job 并按模式分发。设 `await_image_sec=0` 可关闭此模式。

## 行为

- 一条命令 = 一个 Job 目录 = 一个预留 session_id
- 立刻 `stop_event()`，不进默认 LLM
- `/grok` 落盘 `D:\AI-Inbox\jobs\<id>\`，预写库后 spawn CLI；回执 `已接收` → 完成后 `已处理`
- `/grokbot` 直接入库并读取 `workbench/config.json` 的 `bot_webhook_url` / `bot_webhook_key`；成功后状态为 `running`，调用失败则保留为 `pending`；回执 `已交给 Bot` + job_id
- `/grok暂存` 落盘后调用 `workbench/store.py` 的 `upsert_job(status=pending)`；回执 `已暂存` + job_id
- 图片以 AstrBot 消息链为准；只有消息链未保存出图片时才读取 `raw_message.attachments` 兜底
