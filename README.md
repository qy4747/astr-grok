# astrbot_plugin_grok

把当前消息的图文写成一条 Job。三条命令走不同工人。

## 命令

```
/grok              建 Job 目录并立刻排队调研；入账由 Grok 走 Workbench MCP
/grok 附加需求原文
/grokbot           建 Job、直接入库，并通过 Workbench webhook 唤醒资源工人
/grokbot 附加需求原文
/grok暂存          建 Job 目录后，插件直接 upsert_job（pending），不启动工人
/grok暂存 附加需求原文
```

可附带图片，或引用一张带图的消息。余下文字写入 `message.txt`。

## 行为

- 一条命令 = 一个 Job 目录 = 一个预留 session_id
- 立刻 `stop_event()`，不进默认 LLM
- `/grok` 只落盘 `D:\AI-Inbox\jobs\<id>\`，然后 spawn CLI；库记录由 Grok `upsert_job` MCP 写
- `/grokbot` 直接入库并读取 `workbench/config.json` 的 `bot_webhook_url` / `bot_webhook_key`；成功后状态为 `running`，调用失败则保留为 `pending`
- `/grok暂存` 落盘后调用 `workbench/store.py` 的 `upsert_job(status=pending)`，Web 未处理列表立刻能看到
- 图片以 AstrBot 消息链为准；只有消息链未保存出图片时才读取 `raw_message.attachments` 兜底，避免适配器已转换附件后重复落盘
- `/grok` 回执 `已接收` → `已处理`；`/grokbot` 回执 `已交给 Bot` + job_id；`/grok暂存` 回执 `已暂存` + job_id
