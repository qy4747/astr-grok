import asyncio
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from astrbot.api.message_components import Image

from main import GrokPlugin


def make_event(*, sender: str, umo: str, messages=None, text: str = "", raw=None):
    return SimpleNamespace(
        get_messages=lambda: list(messages or []),
        message_obj=SimpleNamespace(raw_message=raw),
        message_str=text,
        unified_msg_origin=umo,
        get_sender_id=lambda: sender,
        plain_result=lambda t: t,
        stop_event=lambda: None,
    )


async def drain(agen):
    items = []
    async for item in agen:
        items.append(item)
    return items


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        plugin = object.__new__(GrokPlugin)
        plugin.config = {
            "jobs_root": tmp,
            "pending_image_ttl_sec": 180,
            "await_image_sec": 12,
            "await_image_debounce_ms": 200,
        }
        plugin._pending_images = {}
        plugin._pending_lock = asyncio.Lock()
        plugin._awaiting = {}
        plugin._running = set()
        plugin.context = SimpleNamespace(send_message=AsyncMock())

        async def download(url: str):
            name = Path(url).stem
            return f"image-{name}".encode(), ".jpg"

        plugin._download_image = download
        plugin._require_runner = lambda: None
        plugin._upsert_job = lambda *a, **k: None
        plugin._post_bot_webhook = AsyncMock()
        plugin._run_job = AsyncMock()

        umo = "aiocqhttp:FriendMessage:10001"
        sender = "10001"

        # 1) 纯命令进入等图，不建 Job
        cmd = make_event(
            sender=sender,
            umo=umo,
            text="/grok 找资源",
            raw={"content": "/grok 找资源"},
        )
        replies = await drain(plugin._begin_command(cmd, "cli"))
        assert replies and "等图中" in replies[0], replies
        key = plugin._pending_key(cmd)
        assert key in plugin._awaiting
        assert list(Path(tmp).glob("job-*")) == []

        # 2) 追加图片 + 防抖 finalize → Job 带 inputs
        img = make_event(
            sender=sender,
            umo=umo,
            messages=[Image.fromURL("https://example.test/alpha.jpg")],
        )
        await plugin.capture_image_only(img)
        staging = plugin._awaiting[key]["staging_dir"]
        assert (staging / "input_01.jpg").read_bytes() == b"image-alpha"
        # 不应写入旧 pending
        assert plugin._pending_images == {}

        await asyncio.sleep(0.35)
        jobs = sorted(Path(tmp).glob("job-*"))
        assert len(jobs) == 1, jobs
        job_dir = jobs[0]
        assert (job_dir / "input_01.jpg").read_bytes() == b"image-alpha"
        assert key not in plugin._awaiting

        # 3) 硬超时、无图 → 空 inputs 的文本 Job
        plugin.config["await_image_sec"] = 1
        plugin.config["await_image_debounce_ms"] = 5000
        cmd2 = make_event(
            sender=sender,
            umo=umo,
            text="/grokbot 只要文字",
            raw={"content": "/grokbot 只要文字"},
        )
        replies2 = await drain(plugin._begin_command(cmd2, "bot"))
        assert replies2 and "等图中" in replies2[0]
        key2 = plugin._pending_key(cmd2)
        assert key2 in plugin._awaiting
        await asyncio.sleep(1.2)
        assert key2 not in plugin._awaiting
        jobs2 = sorted(Path(tmp).glob("job-*"))
        assert len(jobs2) == 2
        latest = jobs2[-1]
        data = (latest / "job.json").read_text(encoding="utf-8")
        assert '"inputs": []' in data or '"inputs":[]' in data.replace(" ", "")
        assert (latest / "message.txt").read_text(encoding="utf-8") == "只要文字"

        # 4) 第二条命令取消第一条等图
        plugin.config["await_image_sec"] = 30
        cmd_a = make_event(
            sender=sender,
            umo=umo,
            text="/grok 第一",
            raw={"content": "/grok 第一"},
        )
        await drain(plugin._begin_command(cmd_a, "cli"))
        key_a = plugin._pending_key(cmd_a)
        staging_a = plugin._awaiting[key_a]["staging_dir"]
        assert staging_a.is_dir()
        cmd_b = make_event(
            sender=sender,
            umo=umo,
            text="/grok 第二",
            raw={"content": "/grok 第二"},
        )
        await drain(plugin._begin_command(cmd_b, "cli"))
        assert not staging_a.exists()
        key_b = plugin._pending_key(cmd_b)
        assert key_b in plugin._awaiting
        assert plugin._awaiting[key_b]["extra"] == "第二"

        # 清理等图任务，避免悬挂
        await plugin._cancel_awaiting(key_b)

        # 5) raw attachments 里的同条图片也必须被前置检测识别，不能误入等图。
        plugin.config["await_image_sec"] = 12
        raw_cmd = make_event(
            sender=sender,
            umo=umo,
            text="/grok暂存 raw附件",
            raw={
                "content": "/grok暂存 raw附件",
                "attachments": [{"url": "https://example.test/raw.jpg"}],
            },
        )
        before = set(Path(tmp).glob("job-*"))
        raw_replies = await drain(plugin._begin_command(raw_cmd, "stash"))
        assert raw_replies and "已暂存" in raw_replies[0], raw_replies
        assert plugin._pending_key(raw_cmd) not in plugin._awaiting
        after = set(Path(tmp).glob("job-*"))
        raw_job = (after - before).pop()
        assert (raw_job / "input_01.jpg").read_bytes() == b"image-raw"

        # 6) append 正在下载时 finalize 必须等 state lock，不能提前删 staging。
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_download(url: str):
            started.set()
            await release.wait()
            return b"image-race-finalize", ".jpg"

        plugin._download_image = slow_download
        plugin.config["await_image_sec"] = 30
        plugin.config["await_image_debounce_ms"] = 5000
        race_cmd = make_event(
            sender=sender,
            umo=umo,
            text="/grok暂存 race-finalize",
            raw={"content": "/grok暂存 race-finalize"},
        )
        await drain(plugin._begin_command(race_cmd, "stash"))
        race_key = plugin._pending_key(race_cmd)
        race_img = make_event(
            sender=sender,
            umo=umo,
            messages=[Image.fromURL("https://example.test/race-finalize.jpg")],
        )
        append_task = asyncio.create_task(plugin.capture_image_only(race_img))
        await started.wait()
        finalize_task = asyncio.create_task(plugin._finalize_awaiting(race_key))
        await asyncio.sleep(0)
        assert not finalize_task.done()

        before = set(Path(tmp).glob("job-*"))
        release.set()
        await append_task
        await finalize_task
        after = set(Path(tmp).glob("job-*"))
        race_job = (after - before).pop()
        assert (race_job / "input_01.jpg").read_bytes() == b"image-race-finalize"
        assert race_key not in plugin._awaiting

        # 7) append 正在下载时 cancel 同样必须等待，随后完整清理 staging。
        started2 = asyncio.Event()
        release2 = asyncio.Event()

        async def slow_download_cancel(url: str):
            started2.set()
            await release2.wait()
            return b"image-race-cancel", ".jpg"

        plugin._download_image = slow_download_cancel
        cancel_cmd = make_event(
            sender=sender,
            umo=umo,
            text="/grok暂存 race-cancel",
            raw={"content": "/grok暂存 race-cancel"},
        )
        await drain(plugin._begin_command(cancel_cmd, "stash"))
        cancel_key = plugin._pending_key(cancel_cmd)
        cancel_staging = plugin._awaiting[cancel_key]["staging_dir"]
        cancel_img = make_event(
            sender=sender,
            umo=umo,
            messages=[Image.fromURL("https://example.test/race-cancel.jpg")],
        )
        append_task2 = asyncio.create_task(plugin.capture_image_only(cancel_img))
        await started2.wait()
        cancel_task = asyncio.create_task(plugin._cancel_awaiting(cancel_key))
        await asyncio.sleep(0)
        assert not cancel_task.done()

        release2.set()
        await append_task2
        await cancel_task
        assert cancel_key not in plugin._awaiting
        assert not cancel_staging.exists()


if __name__ == "__main__":
    asyncio.run(main())
    print("await-images check passed")
