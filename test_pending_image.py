import asyncio
import tempfile
from pathlib import Path
from types import SimpleNamespace

from astrbot.api.message_components import Image

from main import GrokPlugin


def make_event(*, sender: str, umo: str, messages=None, text: str = "", raw=None):
    return SimpleNamespace(
        get_messages=lambda: list(messages or []),
        message_obj=SimpleNamespace(raw_message=raw),
        message_str=text,
        unified_msg_origin=umo,
        get_sender_id=lambda: sender,
    )


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        plugin = object.__new__(GrokPlugin)
        plugin.config = {
            "jobs_root": tmp,
            "pending_image_ttl_sec": 180,
        }
        plugin._pending_images = {}
        plugin._pending_lock = asyncio.Lock()

        async def download(url: str):
            name = Path(url).stem
            return f"image-{name}".encode(), ".jpg"

        plugin._download_image = download

        # QQ 全屏相册会把旧图拆成多条纯图片消息；同一批必须累加而不是覆盖。
        for name in ("one", "two", "three"):
            image_event = make_event(
                sender="10001",
                umo="aiocqhttp:FriendMessage:10001",
                messages=[Image.fromURL(f"https://example.test/{name}.jpg")],
            )
            assert await plugin._cache_pending_images(image_event) == 1

        assert len(plugin._pending_images) == 1
        _created_at, pending_dir = next(iter(plugin._pending_images.values()))
        assert sorted(p.name for p in pending_dir.iterdir()) == [
            "input_01.jpg",
            "input_02.jpg",
            "input_03.jpg",
        ]

        # 别的发送者不能消费这批图。
        other_event = make_event(
            sender="10002",
            umo="aiocqhttp:FriendMessage:10002",
            text="/grok 不该拿到别人的图",
            raw={"content": "/grok 不该拿到别人的图"},
        )
        other_dir = Path(tmp) / "other-job"
        other_dir.mkdir()
        assert plugin._consume_pending_images(other_event, other_dir) == []
        assert len(plugin._pending_images) == 1

        # 同一会话、同一发送者的下一条 /grok 一次性消费全部旧图。
        command_event = make_event(
            sender="10001",
            umo="aiocqhttp:FriendMessage:10001",
            text="/grok 找这三个资源",
            raw={"content": "/grok 找这三个资源"},
        )
        job, job_dir = await plugin._create_job(command_event, "找这三个资源")
        assert job["inputs"] == [
            "input_01.jpg",
            "input_02.jpg",
            "input_03.jpg",
        ], job
        assert (job_dir / "input_01.jpg").read_bytes() == b"image-one"
        assert (job_dir / "input_02.jpg").read_bytes() == b"image-two"
        assert (job_dir / "input_03.jpg").read_bytes() == b"image-three"
        assert plugin._pending_images == {}


if __name__ == "__main__":
    asyncio.run(main())
    print("pending multi-image check passed")
