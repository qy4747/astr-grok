import asyncio
import tempfile
from pathlib import Path
from types import SimpleNamespace

from astrbot.api.message_components import Image

from main import GrokPlugin


async def main() -> None:
    plugin = object.__new__(GrokPlugin)

    async def download(_url: str):
        return b"image", ".jpg"

    plugin._download_image = download
    event = SimpleNamespace(
        get_messages=lambda: [Image.fromURL("https://example.test/converted.jpg")],
        message_obj=SimpleNamespace(
            raw_message=SimpleNamespace(
                attachments=[SimpleNamespace(url="https://example.test/original.jpg")]
            )
        ),
    )
    with tempfile.TemporaryDirectory() as tmp:
        saved = await plugin._save_images(event, Path(tmp))
        assert saved == ["input_01.jpg"], saved


if __name__ == "__main__":
    asyncio.run(main())
