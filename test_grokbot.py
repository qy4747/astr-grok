"""最小自检：/grokbot 的命令剥离与 webhook 请求结构。"""

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from types import SimpleNamespace

from main import GrokPlugin


class Handler(BaseHTTPRequestHandler):
    received = None

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        Handler.received = {
            "authorization": self.headers.get("Authorization"),
            "body": json.loads(self.rfile.read(length)),
        }
        self.send_response(200)
        self.end_headers()

    def log_message(self, *_args):
        pass


async def check():
    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()
    plugin = object.__new__(GrokPlugin)
    event = SimpleNamespace(
        message_obj=SimpleNamespace(raw_message={"content": "/grokbot 找资源"}),
        message_str="",
    )
    assert plugin._extra_request(event) == "找资源"
    plugin._workbench_store = lambda: SimpleNamespace(
        load_config=lambda: {
            "bot_webhook_url": f"http://127.0.0.1:{server.server_port}/hook",
            "bot_webhook_key": "test-key",
        }
    )
    job = {
        "id": "job-test",
        "session_id": "00000000-0000-4000-8000-000000000000",
        "extra_request": "找资源",
        "inputs": ["input_01.png"],
    }
    await plugin._post_bot_webhook(job, Path(r"D:\AI-Inbox\jobs\job-test"))
    thread.join(2)
    server.server_close()
    assert Handler.received["authorization"] == "Bearer test-key"
    assert Handler.received["body"]["mode"] == "research"
    assert Handler.received["body"]["job_id"] == "job-test"


if __name__ == "__main__":
    asyncio.run(check())
    print("grokbot webhook check passed")
