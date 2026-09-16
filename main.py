import asyncio
import importlib.util
import json
import re
import shutil
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

import aiohttp
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.message_components import File, Image, Plain, Reply
from astrbot.api.star import Context, Star, register
from astrbot.core.message.message_event_result import MessageChain

TZ = timezone(timedelta(hours=8))
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
# 长命令必须排在 /grok 前面，否则附加需求会残留命令后缀。
CMD_PREFIXES = ("/grok暂存", "grok暂存", "/grokbot", "grokbot", "/grok", "grok")
UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
HEX_ID_RE = re.compile(r"^[0-9a-fA-F]{16,}$")


@register(
    "astrbot_plugin_grok",
    "shenqing74-cyber",
    "用 /grok 建 Job；/grokbot 交给 Bot；/grok暂存 只入库",
    "1.2.3",
)
class GrokPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config or {}
        self._running: set[asyncio.Task] = set()

    def _jobs_root(self) -> Path:
        return Path(self.config.get("jobs_root") or r"D:\AI-Inbox\jobs")

    def _repo_root(self) -> Path:
        return Path(self.config.get("repo_root") or r"H:\GROK资源整合")

    def _grok_exe(self) -> Path:
        return Path(
            self.config.get("grok_exe") or r"C:\Users\shenq\.grok\bin\grok.exe"
        )

    def _timeout_sec(self) -> int:
        try:
            return max(0, int(self.config.get("timeout_sec") or 0))
        except (TypeError, ValueError):
            return 0

    @filter.command("grokbot", priority=3)
    async def cmd_grok_bot(self, event: AstrMessageEvent):
        """建 Job、直接写库并通过 Workbench webhook 唤醒资源工人。"""
        extra = self._extra_request(event)
        job = None
        try:
            job, job_dir = await self._create_job(event, extra)
            self._upsert_job(job, job_dir)
            self._upsert_job(job, job_dir, status="running")
            try:
                await self._post_bot_webhook(job, job_dir)
            except Exception:
                self._upsert_job(job, job_dir)
                raise
        except Exception as e:
            logger.exception("[grokbot] create or dispatch job failed")
            yield event.plain_result(f"Bot 调研启动失败: {e}")
            event.stop_event()
            return

        yield event.plain_result(f"已交给 Bot\n{job['id']}")
        event.stop_event()

    @filter.command("grok暂存", priority=2)
    async def cmd_grok_stash(self, event: AstrMessageEvent):
        """只入库: /grok暂存 [附加需求]，可附带图片。不排队调研，插件直接写库。"""
        extra = self._extra_request(event)
        try:
            job, job_dir = await self._create_job(event, extra)
            self._upsert_job(job, job_dir)
        except Exception as e:
            logger.exception("[grok] stash job failed")
            yield event.plain_result(f"建 Job 失败: {e}")
            event.stop_event()
            return

        yield event.plain_result(f"已暂存\n{job['id']}")
        event.stop_event()

    @filter.command("grok", priority=1)
    async def cmd_grok(self, event: AstrMessageEvent):
        """建 Job 并立刻调研: /grok [附加需求]，可附带图片。入账由 Grok MCP 负责。"""
        extra = self._extra_request(event)
        try:
            self._require_runner()
            job, job_dir = await self._create_job(event, extra)
        except Exception as e:
            logger.exception("[grok] create job failed")
            yield event.plain_result(f"建 Job 失败: {e}")
            event.stop_event()
            return

        yield event.plain_result("已接收")
        event.stop_event()

        task = asyncio.create_task(
            self._run_job(
                event.unified_msg_origin, job["id"], job["session_id"], job_dir
            )
        )
        self._running.add(task)
        task.add_done_callback(self._running.discard)

    def _require_runner(self) -> None:
        grok_exe = self._grok_exe()
        ps1 = self._repo_root() / "scripts" / "run-job.ps1"
        if not grok_exe.is_file():
            raise FileNotFoundError(f"找不到 grok.exe: {grok_exe}")
        if not ps1.is_file():
            raise FileNotFoundError(f"找不到 run-job.ps1: {ps1}")

    def _extra_request(self, event: AstrMessageEvent) -> str:
        raw = getattr(event.message_obj, "raw_message", None)
        msg = ""
        if raw is not None:
            try:
                if hasattr(raw, "get"):
                    msg = raw.get("content", "") or ""
                else:
                    msg = getattr(raw, "content", "") or ""
            except Exception:
                msg = ""
        if not str(msg).strip():
            msg = event.message_str or ""
        text = str(msg).strip()
        lower = text.lower()
        for prefix in CMD_PREFIXES:
            if lower.startswith(prefix):
                return text[len(prefix) :].strip()
        return text

    async def _create_job(
        self, event: AstrMessageEvent, extra: str
    ) -> tuple[dict, Path]:
        now = datetime.now(TZ)
        job_id = f"job-{now.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
        session_id = str(uuid.uuid4())
        job_dir = self._jobs_root() / job_id
        job_dir.mkdir(parents=True, exist_ok=False)
        (job_dir / "logs").mkdir()

        inputs = await self._save_images(event, job_dir)
        (job_dir / "message.txt").write_text(extra, encoding="utf-8")
        job = {
            "id": job_id,
            "created_at": now.isoformat(timespec="seconds"),
            "status": "pending",
            "session_id": session_id,
            "extra_request": extra,
            "inputs": inputs,
            "message_file": "message.txt",
            "result_file": "result.json",
        }
        (job_dir / "job.json").write_text(
            json.dumps(job, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info(f"[grok] job={job_id} session={session_id} images={len(inputs)}")
        return job, job_dir

    def _workbench_store(self):
        store_py = self._repo_root() / "workbench" / "store.py"
        if not store_py.is_file():
            raise FileNotFoundError(f"找不到 workbench/store.py: {store_py}")
        spec = importlib.util.spec_from_file_location(
            "workbench_store", store_py
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"无法加载 {store_py}")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def _upsert_job(
        self, job: dict, job_dir: Path, status: str = "pending"
    ) -> None:
        """插件直接写 Workbench SQLite，不经 Grok MCP。"""
        mod = self._workbench_store()
        store = mod.Store(mod.load_config())
        try:
            store.upsert_job(
                job_id=str(job["id"]),
                session_id=str(job["session_id"]),
                status=status,
                extra_request=job.get("extra_request") or "",
                inputs=list(job.get("inputs") or []),
                job_dir=str(job_dir),
                created_at=job.get("created_at"),
            )
        finally:
            store.close()
        logger.info(f"[grok] upsert {job['id']} status={status}")

    async def _post_bot_webhook(self, job: dict, job_dir: Path) -> None:
        config = self._workbench_store().load_config()
        url = str(config.get("bot_webhook_url") or "").strip()
        key = str(config.get("bot_webhook_key") or "").strip()
        if not url.startswith(("http://", "https://")) or not key:
            raise RuntimeError("Workbench 尚未配置 bot_webhook_url / bot_webhook_key")

        payload = {
            "job_id": str(job["id"]),
            "job_dir": str(job_dir),
            "session_id": str(job["session_id"]),
            "extra_request": job.get("extra_request") or "",
            "inputs": list(job.get("inputs") or []),
            "mode": "research",
            "requested_at": datetime.now(TZ).isoformat(timespec="seconds"),
        }
        timeout = aiohttp.ClientTimeout(total=20)
        headers = {"Authorization": f"Bearer {key}"}
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status != 200:
                    raise RuntimeError(f"Bot webhook 返回 HTTP {response.status}")
        logger.info(f"[grokbot] dispatched {job['id']}")

    async def _save_images(
        self, event: AstrMessageEvent, job_dir: Path
    ) -> list[str]:
        saved: list[str] = []
        seen: set[str] = set()

        def is_dup(keys: set[str]) -> bool:
            return bool(keys & seen)

        def remember(keys: set[str]) -> None:
            seen.update(k for k in keys if k)

        async def persist_file(src: Path) -> bool:
            if not src.is_file() or src.stat().st_size <= 0:
                return False
            ext = src.suffix.lower()
            if ext not in IMAGE_EXTS:
                ext = ".png"
            name = f"input_{len(saved) + 1:02d}{ext}"
            shutil.copy2(src, job_dir / name)
            saved.append(name)
            remember({str(src.resolve()).lower()})
            return True

        async def persist_url(url: str) -> bool:
            if not url or not url.startswith("http"):
                return False
            data, ext = await self._download_image(url)
            if not data:
                return False
            name = f"input_{len(saved) + 1:02d}{ext}"
            (job_dir / name).write_bytes(data)
            saved.append(name)
            remember(self._identity_keys(url))
            return True

        for seg in self._iter_segments(event):
            if isinstance(seg, Image):
                keys = self._keys_from_component(seg)
                if is_dup(keys):
                    continue
                path = await self._image_to_path(seg)
                if path and await persist_file(Path(path)):
                    remember(keys)
                    continue
                url = self._http_url(
                    getattr(seg, "url", None) or getattr(seg, "file", None)
                )
                if url and await persist_url(url):
                    remember(keys)
            elif isinstance(seg, File):
                name = str(getattr(seg, "name", "") or "")
                file_path = str(getattr(seg, "file_", "") or "").strip()
                url = str(getattr(seg, "url", "") or "").strip()
                if not any(
                    item.lower().endswith(tuple(IMAGE_EXTS))
                    for item in (name, file_path, url)
                    if item
                ):
                    continue
                keys = self._keys_from_component(seg)
                if is_dup(keys):
                    continue
                wrote = False
                if file_path and Path(file_path).is_file():
                    wrote = await persist_file(Path(file_path))
                elif hasattr(seg, "get_file"):
                    try:
                        got = await seg.get_file()
                    except Exception:
                        got = ""
                    if got:
                        wrote = await persist_file(Path(got))
                if not wrote and url.startswith("http"):
                    wrote = await persist_url(url)
                if wrote:
                    remember(keys)

        # AstrBot adapters normally materialize raw attachments into the message
        # chain. Only use raw attachments when that canonical path yielded nothing.
        if not saved:
            for item in self._attachment_items(event):
                keys, url = self._keys_from_attachment(item)
                if is_dup(keys):
                    continue
                if url and await persist_url(url):
                    remember(keys)
        return saved

    def _iter_segments(self, event: AstrMessageEvent):
        if hasattr(event, "get_messages"):
            chain = event.get_messages() or []
        else:
            chain = getattr(event.message_obj, "message", None) or []
        for seg in chain:
            if isinstance(seg, Reply) and getattr(seg, "chain", None):
                for inner in seg.chain:
                    yield inner
            else:
                yield seg

    async def _image_to_path(self, component: Image) -> str | None:
        for method_name in ("convert_to_file_path", "get_file"):
            method = getattr(component, method_name, None)
            if not callable(method):
                continue
            try:
                path = await method()
            except Exception as e:
                logger.warning(f"[grok] {method_name} failed: {e}")
                path = None
            if isinstance(path, str) and path and Path(path).is_file():
                return path
        for attr in ("file", "path"):
            value = getattr(component, attr, None)
            if isinstance(value, str) and value and Path(value).is_file():
                return value
        return None

    def _http_url(self, value: object) -> str:
        text = str(value or "").strip()
        return text if text.startswith("http://") or text.startswith("https://") else ""

    def _identity_keys(self, *values: object) -> set[str]:
        """同一张图的消息段和附件压缩可能不同，按原始 ID/URL 去重，不比文件哈希。"""
        keys: set[str] = set()
        for value in values:
            if value is None:
                continue
            text = str(value).strip()
            if not text:
                continue
            lowered = text.lower()
            if lowered.startswith("http://") or lowered.startswith("https://"):
                parsed = urlparse(text)
                path = unquote(parsed.path or "").rstrip("/")
                keys.add(lowered)
                keys.add(f"{parsed.scheme}://{parsed.netloc}{parsed.path}".lower())
                if path:
                    keys.add(path.lower())
                parts = [p for p in path.split("/") if p]
                if "attachments" in parts:
                    i = parts.index("attachments")
                    rest = parts[i + 1 :]
                    if len(rest) >= 2:
                        keys.add(rest[1].lower())
                        keys.add("/".join(rest[:2]).lower())
                    if rest:
                        keys.add("/".join(rest).lower())
                elif parts:
                    stem = Path(parts[-1]).stem
                    if self._looks_like_image_id(stem):
                        keys.add(stem.lower())
            elif self._looks_like_image_id(Path(text).stem):
                keys.add(Path(text).stem.lower())
                keys.add(lowered)
        return keys

    def _looks_like_image_id(self, value: str) -> bool:
        text = (value or "").strip()
        if not text:
            return False
        if UUID_RE.match(text) or HEX_ID_RE.match(text):
            return True
        if text.isdigit() and len(text) >= 8:
            return True
        if text.endswith(".image"):
            return self._looks_like_image_id(text[: -len(".image")])
        return False

    def _keys_from_component(self, component: object) -> set[str]:
        return self._identity_keys(
            getattr(component, "url", None),
            getattr(component, "file", None),
            getattr(component, "path", None),
            getattr(component, "file_", None),
            getattr(component, "id", None),
            getattr(component, "file_id", None),
            getattr(component, "image_id", None),
            getattr(component, "filename", None),
        )

    def _keys_from_attachment(self, item: object) -> tuple[set[str], str]:
        def attr(name: str) -> object:
            if isinstance(item, dict):
                return item.get(name)
            return getattr(item, name, None)

        url = self._http_url(attr("url"))
        proxy = self._http_url(attr("proxy_url") or attr("proxyUrl"))
        keys = self._identity_keys(
            url,
            proxy,
            attr("id"),
            attr("filename"),
            attr("name"),
        )
        return keys, url or proxy

    def _attachment_items(self, event: AstrMessageEvent) -> list[object]:
        raw = getattr(event.message_obj, "raw_message", None)
        if not raw:
            return []
        try:
            if hasattr(raw, "get"):
                att = raw.get("attachments", None)
            else:
                att = getattr(raw, "attachments", None)
        except Exception:
            return []
        if not att:
            return []
        try:
            items = json.loads(att) if isinstance(att, str) else att
        except Exception:
            return []
        if not isinstance(items, (list, tuple)):
            return []
        return list(items)

    async def _download_image(self, url: str) -> tuple[bytes | None, str]:
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(
                timeout=timeout, trust_env=False
            ) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        return None, ".png"
                    data = await resp.read()
                    if not data:
                        return None, ".png"
                    ext = self._ext_from_url_or_type(
                        url, resp.headers.get("Content-Type", "")
                    )
                    return data, ext
        except Exception as e:
            logger.warning(f"[grok] download image failed: {e}")
            return None, ".png"

    def _ext_from_url_or_type(self, url: str, content_type: str) -> str:
        ctype = (content_type or "").split(";")[0].strip().lower()
        mapping = {
            "image/jpeg": ".jpg",
            "image/jpg": ".jpg",
            "image/png": ".png",
            "image/webp": ".webp",
        }
        if ctype in mapping:
            return mapping[ctype]
        suffix = Path(url.split("?", 1)[0]).suffix.lower()
        if suffix in IMAGE_EXTS:
            return suffix
        return ".png"

    async def _run_job(
        self, umo: str, job_id: str, session_id: str, job_dir: Path
    ) -> None:
        try:
            code = await self._launch_grok(job_dir, session_id)
            text = self._format_done(job_id, job_dir, code)
        except Exception as e:
            logger.exception(f"[grok] job {job_id} failed")
            text = f"已处理\n\n运行失败：{e}"
        await self._send(umo, text)

    async def _launch_grok(self, job_dir: Path, session_id: str) -> int:
        ps1 = self._repo_root() / "scripts" / "run-job.ps1"
        proc = await asyncio.create_subprocess_exec(
            "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ps1),
            "-JobDir",
            str(job_dir),
            "-SessionId",
            session_id,
            "-RepoRoot",
            str(self._repo_root()),
            "-Grok",
            str(self._grok_exe()),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        timeout = self._timeout_sec()
        try:
            if timeout > 0:
                await asyncio.wait_for(proc.communicate(), timeout=timeout)
            else:
                await proc.communicate()
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise TimeoutError(f"Job 超时（{timeout}s）")
        return proc.returncode or 0

    def _format_done(self, job_id: str, job_dir: Path, code: int) -> str:
        result_path = job_dir / "result.json"
        if not result_path.is_file():
            return f"已处理\n\nJob {job_id} 结束（exit={code}），没有 result.json"
        try:
            data = json.loads(result_path.read_text(encoding="utf-8"))
        except Exception as e:
            return f"已处理\n\nJob {job_id} 结束，result.json 读失败：{e}"

        title = str(data.get("title") or "").strip()
        intro = str(data.get("intro") or "").strip()
        tutorial = str(data.get("tutorial") or "").strip()
        resource = data.get("resource_path")
        notes = str(data.get("notes") or "").strip()
        status = str(data.get("status") or "").strip()

        lines = ["已处理"]
        if title:
            lines.extend(["", title])
        if intro:
            lines.extend(["", intro])
        if tutorial:
            lines.extend(["", "教程：", tutorial])
        if resource:
            lines.extend(["", f"资源：{resource}"])
        if notes:
            lines.extend(["", f"备注：{notes}"])
        if status == "failed" or code != 0:
            lines.extend(["", f"（status={status or '?'} exit={code}）"])
        if len(lines) == 1:
            lines.append(f"\nJob {job_id} 完成，result.json 无正文")
        return "\n".join(lines)

    async def _send(self, umo: str, text: str) -> None:
        try:
            await self.context.send_message(umo, MessageChain([Plain(text)]))
        except Exception:
            logger.exception("[grok] send_message failed")
