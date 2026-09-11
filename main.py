import asyncio
import base64
import io
import os
import shutil
import tempfile
import time
import urllib.request
from contextlib import suppress
from pathlib import Path

from PIL import Image

from astrbot.api import logger
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star

try:
    from astrbot.api.message_components import Image as AstrImage
except Exception:
    AstrImage = None

PLUGIN_DIR = Path(__file__).resolve().parent
TMP_ROOT = PLUGIN_DIR / "tmp"
TMP_ROOT.mkdir(parents=True, exist_ok=True)

MAX_EDGE = 1080
MAX_FETCH_MB = 20
TARGET_KB = 1024
B_SECONDS = 10
FIRST_MS = 500
MAX_B64_TOTAL = 1536 * 1024
MAX_WAIT = 4
QUEUE_TIMEOUT = 120
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AstrBot-dual-png"

def _strip_prefix(text):
    t = (text or "").strip()
    for p in ("/双图", "双图", "/dual", "dual"):
        if t.startswith(p):
            return t[len(p):].strip()
    return t

def _parse(text):
    apng, refs = False, []
    for tok in (text or "").strip().split():
        t = tok.strip("\"'")
        if not t:
            continue
        if t == "--apng":
            apng = True
            continue
        if t.startswith("--"):
            continue
        refs.append(t)
    return refs, apng

def _chain_images(event):
    refs = []
    try:
        chain = getattr(getattr(event, "message_obj", None), "message", None) or []
        for seg in chain:
            if not ((AstrImage is not None and isinstance(seg, AstrImage))
                    or seg.__class__.__name__ == "Image"):
                continue
            for a in ("path", "url", "file"):
                v = getattr(seg, a, None)
                if isinstance(v, str) and v.strip():
                    refs.append(v.strip())
                    break
    except Exception as e:
        logger.warning(f"[双图] 解析链图片失败: {e}")
    return refs

def _fetch_bytes_sync(ref):
    ref = (ref or "").strip().strip("\"'")
    if ref.startswith("http://") or ref.startswith("https://"):
        req = urllib.request.Request(ref, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=30) as r:
            blob = r.read(MAX_FETCH_MB * 1024 * 1024 + 1)
        if len(blob) > MAX_FETCH_MB * 1024 * 1024:
            raise ValueError(f"下载超限: {ref[:60]}")
        if len(blob) < 100:
            raise ValueError(f"下载内容太小: {ref[:60]}")
        return blob, None
    if ref.startswith("base64://"):
        return base64.b64decode(ref[len("base64://"):]), None
    if ref.startswith("data:image"):
        return base64.b64decode(ref.split(",", 1)[1]), None
    p = ref[7:] if ref.startswith("file://") else ref
    lp = Path(p).expanduser()
    if not lp.is_absolute():
        for base in (Path.cwd(), PLUGIN_DIR):
            t = (base / p).resolve()
            if t.is_file():
                lp = t
                break
    if not lp.is_file():
        raise FileNotFoundError(f"找不到图片: {ref[:60]}")
    blob = lp.read_bytes()
    if len(blob) > MAX_FETCH_MB * 1024 * 1024:
        raise ValueError(f"文件超限: {lp}")
    return blob, None

async def _raw_send(event, segs):
    api = getattr(getattr(event, "bot", None), "api", None)
    call = getattr(api, "call_action", None)
    if not callable(call):
        raise RuntimeError("非 aiocqhttp 平台，无法发送")
    gid = ""
    with suppress(Exception):
        gid = (event.get_group_id() or "").strip()
    if gid:
        return await asyncio.wait_for(call(
            "send_msg", message_type="group",
            group_id=int(gid), message=segs), 30)
    sid = (event.get_sender_id() or "").strip()
    return await asyncio.wait_for(call(
        "send_msg", message_type="private",
        user_id=int(sid), message=segs), 30)

async def _recall(event, mid):
    if not mid:
        return
    api = getattr(getattr(event, "bot", None), "api", None)
    call = getattr(api, "call_action", None)
    if not callable(call):
        return
    with suppress(Exception):
        await call("delete_msg", message_id=int(mid))

def _next_seq():
    p = PLUGIN_DIR / "fax_seq.dat"
    try:
        n = int(p.read_text(encoding="utf-8").strip() or "0")
    except Exception:
        n = 0
    n += 1
    with suppress(Exception):
        p.write_text(str(n), encoding="utf-8")
    return n

def _fax_name(suffix):
    return f"fax_{time.strftime('%Y%m%d%H%M%S', time.localtime())}{_next_seq():02d}{suffix}"

class _QueueFull(Exception):
    pass

class _QueueTimeout(Exception):
    pass

class _JobGate:
    def __init__(self, max_wait=MAX_WAIT):
        self._sem = asyncio.Semaphore(1)
        self._inside = 0
        self._max_wait = max_wait

    def enter(self):
        if self._inside > self._max_wait:
            raise _QueueFull()
        self._inside += 1
        return self._inside

    async def wait_turn(self, timeout=QUEUE_TIMEOUT):
        try:
            await asyncio.wait_for(self._sem.acquire(), timeout=timeout)
        except (asyncio.TimeoutError, TimeoutError):
            self._inside -= 1
            raise _QueueTimeout() from None
        except BaseException:
            self._inside -= 1
            raise

    def leave(self):
        self._sem.release()
        self._inside -= 1

_GATE = _JobGate()

class DualPngPlugin(Star):
    def __init__(self, context: Context):
        super().__init__(context)

    @filter.command("双图", alias={"dual", "shuangtu"})
    async def dual(self, event: AstrMessageEvent):
        t0 = time.time()
        refs, apng = _parse(_strip_prefix(event.message_str or ""))
        refs += [r for r in _chain_images(event) if r not in refs]
        if len(refs) < 2:
            yield event.plain_result(
                "用法：/双图 <预览图A> <原图B> [--apng]\n"
                "示例：/双图 ./a.jpg ./b.jpg\n"
                "也支持指令后直接附带两张图。\n"
                "合成 APNG 无限循环（第1帧=A，点开播B），默认 .png 文件名发出；\n"
                "加 --apng 改用 .apng 文件名（字节一样）。"
            )
            return
        try:
            pos = _GATE.enter()
        except _QueueFull:
            yield event.plain_result("任务已满（1 个在跑 + 3 个在等），稍后再试。")
            return
        if pos > 1:
            yield event.plain_result(f"排队中，前方还有 {pos - 1} 个任务，请稍候…")
        prog = None
        with suppress(Exception):
            r = await _raw_send(event, [{"type": "text", "data": {"text": "正在处理双图中..."}}])
            prog = (r or {}).get("message_id")
        try:
            await _GATE.wait_turn()
        except _QueueTimeout:
            await _recall(event, prog)
            yield event.plain_result("排队超时（120 秒没轮到），稍后再试。")
            return
        workdir = Path(tempfile.mkdtemp(prefix="dual_", dir=str(TMP_ROOT)))
        try:
            with suppress(Exception):
                os.chmod(workdir, 0o755)
            try:
                (a_blob, _), (b_blob, _) = await asyncio.gather(
                    asyncio.to_thread(_fetch_bytes_sync, refs[0]),
                    asyncio.to_thread(_fetch_bytes_sync, refs[1]),
                )
            except Exception as e:
                yield event.plain_result(f"图片读取失败：{str(e)[:200]}")
                return
            suffix = ".apng" if apng else ".png"
            out_path = workdir / _fax_name(suffix)
            try:
                await asyncio.to_thread(
                    make_dual_apng_sync, a_blob, b_blob,
                    str(out_path), MAX_EDGE, TARGET_KB)
            except Exception as e:
                logger.exception("[双图] 合成失败")
                yield event.plain_result(f"合成失败：{str(e)[:200]}（确认 Pillow 已安装）")
                return
            size = out_path.stat().st_size
            with suppress(Exception):
                os.chmod(out_path, 0o644)
            # 图片消息原图发出：行内预览+播动画全端一致，原字节不重编码；
            # 存盘后缀由QQ按内容定（动图多为.apng），要效果就得认。
            seg = {"type": "image", "data": {
                "file": out_path.resolve().as_uri(),
                "summary": "[动图]", "sub_type": 0}}
            try:
                await _raw_send(event, [seg])
            except Exception:
                if size <= MAX_B64_TOTAL:
                    seg["data"] = {
                        "file": "base64://" + base64.b64encode(out_path.read_bytes()).decode(),
                        "summary": "[动图]", "sub_type": 0}
                    try:
                        await _raw_send(event, [seg])
                    except Exception as e2:
                        yield event.plain_result(f"发送失败：{type(e2).__name__}")
                        return
                else:
                    yield event.plain_result("发送失败且文件太大无法 base64 重试，换小图再试。")
                    return
            yield event.plain_result(
                f"已发出（{time.time()-t0:.1f}s，{size//1024}KB）点击查看原图验证"
            )
        finally:
            _GATE.leave()
            shutil.rmtree(workdir, ignore_errors=True)
            await _recall(event, prog)

    async def terminate(self):
        pass

def _frames(blob, edge):
    im = Image.open(io.BytesIO(blob))
    try:
        im.seek(0)
    except Exception:
        pass
    im = im.convert("RGBA")
    w, h = im.size
    s = min(1.0, edge / max(w, h)) if max(w, h) > 0 else 1.0
    if s < 1.0:
        im = im.resize((max(1, int(w * s)), max(1, int(h * s))), Image.LANCZOS)
    try:
        if im.getchannel("A").getextrema() == (255, 255):
            im = im.convert("RGB")
    except Exception:
        pass
    return im

def make_dual_apng_sync(a_blob, b_blob, output_path,
                        max_edge=MAX_EDGE, target_kb=TARGET_KB):
    total = B_SECONDS * 1000
    half = total // 2
    last_blob = None
    for edge in (1080, 960, 832, 720, 640, 560, 512, 448):
        edge = min(edge, max_edge)
        a = _frames(a_blob, edge)
        b = _frames(b_blob, max(a.size)).resize(a.size, Image.LANCZOS)
        bio = io.BytesIO()
        a.save(bio, format="PNG", save_all=True,
               append_images=[b, b],
               duration=[FIRST_MS, half, total - half],
               loop=0, optimize=True)
        last_blob = bio.getvalue()
        if len(last_blob) <= target_kb * 1024:
            break
    with open(output_path, "wb") as f:
        f.write(last_blob)
    logger.info(f"[双图] APNG 输出 {len(last_blob)//1024}KB")
    return output_path
