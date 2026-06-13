"""
🤖 BOT - SEQUENTIAL MODE v7.1 (BALANCED - SPEED + FEATURES)
Bot xử lý tuần tự từng tài khoản với tất cả features.

CHANGELOG v7.1 - BALANCED APPROACH:
✅ SPEED FIXES (từ v7.0):
  - FIX #1: sequential_updates=False (nhận tin mới ngay)
  - FIX #2: Non-blocking handler (< 1ms, không log)
  - FIX #3: Message queue size: 500 → 50
  - FIX #4: Message workers: 12 → 3
  - FIX #5: Handler timeout 0.01s
  - FIX #6: Page timeout 5s (thay vì 30s)
  - FIX #8: Cache page.url

✅ FEATURES GIỮ LẠI (quan trọng):
  - Dedup code (tránh nạp 2 lần)
  - Prune memory (_prune_site_code_seen)
  - Watchdog tự điền username
  - Cloudflare watchdog check
  - Daily summary report
  - Code extraction (spoiler + marker)
  - Auto-fill startup
  - History logging (CSV + JSONL)
  - Heartbeat monitoring
  - Reconnect with backoff

RESULT: ~1800 dòng, speed +80%, features 100%
"""

import asyncio
import csv
import json
import re
import time
import random
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from telethon import TelegramClient, events
from telethon.tl.types import MessageEntitySpoiler
from playwright.async_api import async_playwright

from config import Config
from logger_setup import logger
from code_validator import CodeValidator
from image_code_extractor import get_image_extractor

# ============================================================
# Domain-specific submit button selectors
# ============================================================
SUBMIT_BUTTON_SELECTORS = {
    # MM88: nút là img.submit-btn bên trong .submit-button-container
    "mm88code.com":     'img.submit-btn, .submit-button-container img, .submit-btn',
    # LLwin: class Tailwind .h-[28px]
    "llwincode.com":    'img[src*="btnnhancode" i], img[alt*="nhan" i]',
    # XX88: class .bottom-[8%]
    "xx88code.com":     'button[aria-label="Nhận code"], button[aria-label*="Nhan code" i]',
    # O8: class .modal-submit-btn
    "o8code.com":       '.modal-submit-btn',
    # NEW88 và QQ88: button type=button với aria-label
    "new88b.today":     'button[aria-label*="Kiểm tra" i]',
    "tangquaqq88.com":  'button[aria-label*="Kiểm tra" i]',
    "uy88code.org":     '#casinoSubmit',
    "mmoocode.shop":    '#casinoSubmit',
}

from database import init_database
from rate_limiter import init_anti_detection
from monitoring import init_monitoring

from features import print_version_info, get_shutdown_handler


class BotState:
    def __init__(self):
        self.playwright_instance = None
        self.connected_browsers = {}
        self.account_pages = {}
        self.context_locks = {}
        self.is_running = True
        self.cf_verified = {}
        self.submission_count = {}
        self._input_cache = {}
        self._input_cache_ttl = 30.0
        self._site_code_seen: dict = {}  # ✅ KEEP: Dedup tracking
        self._site_code_ttl: float = 10.0
        self._page_urls: dict = {}
        self.handler_registered = False


bot_state = BotState()

# ✅ FIX: sequential_updates=False (nhận tin mới ngay, không chờ)
client = TelegramClient(
    Config.SESSION_NAME,
    Config.API_ID,
    Config.API_HASH,
    device_model="Desktop Bot",
    system_version="Windows 10",
    app_version="1.0",
    connection_retries=5,
    retry_delay=1,
    auto_reconnect=True,
    use_ipv6=False,
    flood_sleep_threshold=60,
    receive_updates=True,
    sequential_updates=False,  # ✅ CRITICAL FIX: True → False
)

_systems = None
message_queue = None
message_workers = []

_history_queue: asyncio.Queue = None
_history_writer_task = None

_submit_semaphore: asyncio.Semaphore | None = None
_active_submit_tasks: set[asyncio.Task] = set()


def normalize_domain(url: str) -> str:
    parsed = urlparse(url or "")
    domain = parsed.netloc or parsed.path
    return domain.lower().replace("www.", "").strip("/")


def select_random_code(codes: list) -> str:
    """Lấy ngẫu nhiên 1 code từ danh sách."""
    if not codes:
        return None
    if len(codes) == 1:
        return codes[0]
    return random.choice(codes)


# ============================================================
# 📝 LỊCH SỬ CODE / DAILY MAINTENANCE LOG
# ============================================================
CODE_HISTORY_DIR = Path("logs/code_history")
CODE_HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _write_history_row(row: dict):
    """Ghi 1 dòng vào CSV + JSONL."""
    try:
        fieldnames = [
            "time", "event_type", "channel", "site", "account", "code",
            "source", "status", "telegram_delay", "submit_elapsed",
            "message", "screenshot",
        ]
        csv_path = CODE_HISTORY_DIR / f"code_history_{_today_str()}.csv"
        jsonl_path = CODE_HISTORY_DIR / f"code_history_{_today_str()}.jsonl"

        write_header = not csv_path.exists()
        with csv_path.open("a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerow(row)

        with jsonl_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.debug(f"⚠️ Không ghi được code history: {e}")


async def _history_writer_loop():
    """Worker chạy nền, xử lý queue ghi lịch sử."""
    global _history_queue
    while True:
        try:
            row = await _history_queue.get()
            if row is None:
                break
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _write_history_row, row)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"⚠️ history_writer_loop lỗi: {e}")
        finally:
            try:
                _history_queue.task_done()
            except Exception:
                pass


def start_history_writer():
    """Khởi động background writer task."""
    global _history_queue, _history_writer_task
    _history_queue = asyncio.Queue(maxsize=2000)
    _history_writer_task = asyncio.create_task(_history_writer_loop())
    logger.info("✅ Background history writer đã khởi động")


def get_submit_semaphore() -> asyncio.Semaphore:
    """Semaphore giới hạn concurrent submits."""
    global _submit_semaphore
    if _submit_semaphore is None:
        limit = max(1, int(getattr(Config, "MAX_CONCURRENT_SUBMITS", 2)))
        _submit_semaphore = asyncio.Semaphore(limit)
    return _submit_semaphore


async def submit_code_limited(user: str, code: str, target_url: str, systems: dict):
    """Chạy submit trong semaphore, timeout tối đa 30s để tránh worker kẹt mãi."""
    sem = get_submit_semaphore()
    async with sem:
        delay = float(getattr(Config, "MIN_DELAY_BETWEEN_SUBMITS", 0.8))
        if delay > 0:
            await asyncio.sleep(delay)
        try:
            return await asyncio.wait_for(
                submit_code_safe(user, code, target_url, systems),
                timeout=30.0
            )
        except asyncio.TimeoutError:
            logger.warning(f"⏰ [{user}] submit_code_safe timeout 30s - bỏ qua, chuyển acc tiếp theo")
            return {"success": False, "message": "Timeout 30s"}


def track_submit_task(task: asyncio.Task, label: str = ""):
    """Giữ reference task và log exception."""
    _active_submit_tasks.add(task)

    def _done(t: asyncio.Task):
        _active_submit_tasks.discard(t)
        try:
            result = t.result()
            if isinstance(result, dict):
                ok = "✅" if result.get("success") else "⚠️"
                logger.info(f"{ok} [TASK] {label} | {result.get('message', '')}")
        except asyncio.CancelledError:
            logger.debug(f"🛑 [TASK CANCELLED] {label}")
        except Exception as e:
            logger.error(f"❌ [TASK ERROR] {label}: {e}")

    task.add_done_callback(_done)
    return task


def append_code_history(
    event_type: str,
    code: str = "",
    target_url: str = "",
    account: str = "",
    channel: str = "",
    source: str = "",
    status: str = "",
    telegram_delay=None,
    submit_elapsed=None,
    message: str = "",
    screenshot: str = "",
):
    """Enqueue lịch sử code — KHÔNG block luồng chính."""
    try:
        row = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "event_type": event_type,
            "channel": channel or "",
            "site": normalize_domain(target_url),
            "account": account or "",
            "code": str(code or ""),
            "source": source or "",
            "status": status or "",
            "telegram_delay": "" if telegram_delay is None else f"{float(telegram_delay):.2f}",
            "submit_elapsed": "" if submit_elapsed is None else f"{float(submit_elapsed):.2f}",
            "message": str(message or "").replace("\n", " ")[:300],
            "screenshot": str(screenshot or ""),
        }

        if _history_queue is not None:
            try:
                _history_queue.put_nowait(row)
            except asyncio.QueueFull:
                logger.debug("⚠️ History queue đầy, bỏ qua 1 dòng log")
        else:
            _write_history_row(row)

        return row
    except Exception as e:
        logger.debug(f"⚠️ Không enqueue được code history: {e}")
        return None


# ============================================================
# ✅ KEEP: DEDUP CODE LOGIC (ngăn nạp 2 lần)
# ============================================================

def _prune_site_code_seen():
    """✅ KEEP: Dọn các entry hết TTL trong _site_code_seen để tránh rò rỉ memory."""
    ttl = float(getattr(Config, "SITE_CODE_DEDUP_TTL", 10.0))
    now = time.time()
    expired = [k for k, ts in bot_state._site_code_seen.items() if now - ts > ttl]
    for k in expired:
        del bot_state._site_code_seen[k]
    if expired:
        logger.debug(f"🧹 Đã dọn {len(expired)} entry hết hạn khỏi site_code_seen")


def is_site_code_duplicate(domain: str, code: str) -> bool:
    """✅ KEEP: Check code đã nạp cho domain trong TTL gần đây."""
    ttl = float(getattr(Config, "SITE_CODE_DEDUP_TTL", 10.0))
    now = time.time()
    _prune_site_code_seen()
    key = (domain, code.upper())
    seen_at = bot_state._site_code_seen.get(key)
    if seen_at is not None and now - seen_at < ttl:
        return True
    # Ghi nhận lần đầu thấy
    bot_state._site_code_seen[key] = now
    return False


def build_daily_summary():
    """✅ KEEP: Tạo file tổng kết cuối ngày từ lịch sử RESULT."""
    try:
        csv_path = CODE_HISTORY_DIR / f"code_history_{_today_str()}.csv"
        if not csv_path.exists():
            logger.info("📒 Chưa có lịch sử code hôm nay để tổng kết")
            return None

        summary = {}
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("event_type") != "RESULT":
                    continue
                key = (row.get("site", ""), row.get("account", ""))
                if key not in summary:
                    summary[key] = {"SUCCESS": 0, "FAILED": 0, "UNKNOWN": 0}
                status = row.get("status") or "UNKNOWN"
                if status not in summary[key]:
                    summary[key][status] = 0
                summary[key][status] += 1

        out_path = CODE_HISTORY_DIR / f"daily_summary_{_today_str()}.csv"
        with out_path.open("w", newline="", encoding="utf-8-sig") as f:
            fieldnames = ["date", "site", "account", "success", "failed", "unknown", "total"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for (site, account), counts in sorted(summary.items()):
                success = counts.get("SUCCESS", 0)
                failed = counts.get("FAILED", 0)
                unknown = counts.get("UNKNOWN", 0)
                writer.writerow({
                    "date": _today_str(),
                    "site": site,
                    "account": account,
                    "success": success,
                    "failed": failed,
                    "unknown": unknown,
                    "total": success + failed + unknown,
                })

        logger.info(f"📒 Đã tạo báo cáo cuối ngày: {out_path}")
        return str(out_path)
    except Exception as e:
        logger.warning(f"⚠️ Không tạo được daily summary: {e}")
        return None


def measure_telegram_delay_fast(msg_timestamp) -> float | None:
    """Đo delay nhanh (< 1ms)."""
    try:
        if msg_timestamp.tzinfo is None:
            msg_timestamp = msg_timestamp.replace(tzinfo=timezone.utc)
        now_utc = datetime.now(timezone.utc)
        delay = (now_utc - msg_timestamp).total_seconds()
        return delay
    except Exception:
        return None


def build_unique_account_targets():
    """CHẾ ĐỘ 1 TAB / DOMAIN."""
    items = []
    seen_domains = set()

    sorted_channels = sorted(
        Config.CHANNEL_CONFIG.items(),
        key=lambda item: item[1].get("priority", 999),
    )

    for chat_id, channel_config in sorted_channels:
        target_url = channel_config["url"]
        domain = normalize_domain(target_url)

        if domain in seen_domains:
            continue
        seen_domains.add(domain)

        accounts = channel_config.get("accounts", [])
        if not accounts:
            continue

        first_account = sorted(accounts, key=lambda a: a.get("priority", 999))[0]
        port = get_user_port(first_account["username"])

        items.append(
            {
                "chat_id": chat_id,
                "channel_name": channel_config.get("name", ""),
                "target_url": target_url,
                "domain": domain,
                "port": port,
                "accounts": sorted(accounts, key=lambda a: a.get("priority", 999)),
            }
        )

    return items


def get_user_port(user: str) -> int:
    for port, users_list in getattr(Config, "CDP_CONNECTIONS", {}).items():
        if user in users_list:
            return int(port)
    return 9222


def get_default_account_for_domain(domain: str) -> str | None:
    """✅ KEEP: Lấy account priority cao nhất cho domain (cho watchdog)."""
    for chat_id, cfg in Config.CHANNEL_CONFIG.items():
        if normalize_domain(cfg["url"]) == domain:
            accounts = cfg.get("accounts", [])
            if accounts:
                sorted_acc = sorted(accounts, key=lambda a: a.get("priority", 999))
                return sorted_acc[0]["username"]
    return None


async def verify_telegram_session():
    logger.info("\n" + "=" * 70)
    logger.info("🔐 XÁC MINH TELEGRAM SESSION...")

    try:
        me = await client.get_me()
        logger.info("✅ SESSION HỢP LỆ!")
        logger.info(f"   👤 Username: @{me.username}")
        logger.info(f"   🆔 User ID: {me.id}")
        return True
    except Exception as e:
        logger.error(f"❌ SESSION LỖI: {e}")
        return False


async def verify_channels_and_get_ids():
    logger.info("\n" + "=" * 70)
    logger.info("📡 XÁC MINH CHANNELS...")

    valid_channels = {}
    my_dialogs = {dialog.id: dialog async for dialog in client.iter_dialogs()}

    for chat_id, channel_config in Config.CHANNEL_CONFIG.items():
        if chat_id in my_dialogs:
            logger.info(f"✅ HỢP LỆ: {channel_config['name']}")
            valid_channels[chat_id] = channel_config
        else:
            logger.warning(f"❌ CHƯA THAM GIA: {channel_config['name']}")

    return valid_channels


async def init_systems():
    print_version_info()

    db = init_database(Config.DATABASE_PATH)
    anti_det = init_anti_detection()
    _, _, perf_mon = init_monitoring()  # health monitor tự chạy background

    bot_state.playwright_instance = await async_playwright().start()
    get_shutdown_handler().setup(bot_state)

    start_history_writer()

    return {
        "db": db,
        "anti_detection": anti_det,
        "performance_monitor": perf_mon,
    }


async def safe_is_visible(element) -> bool:
    try:
        return await element.is_visible()
    except Exception:
        return False


def _invalidate_input_cache(key: str):
    """Xóa cache input fields."""
    bot_state._input_cache.pop(key, None)


async def find_input_fields(page, cache_key: str = None):
    """Tìm input fields với cache."""
    now = time.time()

    if cache_key:
        cached = bot_state._input_cache.get(cache_key)
        if cached:
            username_input, code_input, cache_time = cached
            if now - cache_time < bot_state._input_cache_ttl:
                try:
                    if code_input:
                        await code_input.is_visible()
                    return username_input, code_input
                except Exception:
                    _invalidate_input_cache(cache_key)

    username_input = None
    code_input = None

    username_selectors = [
        "#account-code",                          # QQ88
        "#username-input",                        # NEW88
        "#ten_tai_khoan",                         # UY88/MMOO
        "input#username",                         # MM88/LLwin/XX88/O8
        "input[name='username']",                 # MM88/LLwin/XX88/O8
        "input[placeholder*='người dùng' i]",    # MM88/LLwin/XX88/O8
        "input[placeholder*='tên' i]",
        "input[placeholder*='tài' i]",
        "input[placeholder*='tài khoản' i]",
        "input[placeholder*='user' i]",
        "input[placeholder*='đăng nhập' i]",
        "input[name='ten_tai_khoan']",
        "input[id='username']",
        "input[type='text']",
    ]

    code_selectors = [
        "#promo-code",              # QQ88
        "#giftcode-input",          # NEW88
        "input[autocomplete='one-time-code']",  # LLwin
        "input#code",               # MM88/XX88/O8
        "input[name='code']",       # MM88/XX88/O8
        "input[placeholder*='mã code' i]",
        "input[placeholder*='code' i]",
        "input[placeholder*='mã' i]",
        "input[name='giftcode']",
        "input[id='code']",
        "input[id*='code' i]",
        "input[id*='promo' i]",
    ]

    try:
        for selector in username_selectors:
            try:
                element = await page.query_selector(selector)
                if element and await safe_is_visible(element):
                    username_input = element
                    break
            except Exception:
                pass

        for selector in code_selectors:
            try:
                element = await page.query_selector(selector)
                if element and await safe_is_visible(element):
                    code_input = element
                    break
            except Exception:
                pass

        if not username_input or not code_input:
            inputs = await page.query_selector_all(
                "input:not([type='hidden']):not([type='checkbox']):not([type='radio']):not([type='submit'])"
            )

            visible_inputs = []
            for input_element in inputs:
                if await safe_is_visible(input_element):
                    visible_inputs.append(input_element)

            if len(visible_inputs) >= 2:
                if not username_input:
                    username_input = visible_inputs[0]
                if not code_input:
                    code_input = visible_inputs[1]
            elif len(visible_inputs) == 1:
                if not code_input:
                    code_input = visible_inputs[0]

    except Exception as e:
        logger.debug(f"⚠️ Lỗi tìm input fields: {e}")

    if cache_key and code_input:
        bot_state._input_cache[cache_key] = (username_input, code_input, now)

    return username_input, code_input


async def get_input_value(input_element) -> str:
    try:
        value = await input_element.input_value(timeout=1000)
        return value.strip()
    except Exception:
        return ""


async def click_submit_fast(page) -> bool:
    """Bấm submit nhanh - ưu tiên match theo TEXT/aria-label trước (chính xác hơn selector chung)."""

    # ✅ BƯỚC 1: Match theo TEXT/aria-label/alt trước - "Kiểm tra", "Xác thực", "Nhận code"...
    # Tránh click nhầm menu/nút khác qua selector quá rộng như button[type='button']
    try:
        clicked = await page.evaluate("""
            () => {
                const keywords = [
                    'kiểm tra ngay', 'kiem tra ngay',
                    'kiểm tra', 'kiem tra',
                    'xác thực', 'xac thuc',
                    'nhận code', 'nhan code',
                    'nhận ngay', 'nhan ngay',
                    'áp dụng', 'ap dung',
                    'đổi code', 'doi code',
                    'nạp code', 'nap code',
                    'gửi', 'gui',
                    'submit', 'apply', 'check', 'verify'
                ];
                const els = [...document.querySelectorAll('button, a, div[role="button"], span[role="button"], input[type="button"], input[type="submit"]')];
                for (const kw of keywords) {
                    for (const el of els) {
                        if (el.disabled) continue;
                        const aria = (el.getAttribute('aria-label') || '').toLowerCase();
                        const img = el.querySelector('img[alt]');
                        const imgAlt = img ? (img.getAttribute('alt') || '').toLowerCase() : '';
                        const txt = (el.innerText || el.textContent || el.value || '').toLowerCase().trim();
                        if ([txt, aria, imgAlt].some(s => s && s.includes(kw))) {
                            const rect = el.getBoundingClientRect();
                            if (rect.width > 0 && rect.height > 0) {
                                el.click();
                                return true;
                            }
                        }
                    }
                }
                return false;
            }
        """)
        if clicked:
            return True
    except Exception:
        pass

    # ✅ BƯỚC 2: Selector cụ thể (theo class/aria-label đặc trưng)
    specific_selectors = [
        # MM88: img.submit-btn bên trong div
        "img.submit-btn",
        ".submit-button-container img",
        # O8: modal-submit-btn
        ".modal-submit-btn",
        # QQ88 / NEW88: button với aria-label
        "button[aria-label*='Kiểm tra' i]",
        "button[aria-label*='kiem tra' i]",
        # Generic
        ".btn-submit",
        "[class*='btn-submit' i]",
        ".apply-btn",
        "[class*='apply' i]",
        ".submit-btn",
        "[class*='submit' i]",
        "[class*='check' i]",
        "button[type='submit']",
    ]

    for selector in specific_selectors:
        try:
            element = await page.query_selector(selector)
            if element and await safe_is_visible(element):
                await page.evaluate("el => el.click()", element)
                return True
        except Exception:
            pass

    # ⚠️ BƯỚC 3: Fallback cuối - chọn button visible đầu tiên,
    # LOẠI TRỪ menu/nav/close (vd "Home menu" của MM88 floating button)
    try:
        clicked = await page.evaluate("""
            () => {
                const EXCLUDE = /menu|nav|home|close|cancel|toggle|hamburger|back|trở về|huỷ|hủy|đóng/i;
                const candidates = [...document.querySelectorAll(
                    "img.submit-btn, .modal-submit-btn, button[type='submit'], button[type='button'], button, .btn, [class*='btn' i], [class*='submit' i] img"
                )];
                // Ưu tiên type=submit trước, sau đó type=button/khác
                const score = (el) => el.getAttribute('type') === 'submit' ? 0 : 1;
                candidates.sort((a, b) => score(a) - score(b));
                for (const el of candidates) {
                    if (el.disabled) continue;
                    const aria = el.getAttribute('aria-label') || '';
                    const cls = el.className || '';
                    const txt = (el.innerText || el.textContent || '').trim();
                    if (EXCLUDE.test(aria) || EXCLUDE.test(cls) || EXCLUDE.test(txt)) continue;
                    const rect = el.getBoundingClientRect();
                    if (rect.width > 0 && rect.height > 0) {
                        el.click();
                        return true;
                    }
                }
                return false;
            }
        """)
        if clicked:
            return True
    except Exception:
        pass

    try:
        await page.keyboard.press("Enter")
        return True
    except Exception:
        return False


async def handle_cloudflare_popup(page) -> bool:
    """Xử lý popup Cloudflare 'MÃ XÁC THỰC' của QQ88/NEW88.
    Bấm 'Xác thực' nếu Cloudflare đã tick Thành công, ngược lại chờ tối đa 8s.
    Trả về True nếu đã xử lý xong popup, False nếu không có popup.
    """
    try:
        # Kiểm tra có popup không (tìm nút "Xác thực" hoặc "Hủy")
        popup_selectors = [
            "button:has-text('Xác thực')",
            "button:has-text('Xac thuc')",
        ]
        popup_btn = None
        for sel in popup_selectors:
            try:
                el = await page.query_selector(sel)
                if el and await safe_is_visible(el):
                    popup_btn = el
                    break
            except Exception:
                pass

        if not popup_btn:
            return False  # Không có popup

        logger.info("🔒 [CLOUDFLARE] Phát hiện popup xác thực - đang chờ tick...")

        # Chờ Cloudflare tự tick "Thành công" tối đa 8 giây
        for _ in range(16):
            await asyncio.sleep(0.5)
            try:
                # Tìm text "Thành công" hoặc checkmark xanh
                success = await page.evaluate("""
                    () => {
                        const texts = [...document.querySelectorAll('*')];
                        return texts.some(el => {
                            const t = (el.innerText || '').trim();
                            return t === 'Thành công!' || t === 'Thanh cong!';
                        });
                    }
                """)
                if success:
                    logger.info("✅ [CLOUDFLARE] Đã xác minh thành công - bấm Xác thực")
                    break
            except Exception:
                pass

        # Bấm nút "Xác thực"
        for sel in popup_selectors:
            try:
                el = await page.query_selector(sel)
                if el and await safe_is_visible(el):
                    # Ưu tiên bấm nút không bị disabled
                    disabled = await el.get_attribute("disabled")
                    if disabled is None:
                        await page.evaluate("el => el.click()", el)
                        logger.info("✅ [CLOUDFLARE] Đã bấm Xác thực")
                        await asyncio.sleep(0.5)
                        return True
            except Exception:
                pass

        return False

    except Exception as e:
        logger.debug(f"⚠️ Cloudflare popup error: {e}")
        return False


async def take_result_screenshot(page, user: str, code: str, target_url: str, status: str) -> str:
    """✅ KEEP: Chụp ảnh kết quả."""
    if not bool(getattr(Config, "SCREENSHOT_ON_UNKNOWN", False)):
        return ""

    try:
        shot_dir = Path("logs/screenshots")
        shot_dir.mkdir(parents=True, exist_ok=True)
        safe_domain = normalize_domain(target_url).replace(".", "_").replace("/", "_")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = shot_dir / f"{safe_domain}_{user}_{code}_{status}_{ts}.png"
        await page.screenshot(path=str(path), full_page=False)
        return str(path)
    except Exception as e:
        logger.debug(f"⚠️ Không chụp được screenshot: {e}")
        return ""


async def connect_to_cdp_port(port: int):
    if port in bot_state.connected_browsers:
        return bot_state.connected_browsers[port]

    logger.info(f"🖥️ Đang kết nối trình duyệt CDP port {port}...")
    cdp_url = f"http://127.0.0.1:{port}"

    browser = await bot_state.playwright_instance.chromium.connect_over_cdp(cdp_url)
    bot_state.connected_browsers[port] = browser

    logger.info(f"✅ Đã kết nối CDP port {port}")
    return browser


async def _setup_page_performance(page, label: str = ""):
    """✅ KEEP: Tối ưu tốc độ cho tab."""
    _BLOCK_DOMAINS = (
        "google-analytics", "googletagmanager", "doubleclick",
        "facebook.net", "fbcdn.net", "hotjar",
    )
    _BLOCK_TYPES = ("media", "ping")

    async def _handle_route(route):
        req = route.request
        url = req.url.lower()
        rtype = req.resource_type
        
        if "cloudflare" in url:
            await route.continue_()
            return

        if any(d in url for d in _BLOCK_DOMAINS):
            await route.abort()
            return

        if rtype in _BLOCK_TYPES:
            await route.abort()
            return
        await route.continue_()

    try:
        await page.route("**/*", _handle_route)
        logger.debug(f"⚡ [{label}] Đã tối ưu page performance")
    except Exception as e:
        logger.debug(f"⚠️ [{label}] Không setup page: {e}")


async def _wake_tab_for_submit(page):
    """Đánh thức tab trước submit."""
    try:
        await page.bring_to_front()
        await page.evaluate("""
            Object.defineProperty(document, 'visibilityState', {
                get: () => 'visible', configurable: true
            });
        """)
    except Exception:
        pass


async def auto_fill_username_on_startup(page, domain: str, username: str):
    """✅ KEEP: Tự điền username lần đầu."""
    try:
        username_input, _ = await find_input_fields(page, cache_key=None)
        if not username_input:
            logger.warning(f"⚠️ [{domain}] Không tìm thấy ô username")
            return False

        current_value = await get_input_value(username_input)
        
        if current_value.lower() == username.lower():
            logger.info(f"✅ [{domain}] Username '{username}' đã được điền")
            return True

        if current_value == "":
            await username_input.fill(username)
            logger.info(f"✅ [{domain}] Đã tự điền username: {username}")
            return True

        logger.warning(f"⚠️ [{domain}] Ô username đã có giá trị: {current_value}")
        return False

    except Exception as e:
        logger.warning(f"⚠️ [{domain}] Không tự điền username: {e}")
        return False


async def _setup_one_domain_tab(item: dict, assigned_pages: set, assign_lock: asyncio.Lock):
    """Mở/gán 1 tab cho 1 domain. Timeout 15s để không treo mãi."""
    domain = item["domain"]
    try:
        return await asyncio.wait_for(
            _setup_one_domain_tab_inner(item, assigned_pages, assign_lock),
            timeout=15.0
        )
    except asyncio.TimeoutError:
        logger.warning(f"⏰ [{domain}] Setup timeout 15s — bỏ qua, bot vẫn chạy tiếp")
        # Vẫn đăng ký page vào account_pages nếu có thể
        return False
    except Exception as e:
        logger.error(f"❌ [{domain}] Setup lỗi: {e}")
        return False


async def _setup_one_domain_tab_inner(item: dict, assigned_pages: set, assign_lock: asyncio.Lock):
    """Logic thật của setup tab."""
    target_url = item["target_url"]
    domain = item["domain"]
    port = item["port"]
    accounts = item["accounts"]
    key = domain

    logger.info(f"🔌 [{domain}] Kết nối CDP port {port}...")
    browser = await connect_to_cdp_port(port)

    if not browser.contexts:
        logger.error(f"❌ [{domain}] Port {port} không có context")
        return False

    context = browser.contexts[0]
    page = None
    reason = ""

    async with assign_lock:
        for p in context.pages:
            try:
                if domain in p.url.lower() and p not in assigned_pages:
                    page = p
                    reason = "Tìm thấy tab đã mở"
                    assigned_pages.add(page)
                    break
            except Exception:
                pass

        if not page:
            if bool(getattr(Config, "AUTO_OPEN_MISSING_TABS", True)):
                page = await context.new_page()
                assigned_pages.add(page)
                reason = "Bot mở tab mới"
            else:
                logger.error(f"❌ [{domain}] Không có tab")
                return False

    if reason == "Bot mở tab mới":
        logger.info(f"🆕 [{domain}] Đang tải trang...")
        await _setup_page_performance(page, label=domain)
        try:
            await page.goto(target_url, wait_until="domcontentloaded", timeout=8000)
            logger.info(f"🌐 [{domain}] Tải trang xong")
        except Exception as e:
            logger.warning(f"⚠️ [{domain}] Tải trang lỗi (vẫn tiếp tục): {e}")
    else:
        await _setup_page_performance(page, label=domain)

    # Đăng ký page ngay — kể cả khi Cloudflare chưa xong
    bot_state.account_pages[key] = page
    bot_state.context_locks[key] = asyncio.Lock()
    bot_state.cf_verified[key] = True
    bot_state.submission_count[key] = 0

    try:
        await page.bring_to_front()
    except Exception:
        pass

    # Tự điền username
    first_account = accounts[0]["username"] if accounts else ""
    if first_account:
        logger.info(f"👤 [{domain}] Điền username: {first_account}")
        await auto_fill_username_on_startup(page, domain, first_account)

    # Tìm ô code — không block nếu không thấy (Cloudflare chưa xong)
    logger.info(f"🔍 [{domain}] Tìm ô nhập code...")
    _, code_input = await find_input_fields(page)

    if code_input:
        logger.info(f"✅ [{domain}] Sẵn sàng | acc: {[a['username'] for a in accounts]}")
    else:
        logger.warning(f"⚠️ [{domain}] Chưa thấy ô code (có thể Cloudflare) — vẫn đăng ký tab")

    return True


async def preload_browsers_and_accounts():
    """✅ Mở tất cả tab SONG SONG (1 tab/domain) thay vì tuần tự."""
    account_targets = build_unique_account_targets()

    if not account_targets:
        logger.error("❌ Không có kênh nào")
        return

    total_tabs = len(account_targets)
    logger.info(f"🔄 Bắt đầu mở {total_tabs} tab...")
    logger.info("=" * 50)
    for idx, item in enumerate(account_targets, 1):
        domain = item.get("domain", "?")
        accounts = item.get("accounts", [])
        users = [a["username"] for a in accounts]
        logger.info(f"   [{idx}/{total_tabs}] {domain} — acc: {users}")
    logger.info("=" * 50)

    assigned_pages = set()
    assign_lock = asyncio.Lock()
    done_count = 0
    done_lock = asyncio.Lock()

    async def _setup_with_progress(item):
        nonlocal done_count
        result = await _setup_one_domain_tab(item, assigned_pages, assign_lock)
        async with done_lock:
            done_count += 1
            domain = item.get("domain", "?")
            status = "✅" if result is True else "❌"
            logger.info(f"   {status} [{done_count}/{total_tabs}] {domain} {'sẵn sàng' if result is True else 'lỗi'}")
        return result

    tasks = [_setup_with_progress(item) for item in account_targets]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    ok_count = sum(1 for r in results if r is True)
    total = len(bot_state.account_pages)
    logger.info("=" * 50)
    logger.info(f"✅ Hoàn tất: {ok_count}/{total_tabs} tab sẵn sàng")
    if ok_count < total_tabs:
        logger.warning(f"⚠️ {total_tabs - ok_count} tab lỗi — kiểm tra Cloudflare hoặc kết nối")
    logger.info("🤖 BOT ĐANG CHẠY — đang lắng nghe Telegram...")
    logger.info("=" * 50)


# ============================================================
# 🔍 BỘ TRÍCH XUẤT CODE (KEEP: Spoiler + Marker)
# ============================================================

def validate_candidate(code: str, target_url: str, source: str = "normal"):
    """Validate code."""
    try:
        return CodeValidator.validate_code(code, target_url, source=source)
    except TypeError:
        return CodeValidator.validate_code(code, target_url)


def get_filter_group_name(target_url: str) -> str:
    group_name, _ = CodeValidator.get_filter_group(target_url)
    return group_name


def unique_keep_order(items):
    seen = set()
    result = []

    for item in items:
        clean = CodeValidator.clean_code(item)
        if not clean:
            continue

        upper = clean.upper()

        if upper not in seen:
            seen.add(upper)
            result.append(clean)

    return result


def remove_noise_from_text(text: str) -> str:
    """✅ KEEP: Loại bỏ noise từ text."""
    cleaned = text or ""

    cleaned = re.sub(r"https?://\S+", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"www\.\S+", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b[a-zA-Z0-9.-]+\.(com|net|org|vn|app|info)\b", " ", cleaned, flags=re.IGNORECASE)

    cleaned = cleaned.replace("：", ":")
    cleaned = cleaned.replace("|", " ")
    cleaned = cleaned.replace("•", " ")

    return cleaned


def line_has_code_marker(line: str) -> bool:
    """✅ KEEP: Check dòng có marker NHẬN CODE."""
    upper = line.upper()

    markers = [
        "NHẬN CODE NGAY",
        "NHAN CODE NGAY",
        "NHẬN CODE",
        "NHAN CODE",
        "NHẬP CODE",
        "NHAP CODE",
        "PHÁT CODE",
        "PHAT CODE",
        "CODE FREE",
    ]

    return any(marker in upper for marker in markers)


def line_is_noise(line: str) -> bool:
    """✅ KEEP: Check dòng là noise."""
    upper = line.upper().strip()

    if not upper:
        return True

    noise_keywords = [
        "HTTP", "WWW", ".COM", "FACEBOOK", "TELEGRAM", "TIKTOK", "ZALO",
        "CSKH", "BOT", "CHECK LINK", "LINK",
    ]

    return any(keyword in upper for keyword in noise_keywords)


def extract_tokens_from_line(line: str):
    """✅ KEEP: Trích xuất tokens từ dòng."""
    special_chars = re.escape(getattr(Config, "SPECIAL_CODE_CHARS_30", ""))
    min_len = getattr(Config, "CODE_MIN_LENGTH", 6)
    max_len = getattr(Config, "CODE_MAX_LENGTH", 15)
    max_raw_len = max_len + 30

    pattern = rf"[A-Za-z0-9{special_chars}]{{{min_len},{max_raw_len}}}"
    tokens = []

    for candidate in re.findall(pattern, line or ""):
        clean = CodeValidator.clean_code(candidate)
        if min_len <= len(clean) <= max_len:
            tokens.append(candidate)

    return tokens


def extract_spoiler_codes(event, target_url: str):
    """✅ KEEP: Lấy code từ spoiler/làm mờ."""
    codes = []

    if not event.message.entities:
        return codes

    try:
        for entity, entity_text in event.message.get_entities_text():
            if not isinstance(entity, MessageEntitySpoiler):
                continue

            spoiler_text = (entity_text or "").strip()
            if not spoiler_text:
                continue

            spoiler_lines = spoiler_text.splitlines() if "\n" in spoiler_text else [spoiler_text]

            for spoiler_line in spoiler_lines:
                spoiler_line = spoiler_line.strip()
                if not spoiler_line:
                    continue

                tokens = extract_tokens_from_line(spoiler_line)
                if not tokens:
                    tokens = [spoiler_line]

                for token in tokens:
                    validation = validate_candidate(token, target_url, source="spoiler")

                    if validation["valid"]:
                        codes.append(validation["clean_code"])
                        logger.info(f"🔒 Spoiler code: {validation['clean_code']}")

    except Exception as e:
        logger.warning(f"⚠️ Lỗi đọc spoiler: {e}")

    return unique_keep_order(codes)


def extract_marker_near_codes(text: str, target_url: str):
    """✅ KEEP: Lấy code gần dòng marker NHẬN CODE NGAY."""
    cleaned_text = remove_noise_from_text(text)
    lines = [line.strip() for line in cleaned_text.splitlines()]
    codes = []

    for index, line in enumerate(lines):
        if not line_has_code_marker(line):
            continue

        scan_lines = []
        if line:
            scan_lines.append(line)

        # Scan 8 dòng tiếp theo
        for offset in range(1, 9):
            if index + offset < len(lines):
                scan_lines.append(lines[index + offset])

        for scan_line in scan_lines:
            if line_is_noise(scan_line):
                continue

            tokens = extract_tokens_from_line(scan_line)

            for token in tokens:
                clean = CodeValidator.clean_code(token)
                validation = validate_candidate(clean, target_url, source="marker")

                if validation["valid"]:
                    codes.append(validation["clean_code"])
                    logger.info(f"🎯 Marker code: {validation['clean_code']}")

    return unique_keep_order(codes)


def extract_codes_by_regex(text: str, site_type: str = "qq88") -> list:
    """
    Trích code bằng regex theo loại site.
    - QQ88: mix hoa + thường + số (vd: rft3TvGTYh, 0ZTa17bbD2)
    - LLWIN: in hoa + dấu / hoặc * (vd: 4/6/2/S/C/R, 0*D*L*7*T*G)
    """
    if not text:
        return []

    codes = []

    if site_type == "qq88":
        # Từ khóa quảng cáo / chữ thuần túy bị loại.
        # Mã code chứa các từ này (kể cả là 1 phần) đều là spam, KHÔNG phải code thật.
        QQ88_BLACKLIST = {
            "QQ88", "CODE", "DANGNHAP", "GAMEBAI", "NOHU", "CASINO",
            "REVIEWPHIM", "TINTUC", "KHUYENMAI", "GIFTCODE", "FREECODE",
            "CAMERA", "TROLL", "BONGDA", "THETHAO", "MINIGAME",
        }
        pattern = r'[a-zA-Z0-9]{6,15}'
        matches = re.findall(pattern, text)
        for match in matches:
            # ✅ Chặn nếu chứa bất kỳ từ khóa blacklist (kể cả là 1 phần của chuỗi)
            if any(kw in match.upper() for kw in QQ88_BLACKLIST):
                continue
            has_letter = any(c.isalpha() for c in match)
            has_digit = any(c.isdigit() for c in match)
            has_lower = any(c.islower() for c in match)
            has_upper = any(c.isupper() for c in match)
            # Code QQ88 phải có chữ + số, hoặc có cả hoa & thường (mix)
            if has_letter and (has_digit or (has_lower and has_upper)):
                codes.append(match)

    elif site_type == "llwin":
        # LLWIN: in hoa, phân cách bằng bất kỳ dấu đặc biệt nào, ít nhất 4 phần
        LLWIN_SEP = r'[~!@#$%^&*()\-_+{}|:"<>?`=\[\]\\;\',\.\\/]'
        pattern = (
            r'[A-Z0-9]{1,3}' + LLWIN_SEP + r'{1,2}'
            r'[A-Z0-9]{1,3}(?:' + LLWIN_SEP + r'{1,2}[A-Z0-9]{1,3}){2,}'
        )
        codes.extend(re.findall(pattern, text.upper()))

    # Dedup, keep order
    return list(dict.fromkeys(codes))


def extract_codes_from_message(event, raw_text: str, target_url: str):
    """✅ KEEP: Lấy code từ tin nhắn - Spoiler + Marker + Regex."""
    codes = []

    # BƯỚC 1: Spoiler (ưu tiên)
    spoiler_codes = extract_spoiler_codes(event, target_url)
    if spoiler_codes:
        logger.info(f"🎯 Code từ spoiler: {spoiler_codes}")
        return spoiler_codes

    # BƯỚC 2: Marker (NHẬN CODE NGAY)
    marker_codes = extract_marker_near_codes(raw_text, target_url)
    if marker_codes:
        logger.info(f"🎯 Code từ marker: {marker_codes}")
        return marker_codes

    # BƯỚC 3: Regex fallback cho QQ88 và LLWIN
    group_name = get_filter_group_name(target_url)
    if group_name in ("qq88", "llwin"):
        regex_raw = extract_codes_by_regex(raw_text, site_type=group_name)
        regex_codes = []
        for raw in regex_raw:
            validation = validate_candidate(raw, target_url, source="regex")
            if validation["valid"]:
                regex_codes.append(validation["clean_code"])
        if regex_codes:
            logger.info(f"🎯 Code từ regex [{group_name}]: {regex_codes}")
            return regex_codes

    return []


# ============================================================
# ⚡ RESULT DETECTION
# ============================================================

async def _fetch_element_text(page, selector: str) -> str:
    """Lấy text element."""
    try:
        elements = await page.query_selector_all(selector)
        texts = []
        for element in elements:
            try:
                text = await element.inner_text(timeout=300)
                if text and text.strip():
                    texts.append(text.strip())
            except Exception:
                pass
        return " ".join(texts)
    except Exception:
        return ""


async def detect_result_text(page) -> str:
    """✅ KEEP: Detect result text song song — lọc Next.js noise."""

    # ── Bước 1: Ưu tiên SweetAlert2 (QQ88/NEW88 dùng nhiều) ──────────────────
    PRIORITY_SELECTORS = [
        ".swal2-html-container",
        ".swal2-title",
        ".swal2-popup",
        # NEW88 inline notice
        ".text-red-600",
        ".text-green-600",
        "p.mt-1.text-sm",
        # QQ88 rounded popup
        "div[class*='rounded-2xl'] p",
        "div[class*='rounded-xl'] p",
        "div[class*='rounded-lg'] p",
    ]
    for sel in PRIORITY_SELECTORS:
        try:
            txt = await _fetch_element_text(page, sel)
            if txt and len(txt.strip()) >= 3:
                clean = _filter_nextjs_noise(txt.strip())
                if clean:
                    return clean
        except Exception:
            pass

    # ── Bước 2: Các selector chung, thu thập song song ───────────────────────
    result_selectors = [
        "[role='dialog']",
        "[role='alert']",
        "[role='status']",
        ".modal-body",
        ".modal-content",
        ".popup-content",
        ".alert",
        "[class*='success']",
        "[class*='error']",
        "[class*='toast']",
        "[class*='result']",
        "[class*='notify']",
        "[class*='modal']",
        "[class*='popup']",
        "[class*='notification']",
        "div[style*='position: fixed']",
    ]

    tasks = [_fetch_element_text(page, sel) for sel in result_selectors]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    combined = ""
    for r in results:
        if isinstance(r, str) and r.strip():
            filtered = _filter_nextjs_noise(r.strip())
            if filtered:
                combined += filtered + " "

    if len(combined.strip()) >= 3:
        return combined.strip()

    # ── Bước 3: Fallback JS — chỉ lấy text node có keyword kết quả ──────────
    try:
        page_text = await page.evaluate("""
            () => {
                const keywords = [
                    'thành công', 'thanh cong', 'thất bại', 'that bai',
                    'sai', 'lỗi', 'loi', 'đã sử dụng', 'da su dung',
                    'success', 'failed', 'error', 'invalid', 'used',
                    'không hợp lệ', 'khong hop le', 'hết hạn', 'het han'
                ];
                // ❌ Bỏ qua script/style và Next.js noise
                const noisePatterns = [
                    '__next_f', '__NEXT', 'self.__next',
                    'push([', 'stylesheet', '"link"', '"href"',
                    'webpack', 'hydrat'
                ];
                const walker = document.createTreeWalker(
                    document.body, NodeFilter.SHOW_TEXT, null, false
                );
                let node;
                while (node = walker.nextNode()) {
                    const parent = node.parentElement;
                    if (!parent) continue;
                    const tag = parent.tagName || '';
                    if (['SCRIPT','STYLE','NOSCRIPT'].includes(tag)) continue;
                    const txt = (node.textContent || '').trim();
                    if (txt.length < 3) continue;
                    // Loại bỏ Next.js payload
                    if (noisePatterns.some(p => txt.includes(p))) continue;
                    const lower = txt.toLowerCase();
                    if (keywords.some(k => lower.includes(k))) {
                        return txt;
                    }
                }
                return '';
            }
        """)
        if page_text:
            clean = _filter_nextjs_noise(page_text)
            if clean:
                return clean
    except Exception:
        pass

    return ""


def _filter_nextjs_noise(text: str) -> str:
    """Lọc bỏ text là Next.js/webpack payload, trả về chuỗi rỗng nếu là noise."""
    if not text:
        return ""
    noise_markers = [
        "__next_f", "__NEXT", "self.__next",
        'push([1,"', '"stylesheet"', '"link"',
        "webpack", "hydrat", "\"rel\":", "\"href\":",
        ":[[[\"$\"",
    ]
    t = text.strip()
    for marker in noise_markers:
        if marker in t:
            return ""
    # Nếu text bắt đầu bằng dấu hiệu JSON/JS payload
    if t.startswith(('{"', '[["', '[[["', 'self.')):
        return ""
    return t



async def minimize_edge_window():
    """Thu nhỏ cửa sổ Edge xuống taskbar."""
    try:
        import subprocess
        ps = (
            "$p = Get-Process msedge -ErrorAction SilentlyContinue | "
            "Where-Object {$_.MainWindowTitle} | Select-Object -First 1; "
            "if ($p) { "
            "$t = Add-Type -PassThru -Name WinAPI -Namespace U "
            "-MemberDefinition '[DllImport(\"user32.dll\")]public static extern bool ShowWindow(IntPtr h,int n);'; "
            "$t::ShowWindow($p.MainWindowHandle, 6) }"
        )
        subprocess.Popen(["powershell", "-WindowStyle", "Hidden", "-Command", ps], shell=False)
    except Exception:
        pass


async def bring_edge_to_front():
    """Kéo cửa sổ Edge lên foreground."""
    try:
        import subprocess
        ps = (
            "$p = Get-Process msedge -ErrorAction SilentlyContinue | "
            "Where-Object {$_.MainWindowTitle} | Select-Object -First 1; "
            "if ($p) { "
            "$t = Add-Type -PassThru -Name WinFG -Namespace U "
            "-MemberDefinition '[DllImport(\"user32.dll\")]public static extern bool SetForegroundWindow(IntPtr h);'; "
            "$t::SetForegroundWindow($p.MainWindowHandle) }"
        )
        subprocess.Popen(["powershell", "-WindowStyle", "Hidden", "-Command", ps], shell=False)
    except Exception:
        pass


async def submit_code_safe(user: str, code: str, target_url: str, systems: dict):
    start_time = time.time()
    db = systems["db"]
    perf_mon = systems["performance_monitor"]
    domain = normalize_domain(target_url)
    key = domain

    if key not in bot_state.context_locks:
        logger.warning(f"⏭️ [{user} | {domain}] Chưa có tab")
        append_code_history(
            event_type="SKIPPED",
            code=code,
            target_url=target_url,
            account=user,
            status="SKIPPED",
            message="Chưa có tab được gán",
        )
        return {"success": False, "message": "Chưa có tab"}

    try:
        async with bot_state.context_locks[key]:
            page = bot_state.account_pages.get(key)

            if not page:
                logger.warning(f"⏭️ [{user} | {domain}] Không tìm thấy page")
                return {"success": False, "message": "Không tìm thấy page"}

            # ✅ FIX: Cache page_url (không CDP call mỗi lần)
            try:
                if key not in bot_state._page_urls:
                    bot_state._page_urls[key] = page.url
                page_url = bot_state._page_urls[key]
                page_ok = bool(page_url) and page_url != "about:blank"
            except Exception:
                page_ok = False
            
            if not page_ok:
                logger.warning(f"🔄 [{domain}] Tab lỗi, reload...")
                try:
                    await page.goto(target_url, wait_until="domcontentloaded", timeout=5000)
                    await asyncio.sleep(0.5)
                    _invalidate_input_cache(key)
                    bot_state._page_urls[key] = page.url
                    logger.info(f"✅ [{domain}] Reload thành công")
                except Exception as reload_err:
                    logger.error(f"❌ [{domain}] Reload thất bại")
                    append_code_history(
                        event_type="ERROR",
                        code=code,
                        target_url=target_url,
                        account=user,
                        status="ERROR",
                        message=f"Tab chết, reload thất bại",
                    )
                    return {"success": False, "message": "Reload thất bại"}

            await _wake_tab_for_submit(page)

            _invalidate_input_cache(key)
            username_input, code_input = await find_input_fields(page, cache_key=key)

            if not code_input:
                _invalidate_input_cache(key)
                username_input, code_input = await find_input_fields(page, cache_key=key)

            if not code_input:
                logger.warning(f"❌ [{user} | {domain}] Không tìm thấy ô code")
                append_code_history(
                    event_type="ERROR",
                    code=code,
                    target_url=target_url,
                    account=user,
                    status="ERROR",
                    message="Không tìm thấy ô code",
                )
                return {"success": False, "message": "Không tìm thấy ô code"}

            # ✅ FIX REACT: dùng native setter (Object.getOwnPropertyDescriptor) để
            # React nhận đúng giá trị mới -> state cập nhật -> nút submit hết "disabled"
            REACT_FILL_JS = """
                ([el, val]) => {
                    const proto = el.tagName === 'TEXTAREA'
                        ? window.HTMLTextAreaElement.prototype
                        : window.HTMLInputElement.prototype;
                    const setter = Object.getOwnPropertyDescriptor(proto, 'value').set;
                    el.focus();
                    setter.call(el, '');
                    setter.call(el, val);
                    el.dispatchEvent(new Event('input', {bubbles: true}));
                    el.dispatchEvent(new Event('change', {bubbles: true}));
                }
            """

            try:
                if username_input:
                    await page.evaluate(REACT_FILL_JS, [username_input, user])
                await page.evaluate(REACT_FILL_JS, [code_input, code])
            except Exception as e:
                _invalidate_input_cache(key)
                logger.warning(f"❌ [{user} | {domain}] Lỗi nhập form: {e}")
                try:
                    username_input, code_input = await find_input_fields(page, cache_key=key)
                    if code_input:
                        if username_input:
                            await page.evaluate(REACT_FILL_JS, [username_input, user])
                        await page.evaluate(REACT_FILL_JS, [code_input, code])
                    else:
                        raise RuntimeError("Không tìm thấy ô code sau retry")
                except Exception as e2:
                    logger.warning(f"❌ [{user} | {domain}] Lỗi nhập form: {e2}")
                    return {"success": False, "message": str(e2)}

            # ===== SUBMIT THEO DOMAIN =====
            # ✅ FIX: dùng [aria-label*='...' i] (chứa, không phân biệt hoa/thường)
            # thay vì exact-match -> bền hơn nếu site đổi chữ hoa/khoảng trắng
            submit_sel = SUBMIT_BUTTON_SELECTORS.get(domain)
            try:
                clicked_ok = False
                if submit_sel:
                    # ✅ Poll tối đa 1s chờ React enable nút (bỏ disabled) sau khi nhập
                    clicked_ok = await page.evaluate(f"""
                        async () => {{
                            const deadline = Date.now() + 1000;
                            while (Date.now() < deadline) {{
                                const btn = document.querySelector('{submit_sel}');
                                if (btn && !btn.disabled) {{
                                    btn.click();
                                    return true;
                                }}
                                await new Promise(r => setTimeout(r, 100));
                            }}
                            // Hết thời gian: vẫn thử click nếu tìm thấy (kể cả disabled=false do site không dùng thuộc tính này)
                            const btn = document.querySelector('{submit_sel}');
                            if (btn) {{ btn.click(); return true; }}
                            return false;
                        }}
                    """)

                if not clicked_ok:
                    if submit_sel:
                        logger.warning(f"⚠️ [{user}] Không click được nút submit riêng ({submit_sel}), thử fallback...")
                    await click_submit_fast(page)
            except Exception as e:
                logger.warning(f"⚠️ [{user}] Lỗi click submit: {e}")
                await page.keyboard.press("Enter")

            click_elapsed = time.time() - start_time
            logger.info(f"🚀 [{user}] BẤM NẠP {code} ({click_elapsed:.2f}s)")

            # 🔼 Kéo Edge lên đầu màn hình để dễ thấy
            try:
                await page.bring_to_front()
                await bring_edge_to_front()
                await page.evaluate("() => { document.title = '⏳ Đang nhập code...'; }")
            except Exception:
                pass

            # ===== CHỜ POPUP XÁC THỰC (nếu có) =====
            xacthuc_btn = None
            for _ in range(20):  # tối đa 5 giây
                try:
                    btn = await page.query_selector('button:has-text("Xác thực"), button:has-text("Xac thuc")')
                    if btn:
                        disabled = await btn.get_attribute("disabled")
                        if disabled is None:
                            xacthuc_btn = btn
                            break
                except Exception:
                    pass
                await asyncio.sleep(0.25)

            if xacthuc_btn:
                await xacthuc_btn.click()
                logger.info("✅ Đã click Xác thực")

            # ===== CHỜ KẾT QUẢ (tăng lên 5s, poll 150ms) =====
            result_text = ""
            poll_deadline = time.time() + 5.0
            while time.time() < poll_deadline:
                result_text = await detect_result_text(page)
                if result_text.strip():
                    break
                await asyncio.sleep(0.15)

            elapsed = time.time() - start_time
            result_upper = result_text.upper()

            success_keywords = ["THÀNH CÔNG", "SUCCESS", "CỘNG", "OK"]
            failed_keywords = [
                "SAI", "LỖI", "ĐÃ SỬ", "FAILED", "ERROR",
                # ✅ NEW88: "Mã CODE không đúng hoặc không tồn tại..."
                "KHÔNG ĐÚNG", "KHÔNG TỒN TẠI", "KHÔNG HỢP LỆ",
                "HẾT HẠN", "ĐÃ HẾT", "INVALID", "NOT FOUND", "NOT EXIST",
                "KHÔNG TÌM THẤY",
            ]
            point_keywords = ["ĐIỂM", "XU", "COIN", "POINT"]

            is_success = any(keyword in result_upper for keyword in success_keywords)
            is_failed = any(keyword in result_upper for keyword in failed_keywords)
            has_points = any(keyword in result_upper for keyword in point_keywords)

            if is_success and not is_failed:
                logger.info(f"✅ [{user}] THÀNH CÔNG ({elapsed:.2f}s)")
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, db.record_submission, code, user, target_url, "SUCCESS", result_text[:100])
                bot_state.submission_count[key] += 1
                perf_mon.record_task("submit_code", elapsed, True)
                append_code_history(
                    event_type="RESULT",
                    code=code,
                    target_url=target_url,
                    account=user,
                    status="SUCCESS",
                    submit_elapsed=elapsed,
                    message=result_text[:100],
                )
                try:
                    await page.evaluate(f"() => {{ document.title = '✅ {user} - Thành công'; }}")
                    await asyncio.sleep(1.5)
                    await minimize_edge_window()
                except Exception:
                    pass
                return {"success": True, "has_points": has_points, "message": result_text[:100]}

            if len(result_text.strip()) < 5:
                screenshot = await take_result_screenshot(page, user, code, target_url, "UNKNOWN")
                logger.warning(f"⚠️ [{user}] KHÔNG THẤY KẾT QUẢ")
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, db.record_submission, code, user, target_url, "UNKNOWN", "Không thấy popup")
                perf_mon.record_task("submit_code", elapsed, False)
                append_code_history(
                    event_type="RESULT",
                    code=code,
                    target_url=target_url,
                    account=user,
                    status="UNKNOWN",
                    submit_elapsed=elapsed,
                    message="Không thấy popup",
                    screenshot=screenshot,
                )
                return {"success": False, "message": "Không thấy popup"}

            screenshot = await take_result_screenshot(page, user, code, target_url, "FAILED")
            logger.warning(f"❌ [{user}] THẤT BẠI - {result_text[:80]}")
            try:
                await page.evaluate(f"() => {{ document.title = '❌ {user} - Thất bại'; }}")
                await asyncio.sleep(1.5)
                await minimize_edge_window()
            except Exception:
                pass
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, db.record_submission, code, user, target_url, "FAILED", result_text[:100])
            perf_mon.record_task("submit_code", elapsed, False)
            append_code_history(
                event_type="RESULT",
                code=code,
                target_url=target_url,
                account=user,
                status="FAILED",
                submit_elapsed=elapsed,
                message=result_text[:100],
                screenshot=screenshot,
            )
            return {"success": False, "message": result_text[:100]}

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"❌ [{user}] Lỗi submit: {e}")
        perf_mon.record_task("submit_code", elapsed, False)
        append_code_history(
            event_type="ERROR",
            code=code,
            target_url=target_url,
            account=user,
            status="ERROR",
            submit_elapsed=elapsed,
            message=str(e),
        )
        return {"success": False, "message": str(e)}


# ============================================================
# ✅ SEQUENTIAL MODE - XỬ LÝ TUẦN TỰ
# ============================================================

async def _submit_sequential_for_channel(
    codes: list,
    available_accounts: list,
    target_url: str,
    channel_name: str,
    domain: str,
):
    """
    Nhập code TUẦN TỰ từng tài khoản trên 1 tab duy nhất.

    Logic:
    - Lấy ngẫu nhiên 1 code từ danh sách
    - Nhập lần lượt từng acc trên cùng 1 tab (fill username mới mỗi lần)
    - Thành công + CÓ ĐIỂM  → tiếp tục sang acc kế tiếp
    - Thành công + KHÔNG ĐIỂM → dừng (code đã dùng rồi, acc sau cũng vô ích)
    - Thất bại               → thử acc tiếp (code có thể chưa nhập được)
    - Hết acc                → dừng
    """
    if not codes:
        logger.warning(f"⚠️ [{domain}] Không có code")
        return

    selected_code = select_random_code(codes)
    logger.info(f"🎲 [{domain}] Code được chọn: {selected_code} (từ {len(codes)} code)")

    total = len(available_accounts)
    for idx, account in enumerate(available_accounts):
        user = account["username"]
        is_last = (idx == total - 1)

        logger.info(f"🔄 [{domain}] [{idx+1}/{total}] Nhập cho: {user}")

        result = await submit_code_limited(user, selected_code, target_url, _systems)

        success = result.get("success", False) if result else False
        has_points = result.get("has_points", False) if result else False
        msg = result.get("message", "") if result else "Không có kết quả"

        # ✅ Thành công + có điểm → tiếp tục acc kế
        if success and has_points:
            if is_last:
                logger.info(f"✅ [{domain}] [{user}] THÀNH CÔNG + ĐIỂM. Hết acc.")
                return
            logger.info(f"✅ [{domain}] [{user}] THÀNH CÔNG + ĐIỂM → sang acc tiếp")
            continue

        # ⚠️ Thành công nhưng không có điểm → code đã dùng, dừng luôn
        if success and not has_points:
            logger.warning(
                f"⚠️ [{domain}] [{user}] Thành công nhưng không có điểm → dừng\n   📝 {msg[:80]}"
            )
            return

        # ❌ Thất bại → thử acc tiếp (không dừng)
        if is_last:
            logger.warning(f"❌ [{domain}] [{user}] Thất bại. Hết acc, dừng hẳn.")
        else:
            logger.warning(
                f"❌ [{domain}] [{user}] Thất bại → thử acc tiếp\n   📝 {msg[:80]}"
            )


async def process_image_from_telegram(event, channel_config: dict, systems: dict):
    """
    🖼️ Xử lý ảnh chứa code từ Telegram

    Quy trình:
    1. Download ảnh từ Telegram
    2. Dùng OCR (Tesseract) để trích text từ ảnh
    3. Parse/validate code từ text OCR
    4. Trả về list codes hợp lệ
    """
    import tempfile
    import os
    import shutil

    target_url = channel_config.get("url", "")
    channel_name = channel_config.get("name", "Unknown")

    try:
        logger.info("📸 [OCR] Phát hiện ảnh, đang xử lý...")

        temp_dir = tempfile.mkdtemp(prefix="ocr_telegram_", suffix="_temp")
        logger.debug(f"📂 Thư mục tạm: {temp_dir}")

        try:
            # ============ DOWNLOAD ẢNH ============
            image_path = await event.download_media(file=temp_dir)

            if not image_path:
                logger.warning("❌ [OCR] Không thể download ảnh từ Telegram")
                return {
                    'success': False,
                    'codes': [],
                    'message': 'Không thể download ảnh',
                    'text': ''
                }

            logger.info(f"✅ [OCR] Ảnh downloaded: {image_path}")

            # ============ KIỂM TRA OCR EXTRACTOR ============
            extractor = get_image_extractor()

            if extractor is None:
                logger.error("❌ [OCR] Image extractor chưa sẵn sàng")
                return {
                    'success': False,
                    'codes': [],
                    'message': 'OCR chưa sẵn sàng (cài Tesseract)',
                    'text': ''
                }

            # ============ OCR - TRÍCH TEXT TỪ ẢNH ============
            logger.info("🔍 [OCR] Đang trích text từ ảnh...")
            # Thử eng trước (code thường là ASCII), nếu trống thử vie+eng
            extracted_text = extractor.extract_code_from_image(image_path, lang="eng")
            if not extracted_text:
                logger.info("🔍 [OCR] Thử lại với lang=vie+eng...")
                extracted_text = extractor.extract_code_from_image(image_path, lang="vie+eng")

            if not extracted_text:
                logger.warning("⚠️ [OCR] Ảnh không chứa text nhận diện được")
                return {
                    'success': False,
                    'codes': [],
                    'message': 'Ảnh không chứa text',
                    'text': ''
                }

            logger.info(f"✅ [OCR] Trích được text: {len(extracted_text)} ký tự")
            logger.debug(f"📄 [OCR] Nội dung: {extracted_text[:150]}...")

            # ============ PARSE CODES TỪ TEXT ============
            logger.info("📋 [OCR] Đang parse codes từ text...")

            raw_lines = extracted_text.split('\n')
            extracted_codes = []
            skipped = 0

            for line_idx, line in enumerate(raw_lines, 1):
                line = line.strip()

                if not line:
                    continue

                if len(line) < 4:
                    logger.debug(f"   [Dòng {line_idx}] Bỏ qua (quá ngắn): {line}")
                    skipped += 1
                    continue

                clean_code = CodeValidator.clean_code(line)

                if not clean_code or len(clean_code) < 4:
                    logger.debug(f"   [Dòng {line_idx}] Bỏ qua sau clean: {line}")
                    skipped += 1
                    continue

                try:
                    validation = CodeValidator.validate_code(
                        clean_code,
                        target_url=target_url,
                        source="image_ocr"
                    )
                except TypeError:
                    validation = CodeValidator.validate_code(clean_code, target_url)

                if validation['valid']:
                    code_obj = {
                        'code': clean_code,
                        'raw': line,
                        'confidence': 0.9,
                        'reason': validation.get('reason', ''),
                        'line': line_idx
                    }
                    extracted_codes.append(code_obj)
                    logger.info(
                        f"   ✅ [Dòng {line_idx}] CODE HỢP LỆ: {clean_code} "
                        f"(raw: {line[:20]}...)"
                    )
                else:
                    logger.debug(
                        f"   ❌ [Dòng {line_idx}] Bỏ: {clean_code} - {validation.get('reason', '?')}"
                    )
                    skipped += 1

            logger.info(
                f"📊 [OCR] Kết quả: {len(extracted_codes)} code(s) hợp lệ, "
                f"{skipped} dòng bỏ qua"
            )

            if extracted_codes:
                logger.info(f"🎯 [OCR] Codes tìm được: {[c['code'] for c in extracted_codes]}")
                return {
                    'success': True,
                    'codes': extracted_codes,
                    'message': f'✅ OCR thành công: {len(extracted_codes)} code(s)',
                    'text': extracted_text
                }
            else:
                logger.warning("⚠️ [OCR] Không tìm thấy code hợp lệ trong ảnh")
                return {
                    'success': False,
                    'codes': [],
                    'message': '⚠️ Không tìm code hợp lệ',
                    'text': extracted_text
                }

        finally:
            try:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                    logger.debug(f"🧹 [OCR] Xóa thư mục tạm: {temp_dir}")
            except Exception as cleanup_err:
                logger.debug(f"⚠️ [OCR] Lỗi xóa thư mục tạm: {cleanup_err}")

    except Exception as e:
        logger.error(f"❌ [OCR] Lỗi xử lý ảnh: {e}")
        import traceback
        logger.debug(f"   Stack: {traceback.format_exc()}")

        return {
            'success': False,
            'codes': [],
            'message': f'❌ Lỗi OCR: {str(e)}',
            'text': ''
        }


async def submit_codes_from_image(
    user: str,
    codes_data: list,
    target_url: str,
    channel_config: dict,
    systems: dict
):
    """
    📤 Submit nhiều codes từ ảnh OCR vào website

    Quy trình:
    1. Lặp qua từng code
    2. Mỗi code gọi submit_code_safe()
    3. Ghi log kết quả
    4. Delay giữa các submit
    """

    if not codes_data:
        logger.warning("⚠️ [SUBMIT_IMAGE] Không có code để submit")
        return

    channel_name = channel_config.get("name", "Unknown")
    domain = normalize_domain(target_url)

    logger.info(
        f"📤 [SUBMIT_IMAGE] Bắt đầu submit {len(codes_data)} code(s) từ ảnh\n"
        f"   👤 Account: {user}\n"
        f"   🌐 Domain: {domain}\n"
        f"   📡 Channel: {channel_name}"
    )

    submitted_count = 0
    success_count = 0
    failed_count = 0

    for idx, code_item in enumerate(codes_data, 1):
        code = code_item.get('code', '').strip()
        raw = code_item.get('raw', '')

        if not code:
            logger.warning(f"   [OCR #{idx}] Bỏ qua (code trống)")
            continue

        try:
            logger.info(
                f"\n   📮 [OCR #{idx}/{len(codes_data)}] Nhập code\n"
                f"      Code: {code}\n"
                f"      Raw: {raw}\n"
                f"      Target: {target_url}"
            )

            try:
                result = await asyncio.wait_for(
                    submit_code_safe(user, code, target_url, systems),
                    timeout=30.0
                )
            except asyncio.TimeoutError:
                logger.warning(f"⏰ [{user}] OCR submit timeout 30s - bỏ qua")
                result = {"success": False, "message": "Timeout 30s"}

            submitted_count += 1

            if result and result.get('success'):
                success_count += 1
                logger.info(
                    f"      ✅ THÀNH CÔNG\n"
                    f"      📝 {result.get('message', '')[:80]}"
                )
            else:
                failed_count += 1
                error_msg = result.get('message', 'Unknown error') if result else 'Submit lỗi'
                logger.warning(
                    f"      ❌ THẤT BẠI\n"
                    f"      📝 {error_msg[:80]}"
                )

            if idx < len(codes_data):
                delay = float(getattr(Config, "MIN_DELAY_BETWEEN_SUBMITS", 0.8))
                if delay > 0:
                    logger.debug(f"      ⏳ Chờ {delay}s trước code tiếp...")
                    await asyncio.sleep(delay)

        except asyncio.CancelledError:
            logger.warning(f"   [OCR #{idx}] Bị cancel")
            break

        except Exception as e:
            failed_count += 1
            logger.error(f"   ❌ [OCR #{idx}] Lỗi: {e}")
            continue

    logger.info(
        f"\n✅ [SUBMIT_IMAGE] HOÀN THÀNH\n"
        f"   📊 Tổng: {submitted_count} submit\n"
        f"   ✅ Thành công: {success_count}\n"
        f"   ❌ Thất bại: {failed_count}"
    )


async def process_telegram_message(event):
    if not _systems:
        logger.warning("⚠️ [MSG] _systems chưa sẵn sàng - bỏ qua tin nhắn")
        return

    channel_config = Config.CHANNEL_CONFIG.get(event.chat_id)

    if not channel_config:
        logger.warning(f"⚠️ [MSG] chat_id={event.chat_id} không có trong CHANNEL_CONFIG — bỏ qua")
        logger.debug(f"   Các chat_id hợp lệ: {list(Config.CHANNEL_CONFIG.keys())}")
        return

    target_url = channel_config["url"]
    accounts = channel_config["accounts"]
    raw_text = event.message.text or ""

    logger.info(f"\n👀 Message từ: {channel_config['name']}")

    # 🖼️ Nếu là ảnh (không có text)
    if event.media and not raw_text:
        logger.info("🖼️ [MESSAGE] Phát hiện ảnh - đang OCR...")

        default_account = accounts[0]['username'] if accounts else None

        if not default_account:
            logger.warning("⚠️ [MESSAGE] Không có account để xử lý ảnh")
            return

        ocr_result = await process_image_from_telegram(
            event,
            channel_config=channel_config,
            systems=_systems
        )

        if ocr_result['success']:
            logger.info(f"✅ OCR thành công: {len(ocr_result['codes'])} code(s)")
            await submit_codes_from_image(
                user=default_account,
                codes_data=ocr_result['codes'],
                target_url=target_url,
                channel_config=channel_config,
                systems=_systems
            )
        else:
            logger.warning(f"⚠️ OCR thất bại: {ocr_result['message']}")

        return  # Thoát - không xử lý text


    msg_timestamp = event.message.date
    telegram_delay = measure_telegram_delay_fast(msg_timestamp)
    
    if telegram_delay is not None:
        logger.warning(f"⏱️ Delay: {telegram_delay:.2f}s")

    final_codes = extract_codes_from_message(event, raw_text, target_url)

    if not final_codes:
        logger.info("⏭️ Không có code")
        return

    logger.info(f"📋 Codes: {final_codes}")

    for code in final_codes:
        append_code_history(
            event_type="DETECTED",
            code=code,
            target_url=target_url,
            channel=channel_config.get("name", ""),
            source="telegram",
            status="PENDING",
            telegram_delay=telegram_delay,
        )

    domain = normalize_domain(target_url)

    # ✅ KEEP: Dedup check
    final_codes_after_dedup = []
    for code in final_codes:
        if is_site_code_duplicate(domain, code):
            logger.warning(f"⏭️ [DEDUP] Code {code} đã nạp gần đây")
            append_code_history(
                event_type="SKIPPED",
                code=code,
                target_url=target_url,
                channel=channel_config.get("name", ""),
                status="SKIPPED",
                message="Dedup: code đã nạp trong TTL",
            )
        else:
            final_codes_after_dedup.append(code)

    if not final_codes_after_dedup:
        logger.info("⏭️ Tất cả code bị dedup")
        return

    if domain not in bot_state.account_pages:
        logger.warning(f"⚠️ [{domain}] Chưa có tab — tabs hiện tại: {list(bot_state.account_pages.keys())}")
        return

    available_accounts = sorted(accounts, key=lambda a: a.get("priority", 999))

    if not available_accounts:
        logger.warning("⚠️ Không có tài khoản")
        return

    # ✅ 1 task duy nhất cho tất cả codes
    task = asyncio.create_task(
        _submit_sequential_for_channel(
            codes=final_codes_after_dedup,
            available_accounts=available_accounts,
            target_url=target_url,
            channel_name=channel_config.get("name", ""),
            domain=domain,
        )
    )
    track_submit_task(task, label=f"seq|{domain}|{len(final_codes_after_dedup)}")

    logger.warning(f"⚡ Task nhập {len(final_codes_after_dedup)} code cho '{channel_config['name']}'")


# ============================================================
# ✅ MESSAGE WORKERS (FIX: 12 → 3, queue 500 → 50)
# ============================================================

async def message_worker(worker_id: int):
    global message_queue

    logger.info(f"👷 Worker #{worker_id} khởi động")

    while bot_state.is_running:
        try:
            # ✅ FIX #5: Timeout 1.0s → 0.01s
            try:
                event = await asyncio.wait_for(message_queue.get(), timeout=0.01)
            except asyncio.TimeoutError:
                await asyncio.sleep(0.001)
                continue
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ Worker #{worker_id} lỗi: {e}")
            await asyncio.sleep(0.1)
            continue

        try:
            await process_telegram_message(event)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"❌ Worker #{worker_id} xử lý lỗi: {e}")
        finally:
            try:
                message_queue.task_done()
            except Exception:
                pass


def start_message_workers():
    global message_queue, message_workers

    if message_queue is None:
        # ✅ FIX #3: Queue size 500 → 50
        message_queue = asyncio.Queue(maxsize=50)

    if message_workers:
        return

    # ✅ FIX #4: Workers 12 → 3
    worker_count = 3

    for worker_id in range(1, worker_count + 1):
        message_workers.append(asyncio.create_task(message_worker(worker_id)))

    logger.info(f"🚀 Message queue: maxsize=50, workers=3")


async def setup_telegram_handler():
    if bot_state.handler_registered:
        return

    channel_ids = list(Config.CHANNEL_CONFIG.keys())
    start_message_workers()

    @client.on(events.NewMessage(chats=channel_ids))
    async def handler(event):
        # ✅ FIX #1 & #2: Non-blocking handler (< 1ms, không log)
        try:
            msg_timestamp = event.message.date
            received_at = time.perf_counter()
            
            event._handler_received_at = received_at
            event._msg_timestamp = msg_timestamp
            
            try:
                # ✅ FIX #9: Không log khi queue full (block handler)
                message_queue.put_nowait(event)
            except asyncio.QueueFull:
                pass  # Bỏ silently, không log
                
        except Exception:
            pass  # Bỏ silently
    
    bot_state.handler_registered = True
    logger.info("✅ Telegram handler setup xong!")


# ============================================================
# ✅ WATCHDOG - TỰ ĐIỀN USERNAME (KEEP)
# ============================================================

async def auto_fill_usernames_watchdog():
    """✅ KEEP: Watchdog tự động điền username khi bị xóa."""
    last_filled_time = {}

    while bot_state.is_running:
        await asyncio.sleep(10)  # Check mỗi 10 giây
        if not bot_state.account_pages:
            continue

        for domain_key, page in list(bot_state.account_pages.items()):
            try:
                if page.is_closed():
                    last_filled_time.pop(domain_key, None)
                    continue

                username_input, _ = await find_input_fields(page, cache_key=None)
                if not username_input:
                    continue

                current_value = await get_input_value(username_input)

                if current_value.strip():
                    last_filled_time.pop(domain_key, None)
                    continue

                # Ô trống → điền lại
                now = time.time()
                last_filled = last_filled_time.get(domain_key)
                if last_filled and (now - last_filled) < 300:  # 5 phút
                    continue

                default_user = get_default_account_for_domain(domain_key)
                if not default_user:
                    continue

                await page.evaluate(
                    "([el, val]) => { el.value = val; el.dispatchEvent(new Event('input', {bubbles:true})); el.dispatchEvent(new Event('change', {bubbles:true})); }",
                    [username_input, default_user]
                )
                logger.info(f"🔄 [{domain_key}] Tự động điền username '{default_user}'")
                last_filled_time[domain_key] = now

            except Exception as e:
                logger.debug(f"⚠️ Watchdog username error: {e}")


# ============================================================
# ✅ CLOUDFLARE WATCHDOG (KEEP)
# ============================================================

async def cloudflare_watchdog():
    """✅ KEEP: Phát hiện Cloudflare challenge + đưa tab lên."""
    CF_DETECT_SELECTORS = [
        "iframe[src*='challenges.cloudflare.com']",
        "iframe[src*='turnstile']",
        ".cf-turnstile",
        "[data-sitekey]",
    ]

    while bot_state.is_running:
        try:
            interval = random.uniform(300.0, 600.0)  # 5-10 phút
            await asyncio.sleep(interval)
            if not bot_state.account_pages:
                continue

            for key, page in list(bot_state.account_pages.items()):
                try:
                    current_url = page.url
                except Exception:
                    continue

                try:
                    cf_found = (
                        "challenges.cloudflare.com" in current_url
                        or "/cdn-cgi/challenge-platform" in current_url
                    )
                    if not cf_found:
                        for sel in CF_DETECT_SELECTORS:
                            try:
                                el = await page.query_selector(sel)
                                if el and await el.is_visible():
                                    cf_found = True
                                    break
                            except Exception:
                                continue

                    if cf_found:
                        logger.warning(f"⚠️ Cloudflare [{key}] — hãy click xác minh!")
                        try:
                            await page.bring_to_front()
                        except Exception:
                            pass
                except Exception:
                    continue
        except asyncio.CancelledError:
            break
        except Exception:
            pass


async def cleanup_browsers():
    for port, browser in bot_state.connected_browsers.items():
        try:
            await browser.close()
            logger.info(f"✅ Ngắt CDP port {port}")
        except Exception:
            pass


async def main():
    global _systems

    try:
        logger.info("🚀 BOT SEQUENTIAL v7.1 (BALANCED - SPEED + FEATURES) | "
                    "Auto-start | Polling result | Parallel tabs")

        _systems = await init_systems()

        logger.info("\n📨 Kết nối Telegram...")
        try:
            await client.start(catch_up=False)
        except TypeError:
            await client.start()

        if not await verify_telegram_session():
            return

        valid_channels = await verify_channels_and_get_ids()

        if not valid_channels:
            logger.error("❌ Không có channel hợp lệ")
            return

        await setup_telegram_handler()

        await preload_browsers_and_accounts()

        logger.info("✅ Tabs đã sẵn sàng. BOT READY!\n")

        async def heartbeat_loop():
            interval = 300.0
            while bot_state.is_running:
                try:
                    await asyncio.sleep(interval)
                    pages = len(bot_state.account_pages)
                    tasks = len(_active_submit_tasks)
                    logger.info(f"💓 Bot chạy | tabs={pages} | tasks={tasks} | tg={client.is_connected()}")
                except Exception:
                    pass

        asyncio.create_task(heartbeat_loop())
        
        # ✅ KEEP: Watchdog tasks
        asyncio.create_task(auto_fill_usernames_watchdog())
        asyncio.create_task(cloudflare_watchdog())

        _reconnect_delay = 5.0
        _reconnect_backoff = 1.0
        while bot_state.is_running:
            try:
                if not client.is_connected():
                    logger.warning("🔄 Reconnect...")
                    await client.connect()
                await asyncio.wait_for(client.run_until_disconnected(), timeout=600.0)
                break
            except asyncio.TimeoutError:
                logger.warning("⚠️ Telegram timeout, reconnect...")
                await client.connect()
            except (ConnectionError, OSError):
                wait = min(_reconnect_delay * _reconnect_backoff, 60.0)
                logger.warning(f"⚠️ Reconnect sau {wait:.0f}s...")
                await asyncio.sleep(wait)
                _reconnect_backoff = min(_reconnect_backoff * 2, 12)
            except Exception as e:
                logger.error(f"❌ Error: {e}")
                await asyncio.sleep(_reconnect_delay)

    except Exception as e:
        logger.critical(f"❌ Critical: {e}")

    finally:
        logger.info("\n🛑 Dừng bot...")
        bot_state.is_running = False

        if _history_queue is not None:
            try:
                await asyncio.wait_for(_history_queue.join(), timeout=5.0)
            except Exception:
                pass
            if _history_writer_task:
                _history_writer_task.cancel()

        if _active_submit_tasks:
            logger.info(f"⏳ Chờ {len(_active_submit_tasks)} tasks...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*list(_active_submit_tasks), return_exceptions=True),
                    timeout=8.0,
                )
            except Exception:
                for t in list(_active_submit_tasks):
                    t.cancel()

        for worker in message_workers:
            worker.cancel()

        await cleanup_browsers()

        build_daily_summary()

        if bot_state.playwright_instance:
            await bot_state.playwright_instance.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Bot dừng")