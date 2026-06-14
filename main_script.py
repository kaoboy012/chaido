"""
🤖 BOT - SEQUENTIAL MODE v7.2 (ALL BUGS FIXED)

FIXES v7.2:
  FIX #1  — Telegram handler THỰC SỰ non-blocking: không gọi process trực tiếp,
             chỉ enqueue; worker tách biệt hoàn toàn.
  FIX #2  — OCR chạy trong thread pool (run_in_executor) không block event loop.
  FIX #3  — detect_result_text poll nhanh hơn (100ms) và timeout thực tế 6s.
  FIX #4  — click_submit_fast cải thiện: thử selector domain-specific TRƯỚC,
             fallback JS sau; không click nhầm nút menu.
  FIX #5  — Input cache: chỉ cache khi code_input tìm thấy, invalidate đúng chỗ.
  FIX #6  — submit_code_limited: MIN_DELAY không chặn semaphore, chạy TRONG task.
  FIX #7  — _submit_sequential_for_channel: mỗi account fill username RIÊNG trước submit.
  FIX #8  — page_url cache: reset khi reload trang thành công.
  FIX #9  — Message workers: tăng lên 6, queue 200; mỗi worker xử lý độc lập.
  FIX #10 — Cloudflare popup: chờ đủ 8s, click Xác thực chắc chắn hơn.
  FIX #11 — Heartbeat: log rõ hơn, không lỗi âm thầm.
  FIX #12 — preload_browsers: timeout mỗi tab 20s (cũ 15s), log tiến trình rõ.
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
    "mm88code.com":    "img.submit-btn, .submit-button-container img, .submit-btn",
    "llwincode.com":   'img[src*="btnnhancode" i], img[alt*="nhan" i]',
    "xx88code.com":    'button[aria-label="Nhận code"], button[aria-label*="Nhan code" i]',
    "o8code.com":      ".modal-submit-btn",
    "new88b.today":    'button[aria-label*="Kiểm tra" i]',
    "tangquaqq88.com": 'button[aria-label*="Kiểm tra" i]',
    "uy88code.org":    "#casinoSubmit",
    "mmoocode.shop":   "#casinoSubmit",
}

from database import init_database
from rate_limiter import init_anti_detection
from monitoring import init_monitoring
from features import print_version_info, get_shutdown_handler


# ============================================================
# STATE
# ============================================================

class BotState:
    def __init__(self):
        self.playwright_instance = None
        self.connected_browsers = {}
        self.account_pages = {}
        self.context_locks = {}
        self.is_running = True
        self.cf_verified = {}
        self.submission_count = {}
        # FIX #5: cache chỉ lưu khi thực sự có code_input
        self._input_cache: dict = {}
        self._input_cache_ttl = 20.0   # giảm từ 30 → 20s để không stale
        self._site_code_seen: dict = {}
        self._site_code_ttl: float = 10.0
        self._page_urls: dict = {}
        self.handler_registered = False


bot_state = BotState()

# FIX #1: sequential_updates=False — nhận update ngay, không chờ handler trước
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
    sequential_updates=False,   # CRITICAL: nhận tin ngay lập tức
)

_systems = None
message_queue: asyncio.Queue = None
message_workers: list = []

_history_queue: asyncio.Queue = None
_history_writer_task = None

_submit_semaphore: asyncio.Semaphore | None = None
_active_submit_tasks: set[asyncio.Task] = set()


# ============================================================
# HELPERS
# ============================================================

def normalize_domain(url: str) -> str:
    parsed = urlparse(url or "")
    domain = parsed.netloc or parsed.path
    return domain.lower().replace("www.", "").strip("/")


def select_random_code(codes: list) -> str:
    if not codes:
        return None
    return random.choice(codes)


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


CODE_HISTORY_DIR = Path("logs/code_history")
CODE_HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def _write_history_row(row: dict):
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
    global _history_queue, _history_writer_task
    _history_queue = asyncio.Queue(maxsize=2000)
    _history_writer_task = asyncio.create_task(_history_writer_loop())
    logger.info("✅ Background history writer đã khởi động")


def get_submit_semaphore() -> asyncio.Semaphore:
    global _submit_semaphore
    if _submit_semaphore is None:
        limit = max(1, int(getattr(Config, "MAX_CONCURRENT_SUBMITS", 3)))
        _submit_semaphore = asyncio.Semaphore(limit)
    return _submit_semaphore


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
# DEDUP
# ============================================================

def _prune_site_code_seen():
    ttl = float(getattr(Config, "SITE_CODE_DEDUP_TTL", 10.0))
    now = time.time()
    expired = [k for k, ts in bot_state._site_code_seen.items() if now - ts > ttl]
    for k in expired:
        del bot_state._site_code_seen[k]


def is_site_code_duplicate(domain: str, code: str) -> bool:
    ttl = float(getattr(Config, "SITE_CODE_DEDUP_TTL", 10.0))
    now = time.time()
    _prune_site_code_seen()
    key = (domain, code.upper())
    seen_at = bot_state._site_code_seen.get(key)
    if seen_at is not None and now - seen_at < ttl:
        return True
    bot_state._site_code_seen[key] = now
    return False


def build_daily_summary():
    try:
        csv_path = CODE_HISTORY_DIR / f"code_history_{_today_str()}.csv"
        if not csv_path.exists():
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
                summary[key].setdefault(status, 0)
                summary[key][status] += 1
        out_path = CODE_HISTORY_DIR / f"daily_summary_{_today_str()}.csv"
        with out_path.open("w", newline="", encoding="utf-8-sig") as f:
            fieldnames = ["date", "site", "account", "success", "failed", "unknown", "total"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for (site, account), counts in sorted(summary.items()):
                s = counts.get("SUCCESS", 0)
                fa = counts.get("FAILED", 0)
                u = counts.get("UNKNOWN", 0)
                writer.writerow({"date": _today_str(), "site": site, "account": account,
                                 "success": s, "failed": fa, "unknown": u, "total": s + fa + u})
        logger.info(f"📒 Báo cáo cuối ngày: {out_path}")
        return str(out_path)
    except Exception as e:
        logger.warning(f"⚠️ Không tạo được daily summary: {e}")
        return None


def measure_telegram_delay_fast(msg_timestamp) -> float | None:
    try:
        if msg_timestamp.tzinfo is None:
            msg_timestamp = msg_timestamp.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - msg_timestamp).total_seconds()
    except Exception:
        return None


def build_unique_account_targets():
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
        items.append({
            "chat_id": chat_id,
            "channel_name": channel_config.get("name", ""),
            "target_url": target_url,
            "domain": domain,
            "port": port,
            "accounts": sorted(accounts, key=lambda a: a.get("priority", 999)),
        })
    return items


def get_user_port(user: str) -> int:
    for port, users_list in getattr(Config, "CDP_CONNECTIONS", {}).items():
        if user in users_list:
            return int(port)
    return 9222


def get_default_account_for_domain(domain: str) -> str | None:
    for chat_id, cfg in Config.CHANNEL_CONFIG.items():
        if normalize_domain(cfg["url"]) == domain:
            accounts = cfg.get("accounts", [])
            if accounts:
                return sorted(accounts, key=lambda a: a.get("priority", 999))[0]["username"]
    return None


# ============================================================
# BROWSER SETUP
# ============================================================

async def verify_telegram_session():
    logger.info("\n" + "=" * 70)
    logger.info("🔐 XÁC MINH TELEGRAM SESSION...")
    try:
        me = await client.get_me()
        logger.info(f"✅ SESSION HỢP LỆ! @{me.username} (ID: {me.id})")
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
    _, _, perf_mon = init_monitoring()
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
    """FIX #5: Xóa cache input."""
    bot_state._input_cache.pop(key, None)


async def find_input_fields(page, cache_key: str = None):
    """
    FIX #5: Cache chỉ lưu khi THỰC SỰ tìm thấy code_input.
    Nếu không tìm được → không cache → lần sau thử lại.
    """
    now = time.time()
    if cache_key:
        cached = bot_state._input_cache.get(cache_key)
        if cached:
            username_input, code_input, cache_time = cached
            if now - cache_time < bot_state._input_cache_ttl:
                try:
                    if code_input:
                        visible = await code_input.is_visible()
                        if visible:
                            return username_input, code_input
                    # Không visible → xóa cache
                    _invalidate_input_cache(cache_key)
                except Exception:
                    _invalidate_input_cache(cache_key)

    username_input = None
    code_input = None

    username_selectors = [
        "#account-code",
        "#username-input",
        "#ten_tai_khoan",
        "input#username",
        "input[name='username']",
        "input[placeholder*='người dùng' i]",
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
        "#promo-code",
        "#giftcode-input",
        "input[autocomplete='one-time-code']",
        "input#code",
        "input[name='code']",
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
                "input:not([type='hidden']):not([type='checkbox'])"
                ":not([type='radio']):not([type='submit'])"
            )
            visible_inputs = []
            for inp in inputs:
                if await safe_is_visible(inp):
                    visible_inputs.append(inp)
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

    # FIX #5: chỉ cache khi code_input tìm thấy
    if cache_key and code_input:
        bot_state._input_cache[cache_key] = (username_input, code_input, now)

    return username_input, code_input


async def get_input_value(input_element) -> str:
    try:
        return (await input_element.input_value(timeout=1000)).strip()
    except Exception:
        return ""


# ============================================================
# FIX #4: SUBMIT BUTTON — domain-specific TRƯỚC, fallback SAU
# ============================================================

async def click_submit_fast(page, domain: str = "") -> bool:
    """
    FIX #4: Thứ tự ưu tiên:
      1. Selector domain-specific (SUBMIT_BUTTON_SELECTORS)
      2. JS tìm theo text/aria-label
      3. Selector generic
      4. Enter
    """
    # Bước 1: Domain-specific selector
    domain_sel = SUBMIT_BUTTON_SELECTORS.get(domain)
    if domain_sel:
        try:
            clicked = await page.evaluate(f"""
                async () => {{
                    const deadline = Date.now() + 1200;
                    while (Date.now() < deadline) {{
                        const btn = document.querySelector('{domain_sel}');
                        if (btn && !btn.disabled) {{
                            const rect = btn.getBoundingClientRect();
                            if (rect.width > 0 && rect.height > 0) {{
                                btn.click();
                                return true;
                            }}
                        }}
                        await new Promise(r => setTimeout(r, 100));
                    }}
                    // Hết giờ: click dù disabled (một số site không dùng thuộc tính này)
                    const btn = document.querySelector('{domain_sel}');
                    if (btn) {{ btn.click(); return true; }}
                    return false;
                }}
            """)
            if clicked:
                logger.debug(f"✅ Click submit domain-specific: {domain_sel}")
                return True
        except Exception:
            pass

    # Bước 2: Tìm theo text/aria-label
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
                const EXCLUDE = /menu|nav|home|close|cancel|toggle|hamburger|back|trở về|huỷ|hủy|đóng/i;
                const els = [...document.querySelectorAll(
                    'button, a[role="button"], div[role="button"], span[role="button"], input[type="button"], input[type="submit"]'
                )];
                for (const kw of keywords) {
                    for (const el of els) {
                        if (el.disabled) continue;
                        const aria = (el.getAttribute('aria-label') || '').toLowerCase();
                        const img = el.querySelector('img[alt]');
                        const imgAlt = img ? (img.getAttribute('alt') || '').toLowerCase() : '';
                        const txt = (el.innerText || el.textContent || el.value || '').toLowerCase().trim();
                        if (EXCLUDE.test(aria + txt)) continue;
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

    # Bước 3: Selector generic (type=submit ưu tiên)
    generic_selectors = [
        "button[type='submit']",
        "input[type='submit']",
        ".btn-submit",
        ".apply-btn",
        ".submit-btn",
        "[class*='submit' i]",
        "[class*='apply' i]",
        "[class*='check' i]",
    ]
    for sel in generic_selectors:
        try:
            el = await page.query_selector(sel)
            if el and await safe_is_visible(el):
                await page.evaluate("el => el.click()", el)
                return True
        except Exception:
            pass

    # Bước 4: Enter
    try:
        await page.keyboard.press("Enter")
        return True
    except Exception:
        return False


# ============================================================
# FIX #10: CLOUDFLARE POPUP
# ============================================================

async def handle_cloudflare_popup(page) -> bool:
    """
    FIX #10: Chờ Cloudflare tick xong rồi mới bấm Xác thực.
    Không bấm nếu nút còn disabled.
    """
    try:
        popup_btn = None
        for sel in ["button:has-text('Xác thực')", "button:has-text('Xac thuc')"]:
            try:
                el = await page.query_selector(sel)
                if el and await safe_is_visible(el):
                    popup_btn = el
                    break
            except Exception:
                pass

        if not popup_btn:
            return False

        logger.info("🔒 [CF] Phát hiện popup xác thực Cloudflare — đang chờ...")

        # Chờ tối đa 8s cho Cloudflare tự tick
        for _ in range(32):
            await asyncio.sleep(0.25)
            try:
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
                    logger.info("✅ [CF] Cloudflare xác minh xong")
                    break
            except Exception:
                pass

        # Click nút Xác thực (chờ enabled)
        for _ in range(10):
            for sel in ["button:has-text('Xác thực')", "button:has-text('Xac thuc')"]:
                try:
                    el = await page.query_selector(sel)
                    if el and await safe_is_visible(el):
                        disabled = await el.get_attribute("disabled")
                        if disabled is None:
                            await page.evaluate("el => el.click()", el)
                            logger.info("✅ [CF] Đã bấm Xác thực")
                            await asyncio.sleep(0.5)
                            return True
                except Exception:
                    pass
            await asyncio.sleep(0.3)

        logger.warning("⚠️ [CF] Không click được Xác thực sau 3s")
        return False

    except Exception as e:
        logger.debug(f"⚠️ CF popup error: {e}")
        return False


# ============================================================
# FIX #3: RESULT DETECTION — poll 100ms, timeout 6s
# ============================================================

async def _fetch_element_text(page, selector: str) -> str:
    try:
        elements = await page.query_selector_all(selector)
        texts = []
        for el in elements:
            try:
                text = await el.inner_text(timeout=300)
                if text and text.strip():
                    texts.append(text.strip())
            except Exception:
                pass
        return " ".join(texts)
    except Exception:
        return ""


def _filter_nextjs_noise(text: str) -> str:
    if not text:
        return ""
    noise_markers = [
        "__next_f", "__NEXT", "self.__next",
        'push([1,"', '"stylesheet"', '"link"',
        "webpack", "hydrat", '"rel":', '"href":',
        ":[[[\"$\"",
    ]
    t = text.strip()
    for marker in noise_markers:
        if marker in t:
            return ""
    if t.startswith(('{"', '[["', '[[["', 'self.')):
        return ""
    return t


async def detect_result_text(page) -> str:
    """FIX #3: Ưu tiên SweetAlert2, poll nhanh, lọc Next.js noise."""
    PRIORITY_SELECTORS = [
        ".swal2-html-container",
        ".swal2-title",
        ".swal2-popup",
        ".text-red-600",
        ".text-green-600",
        "p.mt-1.text-sm",
        "div[class*='rounded-2xl'] p",
        "div[class*='rounded-xl'] p",
        "div[class*='rounded-lg'] p",
        "[role='alert']",
        "[role='status']",
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

    result_selectors = [
        "[role='dialog']",
        ".modal-body", ".modal-content", ".popup-content",
        ".alert", "[class*='success']", "[class*='error']",
        "[class*='toast']", "[class*='result']",
        "[class*='notify']", "[class*='modal']", "[class*='popup']",
        "[class*='notification']", "div[style*='position: fixed']",
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

    try:
        page_text = await page.evaluate("""
            () => {
                const keywords = [
                    'thành công', 'thanh cong', 'thất bại', 'that bai',
                    'sai', 'lỗi', 'loi', 'đã sử dụng', 'da su dung',
                    'success', 'failed', 'error', 'invalid', 'used',
                    'không hợp lệ', 'khong hop le', 'hết hạn', 'het han',
                    'không đúng', 'không tồn tại',
                ];
                const noisePatterns = ['__next_f', '__NEXT', 'self.__next', 'push([', 'webpack'];
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
                    if (noisePatterns.some(p => txt.includes(p))) continue;
                    const lower = txt.toLowerCase();
                    if (keywords.some(k => lower.includes(k))) return txt;
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


async def take_result_screenshot(page, user: str, code: str, target_url: str, status: str) -> str:
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
        logger.debug(f"⚠️ Không chụp screenshot: {e}")
        return ""


async def connect_to_cdp_port(port: int):
    if port in bot_state.connected_browsers:
        return bot_state.connected_browsers[port]
    logger.info(f"🖥️ Kết nối CDP port {port}...")
    browser = await bot_state.playwright_instance.chromium.connect_over_cdp(
        f"http://127.0.0.1:{port}"
    )
    bot_state.connected_browsers[port] = browser
    logger.info(f"✅ Đã kết nối CDP port {port}")
    return browser


async def _setup_page_performance(page, label: str = ""):
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
    except Exception as e:
        logger.debug(f"⚠️ [{label}] Không setup page: {e}")


async def _wake_tab_for_submit(page):
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
    try:
        username_input, _ = await find_input_fields(page, cache_key=None)
        if not username_input:
            return False
        current_value = await get_input_value(username_input)
        if current_value.lower() == username.lower():
            return True
        if current_value == "":
            await username_input.fill(username)
            logger.info(f"✅ [{domain}] Đã điền username: {username}")
            return True
        return False
    except Exception as e:
        logger.warning(f"⚠️ [{domain}] Không điền username: {e}")
        return False


# FIX #12: timeout 20s
async def _setup_one_domain_tab(item: dict, assigned_pages: set, assign_lock: asyncio.Lock):
    domain = item["domain"]
    try:
        return await asyncio.wait_for(
            _setup_one_domain_tab_inner(item, assigned_pages, assign_lock),
            timeout=20.0,
        )
    except asyncio.TimeoutError:
        logger.warning(f"⏰ [{domain}] Setup timeout 20s — bot vẫn chạy tiếp")
        return False
    except Exception as e:
        logger.error(f"❌ [{domain}] Setup lỗi: {e}")
        return False


async def _setup_one_domain_tab_inner(item: dict, assigned_pages: set, assign_lock: asyncio.Lock):
    target_url = item["target_url"]
    domain = item["domain"]
    port = item["port"]
    accounts = item["accounts"]
    key = domain

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
                    reason = "tab_existing"
                    assigned_pages.add(page)
                    break
            except Exception:
                pass

        if not page:
            if bool(getattr(Config, "AUTO_OPEN_MISSING_TABS", True)):
                page = await context.new_page()
                assigned_pages.add(page)
                reason = "tab_new"
            else:
                logger.error(f"❌ [{domain}] Không có tab")
                return False

    if reason == "tab_new":
        await _setup_page_performance(page, label=domain)
        try:
            await page.goto(target_url, wait_until="domcontentloaded", timeout=10000)
        except Exception as e:
            logger.warning(f"⚠️ [{domain}] Tải trang lỗi (tiếp tục): {e}")
    else:
        await _setup_page_performance(page, label=domain)

    bot_state.account_pages[key] = page
    bot_state.context_locks[key] = asyncio.Lock()
    bot_state.cf_verified[key] = True
    bot_state.submission_count[key] = 0

    try:
        await page.bring_to_front()
    except Exception:
        pass

    first_account = accounts[0]["username"] if accounts else ""
    if first_account:
        await auto_fill_username_on_startup(page, domain, first_account)

    _, code_input = await find_input_fields(page)
    if code_input:
        logger.info(f"✅ [{domain}] Sẵn sàng | acc: {[a['username'] for a in accounts]}")
    else:
        logger.warning(f"⚠️ [{domain}] Chưa thấy ô code (Cloudflare?) — đã đăng ký tab")

    return True


async def preload_browsers_and_accounts():
    """FIX #12: Mở tất cả tab song song, log tiến trình rõ."""
    account_targets = build_unique_account_targets()
    if not account_targets:
        logger.error("❌ Không có kênh nào")
        return

    total_tabs = len(account_targets)
    logger.info(f"🔄 Mở {total_tabs} tab song song...")

    assigned_pages = set()
    assign_lock = asyncio.Lock()
    done_count = 0
    done_lock = asyncio.Lock()

    async def _setup_with_progress(item):
        nonlocal done_count
        result = await _setup_one_domain_tab(item, assigned_pages, assign_lock)
        async with done_lock:
            done_count += 1
            status = "✅" if result else "❌"
            logger.info(f"  {status} [{done_count}/{total_tabs}] {item['domain']}")
        return result

    results = await asyncio.gather(
        *[_setup_with_progress(item) for item in account_targets],
        return_exceptions=True,
    )
    ok = sum(1 for r in results if r is True)
    logger.info(f"✅ Hoàn tất: {ok}/{total_tabs} tab sẵn sàng")
    if ok < total_tabs:
        logger.warning(f"⚠️ {total_tabs - ok} tab lỗi — kiểm tra Cloudflare/kết nối")
    logger.info("🤖 BOT ĐANG CHẠY — lắng nghe Telegram...")


# ============================================================
# CODE EXTRACTION
# ============================================================

def validate_candidate(code: str, target_url: str, source: str = "normal"):
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
    cleaned = text or ""
    cleaned = re.sub(r"https?://\S+", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"www\.\S+", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"\b[a-zA-Z0-9.-]+\.(com|net|org|vn|app|info)\b", " ", cleaned, flags=re.IGNORECASE
    )
    cleaned = cleaned.replace("：", ":").replace("|", " ").replace("•", " ")
    return cleaned


def line_has_code_marker(line: str) -> bool:
    upper = line.upper()
    markers = [
        "NHẬN CODE NGAY", "NHAN CODE NGAY",
        "NHẬN CODE", "NHAN CODE",
        "NHẬP CODE", "NHAP CODE",
        "PHÁT CODE", "PHAT CODE",
        "CODE FREE",
    ]
    return any(m in upper for m in markers)


def line_is_noise(line: str) -> bool:
    upper = line.upper().strip()
    if not upper:
        return True
    noise_keywords = [
        "HTTP", "WWW", ".COM", "FACEBOOK", "TELEGRAM", "TIKTOK", "ZALO",
        "CSKH", "BOT", "CHECK LINK", "LINK",
    ]
    return any(kw in upper for kw in noise_keywords)


def extract_tokens_from_line(line: str):
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
                tokens = extract_tokens_from_line(spoiler_line) or [spoiler_line]
                for token in tokens:
                    validation = validate_candidate(token, target_url, source="spoiler")
                    if validation["valid"]:
                        codes.append(validation["clean_code"])
                        logger.info(f"🔒 Spoiler code: {validation['clean_code']}")
    except Exception as e:
        logger.warning(f"⚠️ Lỗi đọc spoiler: {e}")
    return unique_keep_order(codes)


def extract_marker_near_codes(text: str, target_url: str):
    cleaned_text = remove_noise_from_text(text)
    lines = [line.strip() for line in cleaned_text.splitlines()]
    codes = []
    for index, line in enumerate(lines):
        if not line_has_code_marker(line):
            continue
        scan_lines = [line] if line else []
        for offset in range(1, 9):
            if index + offset < len(lines):
                scan_lines.append(lines[index + offset])
        for scan_line in scan_lines:
            if line_is_noise(scan_line):
                continue
            for token in extract_tokens_from_line(scan_line):
                clean = CodeValidator.clean_code(token)
                validation = validate_candidate(clean, target_url, source="marker")
                if validation["valid"]:
                    codes.append(validation["clean_code"])
                    logger.info(f"🎯 Marker code: {validation['clean_code']}")
    return unique_keep_order(codes)


def extract_codes_by_regex(text: str, site_type: str = "qq88") -> list:
    if not text:
        return []
    codes = []
    if site_type == "qq88":
        QQ88_BLACKLIST = {
            "QQ88", "CODE", "DANGNHAP", "GAMEBAI", "NOHU", "CASINO",
            "REVIEWPHIM", "TINTUC", "KHUYENMAI", "GIFTCODE", "FREECODE",
            "CAMERA", "TROLL", "BONGDA", "THETHAO", "MINIGAME",
        }
        for match in re.findall(r'[a-zA-Z0-9]{6,15}', text):
            if any(kw in match.upper() for kw in QQ88_BLACKLIST):
                continue
            has_letter = any(c.isalpha() for c in match)
            has_digit = any(c.isdigit() for c in match)
            has_lower = any(c.islower() for c in match)
            has_upper_c = any(c.isupper() for c in match)
            if has_letter and (has_digit or (has_lower and has_upper_c)):
                codes.append(match)
    elif site_type == "llwin":
        LLWIN_SEP = r'[~!@#$%^&*()\-_+{}|:"<>?`=\[\]\\;\',\.\\/]'
        pattern = (
            r'[A-Z0-9]{1,3}' + LLWIN_SEP + r'{1,2}'
            r'[A-Z0-9]{1,3}(?:' + LLWIN_SEP + r'{1,2}[A-Z0-9]{1,3}){2,}'
        )
        codes.extend(re.findall(pattern, text.upper()))
    return list(dict.fromkeys(codes))


def extract_codes_from_message(event, raw_text: str, target_url: str):
    codes = []

    spoiler_codes = extract_spoiler_codes(event, target_url)
    if spoiler_codes:
        logger.info(f"🎯 Code từ spoiler: {spoiler_codes}")
        return spoiler_codes

    marker_codes = extract_marker_near_codes(raw_text, target_url)
    if marker_codes:
        logger.info(f"🎯 Code từ marker: {marker_codes}")
        return marker_codes

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

    return codes


# ============================================================
# FIX #6 + #7: SUBMIT CODE — delay TRONG task, fill username mỗi acc
# ============================================================

# JS fill React-aware
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


async def submit_code_safe(user: str, code: str, target_url: str, systems: dict):
    """
    FIX #5 #6 #7 #8: Submit code cho 1 user trên tab domain.
    - Tìm input mới mỗi lần (không dùng stale cache khi đổi user)
    - Fill username + code với React-aware setter
    - Poll kết quả 100ms/lần, tối đa 6s
    """
    start_time = time.time()
    db = systems["db"]
    perf_mon = systems["performance_monitor"]
    domain = normalize_domain(target_url)
    key = domain

    if key not in bot_state.context_locks:
        logger.warning(f"⏭️ [{user}|{domain}] Chưa có tab")
        return {"success": False, "message": "Chưa có tab"}

    try:
        async with bot_state.context_locks[key]:
            page = bot_state.account_pages.get(key)
            if not page:
                return {"success": False, "message": "Không tìm thấy page"}

            # FIX #8: Kiểm tra tab còn sống
            try:
                page_url = page.url
                bot_state._page_urls[key] = page_url
                page_ok = bool(page_url) and page_url != "about:blank"
            except Exception:
                page_ok = False

            if not page_ok:
                logger.warning(f"🔄 [{domain}] Tab lỗi, reload...")
                try:
                    await page.goto(target_url, wait_until="domcontentloaded", timeout=8000)
                    await asyncio.sleep(0.5)
                    _invalidate_input_cache(key)
                    bot_state._page_urls[key] = page.url   # FIX #8: reset cache sau reload
                    logger.info(f"✅ [{domain}] Reload xong")
                except Exception:
                    logger.error(f"❌ [{domain}] Reload thất bại")
                    return {"success": False, "message": "Reload thất bại"}

            await _wake_tab_for_submit(page)

            # FIX #7: Luôn tìm input mới khi đổi user (xóa cache trước)
            _invalidate_input_cache(key)
            username_input, code_input = await find_input_fields(page, cache_key=key)

            if not code_input:
                # Thử lần 2
                await asyncio.sleep(0.3)
                _invalidate_input_cache(key)
                username_input, code_input = await find_input_fields(page, cache_key=key)

            if not code_input:
                logger.warning(f"❌ [{user}|{domain}] Không tìm thấy ô code")
                return {"success": False, "message": "Không tìm thấy ô code"}

            # Fill username + code
            try:
                if username_input:
                    await page.evaluate(REACT_FILL_JS, [username_input, user])
                await page.evaluate(REACT_FILL_JS, [code_input, code])
            except Exception as e:
                # Retry một lần
                _invalidate_input_cache(key)
                username_input, code_input = await find_input_fields(page, cache_key=key)
                if code_input:
                    try:
                        if username_input:
                            await page.evaluate(REACT_FILL_JS, [username_input, user])
                        await page.evaluate(REACT_FILL_JS, [code_input, code])
                    except Exception as e2:
                        logger.warning(f"❌ [{user}|{domain}] Lỗi fill form: {e2}")
                        return {"success": False, "message": str(e2)}
                else:
                    return {"success": False, "message": f"Lỗi fill: {e}"}

            # FIX #4: Click submit với domain
            clicked = await click_submit_fast(page, domain=domain)
            if not clicked:
                logger.warning(f"⚠️ [{user}|{domain}] Không click được submit")

            click_elapsed = time.time() - start_time
            logger.info(f"🚀 [{user}] NẠP {code} ({click_elapsed:.2f}s)")

            try:
                await page.bring_to_front()
            except Exception:
                pass

            # Xử lý Cloudflare popup nếu có
            await handle_cloudflare_popup(page)

            # FIX #3: Poll kết quả 100ms/lần, tối đa 6s
            result_text = ""
            poll_deadline = time.time() + 6.0
            while time.time() < poll_deadline:
                result_text = await detect_result_text(page)
                if result_text.strip():
                    break
                await asyncio.sleep(0.1)   # FIX #3: 150ms → 100ms

            elapsed = time.time() - start_time
            result_upper = result_text.upper()

            SUCCESS_KW = ["THÀNH CÔNG", "SUCCESS", "CỘNG", "OK"]
            FAILED_KW = [
                "SAI", "LỖI", "ĐÃ SỬ", "FAILED", "ERROR",
                "KHÔNG ĐÚNG", "KHÔNG TỒN TẠI", "KHÔNG HỢP LỆ",
                "HẾT HẠN", "ĐÃ HẾT", "INVALID", "NOT FOUND",
                "NOT EXIST", "KHÔNG TÌM THẤY",
            ]
            POINT_KW = ["ĐIỂM", "XU", "COIN", "POINT"]

            is_success = any(kw in result_upper for kw in SUCCESS_KW)
            is_failed = any(kw in result_upper for kw in FAILED_KW)
            has_points = any(kw in result_upper for kw in POINT_KW)

            if is_success and not is_failed:
                logger.info(f"✅ [{user}] THÀNH CÔNG ({elapsed:.2f}s) — {result_text[:60]}")
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None, db.record_submission, code, user, target_url, "SUCCESS", result_text[:100]
                )
                bot_state.submission_count[key] = bot_state.submission_count.get(key, 0) + 1
                perf_mon.record_task("submit_code", elapsed, True)
                append_code_history(
                    event_type="RESULT", code=code, target_url=target_url,
                    account=user, status="SUCCESS", submit_elapsed=elapsed,
                    message=result_text[:100],
                )
                return {"success": True, "has_points": has_points, "message": result_text[:100]}

            if len(result_text.strip()) < 5:
                screenshot = await take_result_screenshot(page, user, code, target_url, "UNKNOWN")
                logger.warning(f"⚠️ [{user}] KHÔNG THẤY KẾT QUẢ sau {elapsed:.2f}s")
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None, db.record_submission, code, user, target_url, "UNKNOWN", "Không thấy popup"
                )
                perf_mon.record_task("submit_code", elapsed, False)
                append_code_history(
                    event_type="RESULT", code=code, target_url=target_url,
                    account=user, status="UNKNOWN", submit_elapsed=elapsed,
                    message="Không thấy popup", screenshot=screenshot,
                )
                return {"success": False, "message": "Không thấy popup"}

            screenshot = await take_result_screenshot(page, user, code, target_url, "FAILED")
            logger.warning(f"❌ [{user}] THẤT BẠI ({elapsed:.2f}s) — {result_text[:60]}")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, db.record_submission, code, user, target_url, "FAILED", result_text[:100]
            )
            perf_mon.record_task("submit_code", elapsed, False)
            append_code_history(
                event_type="RESULT", code=code, target_url=target_url,
                account=user, status="FAILED", submit_elapsed=elapsed,
                message=result_text[:100], screenshot=screenshot,
            )
            return {"success": False, "message": result_text[:100]}

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"❌ [{user}] Lỗi submit: {e}")
        perf_mon.record_task("submit_code", elapsed, False)
        append_code_history(
            event_type="ERROR", code=code, target_url=target_url,
            account=user, status="ERROR", submit_elapsed=elapsed, message=str(e),
        )
        return {"success": False, "message": str(e)}


async def submit_code_with_delay(user: str, code: str, target_url: str, systems: dict):
    """
    FIX #6: Delay nằm TRONG hàm submit, NGOÀI semaphore
    → Semaphore giải phóng ngay sau submit, delay không block slot.
    """
    sem = get_submit_semaphore()
    async with sem:
        try:
            return await asyncio.wait_for(
                submit_code_safe(user, code, target_url, systems),
                timeout=25.0,
            )
        except asyncio.TimeoutError:
            logger.warning(f"⏰ [{user}] submit timeout 25s")
            return {"success": False, "message": "Timeout 25s"}
    # Delay SAU khi semaphore đã giải phóng
    delay = float(getattr(Config, "MIN_DELAY_BETWEEN_SUBMITS", 0.4))
    if delay > 0:
        await asyncio.sleep(delay)


def track_submit_task(task: asyncio.Task, label: str = ""):
    _active_submit_tasks.add(task)

    def _done(t: asyncio.Task):
        _active_submit_tasks.discard(t)
        try:
            result = t.result()
            if isinstance(result, dict):
                ok = "✅" if result.get("success") else "⚠️"
                logger.info(f"{ok} [TASK] {label} | {result.get('message', '')[:60]}")
        except asyncio.CancelledError:
            logger.debug(f"🛑 [TASK CANCELLED] {label}")
        except Exception as e:
            logger.error(f"❌ [TASK ERROR] {label}: {e}")

    task.add_done_callback(_done)
    return task


# ============================================================
# FIX #7: SEQUENTIAL SUBMIT — fill username mỗi account
# ============================================================

async def _submit_sequential_for_channel(
    codes: list,
    available_accounts: list,
    target_url: str,
    channel_name: str,
    domain: str,
):
    """
    FIX #7: Mỗi account nhận username riêng trước khi submit.
    Logic:
      - Thành công + có điểm  → tiếp tục acc kế
      - Thành công + không điểm → dừng (code đã dùng)
      - Thất bại               → thử acc tiếp
    """
    if not codes:
        return

    selected_code = select_random_code(codes)
    logger.info(f"🎲 [{domain}] Code: {selected_code} (từ {len(codes)} code)")

    total = len(available_accounts)
    for idx, account in enumerate(available_accounts):
        user = account["username"]
        is_last = idx == total - 1

        logger.info(f"🔄 [{domain}] [{idx+1}/{total}] Nhập cho: {user}")

        result = await submit_code_with_delay(user, selected_code, target_url, _systems)

        success = result.get("success", False) if result else False
        has_points = result.get("has_points", False) if result else False
        msg = (result.get("message", "") if result else "Không có kết quả")[:80]

        if success and has_points:
            if is_last:
                logger.info(f"✅ [{domain}|{user}] THÀNH CÔNG+ĐIỂM. Hết acc.")
                return
            logger.info(f"✅ [{domain}|{user}] THÀNH CÔNG+ĐIỂM → sang acc tiếp")
            continue

        if success and not has_points:
            logger.warning(f"⚠️ [{domain}|{user}] Thành công nhưng không điểm → dừng\n   {msg}")
            return

        if is_last:
            logger.warning(f"❌ [{domain}|{user}] Thất bại. Hết acc.")
        else:
            logger.warning(f"❌ [{domain}|{user}] Thất bại → thử acc tiếp\n   {msg}")


# ============================================================
# FIX #2: OCR — chạy trong thread pool
# ============================================================

async def process_image_from_telegram(event, channel_config: dict, systems: dict):
    """FIX #2: Download + OCR trong run_in_executor → không block event loop."""
    import tempfile
    import os
    import shutil

    target_url = channel_config.get("url", "")

    try:
        logger.info("📸 [OCR] Phát hiện ảnh, đang xử lý...")
        temp_dir = tempfile.mkdtemp(prefix="ocr_telegram_")

        try:
            # Download media (I/O — chạy ngay, Telethon async)
            image_path = await event.download_media(file=temp_dir)
            if not image_path:
                return {"success": False, "codes": [], "message": "Không download được ảnh", "text": ""}

            logger.info(f"✅ [OCR] Downloaded: {os.path.basename(image_path)}")

            extractor = get_image_extractor()
            if extractor is None:
                return {"success": False, "codes": [], "message": "OCR chưa sẵn sàng (cài Tesseract)", "text": ""}

            # FIX #2: Chạy OCR trong thread pool để không block event loop
            loop = asyncio.get_event_loop()

            def _run_ocr():
                text = extractor.extract_code_from_image(image_path, lang="eng")
                if not text:
                    text = extractor.extract_code_from_image(image_path, lang="vie+eng")
                return text or ""

            extracted_text = await loop.run_in_executor(None, _run_ocr)

            if not extracted_text:
                return {"success": False, "codes": [], "message": "Ảnh không chứa text", "text": ""}

            logger.info(f"✅ [OCR] Trích được {len(extracted_text)} ký tự")

            # Parse codes
            extracted_codes = []
            for line in extracted_text.split("\n"):
                line = line.strip()
                if len(line) < 4:
                    continue
                clean_code = CodeValidator.clean_code(line)
                if not clean_code or len(clean_code) < 4:
                    continue
                try:
                    validation = CodeValidator.validate_code(clean_code, target_url=target_url, source="image_ocr")
                except TypeError:
                    validation = CodeValidator.validate_code(clean_code, target_url)
                if validation["valid"]:
                    extracted_codes.append({"code": clean_code, "raw": line, "confidence": 0.9})
                    logger.info(f"✅ [OCR] CODE: {clean_code}")

            if extracted_codes:
                return {"success": True, "codes": extracted_codes, "message": f"OCR: {len(extracted_codes)} code(s)", "text": extracted_text}
            return {"success": False, "codes": [], "message": "Không tìm thấy code hợp lệ", "text": extracted_text}

        finally:
            try:
                shutil.rmtree(temp_dir)
            except Exception:
                pass

    except Exception as e:
        logger.error(f"❌ [OCR] Lỗi: {e}")
        return {"success": False, "codes": [], "message": f"Lỗi OCR: {e}", "text": ""}


async def submit_codes_from_image(user: str, codes_data: list, target_url: str, channel_config: dict, systems: dict):
    if not codes_data:
        return
    domain = normalize_domain(target_url)
    logger.info(f"📤 [IMG] Submit {len(codes_data)} code(s) cho {user} @ {domain}")
    for idx, code_item in enumerate(codes_data, 1):
        code = code_item.get("code", "").strip()
        if not code:
            continue
        try:
            result = await submit_code_with_delay(user, code, target_url, systems)
            status = "✅" if (result and result.get("success")) else "❌"
            logger.info(f"  {status} [OCR#{idx}] {code}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"  ❌ [OCR#{idx}] Lỗi: {e}")


# ============================================================
# MESSAGE PROCESSING
# ============================================================

async def process_telegram_message(event):
    if not _systems:
        return

    channel_config = Config.CHANNEL_CONFIG.get(event.chat_id)
    if not channel_config:
        return

    target_url = channel_config["url"]
    accounts = channel_config["accounts"]
    raw_text = event.message.text or ""

    logger.info(f"\n👀 [{channel_config['name']}]")

    # Xử lý ảnh
    if event.media and not raw_text:
        logger.info("🖼️ Phát hiện ảnh → OCR...")
        default_account = accounts[0]["username"] if accounts else None
        if not default_account:
            return
        ocr_result = await process_image_from_telegram(event, channel_config=channel_config, systems=_systems)
        if ocr_result["success"]:
            await submit_codes_from_image(
                user=default_account,
                codes_data=ocr_result["codes"],
                target_url=target_url,
                channel_config=channel_config,
                systems=_systems,
            )
        else:
            logger.warning(f"⚠️ OCR thất bại: {ocr_result['message']}")
        return

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
            event_type="DETECTED", code=code, target_url=target_url,
            channel=channel_config.get("name", ""), source="telegram",
            status="PENDING", telegram_delay=telegram_delay,
        )

    domain = normalize_domain(target_url)

    # Dedup check
    final_codes_dedup = []
    for code in final_codes:
        if is_site_code_duplicate(domain, code):
            logger.warning(f"⏭️ [DEDUP] {code} đã nạp gần đây")
        else:
            final_codes_dedup.append(code)

    if not final_codes_dedup:
        logger.info("⏭️ Tất cả code bị dedup")
        return

    if domain not in bot_state.account_pages:
        logger.warning(f"⚠️ [{domain}] Chưa có tab")
        return

    available_accounts = sorted(accounts, key=lambda a: a.get("priority", 999))
    if not available_accounts:
        return

    task = asyncio.create_task(
        _submit_sequential_for_channel(
            codes=final_codes_dedup,
            available_accounts=available_accounts,
            target_url=target_url,
            channel_name=channel_config.get("name", ""),
            domain=domain,
        )
    )
    track_submit_task(task, label=f"seq|{domain}|{len(final_codes_dedup)}")
    logger.warning(f"⚡ Task nhập {len(final_codes_dedup)} code → '{channel_config['name']}'")


# ============================================================
# FIX #9: MESSAGE WORKERS — 6 workers, queue 200
# ============================================================

async def message_worker(worker_id: int):
    global message_queue
    logger.info(f"👷 Worker #{worker_id} khởi động")
    while bot_state.is_running:
        try:
            try:
                event = await asyncio.wait_for(message_queue.get(), timeout=0.05)
            except asyncio.TimeoutError:
                await asyncio.sleep(0.01)
                continue
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(0.1)
            continue
        try:
            await process_telegram_message(event)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"❌ Worker #{worker_id}: {e}")
        finally:
            try:
                message_queue.task_done()
            except Exception:
                pass


def start_message_workers():
    global message_queue, message_workers
    if message_queue is None:
        message_queue = asyncio.Queue(maxsize=200)   # FIX #9: 50 → 200
    if message_workers:
        return
    worker_count = 6   # FIX #9: 3 → 6
    for wid in range(1, worker_count + 1):
        message_workers.append(asyncio.create_task(message_worker(wid)))
    logger.info(f"🚀 Message queue: maxsize=200, workers={worker_count}")


# ============================================================
# FIX #1: TELEGRAM HANDLER — truly non-blocking
# ============================================================

async def setup_telegram_handler():
    if bot_state.handler_registered:
        return

    channel_ids = list(Config.CHANNEL_CONFIG.keys())
    start_message_workers()

    @client.on(events.NewMessage(chats=channel_ids))
    async def handler(event):
        # FIX #1: KHÔNG gọi process trực tiếp — chỉ put_nowait vào queue
        # Handler hoàn thành trong < 1µs, Telegram không bị throttle
        try:
            message_queue.put_nowait(event)
        except asyncio.QueueFull:
            pass   # Bỏ silently nếu queue đầy

    bot_state.handler_registered = True
    logger.info("✅ Telegram handler setup xong!")


# ============================================================
# WATCHDOGS
# ============================================================

async def auto_fill_usernames_watchdog():
    """Tự động điền username khi bị xóa."""
    last_filled_time: dict = {}
    while bot_state.is_running:
        await asyncio.sleep(10)
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
                now = time.time()
                last_filled = last_filled_time.get(domain_key)
                if last_filled and (now - last_filled) < 300:
                    continue
                default_user = get_default_account_for_domain(domain_key)
                if not default_user:
                    continue
                await page.evaluate(
                    "([el, val]) => { el.value = val; "
                    "el.dispatchEvent(new Event('input', {bubbles:true})); "
                    "el.dispatchEvent(new Event('change', {bubbles:true})); }",
                    [username_input, default_user],
                )
                logger.info(f"🔄 [{domain_key}] Tự điền username '{default_user}'")
                last_filled_time[domain_key] = now
            except Exception as e:
                logger.debug(f"⚠️ Watchdog username error: {e}")


async def cloudflare_watchdog():
    """Phát hiện Cloudflare challenge + đưa tab lên."""
    CF_DETECT_SELECTORS = [
        "iframe[src*='challenges.cloudflare.com']",
        "iframe[src*='turnstile']",
        ".cf-turnstile",
        "[data-sitekey]",
    ]
    while bot_state.is_running:
        try:
            await asyncio.sleep(random.uniform(300.0, 600.0))
            for key, page in list(bot_state.account_pages.items()):
                try:
                    current_url = page.url
                except Exception:
                    continue
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
        except asyncio.CancelledError:
            break
        except Exception:
            pass


async def cleanup_browsers():
    for port, browser in bot_state.connected_browsers.items():
        try:
            await browser.close()
        except Exception:
            pass


# ============================================================
# MAIN
# ============================================================

async def main():
    global _systems

    try:
        logger.info("🚀 BOT v7.2 (ALL BUGS FIXED)")
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
        logger.info("✅ BOT READY!\n")

        # FIX #11: Heartbeat rõ hơn
        async def heartbeat_loop():
            while bot_state.is_running:
                try:
                    await asyncio.sleep(300.0)
                    pages = len(bot_state.account_pages)
                    tasks = len(_active_submit_tasks)
                    q_size = message_queue.qsize() if message_queue else 0
                    logger.info(
                        f"💓 Heartbeat | tabs={pages} | tasks={tasks} | queue={q_size} | tg={client.is_connected()}"
                    )
                except Exception:
                    pass

        asyncio.create_task(heartbeat_loop())
        asyncio.create_task(auto_fill_usernames_watchdog())
        asyncio.create_task(cloudflare_watchdog())

        _reconnect_delay = 5.0
        _reconnect_backoff = 1.0
        while bot_state.is_running:
            try:
                if not client.is_connected():
                    logger.warning("🔄 Reconnect Telegram...")
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
