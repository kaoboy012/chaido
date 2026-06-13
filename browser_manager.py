"""
🖥️ BROWSER MANAGER — Microsoft Edge
- Kill TOÀN BỘ Edge trong Task Manager trước khi mở (luôn luôn)
- Mở Edge mới đúng port, đưa cửa sổ lên đầu màn hình
- Reuse tab đang mở đúng domain, chỉ mở thêm tab còn thiếu
- KHÔNG tự điền username/cookie/session — để người dùng tự làm thủ công
"""

import subprocess
import time
import urllib.request
import json
import ctypes
import sys

from logger_setup import logger
from config import Config


# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────
EDGE_PROCESS_NAME = "msedge.exe"

EDGE_EXE = getattr(
    Config,
    "BROWSER_EXE_PATH",
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
)
BROWSER_PROFILE = getattr(Config, "BROWSER_PROFILE", "Default")


# ─────────────────────────────────────────────────────────────
# Kill Edge
# ─────────────────────────────────────────────────────────────

def kill_all_edge():
    """
    Kill TOÀN BỘ tiến trình msedge.exe trong Task Manager.
    Luôn chạy trước khi mở Edge mới để tránh xung đột port.
    """
    logger.info("🔴 Đang tắt toàn bộ Edge trong Task Manager...")
    try:
        result = subprocess.run(
            ["taskkill", "/F", "/IM", EDGE_PROCESS_NAME, "/T"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            logger.info("✅ Đã tắt toàn bộ Edge thành công.")
        else:
            # returncode 128 = không có Edge nào đang chạy → bình thường
            logger.info("ℹ️ Không có Edge nào đang chạy (bình thường).")
    except Exception as e:
        logger.warning(f"⚠️ Không kill được Edge: {e}")

    # Chờ OS giải phóng port và process
    time.sleep(2.0)


# ─────────────────────────────────────────────────────────────
# Kiểm tra CDP
# ─────────────────────────────────────────────────────────────

def _is_cdp_alive(port: int, timeout: float = 2.0) -> bool:
    try:
        url = f"http://localhost:{port}/json/version"
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.status == 200
    except Exception:
        return False


def _get_cdp_tabs(port: int) -> list:
    try:
        url = f"http://localhost:{port}/json"
        with urllib.request.urlopen(url, timeout=3) as resp:
            return json.loads(resp.read())
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────
# Mở Edge + đưa lên đầu màn hình
# ─────────────────────────────────────────────────────────────

def _launch_edge(port: int):
    """
    Mở Microsoft Edge với CDP port.
    Vị trí cửa sổ: góc trên-trái màn hình (0,0) để dễ thấy.
    """
    cmd = [
        EDGE_EXE,
        f"--profile-directory={BROWSER_PROFILE}",
        f"--remote-debugging-port={port}",
        "--remote-debugging-address=127.0.0.1",
        "--no-first-run",
        "--mute-audio",
        "--window-position=0,0",      # Góc trên-trái màn hình
        "--window-size=1280,800",     # Kích thước đủ rộng để dùng
        "--disable-extensions",
    ]
    try:
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logger.info(f"🚀 Đã mở Edge → port {port} | Cửa sổ ở góc trên-trái màn hình")
    except FileNotFoundError:
        logger.error(
            f"❌ Không tìm thấy Edge tại:\n   {EDGE_EXE}\n"
            f"   Sửa BROWSER_EXE_PATH trong .env"
        )
        raise
    except Exception as e:
        logger.error(f"❌ Không mở được Edge: {e}")
        raise


def _wait_for_cdp(port: int, max_wait: float = 20.0, interval: float = 0.5) -> bool:
    deadline = time.time() + max_wait
    while time.time() < deadline:
        if _is_cdp_alive(port):
            return True
        time.sleep(interval)
    return False


def _bring_edge_to_front():
    """
    Đưa cửa sổ Edge lên foreground (Windows API).
    Chạy sau khi launch để đảm bảo cửa sổ hiện lên đầu.
    """
    if sys.platform != "win32":
        return
    try:
        # SetForegroundWindow qua tìm hwnd của Edge
        import ctypes
        EnumWindows = ctypes.windll.user32.EnumWindows
        GetWindowTextW = ctypes.windll.user32.GetWindowTextW
        SetForegroundWindow = ctypes.windll.user32.SetForegroundWindow
        ShowWindow = ctypes.windll.user32.ShowWindow
        IsWindowVisible = ctypes.windll.user32.IsWindowVisible
        SW_RESTORE = 9

        found = []

        def enum_cb(hwnd, _):
            if not IsWindowVisible(hwnd):
                return True
            buf = ctypes.create_unicode_buffer(256)
            GetWindowTextW(hwnd, buf, 256)
            title = buf.value
            if "edge" in title.lower() or "microsoft" in title.lower():
                found.append(hwnd)
            return True

        WNDENUMPROC = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_int))
        EnumWindows(WNDENUMPROC(enum_cb), 0)

        if found:
            hwnd = found[0]
            ShowWindow(hwnd, SW_RESTORE)
            SetForegroundWindow(hwnd)
            logger.info("🖥️ Đã đưa cửa sổ Edge lên đầu màn hình.")
    except Exception as e:
        logger.debug(f"⚠️ Không đưa Edge lên front được (bỏ qua): {e}")


# ─────────────────────────────────────────────────────────────
# HÀM CHÍNH
# ─────────────────────────────────────────────────────────────

def ensure_browsers_ready(
    auto_close_old: bool = None,
    auto_launch: bool = None,
) -> dict:
    """
    Quy trình chuẩn bị Edge trước khi bot chạy:

      1. Kill TOÀN BỘ Edge cũ (luôn luôn — tránh xung đột port)
      2. Mở Edge mới đúng port cho từng CDP_CONNECTIONS
      3. Đưa cửa sổ lên đầu màn hình
      4. KHÔNG tự điền gì — người dùng tự đăng nhập thủ công

    Trả về: {port: [tab_url đang mở]}
    """
    if auto_close_old is None:
        auto_close_old = getattr(Config, "AUTO_CLOSE_OLD_BROWSER", True)
    if auto_launch is None:
        auto_launch = getattr(Config, "AUTO_LAUNCH_BROWSER", True)

    ports = [int(p) for p in Config.CDP_CONNECTIONS.keys()]
    result = {}

    # ── Bước 1: Kill TOÀN BỘ Edge trước (không phân biệt port) ──
    if auto_close_old:
        kill_all_edge()

    # ── Bước 2: Mở Edge mới cho từng port ──
    for port in ports:
        if auto_launch:
            _launch_edge(port)
        else:
            if not _is_cdp_alive(port):
                logger.warning(
                    f"⚠️ Port {port} chưa mở và AUTO_LAUNCH_BROWSER=false. "
                    f"Hãy mở Edge tay với --remote-debugging-port={port}"
                )
                result[port] = []
                continue

    # ── Bước 3: Chờ tất cả port sẵn sàng ──
    if auto_launch:
        for port in ports:
            logger.info(f"⏳ Chờ Edge khởi động (port {port})...")
            if _wait_for_cdp(port, max_wait=20.0):
                tabs = _get_cdp_tabs(port)
                tab_urls = [t.get("url", "") for t in tabs if t.get("type") == "page"]
                logger.info(f"✅ Port {port} sẵn sàng — {len(tab_urls)} tab")
                result[port] = tab_urls
            else:
                logger.error(
                    f"❌ Port {port} không phản hồi sau 20s.\n"
                    f"   Kiểm tra BROWSER_EXE_PATH trong .env"
                )
                result[port] = []

    # ── Bước 4: Đưa cửa sổ lên đầu màn hình ──
    time.sleep(0.5)
    _bring_edge_to_front()

    logger.info(
        "👤 Edge đã mở. Hãy tự đăng nhập / vào đúng trang rồi bấm Enter để bot tiếp tục..."
    )

    return result
