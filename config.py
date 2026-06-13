"""
⚙️ CẤU HÌNH BOT - CHẾ ĐỘ MỞ TRÌNH DUYỆT THỦ CÔNG TRƯỚC
Bot kết nối vào Chrome/Cốc Cốc/Chromium qua CDP port 9222.

Lưu ý:
- API_ID và API_HASH nên nằm trong file .env.
- Không nên commit .env hoặc file *.session lên GitHub.
"""

import os
from dotenv import load_dotenv

load_dotenv()


def get_int_env(key: str, default: int) -> int:
    value = os.getenv(key)
    if value is None or str(value).strip() == "":
        return default
    return int(value)


def get_float_env(key: str, default: float) -> float:
    value = os.getenv(key)
    if value is None or str(value).strip() == "":
        return default
    return float(value)


def get_bool_env(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip().lower() in ("true", "1", "yes", "y", "on")


class Config:
    # ==========================================
    # 🔐 TELEGRAM
    # ==========================================
    API_ID = get_int_env("API_ID", 0)
    API_HASH = os.getenv("API_HASH", "")
    SESSION_NAME = os.getenv("SESSION_NAME", "session_bot_full")

    # ==========================================
    # 📝 LOG / DATABASE
    # ==========================================
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("LOG_FILE", "logs/bot_activity.log")
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    MAX_LOG_SIZE = get_int_env("MAX_LOG_SIZE", 10485760)
    BACKUP_COUNT = get_int_env("BACKUP_COUNT", 5)
    CONSOLE_LOG = True

    DATABASE_PATH = os.getenv("DATABASE_PATH", "data/code_history.db")
    HISTORY_FILE = "logs/history_success.txt"
    BROWSER_PROFILE_DIR = os.getenv("BROWSER_PROFILE_DIR", "HoSo_Bot_Vip")

    # ==========================================
    # 🔎 CODE FILTER
    # ==========================================
    CODE_MIN_LENGTH = 6
    CODE_MAX_LENGTH = 15

    SPECIAL_CODE_CHARS_30 = r"""~!@#$%^&*()_+{}|:"<>?`=[]\;',./"""

    CODE_FILTER_GROUPS = {
        "multi_site_strict": {
            "description": "Dùng chung cho XX88, MM88, O8",
            "url_keywords": ["xx88", "mm88", "o8"],
            "allowed_sites": ["xx88", "mm88", "o8"],

            "allow_numeric": False,
            "allow_random_mix": False,
            "require_uppercase": True,

            "prefer_spoiler": True,
            "marker_scan_lines": 3,
            "allow_fallback": False,

            "special_chars_group": "SPECIAL_CODE_CHARS_30",
            "special_chars": SPECIAL_CODE_CHARS_30,

            "min_special_chars": 0,

            "min_entropy": 2.0,
            "uppercase_min_entropy": 2.5,
            "soft_blacklist": ["CODE", "GAME", "FREE", "VIP", "NAP", "RUT"],
        },
        "llwin": {
            "description": "Dùng riêng cho LLWIN - code IN HOA + dấu / hoặc *",
            "url_keywords": ["llwin", "llw", "88llwin", "84888"],
            "allowed_sites": ["llwin"],

            "allow_numeric": False,
            "allow_random_mix": False,
            "require_uppercase": True,  # ✅ LLWIN luôn in hoa

            "prefer_spoiler": True,
            "marker_scan_lines": 4,
            "allow_fallback": False,

            "special_chars_group": "SPECIAL_CODE_CHARS_30",
            "special_chars": SPECIAL_CODE_CHARS_30,

            "min_special_chars": 2,     # ✅ Code LLWIN luôn có dấu / hoặc *

            "min_entropy": 2.0,
            "uppercase_min_entropy": 2.5,
            "soft_blacklist": ["CODE", "GAME", "FREE", "VIP", "NAP", "RUT"],
        },
        "new88": {
            "description": "Dùng riêng cho các kênh NEW88",
            "url_keywords": ["new88", "new88b"],
            "allowed_sites": ["new88"],
            "allow_numeric": True,
            "allow_random_mix": True,
            "require_uppercase": False,
            "prefer_spoiler": True,
            "marker_scan_lines": 4,
            "allow_fallback": False,
            "min_entropy": 2.25,
            "uppercase_min_entropy": 2.9,
            "soft_blacklist": ["CODE", "GAME", "FREE", "VIP", "NAP", "RUT"],
        },
        "qq88": {
            "description": "Dùng riêng cho các kênh QQ88 - code mix hoa/thường/số",
            "url_keywords": ["qq88", "tangquaqq88"],
            "allowed_sites": ["qq88"],

            "allow_numeric": False,
            "allow_random_mix": True,   # ✅ QQ88 dùng mix hoa + thường + số
            "require_uppercase": False, # ✅ KHÔNG bắt buộc in hoa

            "prefer_spoiler": True,
            "marker_scan_lines": 3,
            "allow_fallback": False,

            "special_chars_group": "SPECIAL_CODE_CHARS_30",
            "special_chars": SPECIAL_CODE_CHARS_30,

            "min_special_chars": 0,

            "min_entropy": 2.0,
            "uppercase_min_entropy": 2.5,
            "soft_blacklist": ["CODE", "GAME", "FREE", "VIP", "NAP", "RUT", "LINK"],
        },
        "uy88": {
            "description": "Dùng riêng cho các kênh UY88",
            "url_keywords": ["uy88", "uy88code"],
            "allowed_sites": ["uy88"],
            "max_clean_length": 45,
            "allow_numeric": True,
            "allow_random_mix": True,
            "require_uppercase": False,
            "prefer_spoiler": True,
            "marker_scan_lines": 3,
            "allow_fallback": False,
            "special_chars_group": "SPECIAL_CODE_CHARS_30",
            "special_chars": SPECIAL_CODE_CHARS_30,
            "min_special_chars": 0,
            "min_entropy": 2.0,
            "uppercase_min_entropy": 2.5,
            "soft_blacklist": ["CODE", "GAME", "FREE", "VIP", "NAP", "RUT"],
        },
        "mmoo": {
            "description": "Dùng riêng cho các kênh MMOO",
            "url_keywords": ["mmoo", "mmoocode.shop"],
            "allowed_sites": ["mmoo"],
            "max_clean_length": 45,
            "allow_numeric": True,
            "allow_random_mix": True,
            "require_uppercase": False,
            "prefer_spoiler": True,
            "marker_scan_lines": 3,
            "allow_fallback": False,
            "special_chars_group": "SPECIAL_CODE_CHARS_30",
            "special_chars": SPECIAL_CODE_CHARS_30,
            "min_special_chars": 0,
            "min_entropy": 2.0,
            "uppercase_min_entropy": 2.5,
            "soft_blacklist": ["CODE", "GAME", "FREE", "VIP", "NAP", "RUT"],
        },
        "default": {
            "description": "Fallback nếu URL chưa thuộc nhóm nào",
            "url_keywords": [],
            "allowed_sites": [],
            "allow_numeric": True,
            "allow_random_mix": True,
            "require_uppercase": False,
            "prefer_spoiler": True,
            "min_entropy": 2.3,
            "uppercase_min_entropy": 2.9,
            "soft_blacklist": ["CODE", "GAME", "FREE", "VIP", "NAP", "RUT"],
        },
    }

    # ==========================================
    # 🖥️ CDP / ACCOUNT
    # ==========================================
    CDP_CONNECTIONS = {
        9222: [
            "kaoboy012",
            "kuuteo012",
            "dad131",
            "dad123",
            "conve99sau",
            "minichan",
            "kuuteo0123",
            "hugolan",
            "hugolan012",
        ]
    }

    BROWSER_TYPE = "chromium"

    BROWSER_EXE_PATH = os.getenv(
        "BROWSER_EXE_PATH",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
    )
    BROWSER_PROFILE = os.getenv("BROWSER_PROFILE", "Default")

    AUTO_CLOSE_OLD_BROWSER = get_bool_env("AUTO_CLOSE_OLD_BROWSER", True)
    AUTO_LAUNCH_BROWSER = get_bool_env("AUTO_LAUNCH_BROWSER", True)

    HEADLESS_MODE = get_bool_env("HEADLESS_MODE", True)
    VIEWPORT_WIDTH = get_int_env("VIEWPORT_WIDTH", 400)
    VIEWPORT_HEIGHT = get_int_env("VIEWPORT_HEIGHT", 300)
    WINDOW_POSITION_X = 0
    WINDOW_POSITION_Y = 0

    AUTO_OPEN_MISSING_TABS = get_bool_env("AUTO_OPEN_MISSING_TABS", True)
    AUTO_OPEN_TAB_WAIT = get_float_env("AUTO_OPEN_TAB_WAIT", 1.0)

    PROFILE_STORAGE_TYPE = "persistent"
    PROFILE_CLEANUP_DAYS = 30
    CLEANUP_OLD_PROFILES = True

    # ==========================================
    # ⏱️ TIMEOUT / SPEED
    # ==========================================
    PAGE_LOAD_TIMEOUT = get_int_env("PAGE_LOAD_TIMEOUT", 30000)
    CLOUDFLARE_WAIT_TIMEOUT = get_int_env("CLOUDFLARE_WAIT_TIMEOUT", 60000)
    CLOUDFLARE_CLICK_SLEEP = get_float_env("CLOUDFLARE_CLICK_SLEEP", 1.5)
    SITE_CODE_DEDUP_TTL = get_float_env("SITE_CODE_DEDUP_TTL", 10.0)
    SUBMIT_TIMEOUT = 5000
    RESULT_WAIT = get_int_env("RESULT_WAIT", 1000)
    BROWSER_SPAWN_TIMEOUT = 15000

    MAX_CONCURRENT_SUBMITS = get_int_env("MAX_CONCURRENT_TASKS", 3)
    MAX_RETRY_FAILED_CODE = 2

    MIN_DELAY_BETWEEN_SUBMITS = get_float_env("MIN_DELAY_BETWEEN_SUBMITS", 0.8)
    REQUESTS_PER_MINUTE = get_int_env("REQUESTS_PER_MINUTE", 30)
    MAX_BURST = get_int_env("MAX_BURST", 5)

    AUTO_SUBMIT_ENABLED = get_bool_env("AUTO_SUBMIT_ENABLED", True)
    AUTO_SUBMIT_DELAY = get_float_env("AUTO_SUBMIT_DELAY", 0.3)
    HUMAN_LIKE_TYPING_SPEED = get_float_env("HUMAN_LIKE_TYPING_SPEED", 0.05)
    RANDOM_DELAY_MIN = get_float_env("RANDOM_DELAY_MIN", 0.1)
    RANDOM_DELAY_MAX = get_float_env("RANDOM_DELAY_MAX", 0.5)

    INPUT_DETECTION_STRATEGY = os.getenv("INPUT_DETECTION_STRATEGY", "advanced")
    INPUT_DETECTION_TIMEOUT = get_int_env("INPUT_DETECTION_TIMEOUT", 5000)
    MULTIPLE_SELECTOR_ATTEMPTS = get_int_env("MULTIPLE_SELECTOR_ATTEMPTS", 4)

    RESULT_DETECTION_METHODS = get_int_env("RESULT_DETECTION_METHODS", 5)
    RESULT_DETECTION_TIMEOUT = get_int_env("RESULT_DETECTION_TIMEOUT", 5000)
    SCREENSHOT_ON_UNKNOWN = get_bool_env("SCREENSHOT_ON_UNKNOWN", False)

    # ==========================================
    # ⚡ TELEGRAM REALTIME / QUEUE MODE
    # ==========================================
    MAX_TELEGRAM_DELAY_SECONDS = get_float_env("MAX_TELEGRAM_DELAY_SECONDS", 8.0)
    MESSAGE_QUEUE_MAXSIZE = get_int_env("MESSAGE_QUEUE_MAXSIZE", 500)
    MESSAGE_WORKERS = get_int_env("MESSAGE_WORKERS", 3)
    HEARTBEAT_INTERVAL = get_float_env("HEARTBEAT_INTERVAL", 300.0)
    TELEGRAM_CATCH_UP = get_bool_env("TELEGRAM_CATCH_UP", False)
    BACKGROUND_MAINTENANCE_DELAY = get_int_env("BACKGROUND_MAINTENANCE_DELAY", 300)
    RUN_SYSTEM_TEST_ON_START = get_bool_env("RUN_SYSTEM_TEST_ON_START", False)

    SESSION_ROTATION_ENABLED = get_bool_env("SESSION_ROTATION_ENABLED", False)
    SESSION_ROTATION_INTERVAL = get_int_env("SESSION_ROTATION_INTERVAL", 100)
    SESSION_ROTATION_DELAY = get_float_env("SESSION_ROTATION_DELAY", 2.0)

    # ==========================================
    # 📡 TELEGRAM CHANNEL CONFIG
    # ==========================================
    CHANNEL_CONFIG = {
        -1003134541072: {
            "name": "MM88VIP Dịch Vụ Giai Nhân",
            "url": "https://mm88code.com",
            "filter_group": "multi_site_strict",
            "priority": 1,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "dad131", "priority": 2},
            ],
        },
        -1003859359508: {
            "name": "LLwin ĐỈNH CAO CHIẾN THẮNG",
            "url": "https://llwincode.com",
            "filter_group": "llwin",
            "priority": 2,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "conve99sau", "priority": 2},
            ],
        },
        -1002626603440: {
            "name": "NEW88 PHÁT C.O.DE NỔ HŨ - BẮN CÁ MIỄN PHÍ",
            "url": "https://new88b.today/giftcode",
            "filter_group": "new88",
            "priority": 3,
            "accounts": [
                {"username": "minichan", "priority": 1},
                {"username": "kuuteo0123", "priority": 2},
            ],
        },
        -1002817093108: {
            "name": "PHÁT CODE XX88",
            "url": "https://xx88code.com/",
            "filter_group": "multi_site_strict",
            "priority": 4,
            "accounts": [
                {"username": "dad131", "priority": 1},
                {"username": "hugolan", "priority": 2},
            ],
        },
        -1003090141840: {
            "name": "CLIP VUI NEW88",
            "url": "https://new88b.today/giftcode",
            "filter_group": "new88",
            "priority": 5,
            "accounts": [
                {"username": "minichan", "priority": 1},
                {"username": "kuuteo0123", "priority": 2},
            ],
        },
        -1003784573497: {
            "name": "o8 SOI KÈO 24/7",
            "url": "https://o8code.com/",
            "filter_group": "multi_site_strict",
            "priority": 6,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "conve99sau", "priority": 2},
            ],
        },
        -1003802387209: {
            "name": "o8 TIN HOT 24H",
            "url": "https://o8code.com/",
            "filter_group": "multi_site_strict",
            "priority": 7,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "conve99sau", "priority": 2},
            ],
        },
        -1003574944644: {
            "name": "o8 - TROLL BÓNG ĐÁ",
            "url": "https://o8code.com/",
            "filter_group": "multi_site_strict",
            "priority": 8,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "conve99sau", "priority": 2},
            ],
        },
        -1002278162941: {
            "name": "QQ88 - TIN HOT 24/7",
            "url": "https://tangquaqq88.com/",
            "filter_group": "qq88",
            "priority": 9,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "kuuteo012", "priority": 2},
                {"username": "dad131", "priority": 3},
            ],
        },
        -1002324210129: {
            "name": "QQ88 - GIẢI TRÍ",
            "url": "https://tangquaqq88.com/",
            "filter_group": "qq88",
            "priority": 10,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "kuuteo012", "priority": 2},
                {"username": "dad131", "priority": 3},
            ],
        },
        -1002377579866: {
            "name": "QQ88 - TIN TỨC MỖI NGÀY",
            "url": "https://tangquaqq88.com/",
            "filter_group": "qq88",
            "priority": 11,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "kuuteo012", "priority": 2},
                {"username": "dad131", "priority": 3},
            ],
        },
        -1002421765170: {
            "name": "QQ88 - KHO GIF",
            "url": "https://tangquaqq88.com/",
            "filter_group": "qq88",
            "priority": 12,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "kuuteo012", "priority": 2},
                {"username": "dad131", "priority": 3},
            ],
        },
        -1002325212717: {
            "name": "QQ88 - REVIEW PHIM HAY",
            "url": "https://tangquaqq88.com/",
            "filter_group": "qq88",
            "priority": 13,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "kuuteo012", "priority": 2},
                {"username": "dad131", "priority": 3},
            ],
        },
        -1003953677009: {
            "name": "UY88 - Kênh chính thức",
            "url": "https://uy88code.org/inputcode/",
            "filter_group": "uy88",
            "priority": 14,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "conve99sau", "priority": 2},
                {"username": "kuuteo012", "priority": 3},
                {"username": "hugolan012", "priority": 4},
            ],
        },
        -1002430702373: {
            "name": "MMOO Kênh chính thức",
            "url": "https://mmoocode.shop/inputcode/",
            "filter_group": "mmoo",
            "priority": 15,
            "accounts": [
                {"username": "dad131", "priority": 1},
                {"username": "dad123", "priority": 2},
            ],
        },
    }

    # ==========================================
    # 🚫 BLACKLIST
    # ==========================================
    CODE_BLACKLIST = [
        "COM",
        "HTTP",
        "HTTPS",
        "WWW",
        "FACEBOOK",
        "TELEGRAM",
        "CHECK",
        "CLIP",
        "VUI",
        "BOT",
        "DAILY",
        "TRUYCAP",
        "BANCA",
        "NOHU",
        "ONLINE",
        "FREE",
        "CODE",
        "GIFTCODE",
        "MINIGAME",
        "GAME",
        "THETHAO",
        "O8THETHAO",
        "BONGDA",
        "TROLL",
    ]

    # ==========================================
    # ⚙️ FEATURE FLAGS
    # ==========================================
    ENABLE_RETRY = True
    ENABLE_CIRCUIT_BREAKER = True
    ENABLE_DATABASE_TRACKING = True
    ENABLE_RATE_LIMITING = True
    ENABLE_MONITORING = True
    ENABLE_AUTO_SESSION_ROTATION = False
    ENABLE_ADVANCED_ANTI_DETECTION = True