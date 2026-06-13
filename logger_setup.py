import logging
import logging.handlers
import os
from dotenv import load_dotenv

load_dotenv()


class ColoredFormatter(logging.Formatter):
    """Formatter với màu sắc cho console"""

    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
    }
    RESET = '\033[0m'

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{log_color}{record.levelname}{self.RESET}"
        return super().format(record)


def setup_logger():
    """Cấu hình logging hệ thống — xoay vòng theo NGÀY (TimedRotatingFileHandler)"""

    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger("bot_logger")
    logger.setLevel(logging.INFO)

    # ✅ FIX LOG: Dùng TimedRotatingFileHandler thay RotatingFileHandler
    # → File tự đổi tên mỗi nửa đêm, giữ 30 ngày, không bị phình file mãi mãi
    file_handler = logging.handlers.TimedRotatingFileHandler(
        "logs/bot_activity.log",
        when="midnight",       # Xoay lúc 00:00 mỗi ngày
        interval=1,            # Mỗi 1 ngày
        backupCount=30,        # Giữ 30 ngày log cũ
        encoding="utf-8",
        utc=False,
    )
    file_handler.suffix = "%Y-%m-%d"   # logs/bot_activity.log.2026-06-01
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter(
        '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%d/%m/%Y %H:%M:%S'
    )
    file_handler.setFormatter(file_format)

    console_handler = logging.StreamHandler()
    # ✅ Mặc định WARNING (console mượt, giảm I/O). Đặt CONSOLE_LOG_LEVEL=INFO trong .env nếu muốn xem chi tiết.
    console_level_name = os.getenv("CONSOLE_LOG_LEVEL", "WARNING").upper()
    console_handler.setLevel(getattr(logging, console_level_name, logging.WARNING))
    console_format = ColoredFormatter(
        '[%(asctime)s] [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_format)

    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


logger = setup_logger()