"""
⏱️ RATE LIMITER & ANTI-BAN SYSTEM (v4.0 - OPTIMIZED)
- Loại bỏ SmartDelayInjector (không dùng trong main_script)
- RateLimiter chỉ delay khi thực sự cần (không delay nếu đã đủ thời gian)
- check_account_health bỏ mở tab google (tốn tài nguyên vô ích)
"""

import time
import asyncio
from typing import Dict
from logger_setup import logger


class RateLimiter:
    """Rate limiter tối giản - chỉ delay khi cần thiết"""

    def __init__(self, min_delay: float = 0.5, requests_per_minute: int = 30):
        self.min_delay = min_delay
        self.requests_per_minute = requests_per_minute
        self.last_request: Dict[str, float] = {}
        self.request_times: Dict[str, list] = {}

    async def wait_if_needed(self, account: str) -> float:
        now = time.time()

        # Delay tối thiểu giữa 2 request
        last = self.last_request.get(account)
        if last is not None:
            elapsed = now - last
            if elapsed < self.min_delay:
                await asyncio.sleep(self.min_delay - elapsed)
                now = time.time()

        # Rate limit per minute
        history = self.request_times.setdefault(account, [])
        cutoff = now - 60
        # Xóa các entry cũ
        while history and history[0] < cutoff:
            history.pop(0)

        if len(history) >= self.requests_per_minute:
            wait = history[0] + 60 - now
            if wait > 0:
                logger.warning(f"⏱️ Rate limit [{account}]: chờ {wait:.1f}s")
                await asyncio.sleep(wait)
                now = time.time()

        self.last_request[account] = now
        history.append(now)
        return now

    def get_remaining_time(self, account: str) -> float:
        if account not in self.last_request:
            return 0.0
        return max(0.0, self.min_delay - (time.time() - self.last_request[account]))

    def get_stats(self, account: str) -> dict:
        history = self.request_times.get(account, [])
        now = time.time()
        recent = sum(1 for t in history if t > now - 60)
        return {
            "requests_last_minute": recent,
            "remaining_time": self.get_remaining_time(account),
            "utilization": f"{(recent / self.requests_per_minute * 100):.1f}%",
        }


class AntiDetectionManager:
    """Anti-detection tối giản - chỉ rate limit, không delay random vô ích"""

    def __init__(self):
        self.rate_limiter = RateLimiter(min_delay=0.5, requests_per_minute=30)

    async def apply_all_protections(self, account: str):
        """Chỉ enforce rate limit, không thêm delay ngẫu nhiên làm chậm bot"""
        await self.rate_limiter.wait_if_needed(account)

    def print_stats(self):
        logger.info("\n" + "="*70)
        logger.info("🛡️ ANTI-DETECTION STATS:")
        for account in self.rate_limiter.last_request:
            stats = self.rate_limiter.get_stats(account)
            logger.info(
                f"  [{account}] {stats['requests_last_minute']}/{self.rate_limiter.requests_per_minute} req/min "
                f"({stats['utilization']}) | wait: {stats['remaining_time']:.2f}s"
            )
        logger.info("="*70 + "\n")


_anti_detection = None

def init_anti_detection() -> AntiDetectionManager:
    global _anti_detection
    _anti_detection = AntiDetectionManager()
    logger.info("✅ Anti-Detection khởi tạo xong (v4.0)")
    return _anti_detection

def get_anti_detection() -> AntiDetectionManager:
    global _anti_detection
    if _anti_detection is None:
        _anti_detection = init_anti_detection()
    return _anti_detection