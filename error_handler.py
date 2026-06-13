"""
🛡️ ERROR HANDLER & RETRY MECHANISM
Xử lý lỗi toàn diện với retry logic, circuit breaker, fallback strategies
"""

import asyncio
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, Any, Optional, Dict
from logger_setup import logger


class ErrorType(Enum):
    """Phân loại lỗi"""
    NETWORK_ERROR = "network"
    TIMEOUT_ERROR = "timeout"
    CLOUDFLARE_ERROR = "cloudflare"
    SELECTOR_ERROR = "selector"
    SUBMIT_ERROR = "submit"
    BROWSER_CRASH = "browser_crash"
    UNKNOWN_ERROR = "unknown"


class RetryStrategy(Enum):
    """Chiến lược retry"""
    EXPONENTIAL_BACKOFF = "exponential"
    LINEAR_BACKOFF = "linear"
    FIXED_DELAY = "fixed"
    AGGRESSIVE = "aggressive"


class CircuitBreakerState(Enum):
    """Trạng thái Circuit Breaker"""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit Breaker Pattern"""
    
    def __init__(self, 
                 failure_threshold: int = 5,
                 recovery_timeout: int = 60,
                 success_threshold: int = 2):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.name = "CircuitBreaker"
    
    async def call_async(self, func, *args, **kwargs):
        """Gọi async function với circuit breaker"""
        if self.state == CircuitBreakerState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitBreakerState.HALF_OPEN
                self.success_count = 0
                logger.info(f"🔄 {self.name}: HALF_OPEN - Kiểm tra khôi phục")
            else:
                raise Exception(f"🔌 {self.name}: OPEN - Từ chối request")
        
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
    
    def _on_success(self):
        self.failure_count = 0
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                logger.info(f"✅ {self.name}: CLOSED - Hệ thống khôi phục")
    
    def _on_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
            logger.warning(f"🔌 {self.name}: Quay lại OPEN")
        elif self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
            logger.error(f"🔌 {self.name}: OPEN - Fail {self.failure_count}/{self.failure_threshold}")
    
    def _should_attempt_reset(self) -> bool:
        if self.last_failure_time is None:
            return False
        elapsed = (datetime.now() - self.last_failure_time).total_seconds()
        return elapsed >= self.recovery_timeout
    
    def reset(self):
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        logger.info(f"🔄 {self.name}: Reset hoàn toàn")
    
    def get_status(self) -> dict:
        return {
            'state': self.state.value,
            'failure_count': self.failure_count,
            'success_count': self.success_count,
            'is_open': self.state == CircuitBreakerState.OPEN
        }


class RetryHandler:
    """Xử lý Retry với multiple strategies"""
    
    def __init__(self, 
                 max_retries: int = 3,
                 strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF,
                 base_delay: float = 1.0):
        self.max_retries = max_retries
        self.strategy = strategy
        self.base_delay = base_delay
    
    def _calculate_delay(self, attempt: int) -> float:
        if self.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            return self.base_delay * (2 ** attempt)
        elif self.strategy == RetryStrategy.LINEAR_BACKOFF:
            return self.base_delay * (attempt + 1)
        elif self.strategy == RetryStrategy.FIXED_DELAY:
            return self.base_delay
        elif self.strategy == RetryStrategy.AGGRESSIVE:
            return 0.5
        return self.base_delay
    
    async def execute_with_retry(self, 
                                 func: Callable,
                                 *args,
                                 error_handler: Optional[Callable] = None,
                                 **kwargs) -> Any:
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                if attempt > 0:
                    logger.debug(f"🔄 Cố gắng {attempt + 1}/{self.max_retries + 1}...")
                
                result = await func(*args, **kwargs)
                
                if attempt > 0:
                    logger.info(f"✅ Thành công sau {attempt} lần retry")
                
                return result
            
            except Exception as e:
                last_exception = e
                
                if error_handler:
                    try:
                        await error_handler(e, attempt)
                    except:
                        pass
                
                if attempt < self.max_retries:
                    delay = self._calculate_delay(attempt)
                    logger.warning(
                        f"⚠️ Lần {attempt + 1} thất bại: {str(e)[:50]}\n"
                        f"   ⏳ Chờ {delay:.1f}s trước retry..."
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"❌ Tất cả {self.max_retries + 1} lần thất bại")
        
        raise last_exception


class FallbackStrategy:
    """Chiến lược fallback"""
    
    def __init__(self):
        self.strategies = []
    
    def add_strategy(self, name: str, func: Callable):
        self.strategies.append({"name": name, "func": func})
    
    async def execute(self, *args, **kwargs) -> Any:
        for strategy in self.strategies:
            try:
                logger.info(f"🔄 Thử chiến lược: {strategy['name']}")
                result = await strategy['func'](*args, **kwargs)
                logger.info(f"✅ Thành công với: {strategy['name']}")
                return result
            except Exception as e:
                logger.warning(f"❌ {strategy['name']} thất bại: {str(e)[:50]}")
                continue
        
        raise Exception(f"❌ Tất cả {len(self.strategies)} chiến lược thất bại")


class ErrorRecovery:
    """Phục hồi từ lỗi"""
    
    @staticmethod
    async def recover_browser_crash(browser_context, page, user: str):
        try:
            logger.warning(f"🔧 [{user}] Phục hồi browser...")
            try:
                await page.close()
            except:
                pass
            
            page = await browser_context.new_page()
            logger.info(f"✅ [{user}] Browser phục hồi")
            return page
        except Exception as e:
            logger.error(f"❌ [{user}] Không thể phục hồi: {e}")
            return None
    
    @staticmethod
    async def recover_cloudflare_error(page):
        try:
            logger.warning("🛡️ Xử lý Cloudflare error...")
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.5)
            await page.reload(wait_until="domcontentloaded", timeout=10000)
            await asyncio.sleep(1)
            logger.info("✅ Cloudflare xử lý xong")
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi xử lý Cloudflare: {e}")
            return False
    
    @staticmethod
    async def recover_selector_error(page, selectors: list):
        for sel in selectors:
            try:
                element = await page.query_selector(sel)
                if element:
                    logger.info(f"✅ Tìm thấy element: {sel}")
                    return element
            except:
                continue
        
        logger.error("❌ Không tìm thấy element nào")
        return None


# Global Circuit Breakers
CIRCUIT_BREAKERS: Dict[str, CircuitBreaker] = {
    "mm88": CircuitBreaker(failure_threshold=5, recovery_timeout=60),
    "llwin": CircuitBreaker(failure_threshold=5, recovery_timeout=60),
    "new88": CircuitBreaker(failure_threshold=5, recovery_timeout=60),
    "browser": CircuitBreaker(failure_threshold=3, recovery_timeout=30),
}


def get_circuit_breaker(website: str) -> CircuitBreaker:
    key = website.split("//")[1].split(".")[0].lower() if "//" in website else "default"
    for k in CIRCUIT_BREAKERS.keys():
        if k in key.lower():
            return CIRCUIT_BREAKERS[k]
    return CIRCUIT_BREAKERS.get("browser", CircuitBreaker())


def log_circuit_breaker_status():
    logger.info("\n" + "="*60)
    logger.info("🔌 TRẠNG THÁI CIRCUIT BREAKER:")
    for name, cb in CIRCUIT_BREAKERS.items():
        status = cb.get_status()
        state_emoji = "🟢" if status['state'] == 'closed' else "🔴" if status['state'] == 'open' else "🟡"
        logger.info(f"   {state_emoji} {name.upper()}: {status['state'].upper()}")
    logger.info("="*60 + "\n")
