"""
📊 MONITORING & HEALTH SYSTEM (v4.0 - OPTIMIZED)
- HealthMonitor: check interval tăng lên 120s (cũ 60s), psutil chạy non-blocking
- PerformanceMonitor: giới hạn history 500 entry thay vì unlimited
- Loại bỏ MonitoringSystem wrapper không cần thiết
"""

import asyncio
import psutil
from datetime import datetime, timedelta
from typing import Dict, List
from logger_setup import logger


class HealthMonitor:
    """Giám sát CPU/RAM - nhẹ, không block event loop"""

    def __init__(self, check_interval: int = 120):
        self.check_interval = check_interval
        self.is_running = False
        self.cpu_threshold = 85.0
        self.memory_threshold = 85.0

    async def _get_metrics_async(self) -> dict:
        """Lấy metrics trong thread pool để không block event loop"""
        loop = asyncio.get_running_loop()
        def _get():
            mem = psutil.virtual_memory()
            return {
                "cpu": psutil.cpu_percent(interval=0.5),
                "memory_pct": mem.percent,
                "memory_mb": mem.used // (1024 * 1024),
            }
        return await loop.run_in_executor(None, _get)

    async def start(self):
        self.is_running = True
        logger.info(f"🏥 Health Monitor bắt đầu (interval={self.check_interval}s)")
        while self.is_running:
            try:
                metrics = await self._get_metrics_async()
                warnings = []
                if metrics["cpu"] > self.cpu_threshold:
                    warnings.append(f"CPU cao: {metrics['cpu']:.1f}%")
                if metrics["memory_pct"] > self.memory_threshold:
                    warnings.append(f"RAM cao: {metrics['memory_pct']:.1f}% ({metrics['memory_mb']}MB)")
                for w in warnings:
                    logger.warning(f"⚠️ {w}")
            except Exception as e:
                logger.debug(f"⚠️ Health check lỗi: {e}")
            await asyncio.sleep(self.check_interval)

    def stop(self):
        self.is_running = False

    def get_current_metrics(self) -> dict:
        """Sync version - chỉ dùng khi không có event loop"""
        try:
            mem = psutil.virtual_memory()
            return {
                "cpu": psutil.cpu_percent(interval=0.2),
                "memory_pct": mem.percent,
                "memory_mb": mem.used // (1024 * 1024),
            }
        except Exception:
            return {"cpu": 0, "memory_pct": 0, "memory_mb": 0}


class AlertManager:
    """Quản lý alerts - đơn giản hóa"""

    def __init__(self):
        self._alerts: List[dict] = []

    def add_alert(self, level: str, message: str, context: dict = None):
        entry = {
            "level": level,
            "message": message,
            "timestamp": datetime.now(),
            "context": context or {},
        }
        self._alerts.append(entry)
        # Giữ tối đa 200 alerts
        if len(self._alerts) > 200:
            self._alerts = self._alerts[-200:]
        emoji = {"INFO": "ℹ️", "WARNING": "⚠️", "ERROR": "❌", "CRITICAL": "🔴"}.get(level, "📢")
        logger.info(f"{emoji} ALERT {level}: {message}")
        return entry

    def get_active_alerts(self) -> List[dict]:
        cutoff = datetime.now() - timedelta(minutes=30)
        return [a for a in self._alerts if a["timestamp"] > cutoff]

    def print_alerts(self):
        active = self.get_active_alerts()
        if not active:
            logger.info("✅ Không có active alerts")
            return
        logger.info(f"🚨 ACTIVE ALERTS ({len(active)}):")
        for a in active:
            logger.info(f"   {a['level']}: {a['message']}")


class PerformanceMonitor:
    """Giám sát performance - giới hạn history để không tốn RAM"""

    MAX_HISTORY = 500

    def __init__(self):
        self.task_times: Dict[str, List[float]] = {}
        self.task_success: Dict[str, int] = {}
        self.task_failed: Dict[str, int] = {}

    def record_task(self, task_name: str, duration: float, success: bool = True):
        times = self.task_times.setdefault(task_name, [])
        times.append(duration)
        # Giới hạn history
        if len(times) > self.MAX_HISTORY:
            self.task_times[task_name] = times[-self.MAX_HISTORY:]

        if success:
            self.task_success[task_name] = self.task_success.get(task_name, 0) + 1
        else:
            self.task_failed[task_name] = self.task_failed.get(task_name, 0) + 1

    def get_task_stats(self, task_name: str) -> dict:
        times = self.task_times.get(task_name, [])
        if not times:
            return {}
        s = self.task_success.get(task_name, 0)
        f = self.task_failed.get(task_name, 0)
        total = s + f
        return {
            "task_name": task_name,
            "total": total,
            "success": s,
            "failed": f,
            "avg_duration": sum(times) / len(times),
            "min_duration": min(times),
            "max_duration": max(times),
            "success_rate": (s / total * 100) if total > 0 else 0,
        }

    def print_stats(self):
        if not self.task_times:
            logger.info("ℹ️ Không có performance data")
            return
        logger.info("\n" + "="*70)
        logger.info("⚡ PERFORMANCE STATS:")
        for name in self.task_times:
            st = self.get_task_stats(name)
            logger.info(
                f"  {name}: ✅{st['success']} ❌{st['failed']} "
                f"| avg={st['avg_duration']:.2f}s | rate={st['success_rate']:.1f}%"
            )
        logger.info("="*70 + "\n")


# Global instances
_health_monitor = None
_alert_manager = None
_perf_monitor = None


def init_monitoring():
    global _health_monitor, _alert_manager, _perf_monitor
    _health_monitor = HealthMonitor(check_interval=120)
    _alert_manager = AlertManager()
    _perf_monitor = PerformanceMonitor()
    # Khởi động health monitor background
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_health_monitor.start())
    except RuntimeError:
        pass  # Không có event loop đang chạy - health monitor sẽ không tự khởi động
    logger.info("✅ Monitoring khởi tạo xong (v4.0)")
    return _health_monitor, _alert_manager, _perf_monitor


def get_health_monitor() -> HealthMonitor:
    global _health_monitor
    if _health_monitor is None:
        init_monitoring()
    return _health_monitor


def get_alert_manager() -> AlertManager:
    global _alert_manager
    if _alert_manager is None:
        init_monitoring()
    return _alert_manager


def get_performance_monitor() -> PerformanceMonitor:
    global _perf_monitor
    if _perf_monitor is None:
        init_monitoring()
    return _perf_monitor