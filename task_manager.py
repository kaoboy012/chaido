"""
📋 TASK MANAGER (v4.0 - SIMPLIFIED)
- Giữ nguyên interface (init_task_system, get_task_queue, get_scheduler)
- Loại bỏ execute_task retry loop (main_script tự xử lý retry)
- Scheduler chạy check mỗi 60s nhưng dùng asyncio.sleep chính xác hơn
"""

import asyncio
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Optional, Callable
from logger_setup import logger


class TaskPriority(Enum):
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"
    SKIPPED = "skipped"


@dataclass
class CodeTask:
    code: str
    channel_id: int
    account_username: str
    target_url: str
    priority: TaskPriority = TaskPriority.NORMAL
    retry_count: int = 0
    max_retries: int = 2
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[str] = None
    error: Optional[str] = None

    def __lt__(self, other):
        if self.priority.value != other.priority.value:
            return self.priority.value < other.priority.value
        return self.created_at < other.created_at

    def mark_started(self):
        self.status = TaskStatus.RUNNING
        self.started_at = datetime.now()

    def mark_success(self, result: str):
        self.status = TaskStatus.SUCCESS
        self.completed_at = datetime.now()
        self.result = result

    def mark_failed(self, error: str):
        self.status = TaskStatus.FAILED
        self.completed_at = datetime.now()
        self.error = error

    def mark_retrying(self):
        self.status = TaskStatus.RETRYING
        self.retry_count += 1

    def get_duration(self) -> Optional[float]:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


class TaskQueue:
    """Priority queue - interface giữ nguyên, bỏ retry loop"""

    def __init__(self, max_concurrent: int = 3):
        self.queue = asyncio.PriorityQueue()
        self.task_history: List[CodeTask] = []
        self.running_count = 0
        self.max_concurrent = max_concurrent
        self._lock = asyncio.Lock()
        # Giới hạn history để không tốn RAM
        self._MAX_HISTORY = 1000

    async def add_task(self, task: CodeTask):
        await self.queue.put((task.priority.value, id(task), task))
        logger.debug(f"📋 Task: {task.code} | {task.account_username} | {task.priority.name}")

    async def get_next_task(self) -> Optional[CodeTask]:
        if self.queue.empty():
            return None
        try:
            _, _, task = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            return task
        except asyncio.TimeoutError:
            return None

    async def can_execute_task(self) -> bool:
        async with self._lock:
            if self.running_count < self.max_concurrent:
                self.running_count += 1
                return True
            return False

    async def execute_task(self, task: CodeTask, executor: Callable):
        try:
            task.mark_started()
            result = await executor(task)
            if result.get("success"):
                task.mark_success(result.get("message", "Thành công"))
            else:
                task.mark_failed(result.get("message", "Thất bại"))
        except Exception as e:
            task.mark_failed(str(e))
            logger.error(f"❌ Task lỗi: {task.code} - {e}")
        finally:
            async with self._lock:
                self.task_history.append(task)
                if len(self.task_history) > self._MAX_HISTORY:
                    self.task_history = self.task_history[-self._MAX_HISTORY:]
                self.running_count -= 1

    def get_stats(self) -> dict:
        total = len(self.task_history)
        success = sum(1 for t in self.task_history if t.status == TaskStatus.SUCCESS)
        failed = sum(1 for t in self.task_history if t.status == TaskStatus.FAILED)
        return {
            "total": total,
            "success": success,
            "failed": failed,
            "pending": self.queue.qsize(),
            "running": self.running_count,
            "success_rate": f"{(success/total*100):.1f}%" if total > 0 else "N/A",
        }

    def print_stats(self):
        stats = self.get_stats()
        logger.info("\n" + "="*70)
        logger.info("📊 TASK QUEUE:")
        logger.info(f"   ✅ {stats['success']} | ❌ {stats['failed']} | ⏳ {stats['pending']} | 🔄 {stats['running']}")
        logger.info(f"   Tỉ lệ: {stats['success_rate']}")
        logger.info("="*70 + "\n")


class Scheduler:
    """Scheduler đơn giản - chạy recurring tasks đúng giờ"""

    def __init__(self):
        self._tasks = []
        self.is_running = False

    def schedule_every(self, hours: int, func: Callable, *args, **kwargs):
        self._tasks.append({
            "interval_seconds": hours * 3600,
            "func": func,
            "args": args,
            "kwargs": kwargs,
            "last_execution": None,
        })

    def schedule_at(self, when: datetime, func: Callable, *args, **kwargs):
        self._tasks.append({
            "when": when,
            "func": func,
            "args": args,
            "kwargs": kwargs,
            "executed": False,
        })

    async def start(self):
        self.is_running = True
        logger.info("🕐 Scheduler bắt đầu")
        while self.is_running:
            now = datetime.now()
            for task in self._tasks:
                try:
                    if "when" in task:
                        if not task["executed"] and now >= task["when"]:
                            await task["func"](*task["args"], **task["kwargs"])
                            task["executed"] = True
                    elif "interval_seconds" in task:
                        last = task["last_execution"]
                        if last is None or (now - last).total_seconds() >= task["interval_seconds"]:
                            await task["func"](*task["args"], **task["kwargs"])
                            task["last_execution"] = now
                except Exception as e:
                    logger.error(f"❌ Scheduler lỗi: {e}")
            await asyncio.sleep(60)

    def stop(self):
        self.is_running = False


# Global instances
_task_queue = None
_scheduler = None


def init_task_system(max_concurrent: int = 3):
    global _task_queue, _scheduler
    _task_queue = TaskQueue(max_concurrent=max_concurrent)
    _scheduler = Scheduler()
    return _task_queue, _scheduler


def get_task_queue() -> TaskQueue:
    global _task_queue
    if _task_queue is None:
        init_task_system()
    return _task_queue


def get_scheduler() -> Scheduler:
    global _scheduler
    if _scheduler is None:
        init_task_system()
    return _scheduler