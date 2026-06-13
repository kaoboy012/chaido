"""
📊 DATABASE MANAGEMENT (v4.1 - ASYNC QUEUE SAFE)
- Tránh asyncio.Lock với SQLite
- Dùng queue để tuần tự hóa writes
- WAL mode + high timeout cho concurrency
"""

import sqlite3
import threading
import asyncio
import queue
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict
from logger_setup import logger


class CodeDatabase:
    """Quản lý database SQLite - async-safe với queue"""

    def __init__(self, db_path: str = "data/code_history.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # ✅ Dùng queue để tuần tự hóa writes (không asyncio.Lock!)
        self._write_queue = queue.Queue()
        self._write_thread = None
        
        # Kết nối chính (chỉ cho thread worker)
        self.conn = None
        self._init_connection()
        self._init_tables()
        
        # Bắt đầu worker thread
        self._start_write_worker()
    
    def _init_connection(self):
        """Tạo connection với WAL mode và timeout cao"""
        self.conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,  # Allow từ thread khác
            timeout=30.0  # Chờ 30s trước raise locked
        )
        self.conn.row_factory = sqlite3.Row
        self._optimize_connection()
    
    def _optimize_connection(self):
        """Bật WAL + pragmas tăng tốc"""
        pragmas = [
            "PRAGMA journal_mode=WAL",        # ✅ Cho phép đọc song song
            "PRAGMA synchronous=NORMAL",      # Cân bằng tốc độ & an toàn
            "PRAGMA cache_size=-32000",       # 32MB cache
            "PRAGMA temp_store=MEMORY",       # Temp trong RAM
            "PRAGMA mmap_size=268435456",     # 256MB memory-mapped I/O
            "PRAGMA busy_timeout=30000",      # 30s retry khi locked
        ]
        for pragma in pragmas:
            try:
                self.conn.execute(pragma)
            except Exception as e:
                logger.warning(f"⚠️ Pragma failed: {pragma} - {e}")
        self.conn.commit()

    def _init_tables(self):
        """Tạo bảng và index"""
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS code_submission (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    account TEXT NOT NULL,
                    website TEXT NOT NULL,
                    status TEXT,
                    result TEXT,
                    submitted_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(code, account)
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS submission_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    account TEXT NOT NULL,
                    website TEXT NOT NULL,
                    status TEXT,
                    result TEXT,
                    attempt INTEGER,
                    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS account_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account TEXT NOT NULL UNIQUE,
                    total_submitted INTEGER DEFAULT 0,
                    total_success INTEGER DEFAULT 0,
                    total_failed INTEGER DEFAULT 0,
                    last_submit TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS website_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    website TEXT NOT NULL UNIQUE,
                    total_submitted INTEGER DEFAULT 0,
                    total_success INTEGER DEFAULT 0,
                    total_failed INTEGER DEFAULT 0,
                    last_submit TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tạo index
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_code ON code_submission(code)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_account ON submission_log(account)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_website ON submission_log(website)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_submitted_at ON submission_log(submitted_at)")
            
            self.conn.commit()
            logger.info("✅ Database tables khởi tạo xong (WAL mode)")
        except Exception as e:
            logger.error(f"❌ Lỗi tạo tables: {e}")
            raise

    def _start_write_worker(self):
        """Bắt đầu worker thread xử lý write queue"""
        self._write_thread = threading.Thread(
            target=self._write_worker_loop,
            daemon=True
        )
        self._write_thread.start()
        logger.info("✅ Database write worker thread started")

    def _write_worker_loop(self):
        """Worker thread xử lý write sequentially"""
        while True:
            try:
                # Chờ task từ queue (blocking)
                task = self._write_queue.get(timeout=1)
                
                if task is None:  # Sentinel: dừng worker
                    break
                
                func, args, kwargs = task
                
                try:
                    # Thực thi write operation
                    func(*args, **kwargs)
                except Exception as e:
                    logger.error(f"❌ Write worker error: {e}")
                
                self._write_queue.task_done()
                
            except queue.Empty:
                # Timeout - tiếp tục
                continue
            except Exception as e:
                logger.error(f"❌ Write worker fatal error: {e}")
                break

    def record_submission(self, code: str, account: str, website: str,
                          status: str, result: str = None, attempt: int = 1):
        """
        Ghi submission vào queue (async-safe, không block)
        
        Trả về ngay lập tức, thực tế ghi xảy ra trong background thread
        """
        def _write():
            try:
                now = datetime.now()
                
                self.conn.execute("""
                    INSERT INTO submission_log (code, account, website, status, result, attempt)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (code, account, website, status, result, attempt))

                self.conn.execute("""
                    INSERT OR REPLACE INTO code_submission
                    (code, account, website, status, result, submitted_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (code, account, website, status, result, now))

                self.conn.execute("""
                    INSERT INTO account_stats (account, total_submitted, last_submit)
                    VALUES (?, 1, ?)
                    ON CONFLICT(account) DO UPDATE SET
                        total_submitted = total_submitted + 1,
                        last_submit = excluded.last_submit
                """, (account, now))

                self.conn.execute("""
                    INSERT INTO website_stats (website, total_submitted, last_submit)
                    VALUES (?, 1, ?)
                    ON CONFLICT(website) DO UPDATE SET
                        total_submitted = total_submitted + 1,
                        last_submit = excluded.last_submit
                """, (website, now))

                if status == "SUCCESS":
                    self.conn.execute(
                        "UPDATE account_stats SET total_success = total_success + 1 WHERE account = ?",
                        (account,))
                    self.conn.execute(
                        "UPDATE website_stats SET total_success = total_success + 1 WHERE website = ?",
                        (website,))
                elif status == "FAILED":
                    self.conn.execute(
                        "UPDATE account_stats SET total_failed = total_failed + 1 WHERE account = ?",
                        (account,))
                    self.conn.execute(
                        "UPDATE website_stats SET total_failed = total_failed + 1 WHERE website = ?",
                        (website,))

                self.conn.commit()
                logger.debug(f"💾 [{account}] Code {code}: {status}")

            except sqlite3.IntegrityError:
                try:
                    # Code đã tồn tại, cập nhật
                    self.conn.execute("""
                        UPDATE code_submission 
                        SET status = ?, result = ?, submitted_at = ?
                        WHERE code = ?
                    """, (status, result, datetime.now(), code))
                    self.conn.commit()
                    logger.debug(f"⚠️ Code {code} cập nhật record")
                except Exception as e:
                    logger.error(f"❌ Update error: {e}")
                    self.conn.rollback()
            except Exception as e:
                logger.error(f"❌ Write error: {e}")
                try:
                    self.conn.rollback()
                except:
                    pass
        
        # ✅ Đưa task vào queue (không chờ)
        self._write_queue.put((_write, (), {}))

    def get_code_status(self, code: str) -> Optional[Dict]:
        """Read operation - từ main connection"""
        try:
            row = self.conn.execute(
                "SELECT * FROM code_submission WHERE code = ?", (code,)
            ).fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"❌ Lỗi get code status: {e}")
            return None

    def get_account_stats(self, account: str) -> Optional[Dict]:
        """Read operation"""
        try:
            row = self.conn.execute(
                "SELECT * FROM account_stats WHERE account = ?", (account,)
            ).fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"❌ Lỗi get account stats: {e}")
            return None

    def get_all_account_stats(self) -> List[Dict]:
        """Read operation"""
        try:
            rows = self.conn.execute(
                "SELECT * FROM account_stats ORDER BY total_submitted DESC"
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"❌ Lỗi get all account stats: {e}")
            return []

    def get_success_rate(self, account: str = None) -> float:
        """Read operation"""
        try:
            if account:
                row = self.conn.execute(
                    "SELECT total_success, total_submitted FROM account_stats WHERE account = ?",
                    (account,)
                ).fetchone()
            else:
                row = self.conn.execute(
                    "SELECT SUM(total_success), SUM(total_submitted) FROM account_stats"
                ).fetchone()
            if row and row[1] and row[1] > 0:
                return (row[0] / row[1]) * 100
            return 0.0
        except Exception as e:
            logger.error(f"❌ Lỗi tính success rate: {e}")
            return 0.0

    def print_stats(self):
        """In thống kê"""
        try:
            logger.info("\n" + "="*70)
            logger.info("📊 THỐNG KÊ:")
            logger.info("\n📱 ACCOUNT STATS:")
            for stat in self.get_all_account_stats()[:10]:
                rate = (stat['total_success'] / stat['total_submitted'] * 100) if stat['total_submitted'] > 0 else 0
                logger.info(
                    f"   {stat['account']}: "
                    f"✅ {stat['total_success']} | ❌ {stat['total_failed']} | "
                    f"Tổng: {stat['total_submitted']} | Tỉ lệ: {rate:.1f}%"
                )
            logger.info("="*70 + "\n")
        except Exception as e:
            logger.error(f"❌ Lỗi print stats: {e}")

    def close(self):
        """Đóng database và worker thread"""
        try:
            # Đợi queue drain
            self._write_queue.join()
            
            # Gửi sentinel để dừng worker
            self._write_queue.put(None)
            
            # Đợi thread kết thúc
            if self._write_thread and self._write_thread.is_alive():
                self._write_thread.join(timeout=5)
            
            # Đóng connection
            if self.conn:
                self.conn.close()
            
            logger.info("✅ Database đã đóng")
        except Exception as e:
            logger.error(f"❌ Lỗi close database: {e}")


_db_instance = None

def init_database(db_path: str = "data/code_history.db") -> CodeDatabase:
    """Khởi tạo database singleton"""
    global _db_instance
    if _db_instance is None:
        _db_instance = CodeDatabase(db_path)
    return _db_instance

def get_database() -> CodeDatabase:
    """Lấy database instance"""
    global _db_instance
    if _db_instance is None:
        _db_instance = init_database()
    return _db_instance
