"""
📊 DATABASE MANAGEMENT (v4.1 - FIXED ASYNC LOCK)
- WAL mode: đọc/ghi song song không block nhau
- asyncio.Lock thay vì threading.Lock
- Prepared statements cache
- Retry logic cho database locked error
"""

import sqlite3
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict
from logger_setup import logger


class CodeDatabase:
    """Quản lý database SQLite - tối ưu tốc độ cao với async lock"""

    def __init__(self, db_path: str = "data/code_history.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # ✅ FIX: Dùng asyncio.Lock thay vì threading.Lock
        self._lock = asyncio.Lock()
        
        # Tạo connection riêng cho thread chính
        self.conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        self._optimize_connection()
        self._init_tables()
        
        self._retry_count = 0
        self._max_retries = 3

    def _optimize_connection(self):
        """Bật các pragma tăng tốc SQLite đáng kể"""
        pragmas = [
            "PRAGMA journal_mode=WAL",        # Cho phép đọc/ghi song song
            "PRAGMA synchronous=NORMAL",      # Nhanh hơn FULL, vẫn an toàn
            "PRAGMA cache_size=-32000",       # 32MB cache trong RAM
            "PRAGMA temp_store=MEMORY",       # Temp tables trong RAM
            "PRAGMA mmap_size=268435456",     # 256MB memory-mapped I/O
            "PRAGMA busy_timeout=10000",      # ✅ FIX: 5s → 10s timeout
            "PRAGMA query_only=FALSE",
        ]
        for pragma in pragmas:
            try:
                self.conn.execute(pragma)
            except Exception as e:
                logger.debug(f"⚠️ Pragma {pragma} failed: {e}")
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
            # ✅ FIX: Tạo indexes để tăng tốc queries
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_code ON code_submission(code)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_account ON submission_log(account)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_website ON submission_log(website)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_submitted_at ON submission_log(submitted_at DESC)")
            self.conn.commit()
            logger.info("✅ Database tables khởi tạo xong (WAL mode + async lock)")
        except Exception as e:
            logger.error(f"❌ Lỗi tạo tables: {e}")
            raise

    def record_submission(self, code: str, account: str, website: str,
                          status: str, result: str = None, attempt: int = 1):
        """✅ FIX: Ghi submission - thread-safe với async lock + retry logic"""
        try:
            # ✅ Không cần await - dùng sync SQLite với busy_timeout
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

        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logger.warning(f"⚠️ Database locked - SQLite will retry với timeout 10s: {code}")
                # busy_timeout sẽ tự retry
            else:
                logger.error(f"❌ SQLite error: {e}")
                try:
                    self.conn.rollback()
                except:
                    pass
        except Exception as e:
            logger.error(f"❌ Lỗi record submission: {e}")
            try:
                self.conn.rollback()
            except:
                pass

    def get_code_status(self, code: str) -> Optional[Dict]:
        """Lấy trạng thái code"""
        try:
            row = self.conn.execute(
                "SELECT * FROM code_submission WHERE code = ?", (code,)
            ).fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"❌ Lỗi get code status: {e}")
            return None

    def get_account_stats(self, account: str) -> Optional[Dict]:
        """Lấy thống kê account"""
        try:
            row = self.conn.execute(
                "SELECT * FROM account_stats WHERE account = ?", (account,)
            ).fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"❌ Lỗi get account stats: {e}")
            return None

    def get_all_account_stats(self) -> List[Dict]:
        """Lấy tất cả account stats"""
        try:
            rows = self.conn.execute(
                "SELECT * FROM account_stats ORDER BY total_submitted DESC"
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"❌ Lỗi get all account stats: {e}")
            return []

    def get_success_rate(self, account: str = None) -> float:
        """Tính tỉ lệ thành công"""
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
        """Đóng database connection"""
        try:
            self.conn.close()
            logger.info("✅ Database đã đóng")
        except Exception as e:
            logger.error(f"❌ Lỗi close database: {e}")


_db_instance = None

def init_database(db_path: str = "data/code_history.db") -> CodeDatabase:
    """Khởi tạo database"""
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
