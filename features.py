"""
✨ THÊM CÁC FEATURES MỚI
Config validation, Statistics, Memory monitoring, Version check
"""

import os
import asyncio
import psutil
import time
import shutil
import signal
from datetime import datetime, timedelta
from pathlib import Path
from logger_setup import logger
from config import Config

# ==========================================
# 📌 VERSION CHECK
# ==========================================

BOT_VERSION = "3.6.2"
BOT_BUILD_DATE = "2026-05-19"

def print_version_info():
    """In thong tin version - 1 dong gon"""
    try:
        import telethon
        import playwright
        try:
            from playwright._impl._driver import compute_driver_executable
            pw_version = "installed"
        except Exception:
            pw_version = "unknown"
        try:
            from importlib.metadata import version as pkg_version
            pw_version = pkg_version("playwright")
        except Exception:
            pass

        logger.info(
            f"📦 Bot v{BOT_VERSION} ({BOT_BUILD_DATE}) | "
            f"Telethon {telethon.__version__} | Playwright {pw_version}"
        )
    except Exception as e:
        logger.error(f"❌ Loi lay version: {e}")

# ==========================================
# ✅ CONFIG VALIDATION
# ==========================================

class ConfigValidator:
    """Kiem tra config hop le"""
    
    @staticmethod
    def validate_all():
        """Kiem tra toan bo config"""
        
        logger.info("\n" + "="*70)
        logger.info("🔍 KIEM TRA CONFIG...")
        logger.info("="*70)
        
        errors = []
        warnings = []
        
        # 2. Check session
        if not Config.SESSION_NAME:
            errors.append("❌ SESSION_NAME trong")
        
        # 3. Check channels
        if not Config.CHANNEL_CONFIG:
            errors.append("❌ Khong co channel config")
        else:
            if len(Config.CHANNEL_CONFIG) == 0:
                errors.append("❌ CHANNEL_CONFIG trong")
        
        # 4. Check delays
        if Config.MIN_DELAY_BETWEEN_SUBMITS < 0.5:
            warnings.append("⚠️ MIN_DELAY_BETWEEN_SUBMITS qua nho (khuyen nghi >= 0.5s)")
        
        if Config.MIN_DELAY_BETWEEN_SUBMITS > 10:
            warnings.append("⚠️ MIN_DELAY_BETWEEN_SUBMITS qua lon (khuyen nghi <= 10s)")
        
        # 5. Check requests per minute
        if Config.REQUESTS_PER_MINUTE < 5:
            warnings.append("⚠️ REQUESTS_PER_MINUTE qua thap (khuyen nghi >= 5)")
        
        if Config.REQUESTS_PER_MINUTE > 100:
            warnings.append("⚠️ REQUESTS_PER_MINUTE qua cao (khuyen nghi <= 100)")
        
        # 6. Check timeouts
        if Config.PAGE_LOAD_TIMEOUT < 10000:
            warnings.append("⚠️ PAGE_LOAD_TIMEOUT qua thap (khuyen nghi >= 10000ms)")
        
        if Config.CLOUDFLARE_WAIT_TIMEOUT < 30000:
            warnings.append("⚠️ CLOUDFLARE_WAIT_TIMEOUT qua thap (khuyen nghi >= 30000ms)")
        
        # 7. Check concurrent tasks
        if Config.MAX_CONCURRENT_SUBMITS < 1:
            errors.append("❌ MAX_CONCURRENT_SUBMITS phai >= 1")
        
        if Config.MAX_CONCURRENT_SUBMITS > 10:
            warnings.append("⚠️ MAX_CONCURRENT_SUBMITS qua lon (khuyen nghi <= 10)")
        
        # 8. Check code length
        if Config.CODE_MIN_LENGTH < 4:
            warnings.append("⚠️ CODE_MIN_LENGTH qua thap (khuyen nghi >= 4)")
        
        if Config.CODE_MAX_LENGTH > 20:
            warnings.append("⚠️ CODE_MAX_LENGTH qua lon (khuyen nghi <= 20)")
        
        if Config.CODE_MIN_LENGTH >= Config.CODE_MAX_LENGTH:
            errors.append("❌ CODE_MIN_LENGTH phai < CODE_MAX_LENGTH")
        
        # 9. Check database path
        try:
            db_path = Path(Config.DATABASE_PATH)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"   ✅ Database path: {Config.DATABASE_PATH}")
        except Exception as e:
            errors.append(f"❌ Loi database path: {e}")
        
        # 10. Check log path
        try:
            log_path = Path(Config.LOG_FILE)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"   ✅ Log path: {Config.LOG_FILE}")
        except Exception as e:
            errors.append(f"❌ Loi log path: {e}")
        
        # Print warnings
        if warnings:
            logger.warning("\n⚠️ CANH BAO:")
            for warning in warnings:
                logger.warning(f"   {warning}")
        
        # Print errors
        if errors:
            logger.error("\n❌ LOI CONFIG:")
            for error in errors:
                logger.error(f"   {error}")
            
            logger.info("\n" + "="*70)
            logger.error("❌ CONFIG KHONG HOP LE! HAY SUA VA CHAY LAI!")
            logger.info("="*70 + "\n")
            return False
        
        logger.info("\n✅ CONFIG HOP LE!")
        logger.info("="*70 + "\n")
        return True

# ==========================================
# 📊 STATISTICS MANAGER
# ==========================================

class StatisticsManager:
    """Quan ly thong ke"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.total_submissions = 0
        self.total_success = 0
        self.total_failed = 0
        self.total_skipped = 0
        self.channels_monitored = len(Config.CHANNEL_CONFIG)
        self.accounts_monitored = sum(
            len(v.get('accounts', [])) for v in Config.CHANNEL_CONFIG.values()
        )
    
    def add_submission(self, success: bool = True):
        """Them 1 submission"""
        self.total_submissions += 1
        if success:
            self.total_success += 1
        else:
            self.total_failed += 1
    
    def add_skipped(self):
        """Them 1 skipped"""
        self.total_skipped += 1
    
    def get_uptime(self) -> str:
        """Lay uptime"""
        elapsed = datetime.now() - self.start_time
        
        days = elapsed.days
        hours = (elapsed.seconds // 3600) % 24
        minutes = (elapsed.seconds // 60) % 60
        seconds = elapsed.seconds % 60
        
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        else:
            return f"{minutes}m {seconds}s"
    
    def get_success_rate(self) -> float:
        """Lay ti le thanh cong"""
        if self.total_submissions == 0:
            return 0.0
        return (self.total_success / self.total_submissions) * 100
    
    def print_dashboard(self):
        """In dashboard thong ke"""
        logger.info("\n" + "="*70)
        logger.info("📊 DASHBOARD THONG KE:")
        logger.info("="*70)
        
        logger.info(f"\n⏱️ THOI GIAN HOAT DONG:")
        logger.info(f"   Uptime: {self.get_uptime()}")
        logger.info(f"   Bat dau: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        logger.info(f"\n📤 SUBMISSIONS:")
        logger.info(f"   ✅ Thanh cong: {self.total_success}")
        logger.info(f"   ❌ That bai: {self.total_failed}")
        logger.info(f"   ⏭️ Bo qua: {self.total_skipped}")
        logger.info(f"   📊 Tong: {self.total_submissions}")
        logger.info(f"   📈 Ti le thanh cong: {self.get_success_rate():.1f}%")
        
        logger.info(f"\n📱 KENH & ACCOUNTS:")
        logger.info(f"   📡 Channels: {self.channels_monitored}")
        logger.info(f"   👥 Accounts: {self.accounts_monitored}")
        
        logger.info("="*70 + "\n")

# Global stats
stats_manager = StatisticsManager()

def get_stats_manager() -> StatisticsManager:
    """Lay stats manager"""
    return stats_manager

# ==========================================
# 💾 SUBMISSION LOGGER
# ==========================================

class SubmissionLogger:
    """Luu log submissions chi tiet"""
    
    def __init__(self, log_file: str = "logs/submissions.log"):
        self.log_file = log_file
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    
    def log_submission(self, code: str, account: str, website: str, result: dict):
        """Ghi submission log"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status = "✅ SUCCESS" if result.get('success') else "❌ FAILED"
        message = result.get('message', 'N/A').replace('\n', ' ')[:100]
        
        log_line = (
            f"[{timestamp}] {status} | "
            f"Code: {code:15} | "
            f"Account: {account:15} | "
            f"Website: {website:30} | "
            f"Msg: {message}\n"
        )
        
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(log_line)
        except Exception as e:
            logger.error(f"❌ Loi ghi submission log: {e}")
    
    def get_summary(self) -> dict:
        """Lay tom tat tu log file"""
        if not Path(self.log_file).exists():
            return {'total': 0, 'success': 0, 'failed': 0}
        
        try:
            with open(self.log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            total = len(lines)
            success = len([l for l in lines if '✅ SUCCESS' in l])
            failed = len([l for l in lines if '❌ FAILED' in l])
            
            return {
                'total': total,
                'success': success,
                'failed': failed,
                'success_rate': (success / total * 100) if total > 0 else 0
            }
        except Exception as e:
            logger.error(f"❌ Loi doc submission log: {e}")
            return {'total': 0, 'success': 0, 'failed': 0}

# Global submission logger
submission_logger = SubmissionLogger()

def get_submission_logger() -> SubmissionLogger:
    """Lay submission logger"""
    return submission_logger

# ==========================================
# 📊 MEMORY MONITOR
# ==========================================

class MemoryMonitor:
    """Theo doi memory usage"""
    
    def __init__(self, threshold: float = 85.0):
        self.threshold = threshold
        self.peak_memory = 0
        self.warning_sent = False
    
    def get_current_memory(self) -> dict:
        """Lay memory hien tai"""
        try:
            memory = psutil.virtual_memory()
            return {
                'percent': memory.percent,
                'used_mb': memory.used // (1024 * 1024),
                'total_mb': memory.total // (1024 * 1024),
                'available_mb': memory.available // (1024 * 1024),
            }
        except Exception as e:
            logger.error(f"❌ Loi lay memory: {e}")
            return {}
    
    async def monitor(self, bot_state, systems):
        """Theo doi memory trong background"""
        logger.info("📊 Memory Monitor bat dau")
        
        while bot_state.is_running:
            try:
                memory = self.get_current_memory()
                
                if not memory:
                    await asyncio.sleep(60)
                    continue
                
                # Update peak
                if memory['percent'] > self.peak_memory:
                    self.peak_memory = memory['percent']
                
                # Check threshold
                if memory['percent'] > self.threshold:
                    if not self.warning_sent:
                        logger.warning(
                            f"⚠️ MEMORY CAO: {memory['percent']:.1f}% "
                            f"({memory['used_mb']}MB / {memory['total_mb']}MB)"
                        )
                        self.warning_sent = True
                        
                        # Trigger cleanup
                        systems['alert_manager'].add_alert(
                            'WARNING',
                            f"Memory cao: {memory['percent']:.1f}%"
                        )
                else:
                    self.warning_sent = False
                
                await asyncio.sleep(60)
            
            except Exception as e:
                logger.error(f"❌ Loi memory monitor: {e}")
                await asyncio.sleep(60)
    
    def print_stats(self):
        """In thong ke memory"""
        memory = self.get_current_memory()
        
        if not memory:
            return
        
        logger.info("\n" + "="*70)
        logger.info("📊 MEMORY STATS:")
        logger.info(f"   Current: {memory['percent']:.1f}% ({memory['used_mb']}MB)")
        logger.info(f"   Available: {memory['available_mb']}MB")
        logger.info(f"   Total: {memory['total_mb']}MB")
        logger.info(f"   Peak: {self.peak_memory:.1f}%")
        logger.info("="*70 + "\n")

# Global memory monitor
memory_monitor = MemoryMonitor(threshold=85.0)

def get_memory_monitor() -> MemoryMonitor:
    """Lay memory monitor"""
    return memory_monitor

# ==========================================
# 🛡️ GRACEFUL SHUTDOWN HANDLER
# ==========================================

class GracefulShutdownHandler:
    """Xu ly Ctrl+C gracefully"""
    
    def __init__(self):
        self.shutdown_initiated = False
        self.shutdown_complete = False
    
    def setup(self, bot_state):
        """Setup signal handlers"""
        
        def signal_handler(signum, frame):
            if self.shutdown_initiated:
                logger.warning("\n⚠️ Nhan tin hieu shutdown lan 2 - Force exit!")
                exit(1)
            
            logger.info("\n" + "="*70)
            logger.info("🛑 NHAN TIN HIEU SHUTDOWN (Ctrl+C)...")
            logger.info("⏳ Dang don dep... vui long cho...")
            logger.info("="*70)
            
            self.shutdown_initiated = True
            bot_state.is_running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        logger.info("✅ Graceful shutdown handler setup")
    
    async def wait_for_shutdown(self, delay: float = 5.0):
        """Cho shutdown xong"""
        await asyncio.sleep(delay)
        self.shutdown_complete = True

# Global shutdown handler
shutdown_handler = GracefulShutdownHandler()

def get_shutdown_handler() -> GracefulShutdownHandler:
    """Lay shutdown handler"""
    return shutdown_handler

# ==========================================
# 📊 ALERTS VIA TELEGRAM
# ==========================================

async def send_alert_to_user(client, message: str, user_id: int = None):
    """Gui alert Telegram cho admin"""
    
    if not user_id:
        logger.debug("⚠️ User ID chua set, bo qua alert")
        return
    
    try:
        # Format message
        alert_msg = (
            f"🚨 **BOT ALERT**\n\n"
            f"{message}\n\n"
            f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        await client.send_message(user_id, alert_msg)
        logger.info(f"✅ Gui alert Telegram cho admin: {message[:50]}")
    except Exception as e:
        logger.error(f"❌ Khong the gui alert: {e}")

# ==========================================
# 📦 DATABASE BACKUP
# ==========================================

class DatabaseBackup:
    """Quan ly database backup"""
    
    def __init__(self, backup_dir: str = "backups"):
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.max_backups = 10
    
    def backup(self, db_path: str) -> bool:
        """Backup database"""
        try:
            db_path = Path(db_path)
            
            if not db_path.exists():
                logger.warning(f"⚠️ Database khong ton tai: {db_path}")
                return False
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.backup_dir / f"code_history_{timestamp}.db"
            
            shutil.copy2(db_path, backup_path)
            logger.info(f"✅ Backup database: {backup_path}")
            
            # Cleanup old backups
            self._cleanup_old_backups()
            
            return True
        
        except Exception as e:
            logger.error(f"❌ Loi backup: {e}")
            return False
    
    def _cleanup_old_backups(self):
        """Xoa backups cu"""
        try:
            backups = sorted(self.backup_dir.glob("code_history_*.db"))
            
            if len(backups) > self.max_backups:
                to_delete = backups[:-self.max_backups]
                for backup in to_delete:
                    backup.unlink()
                    logger.info(f"🧹 Xoa backup cu: {backup.name}")
        
        except Exception as e:
            logger.error(f"❌ Loi cleanup backups: {e}")
    
    async def schedule_daily_backup(self, db_path: str, bot_state):
        """Backup hang ngay luc 2 AM"""
        logger.info("📦 Database backup scheduler bat dau")
        
        while bot_state.is_running:
            try:
                now = datetime.now()
                next_backup = now.replace(hour=2, minute=0, second=0, microsecond=0)
                
                if next_backup <= now:
                    next_backup = next_backup + timedelta(days=1)
                
                wait_seconds = (next_backup - now).total_seconds()
                logger.debug(f"⏳ Backup se chay trong {wait_seconds/3600:.1f} gio nua")
                
                await asyncio.sleep(wait_seconds)
                
                if bot_state.is_running:
                    self.backup(db_path)
            
            except Exception as e:
                logger.error(f"❌ Loi schedule backup: {e}")
                await asyncio.sleep(3600)

# Global backup
db_backup = DatabaseBackup()

def get_db_backup() -> DatabaseBackup:
    """Lay database backup"""
    return db_backup

# ==========================================
# 🧹 CLEANUP OLD PROFILES
# ==========================================

class ProfileCleaner:
    """Don dep profiles cu"""
    
    def __init__(self, max_age_days: int = 7):
        self.max_age_days = max_age_days
    
    async def cleanup(self, profile_base_path: Path) -> int:
        """Xoa profiles cu hon max_age_days"""
        
        if not profile_base_path.exists():
            return 0
        
        now = time.time()
        max_age_seconds = self.max_age_days * 24 * 3600
        deleted_count = 0
        
        try:
            for profile_dir in profile_base_path.iterdir():
                if profile_dir.is_dir():
                    age_seconds = now - profile_dir.stat().st_mtime
                    
                    if age_seconds > max_age_seconds:
                        try:
                            shutil.rmtree(profile_dir)
                            logger.info(f"🧹 Xoa profile cu: {profile_dir.name}")
                            deleted_count += 1
                        except Exception as e:
                            logger.warning(f"⚠️ Khong the xoa profile: {e}")
        
        except Exception as e:
            logger.error(f"❌ Loi cleanup profiles: {e}")
        
        return deleted_count
    
    async def schedule_daily_cleanup(self, profile_base_path: Path, bot_state):
        """Cleanup hang ngay luc 3 AM"""
        logger.info("🧹 Profile cleanup scheduler bat dau")
        
        while bot_state.is_running:
            try:
                now = datetime.now()
                next_cleanup = now.replace(hour=3, minute=0, second=0, microsecond=0)
                
                if next_cleanup <= now:
                    next_cleanup = next_cleanup + timedelta(days=1)
                
                wait_seconds = (next_cleanup - now).total_seconds()
                await asyncio.sleep(wait_seconds)
                
                if bot_state.is_running:
                    deleted = await self.cleanup(profile_base_path)
                    if deleted > 0:
                        logger.info(f"✅ Da xoa {deleted} profiles cu")
            
            except Exception as e:
                logger.error(f"❌ Loi schedule cleanup: {e}")
                await asyncio.sleep(3600)

# Global profile cleaner
profile_cleaner = ProfileCleaner(max_age_days=7)

def get_profile_cleaner() -> ProfileCleaner:
    """Lay profile cleaner"""
    return profile_cleaner

# ==========================================
# 🧪 SYSTEM TEST SUITE
# ==========================================

class SystemTester:
    """Test he thong truoc khi bat dau"""
    
    def __init__(self, client):
        self.client = client
    
    async def test_telegram_connection(self) -> bool:
        """Test ket noi Telegram"""
        try:
            logger.info("🧪 Test ket noi Telegram...")
            me = await self.client.get_me()
            logger.info(f"   ✅ OK - Username: {me.username}")
            return True
        except Exception as e:
            logger.error(f"   ❌ FAIL - {e}")
            return False
    
    async def test_database(self, db) -> bool:
        """Test database hoat dong"""
        try:
            logger.info("🧪 Test database...")
            
            # Thu ghi
            db.record_submission(
                "TEST_CODE_12345",
                "test_account",
                "https://test.com",
                "TEST",
                "Test submission"
            )
            
            # Thu doc
            status = db.get_code_status("TEST_CODE_12345")
            
            if status:
                logger.info(f"   ✅ OK")
                return True
            else:
                logger.info(f"   ⚠️ Write OK nhung read failed")
                return False
        
        except Exception as e:
            logger.error(f"   ❌ FAIL - {e}")
            return False
    
    async def test_all(self, db) -> bool:
        """Test tat ca systems"""
        logger.info("\n" + "="*70)
        logger.info("🧪 HE THONG TEST SUITE")
        logger.info("="*70 + "\n")
        
        results = {}
        
        # Test Telegram
        results['telegram'] = await self.test_telegram_connection()
        await asyncio.sleep(1)
        
        # Test Database
        results['database'] = await self.test_database(db)
        await asyncio.sleep(1)
        
        # Test Config
        results['config'] = ConfigValidator.validate_all()
        await asyncio.sleep(1)
        
        # Summary
        logger.info("\n" + "="*70)
        logger.info("📋 KET QUA TEST:")
        logger.info("="*70)
        
        for name, result in results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            logger.info(f"   {status} - {name}")
        
        all_pass = all(results.values())
        
        if all_pass:
            logger.info("\n✅ TAT CA TEST PASSED!")
        else:
            logger.warning("\n⚠️ MOT SO TEST FAILED!")
        
        logger.info("="*70 + "\n")
        
        return all_pass

def get_system_tester(client) -> SystemTester:
    """Lay system tester"""
    return SystemTester(client)