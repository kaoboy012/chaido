"""
📒 CODE HISTORY LOGGER
Ghi lịch sử mã code để cuối ngày kiểm tra/bảo trì.

Tạo file:
- logs/code_history/code_history_YYYY-MM-DD.csv
- logs/code_history/code_history_YYYY-MM-DD.jsonl
- logs/code_history/daily_summary_YYYY-MM-DD.csv
"""

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse


HISTORY_DIR = Path("logs/code_history")
HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def _now():
    return datetime.now()


def _today_str():
    return _now().strftime("%Y-%m-%d")


def normalize_domain(url: str) -> str:
    try:
        parsed = urlparse(url or "")
        domain = parsed.netloc or parsed.path
        return domain.lower().replace("www.", "").strip("/")
    except Exception:
        return str(url or "")


def history_csv_path(date_str=None) -> Path:
    date_str = date_str or _today_str()
    return HISTORY_DIR / f"code_history_{date_str}.csv"


def history_jsonl_path(date_str=None) -> Path:
    date_str = date_str or _today_str()
    return HISTORY_DIR / f"code_history_{date_str}.jsonl"


def summary_csv_path(date_str=None) -> Path:
    date_str = date_str or _today_str()
    return HISTORY_DIR / f"daily_summary_{date_str}.csv"


FIELDNAMES = [
    "time", "event_type", "channel", "site", "account", "code",
    "source", "status", "telegram_delay", "submit_elapsed",
    "message", "screenshot",
]


def append_code_history(
    event_type: str,
    code: str = "",
    target_url: str = "",
    account: str = "",
    channel: str = "",
    source: str = "",
    status: str = "",
    telegram_delay=None,
    submit_elapsed=None,
    message: str = "",
    screenshot: str = "",
):
    row = {
        "time": _now().strftime("%Y-%m-%d %H:%M:%S"),
        "event_type": event_type,
        "channel": channel or "",
        "site": normalize_domain(target_url),
        "account": account or "",
        "code": str(code or ""),
        "source": source or "",
        "status": status or "",
        "telegram_delay": "" if telegram_delay is None else f"{float(telegram_delay):.2f}",
        "submit_elapsed": "" if submit_elapsed is None else f"{float(submit_elapsed):.2f}",
        "message": str(message or "").replace("\n", " ")[:300],
        "screenshot": str(screenshot or ""),
    }

    csv_path = history_csv_path()
    jsonl_path = history_jsonl_path()
    write_header = not csv_path.exists()

    with csv_path.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerow(row)

    with jsonl_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

    return row


def build_daily_summary(date_str=None):
    date_str = date_str or _today_str()
    csv_path = history_csv_path(date_str)
    if not csv_path.exists():
        return None

    by_site_account = defaultdict(Counter)

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("event_type") != "RESULT":
                continue
            status = row.get("status") or "UNKNOWN"
            site = row.get("site") or ""
            account = row.get("account") or ""
            by_site_account[(site, account)][status] += 1

    out_path = summary_csv_path(date_str)

    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        fieldnames = ["date", "site", "account", "success", "failed", "unknown", "total"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for (site, account), counts in sorted(by_site_account.items()):
            success = counts.get("SUCCESS", 0)
            failed = counts.get("FAILED", 0)
            unknown = counts.get("UNKNOWN", 0)
            writer.writerow({
                "date": date_str,
                "site": site,
                "account": account,
                "success": success,
                "failed": failed,
                "unknown": unknown,
                "total": success + failed + unknown,
            })

    return str(out_path)


if __name__ == "__main__":
    summary = build_daily_summary()
    print(summary or "Chưa có lịch sử hôm nay.")
