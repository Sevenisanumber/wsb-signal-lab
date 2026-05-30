#!/usr/bin/env python3
"""
Daily pipeline orchestrator. Called by cron at 6:00 AM.

Steps (run in order, each independently logged to scrape_log):
  1. fetch_prices    — pull latest Alpaca OHLCV for all tracked tickers
  2. extract_tickers — find ticker mentions in any new posts
  3. calc_returns    — compute forward returns for newly-priced pairs

A step failure is caught, logged, and skipped; later steps still run because
they are idempotent and do not depend on the current step succeeding.

Exit code 0 = all steps succeeded.
Exit code 1 = one or more steps failed (cron/monitoring can detect this).
"""

import os
import sys
import sqlite3
import time
import logging
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT, '.env'))

DB_PATH  = os.path.join(ROOT, 'data', 'wsb.db')
LOG_PATH = os.path.join(ROOT, 'logs', 'daily_pipeline.log')

os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_PATH),
    ],
)
log = logging.getLogger(__name__)


def run_step(conn: sqlite3.Connection, script: str, fn) -> tuple[bool, int]:
    """
    Execute fn(), write result to scrape_log, return (success, row_count).
    Never raises — exceptions are caught and logged as failures.
    """
    started = int(time.time())
    run_id  = conn.execute(
        "INSERT INTO scrape_log (started_at, status, script) VALUES (?, 'running', ?)",
        (started, script),
    ).lastrowid
    conn.commit()

    try:
        result = fn()
        # Normalize result to an integer count
        if isinstance(result, int):
            count = result
        elif isinstance(result, tuple) and result:
            count = result[0] if isinstance(result[0], int) else 0
        else:
            count = 0

        conn.execute(
            """UPDATE scrape_log
               SET finished_at=?, posts_fetched=?, status='success', errors=NULL
             WHERE run_id=?""",
            (int(time.time()), count, run_id),
        )
        conn.commit()
        log.info(f'[{script}] success — {count:,} rows processed')
        return True, count

    except Exception as e:
        conn.execute(
            """UPDATE scrape_log
               SET finished_at=?, status='failure', errors=?
             WHERE run_id=?""",
            (int(time.time()), str(e)[:500], run_id),
        )
        conn.commit()
        log.error(f'[{script}] FAILED: {e}')
        return False, 0


def main() -> int:
    log.info('=== Daily pipeline starting ===')
    conn = sqlite3.connect(DB_PATH)

    # Import here (after sys.path is set) so each module's own logging
    # config doesn't clobber ours — basicConfig above ran first.
    from scrapers.fetch_prices         import fetch_prices
    from scrapers.extract_tickers      import run_extraction
    from scrapers.calc_returns         import calc_returns
    from scrapers.fetch_daily_mentions import fetch_daily_mentions
    from scrapers.daily_report         import generate_report

    def run_daily_report():
        report = generate_report()
        date = datetime.now().strftime('%Y-%m-%d')
        out  = os.path.join(ROOT, 'logs', f'daily_report_{date}.txt')
        with open(out, 'w') as f:
            f.write(report)
            f.write('\n')
        log.info(f'Daily report written to {out}')
        print(report)
        return 1  # non-zero so run_step counts it as a "row"

    steps = [
        ('fetch_prices',         fetch_prices),
        ('extract_tickers',      run_extraction),
        ('calc_returns',         calc_returns),
        ('fetch_daily_mentions', fetch_daily_mentions),
        ('daily_report',         run_daily_report),
    ]

    any_failed = False
    for script, fn in steps:
        ok, count = run_step(conn, script, fn)
        if not ok:
            any_failed = True

    conn.close()
    log.info('=== Daily pipeline complete ===')
    return 1 if any_failed else 0


if __name__ == '__main__':
    sys.exit(main())
