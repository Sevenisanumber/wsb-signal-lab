#!/usr/bin/env python3
"""
Intraday position monitor — WSB Signal Lab

Checks all open paper_trades positions and executes exits when conditions are met.
Designed to run every 30 minutes during market hours via cron; exits silently
if the market is closed (handles weekends, holidays, pre/post-market automatically).

The --daily-summary flag is used by the final 4pm ET cron entry to bypass the
market-open gate and send a Pushover end-of-day portfolio summary.

Exit conditions (same thresholds as paper_trader.py):
  take_profit : unrealized gain >= +15%
  stop_loss   : unrealized loss >= -8%
  time_exit   : position held >= 7 calendar days

Logs every check and any exits to logs/paper_trades.log using the same
format as paper_trader.py so the combined log tells a coherent story.

Usage:
  python scrapers/check_positions.py                    # normal intraday run
  python scrapers/check_positions.py --dry-run          # check without placing orders
  python scrapers/check_positions.py --daily-summary    # final close + EOD summary

Cron (CDT = ET-1h, weekdays):
  */30 8-14 * * 1-5  cd /project && venv/bin/python scrapers/check_positions.py
  0 15 * * 1-5       cd /project && venv/bin/python scrapers/check_positions.py --daily-summary
"""

import argparse
import logging
import os
import sqlite3
import sys
from datetime import datetime

import pytz

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, 'data', 'wsb.db')
LOG_DIR = os.path.join(ROOT, 'logs')

os.makedirs(LOG_DIR, exist_ok=True)

LOG_PATH = os.path.join(LOG_DIR, 'paper_trades.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_PATH),
    ],
)
log = logging.getLogger(__name__)

ET = pytz.timezone('America/New_York')
MARKET_OPEN  = (9, 30)
MARKET_CLOSE = (16, 0)


def local_market_check() -> bool:
    """Fast local gate: weekday between 9:30am–4:00pm ET. No API call."""
    now_et = datetime.now(tz=ET)
    if now_et.weekday() >= 5:
        return False
    t = (now_et.hour, now_et.minute)
    return MARKET_OPEN <= t <= MARKET_CLOSE


def count_open_positions(conn: sqlite3.Connection) -> int:
    return conn.execute(
        "SELECT COUNT(*) FROM paper_trades WHERE status = 'open'"
    ).fetchone()[0]


def send_daily_summary(api, conn: sqlite3.Connection, today: str) -> None:
    """Compute end-of-day portfolio stats and send a Pushover summary."""
    from scrapers.paper_trader import load_open_positions, get_current_price
    from scrapers.notify import send_pushover

    open_positions = load_open_positions(conn)
    n_open = len(open_positions)

    unrealized = 0.0
    for pos in open_positions:
        cur = get_current_price(api, pos['ticker'])
        if cur is not None and pos['entry_price']:
            unrealized += (cur - pos['entry_price']) * pos['shares']

    today_trades = conn.execute(
        "SELECT COUNT(*) FROM paper_trades WHERE entry_date = ? OR exit_date = ?",
        (today, today),
    ).fetchone()[0]

    total_realized = conn.execute(
        "SELECT COALESCE(SUM(pnl), 0) FROM paper_trades WHERE status = 'closed'"
    ).fetchone()[0] or 0.0

    total_pnl = total_realized + unrealized

    def sgn(v: float) -> str:
        return '+' if v >= 0 else ''

    msg = (
        f"{n_open} open position{'s' if n_open != 1 else ''} | "
        f"Unrealized: {sgn(unrealized)}${unrealized:.2f} | "
        f"Today's trades: {today_trades} | "
        f"Total P&L: {sgn(total_pnl)}${total_pnl:.2f}"
    )
    log.info(f'[SUMMARY] {msg}')
    sent = send_pushover(msg, title='WSB Lab Daily Summary')
    log.info(f'[SUMMARY] Notification {"sent" if sent else "failed — check credentials"}')


def main() -> None:
    parser = argparse.ArgumentParser(description='WSB intraday position monitor')
    parser.add_argument('--dry-run', action='store_true',
                        help='Check positions without placing sell orders')
    parser.add_argument('--daily-summary', action='store_true',
                        help='Send EOD portfolio summary (bypasses market-open gate)')
    args = parser.parse_args()

    # ── Fast local gate (skipped for daily-summary which runs at market close) ──
    if not args.daily_summary and not local_market_check():
        sys.exit(0)

    conn = sqlite3.connect(DB_PATH)
    conn.execute('PRAGMA journal_mode=WAL')

    sys.path.insert(0, ROOT)
    from scrapers.paper_trader import (
        init_paper_trades_table,
        make_api,
        market_is_open,
        check_exits,
    )
    init_paper_trades_table(conn)

    api   = make_api()
    today = datetime.now().strftime('%Y-%m-%d')

    # ── Daily summary path (final 4pm ET cron entry) ──────────────────────────
    if args.daily_summary:
        log.info(f'[SUMMARY] End-of-day run | {today}' + (' [DRY RUN]' if args.dry_run else ''))
        # Try final exits if market is still technically open at the boundary
        if market_is_open(api):
            closed = check_exits(api, conn, today=today, dry_run=args.dry_run)
            if closed:
                log.info(f'[SUMMARY] {closed} position(s) closed at market close')
        if not args.dry_run:
            send_daily_summary(api, conn, today)
        else:
            log.info('[SUMMARY] Dry run — skipping Pushover notification')
        conn.close()
        return

    # ── Regular intraday path ──────────────────────────────────────────────────
    n_open = count_open_positions(conn)
    if n_open == 0:
        conn.close()
        sys.exit(0)

    if not market_is_open(api):
        log.info('[MONITOR] Market closed — skipping position check')
        conn.close()
        sys.exit(0)

    log.info(f'[MONITOR] Checking {n_open} open position(s) | {today}'
             + (' [DRY RUN]' if args.dry_run else ''))

    closed = check_exits(api, conn, today=today, dry_run=args.dry_run)

    log.info(f'[MONITOR] Done — {closed} position(s) closed this run')
    conn.close()


if __name__ == '__main__':
    main()
