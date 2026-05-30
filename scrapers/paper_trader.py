#!/usr/bin/env python3
"""
Paper Trader — WSB Signal Lab Phase 4

Reads today's daily report signals and executes paper trades via Alpaca.

Trading rules:
  BUY: score > 70  AND vel_tag == 'HOT'       (3-5x velocity)
  BUY: score > 60  AND vel_tag == 'SLOW_BURN' (<0.5x velocity)
  SKIP: EXTREME velocity (>5x) — always
  Max 3 open positions, $100 per trade, $500 max total exposure
  Exit: 7 calendar days OR +15% profit OR -8% stop loss

Hard limits:
  Never trade price < $5
  Never trade tickers with no Alpaca price data
  Never exceed $500 total paper exposure
  Market must be open before placing any order
  No shorting

Usage:
  python scrapers/paper_trader.py           # run trading logic for today
  python scrapers/paper_trader.py --status  # show open positions and P&L
  python scrapers/paper_trader.py --date 2026-05-30
  python scrapers/paper_trader.py --dry-run # evaluate signals, no orders placed
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, 'data', 'wsb.db')
LOG_DIR = os.path.join(ROOT, 'logs')
sys.path.insert(0, ROOT)

from scrapers.notify import send_pushover

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

# ── Trading constants ─────────────────────────────────────────────────────────

MAX_POSITIONS      = 3
POSITION_SIZE      = 100.0   # dollars per trade
MAX_TOTAL_EXPOSURE = 500.0   # hard cap across all open positions
MIN_PRICE          = 5.0     # penny stock filter

TAKE_PROFIT_PCT = 0.15    # +15%
STOP_LOSS_PCT   = 0.08    # -8%
MAX_HOLD_DAYS   = 7


# ── DB helpers ────────────────────────────────────────────────────────────────

def init_paper_trades_table(conn) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS paper_trades (
            trade_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker        TEXT NOT NULL,
            signal_type   TEXT NOT NULL,
            signal_score  REAL NOT NULL,
            velocity      REAL NOT NULL,
            vel_tag       TEXT NOT NULL,
            entry_date    TEXT NOT NULL,
            entry_price   REAL NOT NULL,
            shares        REAL NOT NULL,
            position_size REAL NOT NULL,
            status        TEXT NOT NULL DEFAULT 'open',
            exit_date     TEXT,
            exit_price    REAL,
            exit_reason   TEXT,
            pnl           REAL,
            pnl_pct       REAL
        );
        CREATE INDEX IF NOT EXISTS idx_pt_status ON paper_trades(status);
        CREATE INDEX IF NOT EXISTS idx_pt_ticker ON paper_trades(ticker);
    """)
    conn.commit()


def load_open_positions(conn) -> list[dict]:
    rows = conn.execute(
        """SELECT trade_id, ticker, signal_type, signal_score, velocity, vel_tag,
                  entry_date, entry_price, shares, position_size
             FROM paper_trades WHERE status = 'open'
             ORDER BY entry_date""",
    ).fetchall()
    return [
        dict(zip(
            ('trade_id', 'ticker', 'signal_type', 'signal_score', 'velocity',
             'vel_tag', 'entry_date', 'entry_price', 'shares', 'position_size'),
            row,
        ))
        for row in rows
    ]


def record_trade(conn, ticker, signal_type, signal_score, velocity, vel_tag,
                 entry_date, entry_price, shares, position_size) -> int:
    cur = conn.execute(
        """INSERT INTO paper_trades
           (ticker, signal_type, signal_score, velocity, vel_tag,
            entry_date, entry_price, shares, position_size)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (ticker, signal_type, signal_score, velocity, vel_tag,
         entry_date, entry_price, shares, position_size),
    )
    conn.commit()
    return cur.lastrowid


def close_trade(conn, trade_id, exit_date, exit_price, exit_reason,
                entry_price, shares) -> tuple[float, float]:
    pnl     = (exit_price - entry_price) * shares
    pnl_pct = (exit_price - entry_price) / entry_price * 100
    conn.execute(
        """UPDATE paper_trades
              SET status='closed', exit_date=?, exit_price=?,
                  exit_reason=?, pnl=?, pnl_pct=?
            WHERE trade_id=?""",
        (exit_date, exit_price, exit_reason, round(pnl, 4), round(pnl_pct, 2), trade_id),
    )
    conn.commit()
    return pnl, pnl_pct


# ── Alpaca helpers ────────────────────────────────────────────────────────────

def make_api():
    from dotenv import load_dotenv
    load_dotenv(os.path.join(ROOT, '.env'))
    import alpaca_trade_api as tradeapi

    api_key    = os.getenv('ALPACA_API_KEY')
    secret_key = os.getenv('ALPACA_SECRET_KEY')
    base_url   = os.getenv('ALPACA_BASE_URL', 'https://paper-api.alpaca.markets')

    if not api_key or not secret_key:
        log.error('Missing ALPACA_API_KEY or ALPACA_SECRET_KEY in .env')
        sys.exit(1)

    return tradeapi.REST(api_key, secret_key, base_url, api_version='v2')


def market_is_open(api) -> bool:
    try:
        clock = api.get_clock()
        return clock.is_open
    except Exception as e:
        log.warning(f'Could not check market clock: {e}')
        return False


def get_current_price(api, ticker: str) -> float | None:
    """Return latest trade price, or None if ticker is unavailable."""
    try:
        bars = api.get_bars(ticker, '1Min', limit=1).df
        if not bars.empty:
            return float(bars.iloc[-1]['close'])
        # Fall back to daily bar
        bars = api.get_bars(ticker, '1Day', limit=1).df
        if not bars.empty:
            return float(bars.iloc[-1]['close'])
        return None
    except Exception:
        return None


def place_buy_order(api, ticker: str, shares: float, dry_run: bool = False) -> dict | None:
    """Submit a fractional market buy order. Returns order details or None on failure."""
    if dry_run:
        return {'dry_run': True}
    try:
        order = api.submit_order(
            symbol=ticker,
            qty=str(round(shares, 6)),
            side='buy',
            type='market',
            time_in_force='day',
        )
        # Poll briefly for fill confirmation (paper orders fill fast)
        for _ in range(10):
            order = api.get_order(order.id)
            if order.status in ('filled', 'partially_filled'):
                break
            time.sleep(0.5)
        return order
    except Exception as e:
        log.error(f'Buy order failed for {ticker}: {e}')
        return None


def place_sell_order(api, ticker: str, shares: float, dry_run: bool = False) -> dict | None:
    """Submit a fractional market sell order."""
    if dry_run:
        return {'dry_run': True}
    try:
        order = api.submit_order(
            symbol=ticker,
            qty=str(round(shares, 6)),
            side='sell',
            type='market',
            time_in_force='day',
        )
        for _ in range(10):
            order = api.get_order(order.id)
            if order.status in ('filled', 'partially_filled'):
                break
            time.sleep(0.5)
        return order
    except Exception as e:
        log.error(f'Sell order failed for {ticker}: {e}')
        return None


# ── Exit logic ────────────────────────────────────────────────────────────────

def check_exits(api, conn, today: str, dry_run: bool = False) -> int:
    """
    Evaluate all open positions against exit conditions.
    Returns number of positions closed.
    """
    positions = load_open_positions(conn)
    if not positions:
        return 0

    closed = 0
    for pos in positions:
        ticker      = pos['ticker']
        entry_price = pos['entry_price']
        shares      = pos['shares']
        entry_date  = pos['entry_date']
        trade_id    = pos['trade_id']

        current_price = get_current_price(api, ticker)
        if current_price is None:
            log.warning(f'[EXIT-SKIP] {ticker} | cannot fetch price — skipping exit check')
            continue

        pnl_pct    = (current_price - entry_price) / entry_price * 100
        days_held  = (datetime.strptime(today, '%Y-%m-%d')
                      - datetime.strptime(entry_date, '%Y-%m-%d')).days

        exit_reason = None
        if pnl_pct >= TAKE_PROFIT_PCT * 100:
            exit_reason = 'take_profit'
        elif pnl_pct <= -STOP_LOSS_PCT * 100:
            exit_reason = 'stop_loss'
        elif days_held >= MAX_HOLD_DAYS:
            exit_reason = 'time_exit'

        if exit_reason:
            order = place_sell_order(api, ticker, shares, dry_run=dry_run)
            if order is None:
                log.error(f'[EXIT-FAIL] {ticker} | sell order rejected — position remains open')
                continue

            actual_exit_price = current_price
            if not dry_run and hasattr(order, 'filled_avg_price') and order.filled_avg_price:
                actual_exit_price = float(order.filled_avg_price)

            pnl, pnl_pct_final = close_trade(
                conn, trade_id, today, actual_exit_price, exit_reason,
                entry_price, shares,
            )
            sign = '+' if pnl >= 0 else ''
            log.info(
                f'[SELL] {ticker} | reason={exit_reason} | days_held={days_held} | '
                f'entry=${entry_price:.2f} exit=${actual_exit_price:.2f} | '
                f'P&L={sign}${pnl:.2f} ({sign}{pnl_pct_final:.1f}%)'
                + (' [DRY RUN]' if dry_run else '')
            )
            if not dry_run:
                send_pushover(
                    f'SOLD {ticker} @ ${actual_exit_price:.2f} | '
                    f'P&L: {sign}${pnl:.2f} ({sign}{pnl_pct_final:.1f}%) | '
                    f'Reason: {exit_reason}'
                )
            closed += 1
        else:
            sign = '+' if pnl_pct >= 0 else ''
            log.info(
                f'[HOLD] {ticker} | days_held={days_held} | '
                f'entry=${entry_price:.2f} current=${current_price:.2f} | '
                f'unrealized={sign}{pnl_pct:.1f}%'
            )

    return closed


# ── Signal loading ────────────────────────────────────────────────────────────

def load_signals(date: str, db_path: str = DB_PATH) -> list[dict]:
    """
    Return scored signal rows for the given date using daily_report internals.
    Returns an empty list if no data exists for that date.
    """
    import sqlite3
    sys.path.insert(0, ROOT)
    from scrapers.daily_report import load_today, compute_velocity, build_rows

    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL')
    ticker_data = load_today(conn, date)
    if not ticker_data:
        conn.close()
        return []
    velocities, _ = compute_velocity(conn, date, ticker_data)
    conn.close()
    return build_rows(ticker_data, velocities)


# ── Entry logic ───────────────────────────────────────────────────────────────

def run_trading(date: str, db_path: str = DB_PATH, dry_run: bool = False) -> None:
    import sqlite3

    api  = make_api()
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL')
    init_paper_trades_table(conn)

    log.info(f'=== Paper trader starting | date={date} dry_run={dry_run} ===')

    # ── 1. Check exits first (works regardless of market being open) ──────────
    if not dry_run and not market_is_open(api):
        log.info('[MARKET] Closed — checking exits only (no new orders)')
        check_exits(api, conn, today=date, dry_run=dry_run)
        conn.close()
        log.info('=== Paper trader done (market closed) ===')
        return

    check_exits(api, conn, today=date, dry_run=dry_run)

    # ── 2. Load today's signals ───────────────────────────────────────────────
    rows = load_signals(date, db_path)
    if not rows:
        log.warning(f'No signal data for {date} — run fetch_daily_mentions first')
        conn.close()
        return

    log.info(f'Loaded {len(rows)} signal rows for {date}')

    # ── 3. Build context for entry decisions ──────────────────────────────────
    open_positions  = load_open_positions(conn)
    held_tickers    = {p['ticker'] for p in open_positions}
    total_exposure  = sum(p['position_size'] for p in open_positions)
    open_count      = len(open_positions)

    log.info(
        f'Open positions: {open_count}/{MAX_POSITIONS} | '
        f'Exposure: ${total_exposure:.2f}/${MAX_TOTAL_EXPOSURE:.0f}'
    )

    # ── 4. Evaluate each signal for entry ─────────────────────────────────────
    for row in rows:
        ticker    = row['ticker']
        score     = row['live_score']
        vel_tag   = row['vel_tag']
        velocity  = row['velocity']
        slow_burn = row['slow_burn']

        # Override vel_tag with SLOW_BURN when slow_burn flag is set
        effective_tag = 'SLOW_BURN' if slow_burn else vel_tag

        # Hard skip: EXTREME velocity
        if vel_tag == 'EXTREME':
            log.info(f'[SKIP] {ticker} | EXTREME velocity ({velocity:.1f}x) — never trade')
            continue

        # Determine if entry conditions are met
        signal_type = None
        if score > 70 and effective_tag == 'HOT':
            signal_type = 'HOT_SCORE'
        elif score > 60 and effective_tag == 'SLOW_BURN':
            signal_type = 'SLOW_BURN'

        if signal_type is None:
            log.debug(f'[SKIP] {ticker} | score={score:.1f} tag={effective_tag} — no entry rule matched')
            continue

        # Position limit checks
        if ticker in held_tickers:
            log.info(f'[SKIP] {ticker} | signal={signal_type} score={score:.1f} | already holding')
            continue

        if open_count >= MAX_POSITIONS:
            log.info(f'[SKIP] {ticker} | signal={signal_type} | max positions ({MAX_POSITIONS}) reached')
            continue

        if total_exposure + POSITION_SIZE > MAX_TOTAL_EXPOSURE:
            log.info(
                f'[SKIP] {ticker} | signal={signal_type} | '
                f'exposure limit: ${total_exposure:.2f}+${POSITION_SIZE:.0f} > ${MAX_TOTAL_EXPOSURE:.0f}'
            )
            continue

        # Price checks
        current_price = get_current_price(api, ticker)
        if current_price is None:
            log.info(f'[SKIP] {ticker} | signal={signal_type} | no Alpaca price data')
            continue

        if current_price < MIN_PRICE:
            log.info(f'[SKIP] {ticker} | signal={signal_type} | penny stock (${current_price:.2f} < ${MIN_PRICE})')
            continue

        # Place order
        shares = POSITION_SIZE / current_price
        order  = place_buy_order(api, ticker, shares, dry_run=dry_run)
        if order is None:
            log.error(f'[BUY-FAIL] {ticker} | order rejected')
            continue

        actual_entry_price = current_price
        actual_shares      = shares
        if not dry_run and hasattr(order, 'filled_avg_price') and order.filled_avg_price:
            actual_entry_price = float(order.filled_avg_price)
            actual_shares      = float(order.filled_qty)

        trade_id = record_trade(
            conn, ticker, signal_type, score, velocity, effective_tag,
            date, actual_entry_price, actual_shares, POSITION_SIZE,
        )

        held_tickers.add(ticker)
        open_count      += 1
        total_exposure  += POSITION_SIZE

        log.info(
            f'[BUY] {ticker} | signal={signal_type} | score={score:.1f} | '
            f'vel={velocity:.1f}x | entry=${actual_entry_price:.2f} | '
            f'shares={actual_shares:.4f} | size=${POSITION_SIZE:.0f} | trade_id={trade_id}'
            + (' [DRY RUN]' if dry_run else '')
        )
        if not dry_run:
            send_pushover(
                f'BUY {ticker} @ ${actual_entry_price:.2f} | '
                f'Signal: {signal_type} {score:.1f} | '
                f'Size: ${POSITION_SIZE:.0f}'
            )

    conn.close()
    log.info('=== Paper trader done ===')


# ── Status report ─────────────────────────────────────────────────────────────

def show_status(db_path: str = DB_PATH) -> None:
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL')
    init_paper_trades_table(conn)

    api   = make_api()
    today = datetime.now().strftime('%Y-%m-%d')

    open_positions = load_open_positions(conn)
    sep  = '═' * 70
    dash = '─' * 70

    print(sep)
    print(f'WSB Signal Lab  —  Paper Trading Status  {today}')
    print(sep)

    # ── Open positions ────────────────────────────────────────────────────────
    print(f'\n── OPEN POSITIONS ({len(open_positions)}/{MAX_POSITIONS}) ────────────────────────')
    if open_positions:
        print(f"  {'Ticker':<6}  {'Signal':<12}  {'Entry':>7}  {'Current':>8}  "
              f"{'P&L $':>7}  {'P&L %':>7}  {'Days':>4}  {'Score':>5}")
        print('  ' + '─' * 66)
        total_cost        = 0.0
        total_unrealized  = 0.0
        for pos in open_positions:
            ticker      = pos['ticker']
            entry_price = pos['entry_price']
            shares      = pos['shares']
            entry_date  = pos['entry_date']
            signal_type = pos['signal_type']
            score       = pos['signal_score']

            current_price = get_current_price(api, ticker)
            days_held = (datetime.strptime(today, '%Y-%m-%d')
                         - datetime.strptime(entry_date, '%Y-%m-%d')).days

            if current_price is not None:
                pnl     = (current_price - entry_price) * shares
                pnl_pct = (current_price - entry_price) / entry_price * 100
                cur_str = f'${current_price:.2f}'
            else:
                pnl = pnl_pct = 0.0
                cur_str = 'N/A'

            total_cost       += pos['position_size']
            total_unrealized += pnl

            sign = '+' if pnl >= 0 else ''
            print(
                f"  {ticker:<6}  {signal_type:<12}  ${entry_price:>6.2f}  "
                f"{cur_str:>8}  {sign}${pnl:>6.2f}  {sign}{pnl_pct:>6.1f}%  "
                f"{days_held:>4}d  {score:>5.1f}"
            )

        print('  ' + '─' * 66)
        sign = '+' if total_unrealized >= 0 else ''
        print(f"  Total exposure: ${total_cost:.2f}  |  Unrealized P&L: {sign}${total_unrealized:.2f}")
    else:
        print('  (no open positions)')

    # ── Closed trades summary ─────────────────────────────────────────────────
    closed = conn.execute(
        """SELECT ticker, signal_type, entry_date, exit_date, entry_price,
                  exit_price, pnl, pnl_pct, exit_reason
             FROM paper_trades WHERE status = 'closed'
             ORDER BY exit_date DESC LIMIT 20"""
    ).fetchall()

    print(f'\n{dash}')
    print(f'── CLOSED TRADES (last {min(len(closed), 20)}) ────────────────────────────────')
    if closed:
        print(f"  {'Ticker':<6}  {'Signal':<12}  {'Entry':>7}  {'Exit':>7}  "
              f"{'P&L $':>7}  {'P&L %':>7}  {'Reason':<12}  Exit Date")
        print('  ' + '─' * 66)
        wins = losses = 0
        total_realized = 0.0
        for row in closed:
            ticker, sig, edate, xdate, eprice, xprice, pnl, pnl_pct, reason = row
            sign = '+' if (pnl or 0) >= 0 else ''
            if (pnl or 0) >= 0:
                wins += 1
            else:
                losses += 1
            total_realized += pnl or 0
            print(
                f"  {ticker:<6}  {sig:<12}  ${eprice:>6.2f}  ${xprice:>6.2f}  "
                f"{sign}${pnl:>6.2f}  {sign}{pnl_pct:>6.1f}%  {reason:<12}  {xdate}"
            )
        total = wins + losses
        win_rate = wins / total * 100 if total else 0
        print('  ' + '─' * 66)
        sign = '+' if total_realized >= 0 else ''
        print(
            f'  Closed: {total} trades  |  Win rate: {win_rate:.0f}%  |  '
            f'Realized P&L: {sign}${total_realized:.2f}'
        )
    else:
        print('  (no closed trades yet)')

    # ── Stats totals ──────────────────────────────────────────────────────────
    stats = conn.execute(
        """SELECT COUNT(*), SUM(pnl), SUM(CASE WHEN pnl >= 0 THEN 1 ELSE 0 END)
             FROM paper_trades WHERE status = 'closed'"""
    ).fetchone()
    total_closed, total_pnl, wins_total = stats
    if total_closed and total_closed > 0:
        print(f'\n{dash}')
        sign = '+' if (total_pnl or 0) >= 0 else ''
        print(
            f'── ALL-TIME: {total_closed} closed trades | '
            f'Win rate: {(wins_total or 0)/total_closed*100:.0f}% | '
            f'Total P&L: {sign}${total_pnl:.2f}'
        )

    print(f'\n{sep}')
    conn.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description='WSB paper trader')
    parser.add_argument('--date',    default=None,       help='Date YYYY-MM-DD (default: today)')
    parser.add_argument('--status',  action='store_true', help='Show open positions and P&L')
    parser.add_argument('--dry-run', action='store_true', help='Evaluate signals without placing orders')
    args = parser.parse_args()

    if args.status:
        show_status(DB_PATH)
        return

    date = args.date or datetime.now().strftime('%Y-%m-%d')
    run_trading(date=date, db_path=DB_PATH, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
