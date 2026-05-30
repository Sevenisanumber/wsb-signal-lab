#!/usr/bin/env python3
"""WSB Signal Lab dashboard — Flask app serving a single-page read-only view."""

import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from flask import Flask, render_template

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, 'data', 'wsb.db')

app = Flask(__name__)


# ── template filters ───────────────────────────────────────────────────────────

@app.template_filter('comma')
def comma_filter(n):
    try:
        return f'{int(n):,}'
    except (TypeError, ValueError):
        return '—'


@app.template_filter('price')
def price_filter(n):
    try:
        return f'${float(n):.2f}'
    except (TypeError, ValueError):
        return '—'


@app.template_filter('pct')
def pct_filter(n):
    """Format a float as a signed percentage string, or None to signal '—'."""
    try:
        f = float(n)
        return f'{f:+.2f}'
    except (TypeError, ValueError):
        return None


@app.template_filter('ts')
def ts_filter(ts):
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    except (TypeError, ValueError):
        return '—'


# ── helpers ────────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── paper trading helpers ──────────────────────────────────────────────────────

def get_current_prices(tickers: list) -> dict:
    """Fetch latest bar prices for tickers from Alpaca in parallel.
    Returns {ticker: price_float} with None for any that fail."""
    result = {t: None for t in tickers}
    if not tickers:
        return result
    try:
        import alpaca_trade_api as tradeapi
        api_key = os.getenv('ALPACA_API_KEY')
        secret  = os.getenv('ALPACA_SECRET_KEY')
        base    = os.getenv('ALPACA_BASE_URL', 'https://paper-api.alpaca.markets')
        if not api_key or not secret:
            return result
        api = tradeapi.REST(api_key, secret, base, api_version='v2')

        def _fetch(ticker):
            try:
                bars = api.get_bars(ticker, '1Min', limit=1).df
                if not bars.empty:
                    return ticker, float(bars.iloc[-1]['close'])
                bars = api.get_bars(ticker, '1Day', limit=1).df
                if not bars.empty:
                    return ticker, float(bars.iloc[-1]['close'])
            except Exception:
                pass
            return ticker, None

        with ThreadPoolExecutor(max_workers=min(len(tickers), 3)) as ex:
            futures = {ex.submit(_fetch, t): t for t in tickers}
            for fut in as_completed(futures, timeout=8):
                try:
                    ticker, price = fut.result()
                    result[ticker] = price
                except Exception:
                    pass
    except Exception:
        pass
    return result


def get_paper_trading_data(conn) -> dict:
    """Query paper_trades and return portfolio summary, open positions, closed trades."""
    empty = {'has_data': False, 'summary': None, 'open_positions': [], 'closed_trades': []}

    # Gracefully handle missing table (first run before paper_trader has ever been called)
    try:
        table_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='paper_trades'"
        ).fetchone()
        if not table_exists:
            return empty

        total_count = conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0]
        if total_count == 0:
            return empty
    except Exception:
        return empty

    today = datetime.now().strftime('%Y-%m-%d')

    # Aggregate stats across all trades
    stats = conn.execute("""
        SELECT
            COUNT(*)                                                          AS total_trades,
            SUM(CASE WHEN status='open'   THEN 1    ELSE 0    END)           AS open_count,
            SUM(CASE WHEN status='closed' THEN 1    ELSE 0    END)           AS closed_count,
            SUM(CASE WHEN status='closed' AND pnl >= 0 THEN 1 ELSE 0 END)    AS wins,
            COALESCE(SUM(CASE WHEN status='closed' THEN pnl  ELSE 0 END), 0) AS realized_pnl,
            COALESCE(SUM(position_size), 0)                                  AS total_deployed
        FROM paper_trades
    """).fetchone()

    closed_count  = stats['closed_count'] or 0
    wins          = stats['wins'] or 0
    realized_pnl  = round(stats['realized_pnl'] or 0, 2)
    total_deployed = stats['total_deployed'] or 0
    win_rate      = round(wins / closed_count * 100) if closed_count > 0 else None

    # Open positions — fetch current prices in parallel
    open_rows = conn.execute("""
        SELECT trade_id, ticker, signal_type, entry_date, entry_price, shares, position_size
          FROM paper_trades WHERE status = 'open'
         ORDER BY entry_date
    """).fetchall()

    open_tickers = [r['ticker'] for r in open_rows]
    cur_prices   = get_current_prices(open_tickers)

    open_positions  = []
    total_unrealized = 0.0
    for r in open_rows:
        cur   = cur_prices.get(r['ticker'])
        entry = r['entry_price']
        days  = (datetime.strptime(today, '%Y-%m-%d') -
                 datetime.strptime(r['entry_date'], '%Y-%m-%d')).days
        if cur is not None and entry:
            upnl     = round((cur - entry) * r['shares'], 2)
            upnl_pct = round((cur - entry) / entry * 100, 2)
            total_unrealized += upnl
        else:
            upnl = upnl_pct = None
        open_positions.append({
            'ticker':        r['ticker'],
            'signal_type':   r['signal_type'],
            'entry_date':    r['entry_date'],
            'entry_price':   entry,
            'current_price': cur,
            'upnl':          upnl,
            'upnl_pct':      upnl_pct,
            'days_held':     days,
        })

    # Total P&L = realized + unrealized (where prices available)
    total_pnl     = round(realized_pnl + total_unrealized, 2)
    total_pnl_pct = round(total_pnl / total_deployed * 100, 1) if total_deployed > 0 else None

    summary = {
        'total_trades':   stats['total_trades'],
        'open_count':     stats['open_count'] or 0,
        'closed_count':   closed_count,
        'win_rate':       win_rate,
        'realized_pnl':   realized_pnl,
        'total_pnl':      total_pnl,
        'total_pnl_pct':  total_pnl_pct,
        'total_deployed': total_deployed,
    }

    # Last 10 closed trades
    closed_rows = conn.execute("""
        SELECT ticker, signal_type, entry_date, exit_date,
               entry_price, exit_price, pnl, pnl_pct, exit_reason
          FROM paper_trades WHERE status = 'closed'
         ORDER BY exit_date DESC
         LIMIT 10
    """).fetchall()

    return {
        'has_data':       True,
        'summary':        summary,
        'open_positions': open_positions,
        'closed_trades':  [dict(r) for r in closed_rows],
    }


# ── routes ─────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    conn = get_db()

    # Dataset bounds
    bounds    = conn.execute(
        'SELECT MIN(created_utc), MAX(created_utc) FROM posts WHERE created_utc > 0'
    ).fetchone()
    latest_ts = bounds[1] or 0
    latest_dt = datetime.fromtimestamp(latest_ts, tz=timezone.utc)
    latest_date = latest_dt.strftime('%Y-%m-%d')
    cutoff_24h  = latest_ts - 86_400
    cutoff_30d  = latest_ts - 30 * 86_400

    # ── pulse: top 10 tickers from last 24h of dataset ────────────────────────
    pulse_raw = conn.execute('''
        SELECT pt.ticker,
               COUNT(*)               AS mentions,
               ROUND(AVG(p.score), 0) AS avg_score
          FROM post_tickers pt
          JOIN posts p ON p.post_id = pt.post_id
         WHERE p.created_utc >= :cutoff
         GROUP BY pt.ticker
         ORDER BY mentions DESC
         LIMIT 10
    ''', {'cutoff': cutoff_24h}).fetchall()

    pulse = []
    for row in pulse_raw:
        ticker = row['ticker']
        pr = conn.execute(
            'SELECT close FROM prices WHERE ticker=? AND date<=? ORDER BY date DESC LIMIT 1',
            (ticker, latest_date)
        ).fetchone()
        prev = conn.execute(
            'SELECT close FROM prices WHERE ticker=? AND date<? ORDER BY date DESC LIMIT 1',
            (ticker, latest_date)
        ).fetchone()
        close = pr['close']   if pr   else None
        prev_c = prev['close'] if prev else None
        chg = round((close - prev_c) / prev_c * 100, 2) if (close and prev_c) else None
        pulse.append({
            'ticker':    ticker,
            'mentions':  row['mentions'],
            'avg_score': int(row['avg_score']) if row['avg_score'] else 0,
            'price':     close,
            'change':    chg,
        })

    # ── signal history: last 30 days, ≥2 mentions, limit 200 rows ─────────────
    history = conn.execute('''
        SELECT DATE(p.created_utc, 'unixepoch')          AS post_date,
               pt.ticker,
               COUNT(*)                                   AS mentions,
               ROUND(AVG(pt.forward_return_1d)  * 100, 2) AS avg_1d,
               ROUND(AVG(pt.forward_return_7d)  * 100, 2) AS avg_7d,
               ROUND(AVG(pt.forward_return_30d) * 100, 2) AS avg_30d
          FROM post_tickers pt
          JOIN posts p ON p.post_id = pt.post_id
         WHERE p.created_utc >= :cutoff
           AND pt.forward_return_1d IS NOT NULL
         GROUP BY post_date, pt.ticker
        HAVING mentions >= 2
         ORDER BY post_date DESC, mentions DESC
         LIMIT 200
    ''', {'cutoff': cutoff_30d}).fetchall()

    # ── today's top tickers from daily_mentions ───────────────────────────────
    dm_date_row = conn.execute('SELECT MAX(date) FROM daily_mentions').fetchone()
    dm_date     = dm_date_row[0] if dm_date_row and dm_date_row[0] else None

    daily_top = []
    if dm_date:
        daily_top_raw = conn.execute('''
            SELECT ticker,
                   SUM(mention_count)                                    AS total,
                   GROUP_CONCAT(subreddit || ':' || mention_count, ', ') AS sources
              FROM daily_mentions
             WHERE date = ?
             GROUP BY ticker
             ORDER BY total DESC
             LIMIT 20
        ''', (dm_date,)).fetchall()
        daily_top = [dict(r) for r in daily_top_raw]

    # ── data health ───────────────────────────────────────────────────────────
    scrape_runs = conn.execute('''
        SELECT run_id, started_at, finished_at, posts_fetched, errors, status, script
          FROM scrape_log
         ORDER BY started_at DESC
         LIMIT 5
    ''').fetchall()

    total_posts   = conn.execute('SELECT COUNT(*) FROM posts').fetchone()[0]
    total_tickers = conn.execute('SELECT COUNT(DISTINCT ticker) FROM post_tickers').fetchone()[0]
    # Use real wall-clock time so the health indicator reflects actual recent runs,
    # not the dataset's historical date range.
    recent_errors = conn.execute(
        "SELECT COUNT(*) FROM scrape_log WHERE status='failure' AND started_at >= ?",
        (int(time.time()) - 86_400,)
    ).fetchone()[0]

    pt = get_paper_trading_data(conn)
    conn.close()

    return render_template(
        'index.html',
        latest_date   = latest_date,
        pulse         = pulse,
        history       = history,
        daily_top     = daily_top,
        dm_date       = dm_date,
        scrape_runs   = scrape_runs,
        total_posts   = total_posts,
        total_tickers = total_tickers,
        recent_errors = recent_errors,
        generated_at  = datetime.now().strftime('%Y-%m-%d %H:%M'),
        pt            = pt,
    )


if __name__ == '__main__':
    port = int(os.getenv('DASHBOARD_PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=False)
