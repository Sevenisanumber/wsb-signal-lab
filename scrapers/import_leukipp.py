#!/usr/bin/env python3
"""
Import leukipp multi-subreddit dataset into the posts table.

Leukipp CSV columns:
    id, author, created, retrieved, edited, pinned, archived, locked,
    removed, deleted, is_self, is_video, is_original_content, title,
    link_flair_text, upvote_ratio, score, gilded, total_awards_received,
    num_comments, num_crossposts, selftext, thumbnail, shortlink

Column mapping to posts schema:
    id              → post_id
    selftext        → body         (nulled if '[deleted]' or empty)
    created         → created_utc  (datetime string → Unix int)
    retrieved       → scraped_at   (datetime string → Unix int)
    shortlink       → url
    link_flair_text → flair
    source                          fixed to 'leukipp'

New columns added to posts if absent:
    total_awards_received  INTEGER
    num_crossposts         INTEGER

(upvote_ratio already exists in the base schema.)

Usage:
    python3 scrapers/import_leukipp.py data/leukipp_wsb.csv wallstreetbets
    python3 scrapers/import_leukipp.py data/leukipp_wsb.csv wallstreetbets --limit 1000
    python3 scrapers/import_leukipp.py data/leukipp_wsb.csv wallstreetbets --dry-run
"""

import argparse
import csv
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, 'data', 'wsb.db')
LOG_PATH = os.path.join(ROOT, 'logs', 'import_leukipp.log')

os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_PATH)],
)
log = logging.getLogger(__name__)

CHUNK_SIZE = 10_000


# ── Schema migration ──────────────────────────────────────────────────────────

NEW_COLUMNS = [
    ('total_awards_received', 'INTEGER'),
    ('num_crossposts',        'INTEGER'),
]


def ensure_columns(conn: sqlite3.Connection) -> None:
    """Add new columns to posts if they don't already exist."""
    existing = {row[1] for row in conn.execute('PRAGMA table_info(posts)')}
    for col, col_type in NEW_COLUMNS:
        if col not in existing:
            conn.execute(f'ALTER TABLE posts ADD COLUMN {col} {col_type}')
            log.info(f'Added column posts.{col} {col_type}')
        else:
            log.info(f'Column posts.{col} already exists — skipping')
    conn.commit()


# ── Type coercions ────────────────────────────────────────────────────────────

def _to_unix(value: str) -> int | None:
    """'YYYY-MM-DD HH:MM:SS' → Unix int. Returns None on failure."""
    if not value or value.strip() in ('', 'nan', 'None', 'null', '1970-01-01 00:00:00'):
        return None
    try:
        return int(float(value))          # already numeric
    except ValueError:
        pass
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d'):
        try:
            dt = datetime.strptime(value.strip(), fmt)
            return int(dt.replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            continue
    return None


def _safe_int(v) -> int | None:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _safe_float(v) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _normalize_bool(v) -> int | None:
    s = str(v).strip().lower() if v is not None else ''
    if s in ('1', 'true', 't', 'yes'):
        return 1
    if s in ('0', 'false', 'f', 'no', ''):
        return 0
    return None


def _clean_body(v: str) -> str | None:
    """Return None for deleted/empty selftext, otherwise the raw text."""
    s = (v or '').strip()
    if not s or s in ('[deleted]', '[removed]'):
        return None
    return s


# ── Import logic ──────────────────────────────────────────────────────────────

INSERT_SQL = """
    INSERT OR IGNORE INTO posts
        (post_id, author, title, body, score, upvote_ratio, num_comments,
         created_utc, url, flair, is_self, scraped_at, source,
         total_awards_received, num_crossposts)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


def _make_record(row: dict, scraped_at_fallback: int) -> tuple | None:
    post_id = row.get('id', '').strip()
    if not post_id:
        return None

    created_utc = _to_unix(row.get('created', ''))
    if created_utc is None:
        return None

    scraped_at = _to_unix(row.get('retrieved', '')) or scraped_at_fallback

    return (
        post_id,
        row.get('author', '').strip() or 'unknown',
        row.get('title', '').strip() or None,
        _clean_body(row.get('selftext', '')),
        _safe_int(row.get('score')),
        _safe_float(row.get('upvote_ratio')),
        _safe_int(row.get('num_comments')),
        created_utc,
        row.get('shortlink', '').strip() or None,
        row.get('link_flair_text', '').strip() or None,
        _normalize_bool(row.get('is_self')),
        scraped_at,
        'leukipp',
        _safe_int(row.get('total_awards_received')),
        _safe_int(row.get('num_crossposts')),
    )


def _flush(conn: sqlite3.Connection, chunk: list[tuple]) -> int:
    conn.executemany(INSERT_SQL, chunk)
    conn.commit()
    return len(chunk)


def import_leukipp(
    csv_path: str,
    subreddit: str,
    db_path: str = DB_PATH,
    limit: int | None = None,
    dry_run: bool = False,
) -> int:
    if not os.path.exists(csv_path):
        log.error(f'CSV not found: {csv_path}')
        sys.exit(1)
    if not os.path.exists(db_path):
        log.error(f'DB not found: {db_path}. Run scripts/init_db.py first.')
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL')

    # ── Before count ─────────────────────────────────────────────────────────
    before = conn.execute("SELECT COUNT(*) FROM posts WHERE source='leukipp'").fetchone()[0]
    total_before = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    log.info(f'Posts before import: {total_before:,} total ({before:,} leukipp)')

    if not dry_run:
        ensure_columns(conn)

    # ── scrape_log entry ──────────────────────────────────────────────────────
    started_at = int(time.time())
    run_id = None
    if not dry_run:
        run_id = conn.execute(
            "INSERT INTO scrape_log (started_at, status, script) VALUES (?, 'running', ?)",
            (started_at, f'import_leukipp:{subreddit}'),
        ).lastrowid
        conn.commit()

    inserted = skipped = row_errors = 0
    chunk: list[tuple] = []
    chunk_num = 0

    log.info(f'Importing {csv_path} (subreddit={subreddit}, limit={limit or "all"}, dry_run={dry_run})')

    try:
        with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if limit is not None and i >= limit:
                    break

                try:
                    record = _make_record(row, scraped_at_fallback=started_at)
                except Exception as e:
                    row_errors += 1
                    skipped += 1
                    continue

                if record is None:
                    skipped += 1
                    continue

                chunk.append(record)

                if len(chunk) >= CHUNK_SIZE:
                    chunk_num += 1
                    if not dry_run:
                        _flush(conn, chunk)
                    inserted += len(chunk)
                    chunk = []
                    log.info(
                        f'  chunk {chunk_num} | {inserted:,} rows processed | '
                        f'{skipped:,} skipped'
                    )

            # Final partial chunk
            if chunk:
                chunk_num += 1
                if not dry_run:
                    _flush(conn, chunk)
                inserted += len(chunk)

        if not dry_run:
            _flush_authors(conn)

        # ── After count ───────────────────────────────────────────────────────
        if not dry_run:
            after = conn.execute("SELECT COUNT(*) FROM posts WHERE source='leukipp'").fetchone()[0]
            total_after = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
            new_rows = total_after - total_before
            log.info(
                f'Posts after import : {total_after:,} total ({after:,} leukipp) | '
                f'+{new_rows:,} new rows (dupes ignored by INSERT OR IGNORE)'
            )

        status = 'success'
        error_text = f'{row_errors} row-level parse errors' if row_errors else None
        log.info(
            f'Import complete: {inserted:,} processed | {skipped:,} skipped | '
            f'{row_errors} parse errors'
            + (' [DRY RUN — nothing written]' if dry_run else '')
        )

    except Exception as e:
        status = 'failure'
        error_text = str(e)
        log.error(f'Import failed: {e}')
        raise

    finally:
        if run_id is not None:
            conn.execute(
                """UPDATE scrape_log
                      SET finished_at=?, posts_fetched=?, errors=?, status=?
                    WHERE run_id=?""",
                (int(time.time()), inserted, error_text, status, run_id),
            )
            conn.commit()
        conn.close()

    return inserted


def _flush_authors(conn: sqlite3.Connection) -> None:
    log.info('Updating authors table...')
    conn.executescript("""
        INSERT OR IGNORE INTO authors (username, first_seen_at, last_seen_at, total_posts_scraped)
        SELECT author, MIN(created_utc), MAX(created_utc), COUNT(*)
          FROM posts
         WHERE author NOT IN ('', '[deleted]', 'AutoModerator')
         GROUP BY author;

        UPDATE authors
           SET last_seen_at = (
               SELECT MAX(created_utc) FROM posts
                WHERE posts.author = authors.username
           ),
           total_posts_scraped = (
               SELECT COUNT(*) FROM posts
                WHERE posts.author = authors.username
           );
    """)
    conn.commit()
    count = conn.execute('SELECT COUNT(*) FROM authors').fetchone()[0]
    log.info(f'Authors table: {count:,} unique authors')


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Import leukipp multi-subreddit dataset into posts table'
    )
    parser.add_argument('csv_path',  help='Path to leukipp CSV file')
    parser.add_argument('subreddit', help='Subreddit name (e.g. wallstreetbets)')
    parser.add_argument('--db',      default=DB_PATH,  help='SQLite DB path')
    parser.add_argument('--limit',   type=int, default=None,
                        help='Import only the first N rows (for testing)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse rows and report counts without writing to DB')
    args = parser.parse_args()

    n = import_leukipp(
        csv_path=args.csv_path,
        subreddit=args.subreddit,
        db_path=args.db,
        limit=args.limit,
        dry_run=args.dry_run,
    )
    if not args.dry_run:
        print(f'\nDone. {n:,} rows processed → {args.db}')
    else:
        print(f'\n[DRY RUN] {n:,} rows would be processed. Nothing written.')


if __name__ == '__main__':
    main()
