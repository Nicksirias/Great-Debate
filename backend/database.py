"""SQLite schema and connection helpers."""

import os
import sqlite3
from pathlib import Path

from flask import g

# Project root (parent of backend/)
_ROOT = Path(__file__).resolve().parent.parent
DATABASE_PATH = os.environ.get("DATABASE_PATH", str(_ROOT / "great_debate.db"))


def _ensure_db_parent_dir() -> None:
    Path(DATABASE_PATH).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def get_db():
    if "db" not in g:
        _ensure_db_parent_dir()
        conn = sqlite3.connect(DATABASE_PATH, detect_types=sqlite3.PARSE_DECLTYPES, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        g.db = conn
    return g.db


def close_db(e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    _ensure_db_parent_dir()
    conn = sqlite3.connect(DATABASE_PATH, timeout=30.0)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            handle TEXT NOT NULL UNIQUE,
            token TEXT NOT NULL UNIQUE,
            rating REAL NOT NULL DEFAULT 1500.0,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS topics (
            day_key TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            side0_label TEXT NOT NULL,
            side1_label TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS debates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic_day_key TEXT NOT NULL,
            user_a_id INTEGER NOT NULL,
            user_b_id INTEGER NOT NULL,
            side_a INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            ended_at TEXT,
            ends_at TEXT NOT NULL,
            last_activity_at TEXT NOT NULL,
            winner_user_id INTEGER,
            judge_json TEXT,
            verdict_status TEXT NOT NULL DEFAULT 'ready',
            judge_error TEXT,
            FOREIGN KEY (user_a_id) REFERENCES users(id),
            FOREIGN KEY (user_b_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            debate_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (debate_id) REFERENCES debates(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS queue_entries (
            user_id INTEGER PRIMARY KEY,
            day_key TEXT NOT NULL,
            side INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS debate_ai_summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            debate_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            side INTEGER NOT NULL,
            topic_day_key TEXT,
            topic_title TEXT,
            position_summary TEXT NOT NULL,
            sentiment_label TEXT NOT NULL,
            sentiment_score REAL,
            toxicity_flags TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (debate_id) REFERENCES debates(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );

        CREATE INDEX IF NOT EXISTS idx_debates_users ON debates(user_a_id, user_b_id);
        CREATE INDEX IF NOT EXISTS idx_messages_debate ON messages(debate_id);
        CREATE INDEX IF NOT EXISTS idx_queue_day_side ON queue_entries(day_key, side);
        CREATE INDEX IF NOT EXISTS idx_ai_summary_debate ON debate_ai_summaries(debate_id);
        CREATE INDEX IF NOT EXISTS idx_ai_summary_sentiment ON debate_ai_summaries(sentiment_label);
        """
    )
    _migrate_schema(conn)
    conn.commit()
    conn.close()


def _has_column(conn, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(r[1] == column_name for r in rows)


def _migrate_schema(conn) -> None:
    if not _has_column(conn, "debates", "verdict_status"):
        conn.execute("ALTER TABLE debates ADD COLUMN verdict_status TEXT NOT NULL DEFAULT 'ready'")
    if not _has_column(conn, "debates", "judge_error"):
        conn.execute("ALTER TABLE debates ADD COLUMN judge_error TEXT")
    if not _has_column(conn, "debate_ai_summaries", "topic_day_key"):
        conn.execute("ALTER TABLE debate_ai_summaries ADD COLUMN topic_day_key TEXT")
    if not _has_column(conn, "debate_ai_summaries", "topic_title"):
        conn.execute("ALTER TABLE debate_ai_summaries ADD COLUMN topic_title TEXT")
