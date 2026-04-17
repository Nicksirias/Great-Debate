"""SQLite schema and connection helpers."""

import os
import sqlite3
from pathlib import Path

from flask import g

# Project root (parent of backend/)
_ROOT = Path(__file__).resolve().parent.parent
DATABASE_PATH = os.environ.get("DATABASE_PATH", str(_ROOT / "great_debate.db"))


def get_db():
    if "db" not in g:
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

        CREATE INDEX IF NOT EXISTS idx_debates_users ON debates(user_a_id, user_b_id);
        CREATE INDEX IF NOT EXISTS idx_messages_debate ON messages(debate_id);
        CREATE INDEX IF NOT EXISTS idx_queue_day_side ON queue_entries(day_key, side);
        """
    )
    conn.commit()
    conn.close()
