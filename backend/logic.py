"""Matchmaking, scoring, and heuristic judging."""

import hashlib
import json
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any

# Same canonical topics as the original UI (rotated by day).
TOPIC_BANK = [
    {
        "title": "Should AI-generated content require visible disclosure labels on all social feeds?",
        "description": "Debate trust, creativity, platform incentives, and whether mandatory labels help or hurt discourse.",
        "sides": [
            "Yes - every AI-assisted post should be clearly labeled.",
            "No - labeling should be optional or context-specific.",
        ],
    },
    {
        "title": "Should universities allow AI writing tools in graded coursework?",
        "description": "Discuss learning outcomes, fairness, access inequity, and what meaningful authorship should mean.",
        "sides": [
            "Yes - AI is the new calculator and should be integrated.",
            "No - unrestricted use undermines core writing skills.",
        ],
    },
    {
        "title": "Should cities prioritize public transit over expanding roads?",
        "description": "Compare economic growth, environmental impact, commuting equity, and implementation realism.",
        "sides": [
            "Yes - invest in transit first for long-term gains.",
            "No - road expansion remains the practical priority.",
        ],
    },
    {
        "title": "Should short-form video platforms be age-restricted for users under 16?",
        "description": "Balance youth safety and development against personal freedom, family agency, and enforcement challenges.",
        "sides": [
            "Yes - strict age gates are necessary for wellbeing.",
            "No - better parental controls are enough.",
        ],
    },
]

MAX_DEBATE_SECONDS = 600
INACTIVITY_SECONDS = 180
K_ELO = 32

JUDGE_NAMES = ["The Analyst", "The Skeptic", "The Empath", "The Strategist", "The Historian"]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def day_key_from_dt(dt: datetime | None = None) -> str:
    d = dt or utc_now()
    return d.date().isoformat()


def topic_for_day(day_key: str) -> dict[str, Any]:
    h = int(hashlib.sha256(day_key.encode()).hexdigest(), 16)
    return TOPIC_BANK[h % len(TOPIC_BANK)]


def tier_for_rating(rating: float) -> str:
    if rating < 1300:
        return "Bronze"
    if rating < 1450:
        return "Silver"
    if rating < 1600:
        return "Gold"
    if rating < 1750:
        return "Platinum"
    return "Champion"


def ensure_topic_row(conn, day_key: str) -> None:
    cur = conn.execute("SELECT 1 FROM topics WHERE day_key = ?", (day_key,))
    if cur.fetchone():
        return
    t = topic_for_day(day_key)
    conn.execute(
        """
        INSERT INTO topics (day_key, title, description, side0_label, side1_label)
        VALUES (?, ?, ?, ?, ?)
        """,
        (day_key, t["title"], t["description"], t["sides"][0], t["sides"][1]),
    )


def elo_expected(ra: float, rb: float) -> float:
    return 1.0 / (1.0 + 10 ** ((rb - ra) / 400.0))


def apply_elo(conn, winner_id: int, loser_id: int) -> None:
    rw = conn.execute("SELECT rating FROM users WHERE id = ?", (winner_id,)).fetchone()
    rl = conn.execute("SELECT rating FROM users WHERE id = ?", (loser_id,)).fetchone()
    if not rw or not rl:
        return
    ra, rb = float(rw["rating"]), float(rl["rating"])
    ew = elo_expected(ra, rb)
    el = elo_expected(rb, ra)
    new_w = ra + K_ELO * (1.0 - ew)
    new_l = rb + K_ELO * (0.0 - el)
    conn.execute(
        "UPDATE users SET rating = ?, wins = wins + 1 WHERE id = ?",
        (new_w, winner_id),
    )
    conn.execute(
        "UPDATE users SET rating = ?, losses = losses + 1 WHERE id = ?",
        (new_l, loser_id),
    )


def _message_stats(rows: list, uid_a: int, uid_b: int) -> tuple[int, int, int, int]:
    chars_a = chars_b = 0
    n_a = n_b = 0
    for r in rows:
        uid = r["user_id"]
        n = len(r["body"] or "")
        if uid == uid_a:
            chars_a += n
            n_a += 1
        elif uid == uid_b:
            chars_b += n
            n_b += 1
    return chars_a, chars_b, n_a, n_b


def judge_transcript(
    messages: list,
    user_a_id: int,
    user_b_id: int,
    handle_a: str,
    handle_b: str,
) -> dict[str, Any]:
    """Heuristic judge; swap for OpenAI in judge_transcript() later."""
    ca, cb, na, nb = _message_stats(messages, user_a_id, user_b_id)
    score_a = ca + 40 * math.log1p(na)
    score_b = cb + 40 * math.log1p(nb)
    rng = random.Random(hash((user_a_id, user_b_id, len(messages))) % (2**32))
    jitter = rng.uniform(-120, 120)
    margin = score_a - score_b + jitter
    votes_a = sum(1 for _ in range(5) if rng.random() < (0.5 + margin / 800.0))
    votes_a = max(0, min(5, votes_a))
    votes_b = 5 - votes_a
    winner_id = user_a_id if votes_a >= votes_b else user_b_id
    loser_id = user_b_id if winner_id == user_a_id else user_a_id

    personas = []
    for i, name in enumerate(JUDGE_NAMES):
        pick_a = i < votes_a
        w = handle_a if pick_a else handle_b
        personas.append(
            {
                "name": name,
                "favored_handle": w,
                "favored_user_id": user_a_id if pick_a else user_b_id,
                "reason": (
                    "Stronger structure and evidence density in the transcript slice I weighed."
                    if pick_a
                    else "More persuasive rebuttals and clarity under time pressure."
                ),
            }
        )

    summary = (
        f"Split decision leaning toward {'@' + handle_a if winner_id == user_a_id else '@' + handle_b}. "
        f"Volume-adjusted engagement score was {score_a:.0f} vs {score_b:.0f} before judge variance."
    )

    return {
        "winner_user_id": winner_id,
        "loser_user_id": loser_id,
        "votes_side_a": votes_a,
        "votes_side_b": votes_b,
        "summary": summary,
        "personas": personas,
    }


def finalize_debate(conn, debate_id: int) -> None:
    row = conn.execute(
        """
        SELECT d.*, ua.handle AS ha, ub.handle AS hb
        FROM debates d
        JOIN users ua ON ua.id = d.user_a_id
        JOIN users ub ON ub.id = d.user_b_id
        WHERE d.id = ? AND d.status = 'active'
        """,
        (debate_id,),
    ).fetchone()
    if not row:
        return

    msgs = conn.execute(
        "SELECT user_id, body FROM messages WHERE debate_id = ? ORDER BY id ASC",
        (debate_id,),
    ).fetchall()

    verdict = judge_transcript(
        msgs,
        row["user_a_id"],
        row["user_b_id"],
        row["ha"],
        row["hb"],
    )
    winner_id = verdict["winner_user_id"]
    loser_id = verdict["loser_user_id"]

    now = utc_now().isoformat()
    conn.execute(
        """
        UPDATE debates
        SET status = 'completed', ended_at = ?, winner_user_id = ?, judge_json = ?
        WHERE id = ?
        """,
        (now, winner_id, json.dumps(verdict), debate_id),
    )
    apply_elo(conn, winner_id, loser_id)
    conn.commit()


def try_match_queue(conn, user_id: int, day_key: str, side: int) -> int | None:
    """Pair with longest-waiting opponent on opposite side. Returns debate_id or None."""
    ensure_topic_row(conn, day_key)
    conn.execute("DELETE FROM queue_entries WHERE user_id = ?", (user_id,))
    conn.execute(
        "INSERT INTO queue_entries (user_id, day_key, side) VALUES (?, ?, ?)",
        (user_id, day_key, side),
    )

    opp = conn.execute(
        """
        SELECT user_id FROM queue_entries
        WHERE day_key = ? AND side != ? AND user_id != ?
        ORDER BY created_at ASC LIMIT 1
        """,
        (day_key, side, user_id),
    ).fetchone()

    if not opp:
        conn.commit()
        return None

    opp_id = opp["user_id"]
    now = utc_now()
    ends = (now + timedelta(seconds=MAX_DEBATE_SECONDS)).isoformat()
    last = now.isoformat()

    if user_id < opp_id:
        ua, ub = user_id, opp_id
        sa = side
    else:
        ua, ub = opp_id, user_id
        sa = 1 - side

    cur = conn.execute(
        """
        INSERT INTO debates (
            topic_day_key, user_a_id, user_b_id, side_a, status, ends_at, last_activity_at
        ) VALUES (?, ?, ?, ?, 'active', ?, ?)
        """,
        (day_key, ua, ub, sa, ends, last),
    )
    debate_id = cur.lastrowid
    conn.execute("DELETE FROM queue_entries WHERE user_id IN (?, ?)", (user_id, opp_id))
    conn.commit()
    return debate_id


def cleanup_inactive_debates(conn) -> None:
    now = utc_now()
    rows = conn.execute(
        "SELECT id, ends_at, last_activity_at FROM debates WHERE status = 'active'"
    ).fetchall()
    for r in rows:
        try:
            ends = datetime.fromisoformat(r["ends_at"].replace("Z", "+00:00"))
            last = datetime.fromisoformat(r["last_activity_at"].replace("Z", "+00:00"))
        except (TypeError, ValueError):
            continue
        if now >= ends:
            finalize_debate(conn, r["id"])
            continue
        if (now - last).total_seconds() > INACTIVITY_SECONDS:
            finalize_debate(conn, r["id"])
