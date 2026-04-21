"""Matchmaking, scoring, and LLM-based judging."""

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any

from openai import OpenAI

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

JUDGE_PROFILES = [
    {
        "name": "Judge 1",
        "bias": "You strongly prefer side A's framing by default.",
    },
    {
        "name": "Judge 2",
        "bias": "You mildly prefer side A's framing by default.",
    },
    {
        "name": "Judge 3",
        "bias": "You are neutral and evaluate only argument quality.",
    },
    {
        "name": "Judge 4",
        "bias": "You mildly prefer side B's framing by default.",
    },
    {
        "name": "Judge 5",
        "bias": "You strongly prefer side B's framing by default.",
    },
]

LLM_TEMPERATURE = 0.2
_openai_client = None


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


def topic_for_user(conn, day_key: str, user_id: int | None = None) -> dict[str, Any]:
    ensure_topic_row(conn, day_key)
    base = conn.execute("SELECT * FROM topics WHERE day_key = ?", (day_key,)).fetchone()
    if user_id is None:
        return {
            "variant": "A",
            "title": base["title"],
            "description": base["description"],
            "sides": [base["side0_label"], base["side1_label"]],
        }
    variants = conn.execute(
        """
        SELECT variant, title, description, side0_label, side1_label
        FROM topic_experiments
        WHERE day_key = ? AND is_active = 1
        ORDER BY variant ASC
        """,
        (day_key,),
    ).fetchall()
    if not variants:
        return {
            "variant": "A",
            "title": base["title"],
            "description": base["description"],
            "sides": [base["side0_label"], base["side1_label"]],
        }
    idx = abs(hash((day_key, user_id))) % len(variants)
    v = variants[idx]
    return {
        "variant": v["variant"],
        "title": v["title"],
        "description": v["description"],
        "sides": [v["side0_label"], v["side1_label"]],
    }


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


def _get_openai_client() -> OpenAI:
    global _openai_client
    if _openai_client is None:
        _openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    return _openai_client


def _get_model_name() -> str:
    return os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")


def _extract_json_block(text: str) -> dict[str, Any]:
    text = (text or "").strip()
    if not text:
        raise ValueError("Empty model response")
    if text.startswith("```"):
        parts = text.split("```")
        if len(parts) >= 3:
            text = parts[1].replace("json", "", 1).strip()
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < 0:
        raise ValueError("No JSON object found in model response")
    return json.loads(text[start : end + 1])


def _chat_json(system_prompt: str, user_prompt: str) -> dict[str, Any]:
    client = _get_openai_client()
    model = _get_model_name()
    out = client.chat.completions.create(
        model=model,
        temperature=LLM_TEMPERATURE,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    content = out.choices[0].message.content or "{}"
    return _extract_json_block(content)


def _format_transcript(messages: list, handle_a: str, handle_b: str, uid_a: int, uid_b: int) -> str:
    lines: list[str] = []
    for m in messages:
        uid = m["user_id"]
        speaker = handle_a if uid == uid_a else handle_b if uid == uid_b else f"user_{uid}"
        body = (m["body"] or "").replace("\n", " ").strip()
        lines.append(f"@{speaker}: {body}")
    return "\n".join(lines) if lines else "(No messages)"


def _moderation_check(
    transcript: str, user_a_id: int, user_b_id: int, handle_a: str, handle_b: str
) -> dict[str, Any]:
    system_prompt = (
        "You are a strict moderation auditor for a 1v1 debate app. "
        "Detect repeated or overt harassment, racism, slurs, hate speech, or direct personal attacks. "
        "Only mark auto_loss when severe or repeated conduct clearly occurs."
    )
    user_prompt = f"""
Return strict JSON with keys:
- auto_loss (boolean)
- loser_user_id (integer or null, must be {user_a_id} or {user_b_id} when auto_loss=true)
- reason (string)
- evidence (array of short quote snippets)

Debaters:
- {user_a_id}: @{handle_a}
- {user_b_id}: @{handle_b}

Transcript:
{transcript}
"""
    result = _chat_json(system_prompt, user_prompt)
    if not result.get("auto_loss"):
        return {"auto_loss": False}
    loser = result.get("loser_user_id")
    if loser not in (user_a_id, user_b_id):
        return {"auto_loss": False}
    return {
        "auto_loss": True,
        "loser_user_id": loser,
        "reason": result.get("reason", "Severe conduct violation detected."),
        "evidence": result.get("evidence", []),
    }


def _judge_once(
    profile: dict[str, str],
    transcript: str,
    user_a_id: int,
    user_b_id: int,
    handle_a: str,
    handle_b: str,
    side0_label: str,
    side1_label: str,
) -> dict[str, Any]:
    system_prompt = (
        "You are one member of a 5-judge panel. "
        f"{profile['bias']} "
        "Bias should act as a prior only. You may still vote against your prior if argument quality is clearly stronger. "
        "To reduce easy sweeps, require clear superiority before crossing to your opposite side. "
        "Evaluate quality, logic, rebuttals, evidence, responsiveness, and clarity."
    )
    user_prompt = f"""
Return strict JSON with keys:
- favored_user_id (integer, {user_a_id} or {user_b_id})
- confidence (number from 0 to 1)
- reason (string, max 220 chars)
- score_user_a (integer 0-100, this is ONLY for @{handle_a}, user_id {user_a_id})
- score_user_b (integer 0-100, this is ONLY for @{handle_b}, user_id {user_b_id})

Debate sides:
- Side A: {side0_label}
- Side B: {side1_label}

Debaters:
- {user_a_id}: @{handle_a}
- {user_b_id}: @{handle_b}

Transcript:
{transcript}
"""
    result = _chat_json(system_prompt, user_prompt)
    favored = result.get("favored_user_id")
    if favored not in (user_a_id, user_b_id):
        raise ValueError(f"Invalid judge vote from {profile['name']}")
    score_a = int(result.get("score_user_a", 50))
    score_b = int(result.get("score_user_b", 50))
    score_a = max(0, min(100, score_a))
    score_b = max(0, min(100, score_b))
    return {
        "name": profile["name"],
        "favored_user_id": favored,
        "reason": str(result.get("reason", "")).strip() or "Preferred argumentative performance.",
        "confidence": float(result.get("confidence", 0.5)),
        "score_a": score_a,
        "score_b": score_b,
    }


def _summarize_positions(
    transcript: str,
    user_a_id: int,
    user_b_id: int,
    handle_a: str,
    handle_b: str,
    side_a: int,
) -> dict[str, Any]:
    system_prompt = (
        "You summarize each debater's position and sentiment signals for analytics. "
        "Return concise, factual summaries."
    )
    user_prompt = f"""
Return strict JSON with key `players`, where players is an array of exactly 2 objects.
Each object keys:
- user_id (int)
- position_summary (string, <= 280 chars)
- sentiment_label (one of: positive, neutral, mixed, negative)
- sentiment_score (number from -1 to 1)
- toxicity_flags (array of strings)

Side mapping:
- side_a index in debate record: {side_a}
- User {user_a_id} (@{handle_a}) is side {side_a}
- User {user_b_id} (@{handle_b}) is side {1 - side_a}

Transcript:
{transcript}
"""
    result = _chat_json(system_prompt, user_prompt)
    players = result.get("players") or []
    mapped: list[dict[str, Any]] = []
    for p in players:
        uid = p.get("user_id")
        if uid not in (user_a_id, user_b_id):
            continue
        mapped.append(
            {
                "user_id": uid,
                "position_summary": str(p.get("position_summary", "")).strip()[:280],
                "sentiment_label": str(p.get("sentiment_label", "mixed")).lower(),
                "sentiment_score": float(p.get("sentiment_score", 0.0)),
                "toxicity_flags": p.get("toxicity_flags") or [],
            }
        )
    if len(mapped) != 2:
        raise ValueError("Could not parse both player summaries")
    return {"players": mapped}


def judge_transcript(
    messages: list,
    user_a_id: int,
    user_b_id: int,
    handle_a: str,
    handle_b: str,
    side_a: int,
    side0_label: str,
    side1_label: str,
) -> dict[str, Any]:
    transcript = _format_transcript(messages, handle_a, handle_b, user_a_id, user_b_id)

    moderation = _moderation_check(transcript, user_a_id, user_b_id, handle_a, handle_b)
    if moderation.get("auto_loss"):
        loser_id = moderation["loser_user_id"]
        winner_id = user_b_id if loser_id == user_a_id else user_a_id
        votes_a = 5 if winner_id == user_a_id else 0
        votes_b = 5 - votes_a
        return {
            "winner_user_id": winner_id,
            "loser_user_id": loser_id,
            "votes_side_a": votes_a,
            "votes_side_b": votes_b,
            "summary": f"Automatic loss for conduct violation: {moderation.get('reason', '')}",
            "personas": [],
            "decision_type": "auto_forfeit",
            "moderation": moderation,
            "quality": {"avg_score_a": 0.0, "avg_score_b": 0.0, "avg_confidence": 1.0},
            "analytics": _summarize_positions(
                transcript, user_a_id, user_b_id, handle_a, handle_b, side_a
            ),
        }

    with ThreadPoolExecutor(max_workers=6) as pool:
        judge_futures = [
            pool.submit(
                _judge_once,
                profile,
                transcript,
                user_a_id,
                user_b_id,
                handle_a,
                handle_b,
                side0_label,
                side1_label,
            )
            for profile in JUDGE_PROFILES
        ]
        summary_future = pool.submit(
            _summarize_positions, transcript, user_a_id, user_b_id, handle_a, handle_b, side_a
        )

        judge_outputs = [f.result() for f in judge_futures]
        analytics = summary_future.result()

    votes_a = sum(1 for j in judge_outputs if j["favored_user_id"] == user_a_id)
    votes_b = 5 - votes_a
    winner_id = user_a_id if votes_a >= votes_b else user_b_id
    loser_id = user_b_id if winner_id == user_a_id else user_a_id
    personas = []
    for j in judge_outputs:
        favored_handle = handle_a if j["favored_user_id"] == user_a_id else handle_b
        personas.append(
            {
                "name": j["name"],
                "favored_handle": favored_handle,
                "favored_user_id": j["favored_user_id"],
                "reason": j["reason"],
                "confidence": round(float(j["confidence"]), 3),
                "score_a": int(j["score_a"]),
                "score_b": int(j["score_b"]),
            }
        )
    avg_score_a = sum(int(j["score_a"]) for j in judge_outputs) / max(1, len(judge_outputs))
    avg_score_b = sum(int(j["score_b"]) for j in judge_outputs) / max(1, len(judge_outputs))
    avg_confidence = sum(float(j["confidence"]) for j in judge_outputs) / max(1, len(judge_outputs))

    return {
        "winner_user_id": winner_id,
        "loser_user_id": loser_id,
        "votes_side_a": votes_a,
        "votes_side_b": votes_b,
        "summary": f"Panel result: {votes_a}-{votes_b} for {'@' + handle_a if winner_id == user_a_id else '@' + handle_b}.",
        "personas": personas,
        "decision_type": "panel_vote",
        "moderation": {"auto_loss": False},
        "quality": {
            "avg_score_a": round(avg_score_a, 2),
            "avg_score_b": round(avg_score_b, 2),
            "avg_confidence": round(avg_confidence, 3),
        },
        "analytics": analytics,
    }


def _finalize_from_row(conn, row) -> dict[str, Any]:
    debate_id = row["id"]
    status = row["status"]
    if status not in ("active", "completed"):
        return {"ok": False, "reason": "not_finalizable"}
    if status == "completed" and (row["verdict_status"] or "") != "pending":
        return {"ok": False, "reason": "already_completed"}
    msgs = conn.execute(
        "SELECT user_id, body FROM messages WHERE debate_id = ? ORDER BY id ASC",
        (debate_id,),
    ).fetchall()
    now = utc_now().isoformat()
    try:
        verdict = judge_transcript(
            msgs,
            row["user_a_id"],
            row["user_b_id"],
            row["ha"],
            row["hb"],
            row["side_a"],
            row["side0_label"],
            row["side1_label"],
        )
        winner_id = verdict["winner_user_id"]
        loser_id = verdict["loser_user_id"]
        conn.execute(
            """
            UPDATE debates
            SET status = 'completed',
                ended_at = ?,
                winner_user_id = ?,
                judge_json = ?,
                verdict_status = 'ready',
                judge_error = NULL
            WHERE id = ?
            """,
            (now, winner_id, json.dumps(verdict), debate_id),
        )
        conn.execute("DELETE FROM debate_ai_summaries WHERE debate_id = ?", (debate_id,))
        for p in verdict.get("analytics", {}).get("players", []):
            uid = p["user_id"]
            side = row["side_a"] if uid == row["user_a_id"] else (1 - row["side_a"])
            conn.execute(
                """
                INSERT INTO debate_ai_summaries (
                    debate_id, user_id, side, topic_day_key, topic_title, position_summary, sentiment_label, sentiment_score, toxicity_flags
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    debate_id,
                    uid,
                    side,
                    row["topic_day_key"],
                    row["topic_title"],
                    p["position_summary"],
                    p["sentiment_label"],
                    p["sentiment_score"],
                    json.dumps(p.get("toxicity_flags") or []),
                ),
            )
        apply_elo(conn, winner_id, loser_id)
        conn.commit()
        return {"ok": True, "status": "ready"}
    except Exception as exc:
        pending_payload = {
            "summary": "AI judging pipeline is currently unavailable. This debate is marked pending judgment.",
            "personas": [],
            "pending_judgment": True,
        }
        conn.execute(
            """
            UPDATE debates
            SET status = 'completed',
                ended_at = COALESCE(ended_at, ?),
                winner_user_id = NULL,
                judge_json = ?,
                verdict_status = 'pending',
                judge_error = ?
            WHERE id = ?
            """,
            (now, json.dumps(pending_payload), str(exc)[:500], debate_id),
        )
        conn.commit()
        return {"ok": False, "status": "pending", "error": str(exc)}


def finalize_debate(conn, debate_id: int) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT d.*, ua.handle AS ha, ub.handle AS hb, t.side0_label, t.side1_label, t.title AS topic_title
        FROM debates d
        JOIN users ua ON ua.id = d.user_a_id
        JOIN users ub ON ub.id = d.user_b_id
        JOIN topics t ON t.day_key = d.topic_day_key
        WHERE d.id = ? AND d.status = 'active'
        """,
        (debate_id,),
    ).fetchone()
    if not row:
        return {"ok": False, "reason": "not_active"}
    return _finalize_from_row(conn, row)


def retry_pending_debate(conn, debate_id: int) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT d.*, ua.handle AS ha, ub.handle AS hb, t.side0_label, t.side1_label, t.title AS topic_title
        FROM debates d
        JOIN users ua ON ua.id = d.user_a_id
        JOIN users ub ON ub.id = d.user_b_id
        JOIN topics t ON t.day_key = d.topic_day_key
        WHERE d.id = ? AND d.status = 'completed' AND d.verdict_status = 'pending'
        """,
        (debate_id,),
    ).fetchone()
    if not row:
        return {"ok": False, "reason": "not_pending"}
    return _finalize_from_row(conn, row)


def try_match_queue(conn, user_id: int, day_key: str, side: int, topic_variant: str = "A") -> int | None:
    """Pair with longest-waiting opponent on opposite side. Returns debate_id or None."""
    ensure_topic_row(conn, day_key)
    conn.execute("DELETE FROM queue_entries WHERE user_id = ?", (user_id,))
    conn.execute(
        "INSERT INTO queue_entries (user_id, day_key, topic_variant, side) VALUES (?, ?, ?, ?)",
        (user_id, day_key, topic_variant, side),
    )

    opp = conn.execute(
        """
        SELECT user_id FROM queue_entries
        WHERE day_key = ? AND topic_variant = ? AND side != ? AND user_id != ?
        ORDER BY created_at ASC LIMIT 1
        """,
        (day_key, topic_variant, side, user_id),
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
            topic_day_key, topic_variant, user_a_id, user_b_id, side_a, status, ends_at, last_activity_at
        ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?)
        """,
        (day_key, topic_variant, ua, ub, sa, ends, last),
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
