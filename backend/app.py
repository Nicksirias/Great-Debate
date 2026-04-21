"""Great Debate Flask app: static frontend + JSON API."""

import json
import os
import re
import secrets
from io import BytesIO
from pathlib import Path

from flask import Flask, jsonify, request, send_file, send_from_directory
from openpyxl import Workbook

from backend.database import close_db, get_db, init_db
from backend import logic

_FRONTEND = Path(__file__).resolve().parent.parent / "frontend"

app = Flask(__name__)
app.teardown_appcontext(close_db)

init_db()


@app.before_request
def _run_cleanup():
    if not request.path.startswith("/api"):
        return None
    conn = get_db()
    logic.cleanup_inactive_debates(conn)


def _json_error(code: int, message: str):
    body = {"error": message}
    r = jsonify(body)
    r.status_code = code
    return r


def _current_user(conn):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth[7:].strip()
    if not token:
        return None
    return conn.execute("SELECT * FROM users WHERE token = ?", (token,)).fetchone()


def _require_user(conn):
    u = _current_user(conn)
    if not u:
        return None, _json_error(401, "Missing or invalid session. Register and send Authorization: Bearer <token>.")
    return u, None


def _require_admin():
    configured = (os.environ.get("ADMIN_TOKEN") or "").strip()
    if not configured:
        return _json_error(503, "ADMIN_TOKEN is not configured on the server.")
    provided = (request.headers.get("X-Admin-Token") or request.args.get("token") or "").strip()
    if provided != configured:
        return _json_error(401, "Missing or invalid admin token.")
    return None


@app.get("/")
def index():
    return send_from_directory(_FRONTEND, "index.html")


@app.get("/admin")
def admin_index():
    return send_from_directory(_FRONTEND, "admin.html")


@app.get("/debate")
def debate_page():
    return send_from_directory(_FRONTEND, "debate.html")


@app.get("/results")
def results_page():
    return send_from_directory(_FRONTEND, "results.html")


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/api/users/register")
def register():
    data = request.get_json(silent=True) or {}
    handle = (data.get("handle") or "").strip()
    if not re.match(r"^[a-zA-Z0-9_]{2,32}$", handle):
        return _json_error(400, "Handle must be 2–32 chars: letters, digits, underscore.")
    conn = get_db()
    exists = conn.execute("SELECT id FROM users WHERE handle = ?", (handle,)).fetchone()
    if exists:
        return _json_error(409, "That handle is already taken.")
    token = secrets.token_hex(32)
    cur = conn.execute(
        "INSERT INTO users (handle, token) VALUES (?, ?)",
        (handle, token),
    )
    conn.commit()
    uid = cur.lastrowid
    row = conn.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()
    return jsonify(
        {
            "token": token,
            "user": {
                "id": row["id"],
                "handle": row["handle"],
                "rating": row["rating"],
                "tier": logic.tier_for_rating(row["rating"]),
            },
        }
    )


@app.get("/api/me")
def me():
    conn = get_db()
    u, err = _require_user(conn)
    if err:
        return err
    return jsonify(
        {
            "id": u["id"],
            "handle": u["handle"],
            "rating": u["rating"],
            "wins": u["wins"],
            "losses": u["losses"],
            "tier": logic.tier_for_rating(u["rating"]),
        }
    )


@app.get("/api/topic/today")
def topic_today():
    conn = get_db()
    dk = logic.day_key_from_dt()
    u = _current_user(conn)
    topic = logic.topic_for_user(conn, dk, u["id"] if u else None)
    conn.commit()
    return jsonify(
        {
            "day_key": dk,
            "variant": topic["variant"],
            "title": topic["title"],
            "description": topic["description"],
            "sides": topic["sides"],
        }
    )


@app.get("/api/leaderboard")
def leaderboard():
    scope = (request.args.get("scope") or "all").lower()
    conn = get_db()
    dk = logic.day_key_from_dt()
    if scope == "daily":
        rows = conn.execute(
            """
            SELECT u.handle, u.rating FROM users u
            WHERE EXISTS (
                SELECT 1 FROM debates d
                WHERE (d.user_a_id = u.id OR d.user_b_id = u.id)
                AND d.status = 'completed'
                AND date(d.ended_at) = date('now')
            )
            ORDER BY u.rating DESC
            LIMIT 10
            """
        ).fetchall()
        if len(rows) < 3:
            rows = conn.execute(
                "SELECT handle, rating FROM users ORDER BY rating DESC LIMIT 10"
            ).fetchall()
    else:
        rows = conn.execute(
            "SELECT handle, rating FROM users ORDER BY rating DESC LIMIT 10"
        ).fetchall()
    return jsonify(
        {
            "scope": scope,
            "rows": [{"handle": r["handle"], "rating": round(r["rating"], 1)} for r in rows],
        }
    )


@app.post("/api/queue/join")
def queue_join():
    data = request.get_json(silent=True) or {}
    side = data.get("side")
    if side not in (0, 1):
        return _json_error(400, 'JSON body must include "side": 0 or 1.')
    conn = get_db()
    u, err = _require_user(conn)
    if err:
        return err

    active = conn.execute(
        """
        SELECT id FROM debates
        WHERE status = 'active' AND (user_a_id = ? OR user_b_id = ?)
        """,
        (u["id"], u["id"]),
    ).fetchone()
    if active:
        return jsonify({"status": "matched", "debate_id": active["id"]})

    dk = logic.day_key_from_dt()
    topic = logic.topic_for_user(conn, dk, u["id"])
    debate_id = logic.try_match_queue(conn, u["id"], dk, side, topic_variant=topic["variant"])
    if debate_id:
        return jsonify({"status": "matched", "debate_id": debate_id})
    return jsonify({"status": "waiting"})


@app.post("/api/queue/leave")
def queue_leave():
    conn = get_db()
    u, err = _require_user(conn)
    if err:
        return err
    conn.execute("DELETE FROM queue_entries WHERE user_id = ?", (u["id"],))
    conn.commit()
    return jsonify({"ok": True})


@app.get("/api/queue/status")
def queue_status():
    conn = get_db()
    u, err = _require_user(conn)
    if err:
        return err

    active = conn.execute(
        """
        SELECT id FROM debates
        WHERE status = 'active' AND (user_a_id = ? OR user_b_id = ?)
        """,
        (u["id"], u["id"]),
    ).fetchone()
    if active:
        return jsonify({"status": "matched", "debate_id": active["id"]})

    q = conn.execute("SELECT 1 FROM queue_entries WHERE user_id = ?", (u["id"],)).fetchone()
    if q:
        return jsonify({"status": "waiting"})
    return jsonify({"status": "idle"})


def _debate_payload(conn, debate_id: int, user_id: int):
    d = conn.execute(
        """
        SELECT d.*, ua.handle AS ha, ub.handle AS hb,
               ua.rating AS ra, ub.rating AS rb,
               COALESCE(te.title, t.title) AS topic_title,
               COALESCE(te.description, t.description) AS topic_desc,
               COALESCE(te.side0_label, t.side0_label) AS side0_label,
               COALESCE(te.side1_label, t.side1_label) AS side1_label
        FROM debates d
        JOIN users ua ON ua.id = d.user_a_id
        JOIN users ub ON ub.id = d.user_b_id
        JOIN topics t ON t.day_key = d.topic_day_key
        LEFT JOIN topic_experiments te ON te.day_key = d.topic_day_key AND te.variant = d.topic_variant
        WHERE d.id = ?
        """,
        (debate_id,),
    ).fetchone()
    if not d:
        return None

    if d["user_a_id"] != user_id and d["user_b_id"] != user_id:
        return "forbidden"

    me_side = d["side_a"] if d["user_a_id"] == user_id else (1 - d["side_a"])
    opp_id = d["user_b_id"] if d["user_a_id"] == user_id else d["user_a_id"]
    opp_handle = d["hb"] if d["user_a_id"] == user_id else d["ha"]

    msgs = conn.execute(
        """
        SELECT m.id, m.user_id, u.handle, m.body, m.created_at
        FROM messages m
        JOIN users u ON u.id = m.user_id
        WHERE m.debate_id = ?
        ORDER BY m.id ASC
        """,
        (debate_id,),
    ).fetchall()

    out = {
        "id": d["id"],
        "status": d["status"],
        "verdict_status": d["verdict_status"] or "ready",
        "topic": {
            "title": d["topic_title"],
            "description": d["topic_desc"],
            "sides": [d["side0_label"], d["side1_label"]],
        },
        "me_side": me_side,
        "opponent": {
            "id": opp_id,
            "handle": opp_handle,
            "rating": d["rb"] if d["user_a_id"] == user_id else d["ra"],
        },
        "me": {
            "id": user_id,
            "handle": d["ha"] if d["user_a_id"] == user_id else d["hb"],
            "rating": d["ra"] if d["user_a_id"] == user_id else d["rb"],
        },
        "ends_at": d["ends_at"],
        "messages": [
            {
                "id": m["id"],
                "user_id": m["user_id"],
                "handle": m["handle"],
                "body": m["body"],
                "created_at": m["created_at"],
            }
            for m in msgs
        ],
    }
    out["stats"] = {
        "message_count_total": len(msgs),
        "message_count_me": sum(1 for m in msgs if m["user_id"] == user_id),
        "message_count_opponent": sum(1 for m in msgs if m["user_id"] == opp_id),
        "char_count_me": sum(len(m["body"] or "") for m in msgs if m["user_id"] == user_id),
        "char_count_opponent": sum(len(m["body"] or "") for m in msgs if m["user_id"] == opp_id),
    }
    if d["status"] == "completed" and d["judge_json"]:
        out["verdict"] = json.loads(d["judge_json"])
        out["winner_user_id"] = d["winner_user_id"]
        if d["judge_error"]:
            out["judge_error"] = d["judge_error"]
    return out


@app.get("/api/debate/<int:debate_id>")
def get_debate(debate_id: int):
    conn = get_db()
    u, err = _require_user(conn)
    if err:
        return err
    payload = _debate_payload(conn, debate_id, u["id"])
    if payload == "forbidden":
        return _json_error(403, "You are not in this debate.")
    if payload is None:
        return _json_error(404, "Debate not found.")
    return jsonify(payload)


@app.post("/api/debate/<int:debate_id>/message")
def post_message(debate_id: int):
    data = request.get_json(silent=True) or {}
    body = (data.get("text") or "").strip()
    if not body:
        return _json_error(400, 'JSON body needs non-empty "text".')
    if len(body) > 8000:
        return _json_error(400, "Message too long (max 8000 chars).")

    conn = get_db()
    u, err = _require_user(conn)
    if err:
        return err

    d = conn.execute(
        "SELECT * FROM debates WHERE id = ? AND status = 'active'",
        (debate_id,),
    ).fetchone()
    if not d:
        return _json_error(404, "No active debate with that id.")

    if d["user_a_id"] != u["id"] and d["user_b_id"] != u["id"]:
        return _json_error(403, "You are not in this debate.")

    now = logic.utc_now().isoformat()
    conn.execute(
        "INSERT INTO messages (debate_id, user_id, body, created_at) VALUES (?, ?, ?, ?)",
        (debate_id, u["id"], body, now),
    )
    conn.execute(
        "UPDATE debates SET last_activity_at = ? WHERE id = ?",
        (now, debate_id),
    )
    conn.commit()
    return jsonify({"ok": True})


@app.post("/api/debate/<int:debate_id>/finish")
def finish_debate(debate_id: int):
    conn = get_db()
    u, err = _require_user(conn)
    if err:
        return err

    d = conn.execute(
        "SELECT * FROM debates WHERE id = ? AND status = 'active'",
        (debate_id,),
    ).fetchone()
    if not d:
        return _json_error(404, "No active debate with that id.")

    if d["user_a_id"] != u["id"] and d["user_b_id"] != u["id"]:
        return _json_error(403, "You are not in this debate.")

    result = logic.finalize_debate(conn, debate_id)
    if result.get("status") == "pending":
        r = jsonify(
            {
                "ok": False,
                "status": "pending",
                "error": "AI judging pipeline is unavailable. Debate marked pending judgment.",
            }
        )
        r.status_code = 503
        return r
    return jsonify({"ok": True, "status": "completed"})


@app.post("/api/admin/retry-pending")
def admin_retry_pending():
    err = _require_admin()
    if err:
        return err
    conn = get_db()
    limit = max(1, min(int(request.args.get("limit") or 10), 100))
    rows = conn.execute(
        """
        SELECT id FROM debates
        WHERE status = 'completed' AND verdict_status = 'pending'
        ORDER BY ended_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    retried = 0
    success = 0
    for r in rows:
        retried += 1
        out = logic.retry_pending_debate(conn, r["id"])
        if out.get("ok"):
            success += 1
    return jsonify({"retried": retried, "succeeded": success, "failed": retried - success})


@app.get("/api/admin/insight-brief")
def admin_insight_brief():
    err = _require_admin()
    if err:
        return err
    conn = get_db()
    where_sql, params = _admin_filter_args()
    rows = _admin_recent_rows(conn, where_sql, params)
    if not rows:
        return jsonify({"brief": "No records in selected filter window."})
    sentiment_counts = {}
    side_counts = {}
    topic_counts = {}
    for r in rows:
        sentiment_counts[r["sentiment_label"]] = sentiment_counts.get(r["sentiment_label"], 0) + 1
        side_counts[r["side_label"]] = side_counts.get(r["side_label"], 0) + 1
        topic_counts[r["topic_title"]] = topic_counts.get(r["topic_title"], 0) + 1
    top_sentiment = max(sentiment_counts, key=sentiment_counts.get)
    top_side = max(side_counts, key=side_counts.get)
    top_topic = max(topic_counts, key=topic_counts.get)
    avg_score = sum(float(r["sentiment_score"] or 0) for r in rows) / max(1, len(rows))
    brief = (
        f"Window summary: {len(rows)} player-opinion records were captured. "
        f"Dominant sentiment was '{top_sentiment}' with an average sentiment score of {avg_score:.2f}. "
        f"The most represented argument side label was '{top_side}', and the top-discussed topic was '{top_topic}'. "
        "Use side-level and topic-level sentiment deltas to identify messaging opportunities and customer concerns."
    )
    return jsonify({"brief": brief})


@app.get("/api/admin/question-intelligence")
def admin_question_intelligence():
    err = _require_admin()
    if err:
        return err
    conn = get_db()
    where_sql, params = _admin_filter_args()
    rows = conn.execute(
        f"""
        SELECT COALESCE(s.topic_title, t.title) AS topic,
               s.side,
               CASE WHEN s.side = 0 THEN t.side0_label ELSE t.side1_label END AS side_label,
               COUNT(*) AS n,
               AVG(s.sentiment_score) AS avg_sentiment
        FROM debate_ai_summaries s
        JOIN debates d ON d.id = s.debate_id
        JOIN topics t ON t.day_key = d.topic_day_key
        {where_sql}
        GROUP BY topic, s.side, side_label
        ORDER BY topic ASC, s.side ASC
        """,
        tuple(params),
    ).fetchall()
    grouped: dict[str, dict] = {}
    for r in rows:
        t = r["topic"]
        if t not in grouped:
            grouped[t] = {"topic": t, "total": 0, "sides": []}
        grouped[t]["total"] += int(r["n"])
        grouped[t]["sides"].append(
            {
                "side": r["side"],
                "side_label": r["side_label"],
                "count": int(r["n"]),
                "avg_sentiment": round(float(r["avg_sentiment"] or 0), 3),
            }
        )
    return jsonify({"topics": list(grouped.values())})


@app.get("/api/admin/cohort-segments")
def admin_cohort_segments():
    err = _require_admin()
    if err:
        return err
    conn = get_db()
    where_sql, params = _admin_filter_args()
    rows = conn.execute(
        f"""
        SELECT s.user_id, s.sentiment_score, u.rating, u.created_at
        FROM debate_ai_summaries s
        JOIN users u ON u.id = s.user_id
        JOIN debates d ON d.id = s.debate_id
        JOIN topics t ON t.day_key = d.topic_day_key
        {where_sql}
        """,
        tuple(params),
    ).fetchall()
    def tier(rating: float) -> str:
        return logic.tier_for_rating(float(rating or 1500.0))
    by_tier: dict[str, dict] = {}
    by_user_count: dict[int, int] = {}
    for r in rows:
        tr = tier(r["rating"])
        if tr not in by_tier:
            by_tier[tr] = {"tier": tr, "count": 0, "sentiment_total": 0.0}
        by_tier[tr]["count"] += 1
        by_tier[tr]["sentiment_total"] += float(r["sentiment_score"] or 0)
        by_user_count[r["user_id"]] = by_user_count.get(r["user_id"], 0) + 1
    tiers = [
        {
            "tier": k,
            "count": v["count"],
            "avg_sentiment": round(v["sentiment_total"] / max(1, v["count"]), 3),
        }
        for k, v in by_tier.items()
    ]
    new_users = sum(1 for _, c in by_user_count.items() if c == 1)
    returning = sum(1 for _, c in by_user_count.items() if c > 1)
    return jsonify({"tiers": tiers, "user_frequency": {"new": new_users, "returning": returning}})


@app.get("/api/admin/toxicity-trends")
def admin_toxicity_trends():
    err = _require_admin()
    if err:
        return err
    conn = get_db()
    where_sql, params = _admin_filter_args()
    rows = _admin_recent_rows(conn, where_sql, params)
    by_flag: dict[str, int] = {}
    by_day: dict[str, int] = {}
    for r in rows:
        day = str(r["created_at"])[:10]
        flags = json.loads(r["toxicity_flags"] or "[]")
        if flags:
            by_day[day] = by_day.get(day, 0) + 1
        for f in flags:
            by_flag[f] = by_flag.get(f, 0) + 1
    return jsonify(
        {
            "by_flag": [{"flag": k, "count": v} for k, v in sorted(by_flag.items(), key=lambda x: -x[1])],
            "by_day": [{"day": k, "count": v} for k, v in sorted(by_day.items(), key=lambda x: x[0])],
        }
    )


@app.get("/api/admin/debater-profiles")
def admin_debater_profiles():
    err = _require_admin()
    if err:
        return err
    conn = get_db()
    where_sql, params = _admin_filter_args()
    rows = conn.execute(
        f"""
        SELECT s.user_id, u.handle, u.rating, u.wins, u.losses, s.sentiment_score, s.debate_id
        FROM debate_ai_summaries s
        JOIN users u ON u.id = s.user_id
        JOIN debates d ON d.id = s.debate_id
        JOIN topics t ON t.day_key = d.topic_day_key
        {where_sql}
        """,
        tuple(params),
    ).fetchall()
    debate_votes = conn.execute(
        """
        SELECT id, user_a_id, user_b_id, side_a, judge_json
        FROM debates
        WHERE status='completed' AND verdict_status='ready' AND judge_json IS NOT NULL
        """
    ).fetchall()
    margin_by_user: dict[int, list[float]] = {}
    for d in debate_votes:
        try:
            j = json.loads(d["judge_json"] or "{}")
            va = float(j.get("votes_side_a", 0))
            vb = float(j.get("votes_side_b", 0))
        except Exception:
            continue
        margin_a = va - vb
        margin_b = vb - va
        margin_by_user.setdefault(d["user_a_id"], []).append(margin_a)
        margin_by_user.setdefault(d["user_b_id"], []).append(margin_b)
    out: dict[int, dict] = {}
    for r in rows:
        uid = r["user_id"]
        if uid not in out:
            out[uid] = {
                "user_id": uid,
                "handle": r["handle"],
                "rating": r["rating"],
                "wins": r["wins"],
                "losses": r["losses"],
                "records": 0,
                "sent_total": 0.0,
            }
        out[uid]["records"] += 1
        out[uid]["sent_total"] += float(r["sentiment_score"] or 0)
    profiles = []
    for uid, p in out.items():
        margins = margin_by_user.get(uid, [])
        profiles.append(
            {
                "user_id": uid,
                "handle": p["handle"],
                "rating": p["rating"],
                "wins": p["wins"],
                "losses": p["losses"],
                "records": p["records"],
                "avg_sentiment": round(p["sent_total"] / max(1, p["records"]), 3),
                "persuasion_delta": round(sum(margins) / max(1, len(margins)), 3),
            }
        )
    profiles.sort(key=lambda x: (-x["records"], -x["rating"]))
    return jsonify({"profiles": profiles[:50]})


@app.get("/api/admin/experiments")
def admin_experiments():
    err = _require_admin()
    if err:
        return err
    day_key = (request.args.get("day_key") or logic.day_key_from_dt()).strip()
    conn = get_db()
    rows = conn.execute(
        """
        SELECT day_key, variant, title, description, side0_label, side1_label, is_active, created_at
        FROM topic_experiments
        WHERE day_key = ?
        ORDER BY variant ASC
        """,
        (day_key,),
    ).fetchall()
    return jsonify({"day_key": day_key, "variants": [dict(r) for r in rows]})


@app.post("/api/admin/experiments")
def admin_create_experiment():
    err = _require_admin()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    day_key = (data.get("day_key") or logic.day_key_from_dt()).strip()
    variant = (data.get("variant") or "").strip().upper()
    title = (data.get("title") or "").strip()
    description = (data.get("description") or "").strip()
    sides = data.get("sides") or []
    if not re.match(r"^[A-Z0-9]{1,8}$", variant):
        return _json_error(400, "Variant must be 1-8 chars (A-Z, 0-9).")
    if len(sides) != 2:
        return _json_error(400, "sides must contain exactly 2 labels.")
    conn = get_db()
    conn.execute(
        """
        INSERT OR REPLACE INTO topic_experiments
        (day_key, variant, title, description, side0_label, side1_label, is_active)
        VALUES (?, ?, ?, ?, ?, ?, 1)
        """,
        (day_key, variant, title, description, str(sides[0]), str(sides[1])),
    )
    conn.commit()
    return jsonify({"ok": True, "day_key": day_key, "variant": variant})


def _admin_filter_args():
    start = (request.args.get("start") or "").strip()
    end = (request.args.get("end") or "").strip()
    topic = (request.args.get("topic") or "").strip()
    side = request.args.get("side")
    where = []
    params = []
    if start:
        where.append("date(s.created_at) >= date(?)")
        params.append(start)
    if end:
        where.append("date(s.created_at) <= date(?)")
        params.append(end)
    if topic:
        where.append("LOWER(COALESCE(s.topic_title, t.title)) LIKE ?")
        params.append(f"%{topic.lower()}%")
    if side in ("0", "1"):
        where.append("s.side = ?")
        params.append(int(side))
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    return where_sql, params


def _admin_recent_rows(conn, where_sql: str, params: list):
    return conn.execute(
        f"""
        SELECT s.debate_id, s.user_id, u.handle, s.side,
               COALESCE(s.topic_title, t.title) AS topic_title,
               CASE WHEN s.side = 0 THEN t.side0_label ELSE t.side1_label END AS side_label,
               s.position_summary, s.sentiment_label, s.sentiment_score,
               s.toxicity_flags, s.created_at
        FROM debate_ai_summaries s
        JOIN users u ON u.id = s.user_id
        JOIN debates d ON d.id = s.debate_id
        JOIN topics t ON t.day_key = d.topic_day_key
        {where_sql}
        ORDER BY s.id DESC
        LIMIT 300
        """,
        tuple(params),
    ).fetchall()


@app.get("/api/admin/sentiment")
def admin_sentiment():
    err = _require_admin()
    if err:
        return err
    conn = get_db()
    where_sql, params = _admin_filter_args()
    rows = conn.execute(
        f"""
        SELECT sentiment_label, COUNT(*) AS n
        FROM debate_ai_summaries s
        JOIN debates d ON d.id = s.debate_id
        JOIN topics t ON t.day_key = d.topic_day_key
        {where_sql}
        GROUP BY sentiment_label
        ORDER BY n DESC
        """,
        tuple(params),
    ).fetchall()
    recent = _admin_recent_rows(conn, where_sql, params)
    topics = conn.execute(
        f"""
        SELECT COALESCE(s.topic_title, t.title) AS topic, COUNT(*) AS n, AVG(s.sentiment_score) AS avg_sentiment
        FROM debate_ai_summaries s
        JOIN debates d ON d.id = s.debate_id
        JOIN topics t ON t.day_key = d.topic_day_key
        {where_sql}
        GROUP BY topic
        ORDER BY n DESC
        LIMIT 12
        """,
        tuple(params),
    ).fetchall()
    side_stats = conn.execute(
        f"""
        SELECT s.side,
               CASE WHEN s.side = 0 THEN t.side0_label ELSE t.side1_label END AS side_label,
               COUNT(*) AS n,
               AVG(s.sentiment_score) AS avg_sentiment
        FROM debate_ai_summaries s
        JOIN debates d ON d.id = s.debate_id
        JOIN topics t ON t.day_key = d.topic_day_key
        {where_sql}
        GROUP BY s.side, side_label
        ORDER BY n DESC
        LIMIT 20
        """,
        tuple(params),
    ).fetchall()
    daily = conn.execute(
        f"""
        SELECT substr(s.created_at, 1, 10) AS day, COUNT(*) AS n, AVG(s.sentiment_score) AS avg_sentiment
        FROM debate_ai_summaries s
        JOIN debates d ON d.id = s.debate_id
        JOIN topics t ON t.day_key = d.topic_day_key
        {where_sql}
        GROUP BY day
        ORDER BY day ASC
        """,
        tuple(params),
    ).fetchall()
    sweep = conn.execute(
        f"""
        SELECT
          SUM(CASE
                WHEN d.status='completed'
                 AND d.verdict_status='ready'
                 AND json_extract(d.judge_json, '$.votes_side_a') IN (0,5)
                THEN 1 ELSE 0 END) AS sweep_count,
          SUM(CASE
                WHEN d.status='completed'
                 AND d.verdict_status='ready'
                THEN 1 ELSE 0 END) AS judged_count
        FROM debates d
        JOIN topics t ON t.day_key = d.topic_day_key
        WHERE d.id IN (
            SELECT DISTINCT s.debate_id
            FROM debate_ai_summaries s
            JOIN debates d2 ON d2.id = s.debate_id
            JOIN topics t2 ON t2.day_key = d2.topic_day_key
            {where_sql.replace("s.", "s.").replace("t.", "t2.")}
        )
        """
        ,
        tuple(params),
    ).fetchone()
    return jsonify(
        {
            "counts": [{"sentiment": r["sentiment_label"], "count": r["n"]} for r in rows],
            "topics": [
                {"topic": r["topic"], "count": r["n"], "avg_sentiment": round(float(r["avg_sentiment"] or 0), 3)}
                for r in topics
            ],
            "sides": [
                {
                    "side": r["side"],
                    "side_label": r["side_label"],
                    "count": r["n"],
                    "avg_sentiment": round(float(r["avg_sentiment"] or 0), 3),
                }
                for r in side_stats
            ],
            "daily": [
                {"day": r["day"], "count": r["n"], "avg_sentiment": round(float(r["avg_sentiment"] or 0), 3)}
                for r in daily
            ],
            "kpis": {
                "records": len(recent),
                "sweep_rate": round(
                    (float(sweep["sweep_count"] or 0) / float(sweep["judged_count"] or 1)) * 100.0, 2
                ),
            },
            "recent": [
                {
                    "debate_id": r["debate_id"],
                    "user_id": r["user_id"],
                    "handle": r["handle"],
                    "side": r["side"],
                    "side_label": r["side_label"],
                    "topic_title": r["topic_title"],
                    "position_summary": r["position_summary"],
                    "sentiment_label": r["sentiment_label"],
                    "sentiment_score": r["sentiment_score"],
                    "toxicity_flags": json.loads(r["toxicity_flags"] or "[]"),
                    "created_at": r["created_at"],
                }
                for r in recent
            ],
        }
    )


@app.get("/api/admin/sentiment/export.xlsx")
def admin_sentiment_export():
    err = _require_admin()
    if err:
        return err
    conn = get_db()
    where_sql, params = _admin_filter_args()
    recent = _admin_recent_rows(conn, where_sql, params)
    wb = Workbook()
    ws = wb.active
    ws.title = "Sentiment Export"
    ws.append(
        [
            "debate_id",
            "user_id",
            "handle",
            "topic_title",
            "side",
            "side_label",
            "sentiment_label",
            "sentiment_score",
            "position_summary",
            "toxicity_flags",
            "created_at",
        ]
    )
    for r in recent:
        ws.append(
            [
                r["debate_id"],
                r["user_id"],
                r["handle"],
                r["topic_title"],
                r["side"],
                r["side_label"],
                r["sentiment_label"],
                float(r["sentiment_score"] or 0),
                r["position_summary"],
                ", ".join(json.loads(r["toxicity_flags"] or "[]")),
                r["created_at"],
            ]
        )
    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    return send_file(
        bio,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="great_debate_sentiment_export.xlsx",
    )

