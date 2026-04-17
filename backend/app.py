"""Great Debate Flask app: static frontend + JSON API."""

import json
import re
import secrets
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

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


@app.get("/")
def index():
    return send_from_directory(_FRONTEND, "index.html")


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
    logic.ensure_topic_row(conn, dk)
    conn.commit()
    row = conn.execute("SELECT * FROM topics WHERE day_key = ?", (dk,)).fetchone()
    return jsonify(
        {
            "day_key": row["day_key"],
            "title": row["title"],
            "description": row["description"],
            "sides": [row["side0_label"], row["side1_label"]],
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
    debate_id = logic.try_match_queue(conn, u["id"], dk, side)
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
               t.title AS topic_title, t.description AS topic_desc,
               t.side0_label, t.side1_label
        FROM debates d
        JOIN users ua ON ua.id = d.user_a_id
        JOIN users ub ON ub.id = d.user_b_id
        JOIN topics t ON t.day_key = d.topic_day_key
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
        "topic": {
            "title": d["topic_title"],
            "description": d["topic_desc"],
            "sides": [d["side0_label"], d["side1_label"]],
        },
        "me_side": me_side,
        "opponent": {"id": opp_id, "handle": opp_handle},
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
    if d["status"] == "completed" and d["judge_json"]:
        out["verdict"] = json.loads(d["judge_json"])
        out["winner_user_id"] = d["winner_user_id"]
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

    logic.finalize_debate(conn, debate_id)
    return jsonify({"ok": True, "status": "completed"})

