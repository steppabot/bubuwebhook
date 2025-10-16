import os
from datetime import datetime
from typing import Any, Dict, List

from flask import Flask, request, jsonify, abort
import psycopg

app = Flask(__name__)

# --- DB helpers (single conn per request) ---
def get_conn():
    dsn = os.environ["DATABASE_URL"]
    # be safe if not present
    if "sslmode=" not in dsn:
        if "?" in dsn:
            dsn += "&sslmode=require"
        else:
            dsn += "?sslmode=require"
    return psycopg.connect(dsn)  # we'll commit once per request

def _parse_iso(s: str | None):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

# --- Core persistence (use an existing cursor) ---
def ensure_user(cur, user_id: int):
    cur.execute("INSERT INTO users (user_id) VALUES (%s) ON CONFLICT DO NOTHING;", (user_id,))

def upsert_entitlement(cur, ent: Dict[str, Any]):
    status = (ent.get("status") or "active").lower()
    cur.execute("""
        INSERT INTO entitlements (
            entitlement_id, user_id, sku_id, starts_at, ends_at, is_gift, status
        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (entitlement_id) DO UPDATE
           SET user_id   = EXCLUDED.user_id,
               sku_id    = EXCLUDED.sku_id,
               starts_at = EXCLUDED.starts_at,
               ends_at   = EXCLUDED.ends_at,
               is_gift   = EXCLUDED.is_gift,
               status    = EXCLUDED.status,
               updated_at= NOW();
    """, (
        ent["id"],
        int(ent["user_id"]),
        int(ent["sku_id"]),
        _parse_iso(ent.get("starts_at")),
        _parse_iso(ent.get("ends_at")),
        bool(ent.get("is_gift", False)),
        status,
    ))

def mark_premium(cur, user_id: int, ends_at):
    ensure_user(cur, user_id)
    cur.execute("""
        UPDATE users
           SET tier = 'premium',
               premium_expires_at = %s
         WHERE user_id = %s;
    """, (ends_at, user_id))

def remove_premium_if_no_active(cur, user_id: int):
    """Only downgrade if the user has no active entitlements left."""
    cur.execute("""
        SELECT 1
          FROM entitlements
         WHERE user_id = %s
           AND status = 'active'
         LIMIT 1;
    """, (user_id,))
    still_active = cur.fetchone() is not None
    if not still_active:
        cur.execute("""
            UPDATE users
               SET tier = 'free',
                   premium_expires_at = NULL
             WHERE user_id = %s;
        """, (user_id,))

def delete_entitlement(cur, entitlement_id: str):
    cur.execute("DELETE FROM entitlements WHERE entitlement_id=%s;", (entitlement_id,))

# --- Event handling (uses one cursor) ---
def handle_event(cur, evt: Dict[str, Any]):
    etype = (evt.get("type") or "").upper()
    data  = evt.get("data", {})
    items: List[Dict[str, Any]] = data if isinstance(data, list) else [data]

    for item in items:
        uid = int(item["user_id"])

        if etype == "ENTITLEMENT_CREATE":
            ensure_user(cur, uid)
            upsert_entitlement(cur, {**item, "status": item.get("status") or "active"})
            mark_premium(cur, uid, _parse_iso(item.get("ends_at")))
            # no downgrade check needed (we just activated)

        elif etype == "ENTITLEMENT_UPDATE":
            ensure_user(cur, uid)
            upsert_entitlement(cur, item)
            status = (item.get("status") or "").lower()
            if status in ("active", "fulfilled"):
                mark_premium(cur, uid, _parse_iso(item.get("ends_at")))
            elif status in ("revoked", "expired", "canceled", "cancelled"):
                remove_premium_if_no_active(cur, uid)

        elif etype == "ENTITLEMENT_DELETE":
            # delete the record, then see if any active ones remain
            delete_entitlement(cur, item["id"])
            remove_premium_if_no_active(cur, uid)

        else:
            # ignore unrelated events
            pass

# --- Webhook route ---
@app.route("/discord/monetization", methods=["GET", "HEAD", "POST"])
def monetization():
    # Discord dashboard "Test Delivery" uses GET/HEAD
    if request.method in ("GET", "HEAD"):
        return "ok", 200

    # Real events (can be a single object or a list)
    payload = request.get_json(force=True, silent=False)

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if isinstance(payload, list):
                    for evt in payload:
                        handle_event(cur, evt)          # <-- pass cursor
                else:
                    handle_event(cur, payload)          # <-- pass cursor
            conn.commit()
    except Exception as e:
        # Optional: log e
        try:
            conn.rollback()
        except Exception:
            pass
        abort(400, description="failed to process webhook")

    return jsonify({"ok": True})
