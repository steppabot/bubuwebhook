import os
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import Flask, request, jsonify, abort
import psycopg

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# --- DB helpers (single conn per request) ---
def get_conn():
    dsn = os.environ["DATABASE_URL"]
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    # psycopg v3 connection; context manager will commit/rollback on exit if autocommit=False
    return psycopg.connect(dsn)

def _parse_iso(s: Optional[str]):
    """
    Discord timestamps come like '2025-01-02T03:04:05.678Z' — convert to aware UTC datetimes.
    """
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

# --- Core persistence (use an existing cursor) ---
def ensure_user(cur, user_id: int):
    cur.execute(
        "INSERT INTO users (user_id) VALUES (%s) ON CONFLICT DO NOTHING;",
        (user_id,)
    )

def _normalize_status(raw: Optional[str]) -> str:
    s = (raw or "").strip().lower()
    if s in ("active", "fulfilled"):
        return "active"
    if s in ("revoked", "expired", "canceled", "cancelled"):
        return "revoked"
    # treat unknown/empty as active to avoid accidental downgrades on weird payloads
    return "active" if not s else s

def upsert_entitlement(cur, ent: Dict[str, Any]):
    """
    Upsert the entitlement row. 'status' is normalized to 'active' or 'revoked' for gating.
    """
    status = _normalize_status(ent.get("status"))
    cur.execute(
        """
        INSERT INTO entitlements (
            entitlement_id, user_id, sku_id, starts_at, ends_at, is_gift, status
        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (entitlement_id) DO UPDATE
           SET user_id    = EXCLUDED.user_id,
               sku_id     = EXCLUDED.sku_id,
               starts_at  = EXCLUDED.starts_at,
               ends_at    = EXCLUDED.ends_at,
               is_gift    = EXCLUDED.is_gift,
               status     = EXCLUDED.status,
               updated_at = NOW();
        """,
        (
            ent["id"],
            int(ent["user_id"]),
            int(ent["sku_id"]),
            _parse_iso(ent.get("starts_at")),
            _parse_iso(ent.get("ends_at")),
            bool(ent.get("is_gift") or False),
            status,
        ),
    )

def mark_premium(cur, user_id: int, ends_at):
    ensure_user(cur, user_id)
    cur.execute(
        """
        UPDATE users
           SET tier = 'premium',
               premium_expires_at = %s
         WHERE user_id = %s;
        """,
        (ends_at, user_id),
    )

def remove_premium_if_no_active(cur, user_id: int):
    """
    Only downgrade if there are no still-active entitlements.
    We consider an entitlement 'active' if status='active' AND ends_at is null or in the future.
    """
    cur.execute(
        """
        SELECT 1
          FROM entitlements
         WHERE user_id = %s
           AND status = 'active'
           AND (ends_at IS NULL OR ends_at > NOW())
         LIMIT 1;
        """,
        (user_id,),
    )
    if cur.fetchone() is None:
        cur.execute(
            """
            UPDATE users
               SET tier = 'free',
                   premium_expires_at = NULL
             WHERE user_id = %s;
            """,
            (user_id,),
        )

def delete_entitlement(cur, entitlement_id: str) -> Optional[int]:
    """
    Delete entitlement; return user_id if we could resolve it (for subsequent downgrade check).
    """
    user_id: Optional[int] = None
    try:
        cur.execute(
            "SELECT user_id FROM entitlements WHERE entitlement_id=%s;",
            (entitlement_id,),
        )
        row = cur.fetchone()
        if row:
            user_id = int(row[0])
    except Exception:
        pass

    cur.execute(
        "DELETE FROM entitlements WHERE entitlement_id=%s;",
        (entitlement_id,),
    )
    return user_id

# --- Event handling (uses one cursor) ---
def handle_event(cur, evt: Dict[str, Any]):
    """
    Discord monetization schema (summary):
      {
        "type": "ENTITLEMENT_CREATE" | "ENTITLEMENT_UPDATE" | "ENTITLEMENT_DELETE",
        "data": { ... } | [ { ... }, ... ]
      }
    Each data item includes at least: id, user_id, sku_id, starts_at, ends_at, status, is_gift
    """
    etype = (evt.get("type") or "").upper()
    data = evt.get("data", {})
    items: List[Dict[str, Any]] = data if isinstance(data, list) else [data]

    for item in items:
        if not item:
            continue

        # Some DELETE payloads may not include user_id; we handle that below.
        uid = int(item.get("user_id") or 0)

        if etype == "ENTITLEMENT_CREATE":
            ensure_user(cur, uid)
            upsert_entitlement(cur, {**item, "status": item.get("status") or "active"})
            mark_premium(cur, uid, _parse_iso(item.get("ends_at")))

        elif etype == "ENTITLEMENT_UPDATE":
            ensure_user(cur, uid)
            upsert_entitlement(cur, item)
            status = _normalize_status(item.get("status"))
            if status == "active":
                mark_premium(cur, uid, _parse_iso(item.get("ends_at")))
            else:
                remove_premium_if_no_active(cur, uid)

        elif etype == "ENTITLEMENT_DELETE":
            # Resolve user_id if missing, then delete, then conditional downgrade
            ent_id = item.get("id")
            if not ent_id:
                # nothing we can do safely
                logging.warning("DELETE without entitlement id: %r", item)
                continue
            resolved_uid = delete_entitlement(cur, ent_id)
            final_uid = uid or resolved_uid
            if final_uid:
                remove_premium_if_no_active(cur, final_uid)

        else:
            # ignore unrelated events
            logging.info("Ignoring event type: %s", etype)

# --- Webhook route ---
@app.route("/discord/monetization", methods=["GET", "HEAD", "POST"])
def monetization():
    # Discord dashboard verification / test
    if request.method in ("GET", "HEAD"):
        return "ok", 200

    # Real events (can be a single object or a list)
    payload = request.get_json(force=True, silent=True)
    if payload is None:
        abort(400, description="invalid json")

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if isinstance(payload, list):
                    for evt in payload:
                        if isinstance(evt, dict):
                            handle_event(cur, evt)
                elif isinstance(payload, dict):
                    handle_event(cur, payload)
                else:
                    abort(400, description="unexpected payload")
            # context manager will commit; explicit commit is fine too:
            conn.commit()
    except Exception as e:
        logging.exception("Webhook error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        abort(400, description="failed to process webhook")

    return jsonify({"ok": True})
