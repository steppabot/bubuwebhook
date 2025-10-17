import os
import json
import time
import logging
import binascii
from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import Flask, request, jsonify, abort
import psycopg
from nacl.signing import VerifyKey
from nacl.exceptions import BadSignatureError

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

DISCORD_PUBLIC_KEY = os.environ.get("DISCORD_PUBLIC_KEY", "").strip()

# ======================================================
#                     DB HELPERS
# ======================================================
def get_conn():
    dsn = os.environ["DATABASE_URL"]
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return psycopg.connect(dsn)

def _parse_iso(s: Optional[str]):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

# ======================================================
#                 DB CORE OPERATIONS
# ======================================================
def ensure_user(cur, user_id: int):
    cur.execute(
        "INSERT INTO users (user_id) VALUES (%s) ON CONFLICT DO NOTHING;",
        (user_id,),
    )

def _normalize_status(raw: Optional[str]) -> str:
    s = (raw or "").strip().lower()
    if s in ("active", "fulfilled"):
        return "active"
    if s in ("revoked", "expired", "canceled", "cancelled"):
        return "revoked"
    return "active" if not s else s

def upsert_entitlement(cur, ent: Dict[str, Any]):
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

# ======================================================
#                  EVENT NORMALIZATION
# ======================================================
def _event_name(evt: Dict[str, Any]) -> str:
    """
    Normalizes Discord webhook event name.
    Handles both string and int 'type' values.
    """
    t = evt.get("type")

    # String case
    if isinstance(t, str):
        return t.upper().strip()

    # Int case (e.g., ping)
    if isinstance(t, int):
        if t == 1:
            return "PING"
        alt = (evt.get("event") or evt.get("event_type") or "").strip()
        if alt:
            return alt.upper()
        return ""

    # Fallback
    alt = (evt.get("event") or evt.get("event_type") or "").strip()
    return alt.upper()

# ======================================================
#                    EVENT HANDLER
# ======================================================
def handle_event(cur, evt: Dict[str, Any]):
    """
    Expects an event dict shaped like:
      { "type": "ENTITLEMENT_CREATE", "data": {...} }
    """
    etype = _event_name(evt)

    if etype in ("PING", "", None):
        logging.info("Ignoring webhook event type=%r (ping or unknown)", etype)
        return

    data = evt.get("data", {})
    items: List[Dict[str, Any]] = data if isinstance(data, list) else [data]

    for item in items:
        if not item:
            continue

        # Flatten if entitlement is nested
        if "entitlement" in item and isinstance(item["entitlement"], dict):
            item = item["entitlement"]

        uid_raw = item.get("user_id") or item.get("user", {}).get("id")
        if not uid_raw:
            logging.warning("No user_id in entitlement payload: %r", item)
            continue
        uid = int(uid_raw)

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
            ent_id = item.get("id") or item.get("entitlement_id")
            if not ent_id:
                logging.warning("DELETE without entitlement id: %r", item)
                continue
            resolved_uid = delete_entitlement(cur, ent_id)
            final_uid = uid or resolved_uid
            if final_uid:
                remove_premium_if_no_active(cur, final_uid)

        else:
            logging.info("Unhandled event type: %s", etype)

# ======================================================
#              DISCORD SIGNATURE VERIFICATION
# ======================================================
def _verify_discord_request(req):
    """
    Monetization Webhook Events are signed using Ed25519 (same as Interactions).
    Headers:
      - X-Signature-Ed25519
      - X-Signature-Timestamp
    """
    if not DISCORD_PUBLIC_KEY:
        app.logger.error("DISCORD_PUBLIC_KEY is not set")
        abort(500, description="server not configured")

    sig = req.headers.get("X-Signature-Ed25519")
    ts = req.headers.get("X-Signature-Timestamp")
    if not sig or not ts:
        abort(401, description="missing signature headers")

    # Optional: reject stale requests (>10min)
    try:
        now = int(time.time())
        ts_i = int(ts)
        if abs(now - ts_i) > 600:
            abort(401, description="stale request")
    except Exception:
        abort(401, description="bad timestamp")

    body = req.get_data(cache=True, as_text=False)
    try:
        verify_key = VerifyKey(bytes.fromhex(DISCORD_PUBLIC_KEY))
        verify_key.verify(ts.encode() + body, bytes.fromhex(sig))
    except (BadSignatureError, binascii.Error, ValueError):
        abort(401, description="invalid signature")

# ======================================================
#                     FLASK ROUTES
# ======================================================
@app.route("/discord/monetization", methods=["GET", "HEAD", "POST"])
def monetization():
    # Health check
    if request.method in ("GET", "HEAD"):
        return "ok", 200

    # Log everything for visibility
    try:
        app.logger.info("Webhook -> Method: %s", request.method)
        masked_headers = {
            k: v
            for k, v in request.headers.items()
            if k.lower().startswith("x-signature") or k.lower() in ("user-agent", "content-type")
        }
        app.logger.info("Webhook -> Headers: %s", masked_headers)
        app.logger.info("Webhook -> Raw body: %s", request.get_data(as_text=True))
    except Exception as e:
        app.logger.warning("Failed to log incoming request: %s", e)

    # Verify Discord signature
    _verify_discord_request(request)

    # Parse payload
    payload = request.get_json(force=True, silent=True)
    if payload is None:
        abort(400, description="invalid json")

    app.logger.info("Webhook -> Parsed payload: %s", payload)

    # Envelope handling:
    # If Discord wraps the real event inside an "event" object,
    # process that. Only treat as ping if no event is present.
    if isinstance(payload, dict) and "event" in payload and isinstance(payload["event"], dict):
        evt = payload["event"]
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    handle_event(cur, evt)
                conn.commit()
        except Exception as e:
            logging.exception("Webhook error (enveloped): %s", e)
            try:
                conn.rollback()
            except Exception:
                pass
            abort(400, description="failed to process webhook")
        return jsonify({"ok": True})

    # Legacy/simple case: payload itself is the event or a batch of events
    # Quick ping ACK only when there's no embedded event
    if isinstance(payload, dict) and payload.get("type") == 1:
        app.logger.info("Webhook PING (no embedded event); returning 200")
        return jsonify({"ok": True, "pong": True})

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
            conn.commit()
    except Exception as e:
        logging.exception("Webhook error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        abort(400, description="failed to process webhook")

    return jsonify({"ok": True})

@app.route("/", methods=["GET"])
def root():
    return "alive", 200
