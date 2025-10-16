import os
from datetime import datetime
from typing import Any, Dict, List

from flask import Flask, request, jsonify, abort
import psycopg

# --- Flask app ---
app = Flask(__name__)

# --- DB helpers ---
def get_conn():
    # Heroku Postgres URL is in DATABASE_URL
    return psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)

def _parse_iso(s: str | None):
    if not s:
        return None
    # Discord sends ISO8601 with "Z" sometimes
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def upsert_entitlement(ent: Dict[str, Any]):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO entitlements (
                entitlement_id, user_id, sku_id, starts_at, ends_at, is_gift, status
            ) VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (entitlement_id) DO UPDATE
               SET user_id = EXCLUDED.user_id,
                   sku_id   = EXCLUDED.sku_id,
                   starts_at= EXCLUDED.starts_at,
                   ends_at  = EXCLUDED.ends_at,
                   is_gift  = EXCLUDED.is_gift,
                   status   = EXCLUDED.status;
        """, (
            ent["id"],
            int(ent["user_id"]),
            int(ent["sku_id"]),
            _parse_iso(ent.get("starts_at")),
            _parse_iso(ent.get("ends_at")),
            bool(ent.get("is_gift", False)),
            (ent.get("status") or "active"),
        ))

def mark_premium(user_id: int, ends_at):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE users
               SET tier = 'premium',
                   premium_expires_at = %s
             WHERE user_id = %s;
        """, (ends_at, user_id))

def remove_premium_if_expired_or_forced(user_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE users
               SET tier = 'free',
                   premium_expires_at = NULL
             WHERE user_id = %s
               AND (premium_expires_at IS NULL OR premium_expires_at <= NOW());
        """, (user_id,))

def delete_entitlement(entitlement_id: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM entitlements WHERE entitlement_id=%s;", (entitlement_id,))

# --- Core event handling ---
def handle_event(evt: Dict[str, Any]):
    etype = evt.get("type")
    data  = evt.get("data", {})
    items: List[Dict[str, Any]] = data if isinstance(data, list) else [data]

    for item in items:
        # Normalize possible keys
        uid = int(item["user_id"])
        if etype == "ENTITLEMENT_CREATE":
            upsert_entitlement({**item, "status": item.get("status") or "active"})
            mark_premium(uid, _parse_iso(item.get("ends_at")))

        elif etype == "ENTITLEMENT_UPDATE":
            upsert_entitlement(item)
            status = (item.get("status") or "").lower()
            if status in ("active", "fulfilled"):
                mark_premium(uid, _parse_iso(item.get("ends_at")))
            elif status in ("revoked", "expired", "canceled", "cancelled"):
                remove_premium_if_expired_or_forced(uid)

        elif etype == "ENTITLEMENT_DELETE":
            delete_entitlement(item["id"])
            remove_premium_if_expired_or_forced(uid)

        else:
            # Not a monetization event you care about — ignore
            pass

# --- Webhook route ---
@app.post("/discord/monetization")
def monetization():
    # If Discord sent a single object, ensure we treat it the same as a list
    payload = request.get_json(force=True, silent=False)

    # (Optional, recommended) uncomment to require Discord signature:
    # from nacl.signing import VerifyKey
    # from nacl.exceptions import BadSignatureError
    # DISCORD_PUBLIC_KEY = os.environ.get("DISCORD_PUBLIC_KEY")
    # sig = request.headers.get("X-Signature-Ed25519")
    # ts  = request.headers.get("X-Signature-Timestamp")
    # if not (DISCORD_PUBLIC_KEY and sig and ts):
    #     abort(401, "Missing signature.")
    # try:
    #     VerifyKey(bytes.fromhex(DISCORD_PUBLIC_KEY)).verify(
    #         ts.encode() + request.get_data(cache=False),
    #         bytes.fromhex(sig)
    #     )
    # except BadSignatureError:
    #     abort(401, "Invalid signature.")

    if isinstance(payload, list):
        for evt in payload:
            handle_event(evt)
    else:
        handle_event(payload)

    # 200/204 both fine; return JSON to make debugging easier
    return jsonify({"ok": True})
