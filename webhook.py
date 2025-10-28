import os
import json
import stripe
import psycopg2
from flask import Flask, request, jsonify
from datetime import datetime, timezone
import requests

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
BUBU_DATABASE_URL   = os.getenv("DATABASE_URL")
STRIPE_SECRET_KEY   = os.getenv("STRIPE_SECRET_KEY")
WEBHOOK_SECRET      = os.getenv("STRIPE_WEBHOOK_SECRET")
PREMIUM_PRICE_ID    = os.getenv("PREMIUM_PRICE_ID")              # e.g. price_123
DISCORD_API_BASE    = os.getenv("DISCORD_API_BASE", "https://discord.com/api/v10")
SUPPORT_WEBHOOK     = os.getenv("SUPPORT_WEBHOOK")               # optional

assert BUBU_DATABASE_URL and STRIPE_SECRET_KEY and WEBHOOK_SECRET and PREMIUM_PRICE_ID, \
    "Missing one of: BUBU_DATABASE_URL, STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET, PREMIUM_PRICE_ID"

stripe.api_key = STRIPE_SECRET_KEY
app = Flask(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(BUBU_DATABASE_URL, sslmode="require", options="-c timezone=UTC")

def upsert_event(event_id: str, event_type: str, payload: dict) -> bool:
    """
    Returns True if this event is NEW and should be processed.
    Returns False if we've already seen it.
    """
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stripe_webhook_events (event_id, event_type, payload)
            VALUES (%s, %s, %s::jsonb)
            ON CONFLICT (event_id) DO NOTHING
        """, (event_id, event_type, json.dumps(payload)))
        return cur.rowcount == 1

def mark_event_processed(event_id: str):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("UPDATE stripe_webhook_events SET processed_at = NOW() WHERE event_id = %s", (event_id,))

def upsert_stripe_customer(user_id: int, customer_id: str):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stripe_customers (user_id, customer_id)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET customer_id = EXCLUDED.customer_id
        """, (user_id, customer_id))

def upsert_stripe_subscription(user_id: int, sub_obj: dict):
    # extract fields safely
    sub_id  = sub_obj.get("id")
    status  = sub_obj.get("status")
    cancel_at_period_end = bool(sub_obj.get("cancel_at_period_end", False))
    cpe    = sub_obj.get("current_period_end")
    cps    = sub_obj.get("current_period_start")
    price  = None
    try:
        items = sub_obj.get("items", {}).get("data", [])
        if items:
            price = items[0].get("price", {}).get("id")
    except Exception:
        price = None

    cpe_dt = datetime.fromtimestamp(cpe, tz=timezone.utc) if cpe else None
    cps_dt = datetime.fromtimestamp(cps, tz=timezone.utc) if cps else None

    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stripe_subscriptions
              (subscription_id, user_id, price_id, status, current_period_start,
               current_period_end, cancel_at_period_end, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (subscription_id) DO UPDATE
               SET user_id = EXCLUDED.user_id,
                   price_id = EXCLUDED.price_id,
                   status   = EXCLUDED.status,
                   current_period_start = EXCLUDED.current_period_start,
                   current_period_end   = EXCLUDED.current_period_end,
                   cancel_at_period_end = EXCLUDED.cancel_at_period_end,
                   updated_at = NOW()
        """, (sub_id, user_id, price, status, cps_dt, cpe_dt, cancel_at_period_end))

def ensure_user(user_id: int):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (user_id) VALUES (%s)
            ON CONFLICT (user_id) DO NOTHING
        """, (user_id,))

def set_premium(user_id: int, until: datetime | None, source: str, action: str):
    ensure_user(user_id)
    with get_db() as conn, conn.cursor() as cur:
        # if until is None: fall back to NOW()+30d (but we prefer explicit period end)
        if until is None:
            until = datetime.now(timezone.utc)
        cur.execute("""
            UPDATE users
               SET tier = 'premium', premium_until = %s
             WHERE user_id = %s
        """, (until, user_id))
        cur.execute("""
            INSERT INTO premium_audit (user_id, action, source, meta)
            VALUES (%s, %s, %s, %s::jsonb)
        """, (user_id, action, source, json.dumps({"premium_until": until.isoformat()})))

def set_free(user_id: int, source: str, reason: str):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE users
               SET tier = 'free', premium_until = NULL
             WHERE user_id = %s
        """, (user_id,))
        cur.execute("""
            INSERT INTO premium_audit (user_id, action, source, meta)
            VALUES (%s, 'revoke', %s, %s::jsonb)
        """, (user_id, source, json.dumps({"reason": reason})))

def patch_interaction_original(application_id: int | str, interaction_token: str, payload: dict):
    url = f"{DISCORD_API_BASE}/webhooks/{int(application_id)}/{interaction_token}/messages/@original"
    try:
        r = requests.patch(url, json=payload, timeout=8)
        print(f"[discord] PATCH @original -> {r.status_code} {r.text[:200]}")
    except Exception as e:
        print("⚠️ PATCH failed:", e)

def find_checkout_mapping(stripe_session_id: str):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT interaction_token, application_id, user_id
              FROM premium_checkout_sessions
             WHERE stripe_session_id = %s
        """, (stripe_session_id,))
        return cur.fetchone()  # (token, app_id, user_id) or None

# ─────────────────────────────────────────────────────────────────────────────
# Core sync logic
# ─────────────────────────────────────────────────────────────────────────────
def sync_user_from_subscription(sub_id: str, explicit_user_id: int | None = None) -> int | None:
    """
    Fetches Stripe subscription and mirrors to DB + users.tier/premium_until.
    Returns the discord user_id (int) if resolved.
    """
    sub = stripe.Subscription.retrieve(sub_id, expand=["items.data.price", "customer"])
    status = sub.get("status")
    cpe    = sub.get("current_period_end")
    until  = datetime.fromtimestamp(cpe, tz=timezone.utc) if cpe else None
    price_id = None
    try:
        items = sub.get("items", {}).get("data", [])
        if items:
            price_id = items[0].get("price", {}).get("id")
    except Exception:
        pass

    # sanity: only handle our premium price
    if price_id != PREMIUM_PRICE_ID:
        print(f"↪️ Ignoring subscription {sub_id} with price {price_id} (not PREMIUM_PRICE_ID)")
        return None

    # Resolve user_id: prefer sub.metadata.user_id; fallback to explicit_user_id; else lookup existing row
    md = sub.get("metadata") or {}
    user_id = explicit_user_id or safe_int(md.get("user_id"))
    if user_id is None:
        # fallback to previously stored subscription row
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("SELECT user_id FROM stripe_subscriptions WHERE subscription_id = %s", (sub_id,))
            row = cur.fetchone()
            user_id = row[0] if row else None

    if user_id is None:
        print(f"⚠️ Could not resolve user_id for subscription {sub_id}")
        return None

    # Mirror customer
    cust_id = sub.get("customer")
    if isinstance(cust_id, dict):
        cust_id = cust_id.get("id")
    if cust_id:
        upsert_stripe_customer(user_id, cust_id)

    # Mirror subscription row
    upsert_stripe_subscription(user_id, sub)

    # Apply user tier state
    if status in ("trialing", "active"):
        set_premium(user_id, until, source="stripe", action="sync")
    else:
        # Non-active states get no extension; only revoke on deleted/canceled handler
        print(f"ℹ️ Subscription {sub_id} status={status}; not promoting to premium")
    return user_id

def safe_int(v):
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

def post_support(msg: str):
    if not SUPPORT_WEBHOOK:
        return
    try:
        requests.post(SUPPORT_WEBHOOK, json={"content": msg}, timeout=5)
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────────────────────
# Webhook
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/stripe-webhook", methods=["POST"])
def stripe_webhook():
    payload = request.data
    sig = request.headers.get("stripe-signature")

    try:
        event = stripe.Webhook.construct_event(payload, sig, WEBHOOK_SECRET)
    except ValueError:
        return "Invalid payload", 400
    except stripe.error.SignatureVerificationError:
        return "Invalid signature", 400

    event_id   = event.get("id")
    event_type = event.get("type")
    obj        = event.get("data", {}).get("object", {})

    # Deduplicate
    if not upsert_event(event_id, event_type, obj):
        return jsonify(ok=True, dedup=True)

    try:
        # ── Checkout completed (new sub) ──────────────────────────────────────
        if event_type == "checkout.session.completed":
            session = obj
            if session.get("mode") == "subscription":
                stripe_session_id = session.get("id")
                sub_id = session.get("subscription")
                user_id = safe_int(session.get("client_reference_id"))  # we pass discord user id here
                price_id = None

                # Optional: ensure it's our premium price before proceeding
                try:
                    sub = stripe.Subscription.retrieve(sub_id, expand=["items.data.price"])
                    items = sub.get("items", {}).get("data", [])
                    if items:
                        price_id = items[0].get("price", {}).get("id")
                except Exception as e:
                    print("⚠️ Could not fetch sub on checkout:", e)

                if price_id != PREMIUM_PRICE_ID:
                    print(f"↪️ Ignoring checkout for non-premium price {price_id}")
                    return jsonify(ok=True)

                # Mirror all rows + promote user
                resolved_user_id = sync_user_from_subscription(sub_id, explicit_user_id=user_id)

                # If we saved mapping, PATCH @original with success embed
                mapping = find_checkout_mapping(stripe_session_id)
                if mapping:
                    interaction_token, application_id, mapped_user = mapping
                    if not resolved_user_id or mapped_user != resolved_user_id:
                        # fallback; still patch success without user check
                        pass

                    payload = {
                        "embeds": [{
                            "title": "✅ Bubu Bot Premium Activated",
                            "description": "Thanks for your support! You now have access to all premium features.",
                            "color": 0x50BF84
                        }],
                        "components": []
                    }
                    patch_interaction_original(application_id, interaction_token, payload)

                post_support(f"🎉 User `{resolved_user_id}` started Bubu Bot Premium (sub `{sub_id}`).")

        # ── Renewals ──────────────────────────────────────────────────────────
        elif event_type == "invoice.payment_succeeded":
            invoice = obj
            sub_id = invoice.get("subscription")
            if sub_id:
                user_id = sync_user_from_subscription(sub_id)
                if user_id:
                    post_support(f"🔁 Premium renewed for user `{user_id}` (sub `{sub_id}`).")

        # ── Payment failed (we DO NOT immediately revoke; wait for cancel/delete) ─
        elif event_type == "invoice.payment_failed":
            invoice = obj
            sub_id = invoice.get("subscription")
            if sub_id:
                # still sync rows (status may be past_due)
                sync_user_from_subscription(sub_id)

        # ── Subscription updated (e.g., cancel_at_period_end toggled) ─────────
        elif event_type == "customer.subscription.updated":
            sub = obj
            sub_id = sub.get("id")
            user_id = sync_user_from_subscription(sub_id)

        # ── Subscription canceled/deleted (hard revoke) ───────────────────────
        elif event_type == "customer.subscription.deleted":
            sub = obj
            sub_id = sub.get("id")
            user_id = sync_user_from_subscription(sub_id)  # final state mirror
            # Resolve user from our table if not in metadata
            if not user_id:
                with get_db() as conn, conn.cursor() as cur:
                    cur.execute("SELECT user_id FROM stripe_subscriptions WHERE subscription_id=%s", (sub_id,))
                    row = cur.fetchone()
                    user_id = row[0] if row else None
            if user_id:
                set_free(user_id, source="stripe", reason="subscription_deleted")
                post_support(f"❌ Premium canceled for user `{user_id}` (sub `{sub_id}`).")

    finally:
        mark_event_processed(event_id)

    return jsonify(ok=True)

@app.route("/")
def home():
    return "Bubu Bot Stripe Webhook OK"
