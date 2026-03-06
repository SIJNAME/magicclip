from datetime import datetime, timezone

from psycopg2.extras import RealDictCursor
import stripe

from src.config import settings
from src.db.database import get_connection
from src.db.repository import register_stripe_event_once, upsert_subscription


def _stripe_client() -> None:
    if not settings.stripe_api_key:
        raise RuntimeError("STRIPE_API_KEY is required")
    stripe.api_key = settings.stripe_api_key


def plan_code_from_price(price_id: str) -> str:
    if price_id == settings.stripe_price_pro:
        return "pro"
    if settings.stripe_price_free and price_id == settings.stripe_price_free:
        return "free"
    return "free"


def create_checkout_session(user_id: str, email: str, success_url: str, cancel_url: str) -> dict:
    _stripe_client()
    if not settings.stripe_price_pro:
        raise RuntimeError("STRIPE_PRICE_PRO is required")
    session = stripe.checkout.Session.create(
        mode="subscription",
        line_items=[{"price": settings.stripe_price_pro, "quantity": 1}],
        customer_email=email,
        metadata={"user_id": user_id},
        success_url=success_url,
        cancel_url=cancel_url,
    )
    return {"id": session["id"], "url": session["url"]}


def handle_webhook(payload: bytes, sig_header: str | None) -> None:
    _stripe_client()
    if not settings.stripe_webhook_secret:
        raise RuntimeError("STRIPE_WEBHOOK_SECRET is required")
    event = stripe.Webhook.construct_event(payload, sig_header or "", settings.stripe_webhook_secret)
    etype = event["type"]
    event_id = event.get("id", "")
    if not event_id:
        raise RuntimeError("Missing Stripe event id")
    if not register_stripe_event_once(event_id, etype, event):
        return

    if etype == "checkout.session.completed":
        session = event["data"]["object"]
        user_id = session.get("metadata", {}).get("user_id")
        if not user_id:
            return
        subscription_id = session.get("subscription")
        customer_id = session.get("customer")
        upsert_subscription(
            user_id=user_id,
            plan_code="pro",
            status="active",
            stripe_customer_id=customer_id,
            stripe_subscription_id=subscription_id,
            current_period_end=None,
        )
        return

    if etype == "customer.subscription.updated" or etype == "customer.subscription.deleted":
        sub = event["data"]["object"]
        customer_id = sub.get("customer")
        status = sub.get("status", "canceled")
        items = (sub.get("items") or {}).get("data") or []
        price_id = ""
        if items and items[0].get("price"):
            price_id = items[0]["price"].get("id", "")
        plan_code = plan_code_from_price(price_id)
        period_end_ts = sub.get("current_period_end")
        period_end = None
        if period_end_ts:
            period_end = datetime.fromtimestamp(int(period_end_ts), tz=timezone.utc).isoformat()

        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT user_id FROM subscriptions WHERE stripe_customer_id = %s", (customer_id,))
                row = cur.fetchone()
            conn.commit()
        if not row:
            return
        upsert_subscription(
            user_id=row["user_id"],
            plan_code=plan_code,
            status=status,
            stripe_customer_id=customer_id,
            stripe_subscription_id=sub.get("id"),
            current_period_end=period_end,
        )
