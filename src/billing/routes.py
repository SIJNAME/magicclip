from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel, HttpUrl

from src.auth.deps import get_current_user
from src.billing.service import create_checkout_session, handle_webhook

router = APIRouter(prefix="/billing", tags=["billing"])


class CheckoutRequest(BaseModel):
    success_url: HttpUrl
    cancel_url: HttpUrl


@router.post("/checkout")
def create_checkout(payload: CheckoutRequest, current_user: dict = Depends(get_current_user)):
    try:
        return create_checkout_session(
            user_id=current_user["id"],
            email=current_user["email"],
            success_url=str(payload.success_url),
            cancel_url=str(payload.cancel_url),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/webhook")
async def stripe_webhook(request: Request, stripe_signature: str | None = Header(default=None)):
    payload = await request.body()
    try:
        handle_webhook(payload, stripe_signature)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True}

