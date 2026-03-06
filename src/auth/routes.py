from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr, Field

from src.auth.security import hash_password, issue_token, verify_password
from src.db.repository import create_user, get_user_by_email, upsert_subscription

router = APIRouter(prefix="/auth", tags=["auth"])


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class AuthResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


@router.post("/register", response_model=AuthResponse)
def register(payload: RegisterRequest):
    if get_user_by_email(payload.email):
        raise HTTPException(status_code=409, detail="Email already registered")
    user = create_user(payload.email, hash_password(payload.password))
    upsert_subscription(
        user_id=user["id"],
        plan_code="free",
        status="active",
        stripe_customer_id=None,
        stripe_subscription_id=None,
        current_period_end=None,
    )
    token = issue_token(user["id"], user["email"])
    return AuthResponse(access_token=token)


@router.post("/login", response_model=AuthResponse)
def login(payload: LoginRequest):
    user = get_user_by_email(payload.email)
    if not user or not verify_password(payload.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = issue_token(user["id"], user["email"])
    return AuthResponse(access_token=token)
