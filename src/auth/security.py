import base64
import hashlib
import hmac
import os
from datetime import datetime, timedelta, timezone

import jwt

from src.config import settings


def hash_password(password: str) -> str:
    if not password:
        raise ValueError("Password is required")
    salt = os.urandom(16)
    iterations = 120000
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return "pbkdf2_sha256${}${}${}".format(
        iterations,
        base64.b64encode(salt).decode("ascii"),
        base64.b64encode(digest).decode("ascii"),
    )


def verify_password(password: str, stored_hash: str) -> bool:
    algo, iterations, salt_b64, digest_b64 = stored_hash.split("$", 3)
    if algo != "pbkdf2_sha256":
        return False
    salt = base64.b64decode(salt_b64.encode("ascii"))
    expected = base64.b64decode(digest_b64.encode("ascii"))
    actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, int(iterations))
    return hmac.compare_digest(actual, expected)


def issue_token(user_id: str, email: str) -> str:
    if not settings.jwt_secret:
        raise RuntimeError("JWT_SECRET is required")
    now = datetime.now(timezone.utc)
    exp = now + timedelta(minutes=settings.jwt_expires_minutes)
    payload = {
        "sub": user_id,
        "email": email,
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def decode_token(token: str) -> dict:
    if not settings.jwt_secret:
        raise RuntimeError("JWT_SECRET is required")
    return jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])

