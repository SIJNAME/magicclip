from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.auth.security import decode_token
from src.db.repository import get_user_by_id
from src.rate_limit import enforce_user_rate_limit

bearer = HTTPBearer(auto_error=True)


def get_current_user(request: Request, credentials: HTTPAuthorizationCredentials = Depends(bearer)) -> dict:
    try:
        payload = decode_token(credentials.credentials)
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Invalid token") from exc
    user = get_user_by_id(payload.get("sub", ""))
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    enforce_user_rate_limit(user["id"], request.url.path)
    return user
