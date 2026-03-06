from fastapi import APIRouter, Depends

from src.auth.deps import get_current_user
from src.usage.service import get_usage_summary

router = APIRouter(prefix="/usage", tags=["usage"])


@router.get("/me")
def my_usage(current_user: dict = Depends(get_current_user)):
    return get_usage_summary(current_user["id"])

