"""Verificación del JWT RS256 que emite auth-service (Kong ya lo validó; defensa en profundidad)."""
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import jwt
from fastapi import Header, HTTPException

from .config import get_settings


@dataclass(frozen=True)
class Principal:
    user_id: str
    role: str
    email: str | None = None


@lru_cache(maxsize=1)
def _public_key() -> str:
    return Path(get_settings().jwt_public_key_path).read_text(encoding="utf-8")


def current_user(authorization: str | None = Header(default=None)) -> Principal:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="falta el token")
    token = authorization[len("Bearer "):]
    try:
        claims = jwt.decode(token, _public_key(), algorithms=["RS256"], issuer=get_settings().jwt_issuer)
    except jwt.PyJWTError as err:
        raise HTTPException(status_code=401, detail="token inválido") from err
    return Principal(user_id=claims["sub"], role=claims.get("role", "user"), email=claims.get("email"))
