"""RNF8: límite de solicitudes de IA por usuario, por minuto y por día (ventanas fijas en Redis)."""
import time

from redis import asyncio as aioredis

from .config import get_settings

_redis = None


async def connect():
    global _redis
    url = get_settings().redis_url
    if url and _redis is None:
        _redis = aioredis.from_url(url, decode_responses=True)
    return _redis


async def close():
    global _redis
    if _redis is not None:
        await _redis.aclose()
    _redis = None


# Respaldo en memoria si no hay Redis (desarrollo): por proceso, no compartido entre réplicas.
_local: dict[str, int] = {}


async def hit(user_id: str, limit: int | None = None, scope: str = "chat") -> tuple[bool, int]:
    """Cuenta una solicitud en el ámbito dado (chat o descriptions). Devuelve (permitida, segundos hasta la próxima ventana)."""
    if limit is None:
        limit = get_settings().chat_rate_limit_per_min if scope == "chat" else get_settings().description_rate_limit_per_min
    window = int(time.time() // 60)
    key = f"ai:{scope}:{user_id}:{window}"
    retry_after = 60 - int(time.time() % 60)
    if _redis is not None:
        count = await _redis.incr(key)
        if count == 1:
            await _redis.expire(key, 70)
    else:
        for k in [k for k in _local if not k.endswith(f":{window}")]:
            _local.pop(k, None)
        _local[key] = _local.get(key, 0) + 1
        count = _local[key]
    return count <= limit, retry_after


_local_day: dict[str, int] = {}


async def hit_daily(user_id: str, scope: str = "chat", limit: int | None = None) -> tuple[bool, int]:
    """Cuenta una solicitud en la ventana del día (UTC). Devuelve (permitida, segundos hasta el día siguiente)."""
    if limit is None:
        limit = get_settings().chat_rate_limit_per_day if scope == "chat" else get_settings().description_rate_limit_per_day
    now = time.time()
    day = int(now // 86400)
    key = f"ai:{scope}:day:{user_id}:{day}"
    retry_after = 86400 - int(now % 86400)
    if _redis is not None:
        count = await _redis.incr(key)
        if count == 1:
            await _redis.expire(key, 90000)
    else:
        for k in [k for k in _local_day if not k.endswith(f":{day}")]:
            _local_day.pop(k, None)
        _local_day[key] = _local_day.get(key, 0) + 1
        count = _local_day[key]
    return count <= limit, retry_after
