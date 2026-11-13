"""RNF8: límite de solicitudes de chat por usuario por minuto (ventana fija en Redis)."""
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


async def hit(user_id: str, limit: int | None = None) -> tuple[bool, int]:
    """Cuenta una solicitud. Devuelve (permitida, segundos hasta la próxima ventana)."""
    limit = limit or get_settings().chat_rate_limit_per_min
    window = int(time.time() // 60)
    key = f"ai:chat:{user_id}:{window}"
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
