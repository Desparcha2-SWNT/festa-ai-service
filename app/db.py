"""MongoDB (ai_db) con el cliente asíncrono de pymongo."""
from pymongo import ASCENDING, DESCENDING, AsyncMongoClient

from .config import get_settings

_client: AsyncMongoClient | None = None
_db = None


async def connect():
    global _client, _db
    if _db is not None:
        return _db
    _client = AsyncMongoClient(get_settings().mongo_url, tz_aware=True)
    _db = _client.get_default_database()
    await ensure_indexes(_db)
    return _db


async def ensure_indexes(db) -> None:
    await db.outbox.create_index([("sentAt", ASCENDING), ("createdAt", ASCENDING)])
    await db.processed_events.create_index("processedAt", expireAfterSeconds=30 * 24 * 3600)
    await db.catalog.create_index([("status", ASCENDING), ("startsAt", ASCENDING)])
    await db.catalog.create_index([("location", "2dsphere")])
    await db.descriptions.create_index([("requestedBy", ASCENDING), ("draftId", ASCENDING)], unique=True)
    await db.chat_sessions.create_index([("userId", ASCENDING), ("updatedAt", DESCENDING)])
    # Historial de chat: se borra solo a los 30 días (minimización de datos, Ley 1581).
    await db.chat_sessions.create_index("updatedAt", expireAfterSeconds=30 * 24 * 3600)


def get_db():
    if _db is None:
        raise RuntimeError("MongoDB no está conectado")
    return _db


def set_db_for_tests(db) -> None:
    global _db
    _db = db


async def close() -> None:
    global _client, _db
    if _client is not None:
        await _client.close()
    _client = None
    _db = None
