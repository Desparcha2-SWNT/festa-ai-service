"""MongoDB (ai_db) con el cliente asíncrono de pymongo."""
from contextlib import suppress

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.errors import OperationFailure

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
    # Único solo cuando hay quien pidió la descripción: al borrar una cuenta, sus descripciones quedan con
    # requestedBy null y no deben chocar entre sí. El índice anterior (sin filtro) se elimina si existe.
    with suppress(OperationFailure):
        await db.descriptions.drop_index("requestedBy_1_draftId_1")
    await db.descriptions.create_index(
        [("requestedBy", ASCENDING), ("draftId", ASCENDING)],
        unique=True,
        name="requestedBy_draftId_unique",
        partialFilterExpression={"requestedBy": {"$type": "string"}},
    )
    await db.chat_sessions.create_index([("userId", ASCENDING), ("updatedAt", DESCENDING)])
    # Mensajes guardados junto a su resultado que aún no pasan al outbox (ver messaging/pending.py).
    for collection in ("descriptions", "chat_sessions"):
        await db[collection].create_index("pendingEvents.at", sparse=True)
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
