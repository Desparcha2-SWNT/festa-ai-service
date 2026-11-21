"""Outbox en MongoDB (contracts/README.md § Publicar).

Sin transacciones entre colecciones, el outbox es idempotente por id: lo que se publica por causa
de un mensaje consumido usa un eventId determinista derivado de (causationId, routingKey, clave),
el mismo algoritmo que notification-service (UUID v8 sobre SHA-256).
"""
import hashlib
import uuid
from datetime import UTC, datetime


def deterministic_uuid(text: str) -> str:
    """UUID v8 a partir de SHA-256 (mismo algoritmo que notification-service): estable y sin SHA-1."""
    raw = bytearray(hashlib.sha256(text.encode("utf-8")).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x80
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


async def enqueue(
    db, *, exchange: str, routing_key: str, payload: dict, correlation_id=None, causation_id=None, key: str = "", event_id: str | None = None
) -> str:
    """Upsert por _id: con `event_id` fijo (ver pending.py) repetir el enqueue no duplica el mensaje."""
    if event_id is None:
        event_id = deterministic_uuid(f"{causation_id}:{routing_key}:{key}") if causation_id else str(uuid.uuid4())
    await db.outbox.update_one(
        {"_id": event_id},
        {
            "$setOnInsert": {
                "exchange": exchange,
                "routingKey": routing_key,
                "payload": payload,
                "correlationId": correlation_id,
                "causationId": causation_id,
                "createdAt": datetime.now(UTC),
                "sentAt": None,
            }
        },
        upsert=True,
    )
    return event_id
