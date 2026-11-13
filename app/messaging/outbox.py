"""Outbox en MongoDB (contracts/README.md § Publicar).

Sin transacciones entre colecciones, el outbox es idempotente por id: lo que se publica por causa
de un mensaje consumido usa un eventId determinista derivado de (causationId, routingKey, clave),
el mismo algoritmo que notification-service y analytics-service.
"""
import hashlib
import uuid
from datetime import UTC, datetime


def deterministic_uuid(text: str) -> str:
    raw = bytearray(hashlib.sha1(text.encode("utf-8")).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x50
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


async def enqueue(db, *, exchange: str, routing_key: str, payload: dict, correlation_id=None, causation_id=None, key: str = "") -> str:
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
