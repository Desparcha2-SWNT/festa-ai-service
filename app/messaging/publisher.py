"""Publica el outbox con publisher confirms; cada fila se alquila para no publicarla dos veces."""
import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta

import aio_pika
from pymongo import ReturnDocument

from ..accounts import is_deleted
from ..config import get_settings
from ..db import get_db
from .pending import relay_stuck

log = logging.getLogger("ai-service.outbox")


def build_envelope(row: dict) -> dict:
    created = row["createdAt"]
    if created.tzinfo is None:
        created = created.replace(tzinfo=UTC)
    return {
        "eventId": row["_id"],
        "eventType": row["routingKey"],
        "version": 1,
        "occurredAt": created.isoformat().replace("+00:00", "Z"),
        "producer": get_settings().service_name,
        "correlationId": row.get("correlationId"),
        "causationId": row.get("causationId"),
        "data": row["payload"],
    }


async def _cancelled_after_claim(db, row: dict) -> bool:
    """¿Se borró la cuenta (o la fila) después de reclamarla?

    `account.deleted` escribe la lápida y luego borra del outbox lo no enviado, pero no puede quitarle al
    publicador una fila que ya reclamó y tiene en memoria. Por eso se vuelve a mirar justo antes de publicar:
    si la fila ya no existe o la persona tiene lápida, no se publica y la fila se descarta.
    """
    payload = row.get("payload") or {}
    user_id = payload.get("userId") or payload.get("requestedBy")
    if await db.outbox.find_one({"_id": row["_id"]}, {"_id": 1}) is None:
        return True
    if await is_deleted(db, user_id):
        await db.outbox.delete_one({"_id": row["_id"], "sentAt": None})
        return True
    return False


async def publish_pending(channel, db=None, batch: int = 50) -> int:
    db = db if db is not None else get_db()
    sent = 0
    for _ in range(batch):
        now = datetime.now(UTC)
        row = await db.outbox.find_one_and_update(
            {"sentAt": None, "$or": [{"lockedUntil": {"$exists": False}}, {"lockedUntil": {"$lt": now}}]},
            {"$set": {"lockedUntil": now + timedelta(seconds=30)}},
            sort=[("createdAt", 1)],
            return_document=ReturnDocument.AFTER,
        )
        if row is None:
            break
        envelope = build_envelope(row)
        exchange = await channel.get_exchange(row["exchange"], ensure=False)
        if await _cancelled_after_claim(db, row):
            continue
        await exchange.publish(
            aio_pika.Message(
                body=json.dumps(envelope, ensure_ascii=False).encode("utf-8"),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                message_id=envelope["eventId"],
                type=envelope["eventType"],
                headers={"x-correlation-id": row["correlationId"]} if row.get("correlationId") else {},
            ),
            routing_key=row["routingKey"],
        )
        await db.outbox.update_one({"_id": row["_id"]}, {"$set": {"sentAt": datetime.now(UTC)}, "$unset": {"lockedUntil": ""}})
        sent += 1
    return sent


async def run_publisher(channel, stop: asyncio.Event, interval_s: float = 2.0) -> None:
    while not stop.is_set():
        try:
            # Primero lo que quedó guardado junto a su resultado sin llegar al outbox (ver pending.py).
            await relay_stuck(get_db())
            await publish_pending(channel)
        except Exception:  # noqa: BLE001
            log.exception("fallo publicando el outbox, se reintenta")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except TimeoutError:
            pass
