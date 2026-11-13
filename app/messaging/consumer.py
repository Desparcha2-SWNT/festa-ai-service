"""Consume ai-service.inbox siguiendo contracts/README.md § Consumir."""
import json
import logging
from datetime import UTC, datetime

import aio_pika

from ..db import get_db
from .handlers import HANDLERS
from .registry import SchemaRegistry

log = logging.getLogger("ai-service.inbox")

QUEUE = "ai-service.inbox"
RETRY_QUEUE = "ai-service.inbox.retry"
MAX_RETRIES = 3


async def _dlq(message, reason: str, *args) -> str:
    log.error(reason, *args)
    await message.reject(requeue=False)
    return "dlq"


async def handle(message, registry: SchemaRegistry, channel, db=None) -> str:
    """Procesa un mensaje. Devuelve "ack", "dlq", "retry" o "duplicate" (útil en pruebas)."""
    try:
        envelope = json.loads(message.body)
    except (ValueError, UnicodeDecodeError):
        return await _dlq(message, "mensaje no es JSON, va a la DLQ")

    if not isinstance(envelope, dict) or registry.envelope_errors(envelope):
        return await _dlq(message, "sobre inválido, va a la DLQ")

    event_type = envelope["eventType"]
    expected_producer = registry.producers.get(event_type)
    if expected_producer and envelope["producer"] != expected_producer:
        return await _dlq(message, "productor %s no es dueño de %s, va a la DLQ", envelope["producer"], event_type)
    expected_exchange = registry.exchanges.get(event_type)
    if expected_exchange and message.exchange != expected_exchange:
        return await _dlq(message, "%s llegó por %s y no por %s, va a la DLQ", event_type, message.exchange, expected_exchange)
    if registry.data_errors(event_type, envelope["data"]):
        return await _dlq(message, "data de %s no cumple su esquema, va a la DLQ", event_type)

    handler = HANDLERS.get(event_type)
    if handler is None:
        await message.ack()
        return "ack"

    db = db if db is not None else get_db()
    try:
        if await db.processed_events.find_one({"_id": envelope["eventId"]}):
            await message.ack()
            return "duplicate"
        await handler(envelope["data"], envelope, db)
        await db.processed_events.update_one(
            {"_id": envelope["eventId"]},
            {"$setOnInsert": {"eventType": event_type, "processedAt": datetime.now(UTC)}},
            upsert=True,
        )
        await message.ack()
        return "ack"
    except Exception as err:  # noqa: BLE001
        return await handle_failure(message, channel, event_type, err)


async def handle_failure(message, channel, event_type: str, err: Exception) -> str:
    raw = (message.headers or {}).get("x-retry-count", 0)
    try:
        retry_count = int(str(raw))
        if retry_count < 0:
            raise ValueError
    except (TypeError, ValueError):
        return await _dlq(message, "x-retry-count inválido (%r), va a la DLQ", raw)
    if retry_count >= MAX_RETRIES:
        return await _dlq(message, "reintentos agotados para %s: %s", event_type, err)

    log.warning("falla transitoria en %s (%s), se reintenta", event_type, err)
    await channel.default_exchange.publish(
        aio_pika.Message(
            body=message.body,
            content_type=message.content_type,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=message.message_id,
            type=message.type,
            headers={**(message.headers or {}), "x-retry-count": retry_count + 1},
        ),
        routing_key=RETRY_QUEUE,
    )
    await message.ack()
    return "retry"


async def start_consumer(channel, registry: SchemaRegistry):
    await channel.set_qos(prefetch_count=10)
    queue = await channel.get_queue(QUEUE, ensure=False)

    async def on_message(message):
        try:
            await handle(message, registry, channel)
        except Exception:  # noqa: BLE001
            log.exception("error inesperado en el inbox")

    return await queue.consume(on_message)
