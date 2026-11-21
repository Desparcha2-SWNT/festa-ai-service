"""Consume ai-service.inbox siguiendo contracts/README.md § Consumir."""
import hashlib
import hmac
import json
import logging
from contextlib import suppress
from datetime import UTC, datetime

import aio_pika

from ..config import get_settings
from ..db import get_db
from .handlers import HANDLERS
from .registry import SchemaRegistry

log = logging.getLogger("ai-service.inbox")

QUEUE = "ai-service.inbox"
RETRY_QUEUE = "ai-service.inbox.retry"
MAX_RETRIES = 3


ORIGIN_HEADER = "x-origin-exchange"
SIGNATURE_HEADER = "x-origin-signature"


def _sign_origin(origin: str, body: bytes) -> str:
    """Firma del origen de un reintento, atada al contenido exacto del mensaje (no se puede pasar a otro)."""
    digest = hashlib.sha256(body).hexdigest()
    key = get_settings().inbox_retry_key.encode()
    return hmac.new(key, f"{origin}{chr(10)}{digest}".encode(), hashlib.sha256).hexdigest()


def _origin_exchange(message) -> str:
    """Exchange por el que llegó el mensaje la primera vez.

    Una entrega inicial trae su exchange. Un reintento vuelve por el default exchange (exchange vacío) y la cola
    de reintento no conserva el original en `x-death`, así que el origen viaja en una cabecera que se pone al
    republicar (ORIGIN_HEADER) y que este servicio firma (SIGNATURE_HEADER). Con las credenciales de RabbitMQ
    compartidas, cualquiera puede publicar directo en la inbox y poner la cabecera; sin la clave de este
    servicio no puede firmarla, así que un origen sin firma válida cuenta como vacío y va a la DLQ.
    """
    if message.exchange:
        return message.exchange
    headers = message.headers or {}
    origin = headers.get(ORIGIN_HEADER)
    signature = headers.get(SIGNATURE_HEADER)
    if not isinstance(origin, str) or not isinstance(signature, str):
        return ""
    if hmac.compare_digest(_sign_origin(origin, message.body), signature):
        return origin
    return ""


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
    origin = _origin_exchange(message)
    if expected_exchange and origin != expected_exchange:
        return await _dlq(message, "%s llegó por %s y no por %s, va a la DLQ", event_type, origin, expected_exchange)
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
    origin = _origin_exchange(message)
    await channel.default_exchange.publish(
        aio_pika.Message(
            body=message.body,
            content_type=message.content_type,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=message.message_id,
            type=message.type,
            # El exchange de origen se guarda aquí: al volver por la cola de reintento, RabbitMQ lo cambia por "".
            headers={
                **(message.headers or {}),
                "x-retry-count": retry_count + 1,
                ORIGIN_HEADER: origin,
                SIGNATURE_HEADER: _sign_origin(origin, message.body),
            },
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
            # Sin confirmar, el mensaje ocupa el prefetch para siempre: se devuelve a la cola para reintentar.
            log.exception("error inesperado en el inbox, se devuelve a la cola")
            with suppress(Exception):
                await message.nack(requeue=True)

    return await queue.consume(on_message)
