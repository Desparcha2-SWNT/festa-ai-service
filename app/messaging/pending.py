"""Intención de publicación guardada junto al resultado (outbox embebido).

Mongo sin réplica no tiene transacciones entre colecciones, pero sí escrituras atómicas sobre UN
documento. Por eso la descripción generada y la conversación guardan su mensaje pendiente en
`pendingEvents` dentro de la misma escritura que el resultado: o quedan ambos o ninguno.

Después, `relay()` copia cada pendiente al outbox con su eventId fijo (upsert por _id, idempotente)
y lo saca del documento. Si el proceso muere entre guardar y copiar, `relay_stuck()` (lo corre el
publicador del outbox en cada vuelta) termina el trabajo: el mensaje nunca se pierde y, como el
eventId es estable, tampoco se duplica.
"""
import uuid
from datetime import UTC, datetime, timedelta

from ..accounts import cancel_unsent, is_deleted
from .outbox import enqueue

COLLECTIONS = ("descriptions", "chat_sessions")
# Lo que la propia petición no alcanzó a copiar en este tiempo lo recoge el barrido.
STUCK_AFTER = timedelta(seconds=10)


def pending_event(*, exchange: str, routing_key: str, payload: dict, correlation_id=None, event_id: str | None = None) -> dict:
    return {
        "eventId": event_id or str(uuid.uuid4()),
        "exchange": exchange,
        "routingKey": routing_key,
        "payload": payload,
        "correlationId": correlation_id,
        "at": datetime.now(UTC),
    }


async def relay(db, collection: str, doc_id: str, events: list[dict]) -> None:
    """Copia los pendientes al outbox y los saca del documento (las dos cosas son idempotentes).

    Un pendiente lleva el id de la persona (userId / requestedBy). Si su cuenta ya se borró, no se copia: el
    barrido pudo leer el documento antes de que account.deleted lo limpiara y, sin esto, recrearía en el
    outbox un mensaje con su id. La lápida se mira antes y también después de encolar: account.deleted la
    escribe ANTES de cancelar el outbox, así que en cualquier intercalado o el borrado ve el mensaje y lo
    cancela, o este lo ve y lo cancela él mismo.
    """
    for ev in events:
        user_id = ev["payload"].get("userId") or ev["payload"].get("requestedBy")
        if not await is_deleted(db, user_id):
            await enqueue(
                db,
                exchange=ev["exchange"],
                routing_key=ev["routingKey"],
                payload=ev["payload"],
                correlation_id=ev.get("correlationId"),
                event_id=ev["eventId"],
            )
            if await is_deleted(db, user_id):
                await cancel_unsent(db, user_id, [ev["eventId"]])
        await db[collection].update_one({"_id": doc_id}, {"$pull": {"pendingEvents": {"eventId": ev["eventId"]}}})


async def relay_stuck(db, now: datetime | None = None, limit: int = 100) -> int:
    """Recupera los pendientes que quedaron sin copiar (p. ej. el proceso murió tras guardar)."""
    cutoff = (now or datetime.now(UTC)) - STUCK_AFTER
    relayed = 0
    for collection in COLLECTIONS:
        cursor = db[collection].find({"pendingEvents.at": {"$lte": cutoff}}, {"pendingEvents": 1}).limit(limit)
        async for doc in cursor:
            await relay(db, collection, doc["_id"], doc.get("pendingEvents", []))
            relayed += len(doc.get("pendingEvents", []))
    return relayed
