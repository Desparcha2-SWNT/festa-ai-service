"""Handlers del inbox de ai-service (contracts/README.md § Qué consume cada servicio).

Firma: (data, envelope, db). Sin transacciones (Mongo sin réplica), así que cada escritura se
condiciona por versión o por marca de tiempo: una entrega atrasada no pisa una más reciente, y un
estado terminal (cancelado, expirado) no vuelve a «publicado» aunque llegue una foto vieja.

Por eso el marcador processed_events puede ir aparte del efecto (consumer.py): si el handler aplicó
y falló el marcador, el reintento repite el mismo handler y el resultado es idéntico. Cada efecto es
un valor fijo (cancelado, expirado, agotado, lápida, borrado, comercio desvinculado) o está condicionado por version,
updatedAt, grantedAt o revokedAt. tests/test_replay.py reaplica cada handler después de uno más nuevo.
"""
from datetime import UTC, datetime

from pymongo.errors import DuplicateKeyError

from ..accounts import anonymize_sent, cancel_unsent, is_deleted

TERMINAL_STATUSES = ["cancelled", "expired"]


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


async def _upsert_if_allowed(col, _id, allowed: dict, update: dict) -> None:
    """Aplica `update` si el documento no existe o cumple `allowed`.

    Si dos entregas crean el documento a la vez, una recibe E11000: se reintenta una vez para que
    la más reciente vuelva a evaluar `allowed` contra el documento ya creado y no se pierda.
    """
    for _ in range(2):
        try:
            await col.update_one({"_id": _id, **allowed}, update, upsert=True)
            return
        except DuplicateKeyError:
            continue


async def _catalog_snapshot(d, e, db):
    """event.published / event.updated: catálogo para recomendar; ignora versiones viejas y estados terminales."""
    doc = {
        "title": d["title"],
        "description": d["description"][:600],
        "category": d["category"],
        "tags": d.get("tags", []),
        "organizerName": d.get("organizerName"),
        "venueName": d["venue"]["name"],
        "venueAddress": d["venue"]["address"],
        "location": {"type": "Point", "coordinates": [d["venue"]["location"]["lng"], d["venue"]["location"]["lat"]]},
        "startsAt": _dt(d["startsAt"]),
        "endsAt": _dt(d["endsAt"]),
        "ticketed": d["ticketed"],
        "priceCents": d["priceCents"],
        "isLive": d["isLive"],
        "status": d["status"],
        "version": d["version"],
    }
    allowed = {
        "$and": [
            {"$or": [{"version": {"$exists": False}}, {"version": {"$lt": d["version"]}}]},
            # Un cancelado o expirado (aunque llegó antes que la foto) no se reemplaza por una publicación vieja.
            {"status": {"$nin": TERMINAL_STATUSES}},
        ]
    }
    # soldOut solo se fija al crear; si ya llegó event.sold_out antes, se conserva.
    await _upsert_if_allowed(db.catalog, d["eventId"], allowed, {"$set": doc, "$setOnInsert": {"soldOut": False}})


async def _set_status(db, event_id: str, status: str):
    # upsert: si el estado llega antes que la foto, queda como marca; la foto no la sobrescribe.
    await db.catalog.update_one({"_id": event_id}, {"$set": {"status": status}}, upsert=True)


async def _sold_out(db, event_id: str):
    await db.catalog.update_one({"_id": event_id}, {"$set": {"soldOut": True}}, upsert=True)


async def _write_prefs_unless_deleted(db, user_id: str, allowed: dict, update: dict) -> None:
    """Escribe preferencias salvo que la cuenta esté borrada, también si el borrado llega a la vez.

    La lápida se mira antes (evita el trabajo) y DESPUÉS de escribir: `account.deleted` la escribe
    antes de borrar, así que si este upsert cayó después de su borrado, aquí se ve y se deshace.
    """
    if await is_deleted(db, user_id):
        return
    await _upsert_if_allowed(db.user_prefs, user_id, allowed, update)
    if await is_deleted(db, user_id):
        await db.user_prefs.delete_one({"_id": user_id})


async def _consent(db, user_id: str | None, at_iso: str, granted: bool):
    """RNF5: el último cambio de consentimiento gana, por fecha del evento y no por orden de llegada."""
    if not user_id:
        return
    at = _dt(at_iso)
    allowed = {"$or": [{"locationConsentAt": {"$exists": False}}, {"locationConsentAt": {"$lt": at}}]}
    await _write_prefs_unless_deleted(db, user_id, allowed, {"$set": {"locationConsent": granted, "locationConsentAt": at}})


# Comercio cuyo dueño borró su cuenta: sin vínculo personal y sin verificación.
_UNLINKED = {"$set": {"userId": None, "verified": False, "ownerDeleted": True}}


async def _account_deleted(d, e, db):
    # Ley 1581: borra historial de chat y preferencias. Las descripciones generadas son contenido
    # del evento, no datos personales: se conservan sin el id de quien las pidió.
    # La lápida va primero y solo guarda el id (sin datos personales) para que un perfil o consentimiento
    # tardío no recree preferencias después del borrado.
    await db.deleted_accounts.update_one(
        {"_id": d["userId"]}, {"$setOnInsert": {"deletedAt": datetime.now(UTC)}}, upsert=True
    )
    user_id = d["userId"]
    await db.chat_sessions.delete_many({"userId": user_id})
    await db.user_prefs.delete_one({"_id": user_id})
    # Los mensajes pendientes dentro de la descripción llevan requestedBy: se cancelan con ella.
    await db.descriptions.update_many(
        {"requestedBy": user_id}, {"$set": {"requestedBy": None}, "$unset": {"pendingEvents": ""}}
    )
    await db.merchants.update_many({"userId": user_id}, _UNLINKED)
    # Lo que ya pasó al outbox y no se ha publicado también se cancela (ver accounts.cancel_unsent).
    await cancel_unsent(db, user_id)
    # Y lo ya publicado conserva la copia en el outbox: se anonimiza (ver accounts.anonymize_sent).
    await anonymize_sent(db, user_id)


async def _profile_updated(d, e, db):
    """profile.updated: el perfil más reciente (por updatedAt, obligatorio en el contrato) gana."""
    at = _dt(d["updatedAt"])
    allowed = {"$or": [{"profileUpdatedAt": {"$exists": False}}, {"profileUpdatedAt": {"$lt": at}}]}
    await _write_prefs_unless_deleted(
        db,
        d["userId"],
        allowed,
        {"$set": {"displayName": d["displayName"], "interests": d["interests"], "homeZone": d.get("homeZone"), "profileUpdatedAt": at}},
    )


async def _merchant_updated(d, e, db):
    """merchant.updated: el más reciente (por updatedAt, obligatorio en el contrato) gana.

    `verified` se guarda tal cual (puede volver a false si se revoca) y descriptions.py lo exige.
    Si el dueño borró su cuenta, el comercio queda sin vínculo personal aunque esta entrega llegue tarde.
    """
    if await is_deleted(db, d["userId"]):
        await db.merchants.update_one({"_id": d["merchantId"]}, _UNLINKED)
        return
    at = _dt(d["updatedAt"])
    allowed = {"$or": [{"updatedAt": {"$exists": False}}, {"updatedAt": {"$lt": at}}]}
    await _upsert_if_allowed(
        db.merchants,
        d["merchantId"],
        allowed,
        {"$set": {"userId": d["userId"], "businessName": d["businessName"], "category": d["category"], "place": d["place"],
                  "verified": d["verified"] is True, "updatedAt": at}},
    )
    if await is_deleted(db, d["userId"]):  # account.deleted llegó entre la consulta y la escritura
        await db.merchants.update_one({"_id": d["merchantId"]}, _UNLINKED)


HANDLERS = {
    "account.deleted": _account_deleted,
    "profile.updated": _profile_updated,
    "location_consent.granted": lambda d, e, db: _consent(db, d["userId"], d["grantedAt"], True),
    "location_consent.revoked": lambda d, e, db: _consent(db, d["userId"], d["revokedAt"], False),
    "merchant.updated": _merchant_updated,
    "event.published": _catalog_snapshot,
    "event.updated": _catalog_snapshot,
    "event.cancelled": lambda d, e, db: _set_status(db, d["eventId"], "cancelled"),
    "event.expired": lambda d, e, db: _set_status(db, d["eventId"], "expired"),
    "event.sold_out": lambda d, e, db: _sold_out(db, d["eventId"]),
}
