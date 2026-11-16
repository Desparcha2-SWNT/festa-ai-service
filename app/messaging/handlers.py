"""Handlers del inbox de ai-service (contracts/README.md § Qué consume cada servicio).

Firma: (data, envelope, db). Sin transacciones (Mongo sin réplica), así que cada escritura se
condiciona por versión o por marca de tiempo: una entrega atrasada no pisa una más reciente, y un
estado terminal (cancelado, expirado) no vuelve a «publicado» aunque llegue una foto vieja.
"""
from datetime import UTC, datetime

from pymongo.errors import DuplicateKeyError

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


async def _is_deleted(db, user_id: str) -> bool:
    """Lápida de cuenta eliminada: una entrega tardía de su perfil o consentimiento no la vuelve a crear."""
    return await db.deleted_accounts.find_one({"_id": user_id}) is not None


async def _consent(db, user_id: str | None, at_iso: str, granted: bool):
    """RNF5: el último cambio de consentimiento gana, por fecha del evento y no por orden de llegada."""
    if not user_id or await _is_deleted(db, user_id):
        return
    at = _dt(at_iso)
    allowed = {"$or": [{"locationConsentAt": {"$exists": False}}, {"locationConsentAt": {"$lt": at}}]}
    await _upsert_if_allowed(db.user_prefs, user_id, allowed, {"$set": {"locationConsent": granted, "locationConsentAt": at}})


async def _account_deleted(d, e, db):
    # Ley 1581: borra historial de chat y preferencias. Las descripciones generadas son contenido
    # del evento, no datos personales: se conservan sin el id de quien las pidió.
    # La lápida va primero y solo guarda el id (sin datos personales) para que un perfil o consentimiento
    # tardío no recree preferencias después del borrado.
    await db.deleted_accounts.update_one(
        {"_id": d["userId"]}, {"$setOnInsert": {"deletedAt": datetime.now(UTC)}}, upsert=True
    )
    await db.chat_sessions.delete_many({"userId": d["userId"]})
    await db.user_prefs.delete_one({"_id": d["userId"]})
    await db.descriptions.update_many({"requestedBy": d["userId"]}, {"$set": {"requestedBy": None}})


async def _profile_updated(d, e, db):
    """profile.updated: el perfil más reciente (por updatedAt, obligatorio en el contrato) gana."""
    if await _is_deleted(db, d["userId"]):
        return
    at = _dt(d["updatedAt"])
    allowed = {"$or": [{"profileUpdatedAt": {"$exists": False}}, {"profileUpdatedAt": {"$lt": at}}]}
    await _upsert_if_allowed(
        db.user_prefs,
        d["userId"],
        allowed,
        {"$set": {"displayName": d["displayName"], "interests": d["interests"], "homeZone": d.get("homeZone"), "profileUpdatedAt": at}},
    )


async def _merchant_updated(d, e, db):
    await db.merchants.update_one(
        {"_id": d["merchantId"]},
        {"$set": {"userId": d["userId"], "businessName": d["businessName"], "category": d["category"], "place": d["place"]}},
        upsert=True,
    )


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
