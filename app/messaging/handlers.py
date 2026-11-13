"""Handlers del inbox de ai-service (contracts/README.md § Qué consume cada servicio).

Firma: (data, envelope, db). Todos son idempotentes.
"""
from datetime import datetime


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


async def _catalog_snapshot(d, e, db):
    """event.published / event.updated: catálogo para recomendar; ignora versiones viejas."""
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
    res = await db.catalog.update_one(
        {"_id": d["eventId"], "$or": [{"version": {"$exists": False}}, {"version": {"$lt": d["version"]}}]},
        {"$set": doc},
    )
    if res.matched_count == 0:
        await db.catalog.update_one({"_id": d["eventId"]}, {"$setOnInsert": {**doc, "soldOut": False}}, upsert=True)


async def _set_status(db, event_id: str, status: str):
    await db.catalog.update_one({"_id": event_id}, {"$set": {"status": status}})


async def _account_deleted(d, e, db):
    # Ley 1581: borra historial de chat y preferencias. Las descripciones generadas son contenido
    # del evento, no datos personales: se conservan sin el id de quien las pidió.
    await db.chat_sessions.delete_many({"userId": d["userId"]})
    await db.user_prefs.delete_one({"_id": d["userId"]})
    await db.descriptions.update_many({"requestedBy": d["userId"]}, {"$set": {"requestedBy": None}})


async def _profile_updated(d, e, db):
    await db.user_prefs.update_one(
        {"_id": d["userId"]},
        {"$set": {"displayName": d["displayName"], "interests": d["interests"], "homeZone": d.get("homeZone")}},
        upsert=True,
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
    "location_consent.granted": lambda d, e, db: db.user_prefs.update_one({"_id": d["userId"]}, {"$set": {"locationConsent": True}}, upsert=True),
    "location_consent.revoked": lambda d, e, db: db.user_prefs.update_one({"_id": d["userId"]}, {"$set": {"locationConsent": False}}, upsert=True),
    "merchant.updated": _merchant_updated,
    "event.published": _catalog_snapshot,
    "event.updated": _catalog_snapshot,
    "event.cancelled": lambda d, e, db: _set_status(db, d["eventId"], "cancelled"),
    "event.expired": lambda d, e, db: _set_status(db, d["eventId"], "expired"),
    "event.sold_out": lambda d, e, db: db.catalog.update_one({"_id": d["eventId"]}, {"$set": {"soldOut": True}}),
}
