"""El marcador processed_events va aparte del efecto (consumer.py). Si el handler aplicó y falló el
marcador, el reintento repite el handler: debe dejar el MISMO estado, aunque entre medio haya llegado
un mensaje más nuevo. Cada caso aplica el viejo, luego el nuevo y reaplica el viejo.
"""
import copy
import uuid

import pytest

from app.messaging.handlers import HANDLERS

from .conftest import HAS_CONTRACTS, contract_example

pytestmark = pytest.mark.skipif(not HAS_CONTRACTS, reason="sin festa-infra/contracts")

USER = "0b9c7d6e-5f4a-4b3c-8d2e-1a0b9c8d7e6f"


async def _run(db, event_type, data):
    await HANDLERS[event_type](copy.deepcopy(data), {"eventId": str(uuid.uuid4()), "eventType": event_type}, db)


async def _snapshot(db):
    out = {}
    for name in ("catalog", "user_prefs", "merchants", "deleted_accounts"):
        out[name] = await db[name].find({}, {"deletedAt": 0}).sort("_id", 1).to_list(100)
    return out


async def _old_new_old(db, event_type, old, new):
    await _run(db, event_type, old)
    await _run(db, event_type, new)
    after_new = await _snapshot(db)
    await _run(db, event_type, old)  # reintento tardío del primero (falló su marcador)
    assert await _snapshot(db) == after_new


async def test_profile_updated_viejo_reintentado_no_pisa_el_nuevo(mongo_db):
    old = {"userId": USER, "displayName": "Ana", "interests": ["musical"], "updatedAt": "2026-11-01T10:00:00Z"}
    new = {**old, "displayName": "Ana M.", "interests": ["deportivo"], "updatedAt": "2026-11-01T11:00:00Z"}
    await _old_new_old(mongo_db, "profile.updated", old, new)


async def test_consentimiento_viejo_reintentado_no_pisa_la_revocacion(mongo_db):
    await _run(mongo_db, "location_consent.granted", {"userId": USER, "grantedAt": "2026-11-01T10:00:00Z"})
    await _run(mongo_db, "location_consent.revoked", {"userId": USER, "revokedAt": "2026-11-01T11:00:00Z"})
    await _run(mongo_db, "location_consent.granted", {"userId": USER, "grantedAt": "2026-11-01T10:00:00Z"})
    assert (await mongo_db.user_prefs.find_one({"_id": USER}))["locationConsent"] is False


async def test_merchant_updated_viejo_reintentado_no_pisa_el_nuevo(mongo_db):
    old = contract_example("user", "merchant.updated")
    old = {**old, "businessName": "Café Viejo", "updatedAt": "2026-11-01T10:00:00Z"}
    new = {**old, "businessName": "Café Nuevo", "updatedAt": "2026-11-01T11:00:00Z"}
    await _old_new_old(mongo_db, "merchant.updated", old, new)
    assert (await mongo_db.merchants.find_one({"_id": old["merchantId"]}))["businessName"] == "Café Nuevo"


async def test_foto_de_evento_vieja_reintentada_no_pisa_la_nueva(mongo_db):
    old = {**contract_example("event", "event.published"), "version": 1, "title": "Versión 1"}
    new = {**old, "version": 2, "title": "Versión 2"}
    await _old_new_old(mongo_db, "event.published", old, new)


@pytest.mark.parametrize("event_type", ["event.cancelled", "event.expired", "event.sold_out"])
async def test_estados_fijos_reintentados_dan_el_mismo_resultado(mongo_db, event_type):
    snapshot = contract_example("event", "event.published")
    await _run(mongo_db, "event.published", snapshot)
    data = {"eventId": snapshot["eventId"]}
    await _run(mongo_db, event_type, data)
    once = await _snapshot(mongo_db)
    await _run(mongo_db, "event.updated", {**snapshot, "version": snapshot["version"] + 1})
    await _run(mongo_db, event_type, data)
    twice = await _snapshot(mongo_db)
    # Un estado terminal no lo reabre una foto posterior; agotado se conserva.
    assert twice["catalog"][0]["status"] == once["catalog"][0]["status"]
    assert twice["catalog"][0]["soldOut"] == once["catalog"][0]["soldOut"]


async def test_account_deleted_reintentado_es_idempotente(mongo_db):
    await _run(mongo_db, "profile.updated", {"userId": USER, "displayName": "Ana", "interests": [], "updatedAt": "2026-11-01T10:00:00Z"})
    await _run(mongo_db, "account.deleted", {"userId": USER, "deletedAt": "2026-11-01T11:00:00Z"})
    once = await _snapshot(mongo_db)
    await _run(mongo_db, "account.deleted", {"userId": USER, "deletedAt": "2026-11-01T11:00:00Z"})
    assert await _snapshot(mongo_db) == once
