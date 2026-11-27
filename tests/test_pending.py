"""El resultado y su mensaje se guardan juntos: una caída entre guardar y copiar al outbox no pierde el mensaje."""
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from app import chat, descriptions, ratelimit
from app.messaging.pending import relay_stuck

from .conftest import HAS_CONTRACTS, validate_contract, verified_merchant
from .test_api import USER, client, publish_event

pytestmark = pytest.mark.skipif(not HAS_CONTRACTS, reason="sin festa-infra/contracts")


async def _crash(*_args, **_kwargs):
    raise RuntimeError("el proceso murió después de guardar")


@pytest.fixture(autouse=True)
def _fresh_limiter(monkeypatch):
    monkeypatch.setattr(ratelimit, "_local", {})


async def test_descripcion_guardada_sin_copiar_al_outbox_se_repara_en_la_siguiente_peticion(mongo_db, monkeypatch):
    body = {"draftId": str(uuid.uuid4()), "name": "Noche de jazz", "location": "Bloque G", "activityType": "concierto",
            "merchantId": await verified_merchant(mongo_db, USER)}
    original_relay = descriptions.relay
    monkeypatch.setattr(descriptions, "relay", _crash)
    async with client() as c:
        with pytest.raises(RuntimeError):
            await c.post("/ai/descriptions", json=body)
    saved = await mongo_db.descriptions.find_one({"draftId": body["draftId"]})
    assert saved["status"] == "done" and len(saved["pendingEvents"]) == 1  # quedó la intención junto al resultado
    assert await mongo_db.outbox.count_documents({}) == 0

    monkeypatch.setattr(descriptions, "relay", original_relay)
    async with client() as c:
        again = await c.post("/ai/descriptions", json=body)
    assert again.json()["cached"] is True
    rows = await mongo_db.outbox.find({"routingKey": "description.generated"}).to_list(10)
    assert len(rows) == 1 and rows[0]["_id"] == saved["pendingEvents"][0]["eventId"]
    assert validate_contract("ai", "description.generated", rows[0]["payload"]) == []
    assert not (await mongo_db.descriptions.find_one({"_id": saved["_id"]})).get("pendingEvents")


async def test_turno_de_chat_guardado_sin_copiar_lo_recupera_el_barrido(mongo_db, monkeypatch):
    await publish_event(mongo_db, category="musical")
    monkeypatch.setattr(chat, "relay", _crash)
    async with client() as c:
        with pytest.raises(RuntimeError):
            await c.post("/ai/chat", json={"message": "quiero música en vivo"})
    session = await mongo_db.chat_sessions.find_one({})
    assert len(session["messages"]) == 2 and len(session["pendingEvents"]) == 1
    assert await mongo_db.outbox.count_documents({"routingKey": "recommendation.served"}) == 0

    # Antes de STUCK_AFTER el barrido no lo toca (la petición aún podría estar copiándolo).
    assert await relay_stuck(mongo_db) == 0
    later = datetime.now(UTC) + timedelta(minutes=1)
    assert await relay_stuck(mongo_db, now=later) == 1
    assert await relay_stuck(mongo_db, now=later) == 0  # idempotente: ya no queda pendiente

    row = await mongo_db.outbox.find_one({"routingKey": "recommendation.served"})
    assert row["_id"] == session["pendingEvents"][0]["eventId"]
    assert validate_contract("ai", "recommendation.served", row["payload"]) == []


async def test_copiar_dos_veces_no_duplica_el_mensaje(mongo_db):
    async with client() as c:
        res = await c.post("/ai/descriptions", json={"draftId": str(uuid.uuid4()), "name": "Taller", "location": "Bloque B",
                                                     "activityType": "taller", "merchantId": await verified_merchant(mongo_db, USER)})
    assert res.status_code == 200
    row = await mongo_db.outbox.find_one({"routingKey": "description.generated"})
    # Simula que el barrido y la petición copiaron el mismo pendiente.
    doc = await mongo_db.descriptions.find_one({})
    await mongo_db.descriptions.update_one({"_id": doc["_id"]}, {"$push": {"pendingEvents": {
        "eventId": row["_id"], "exchange": "festa.ai", "routingKey": "description.generated", "payload": row["payload"],
        "correlationId": None, "at": datetime.now(UTC) - timedelta(minutes=5),
    }}})
    assert await relay_stuck(mongo_db) == 1
    assert await mongo_db.outbox.count_documents({"routingKey": "description.generated"}) == 1
