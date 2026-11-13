"""RF8 y RF9 de punta a punta contra MongoDB real con el proveedor local `fake`."""
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from app import ratelimit
from app.main import create_app
from app.messaging.handlers import HANDLERS

from .conftest import HAS_CONTRACTS, contract_example, token, validate_contract

USER = "0b9c7d6e-5f4a-4b3c-8d2e-1a0b9c8d7e6f"
OTHER = "2b9c7d6e-5f4a-4b3c-8d2e-1a0b9c8d7e6f"
pytestmark = pytest.mark.skipif(not HAS_CONTRACTS, reason="sin festa-infra/contracts")


def client(sub=USER):
    transport = httpx.ASGITransport(app=create_app(skip_infra=True))
    return httpx.AsyncClient(transport=transport, base_url="http://test", headers={"Authorization": f"Bearer {token(sub)}"})


async def publish_event(db, *, category="musical", hours=3, **extra):
    data = contract_example("event", "event.published")
    starts = datetime.now(UTC) + timedelta(hours=hours)
    data.update(eventId=str(uuid.uuid4()), category=category, startsAt=starts.isoformat().replace("+00:00", "Z"),
                endsAt=(starts + timedelta(hours=2)).isoformat().replace("+00:00", "Z"), **extra)
    await HANDLERS["event.published"](data, {"eventId": str(uuid.uuid4())}, db)
    return data


async def test_requires_token(mongo_db):
    transport = httpx.ASGITransport(app=create_app(skip_infra=True))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        assert (await c.post("/ai/chat", json={"message": "hola"})).status_code == 401


async def test_rf9_description_generated_once_and_contract(mongo_db):
    draft = str(uuid.uuid4())
    body = {"draftId": draft, "name": "Noche de jazz", "location": "Bloque G", "activityType": "concierto en vivo"}
    async with client() as c:
        first = await c.post("/ai/descriptions", json=body)
        second = await c.post("/ai/descriptions", json=body)
    assert first.status_code == 200 and first.json()["cached"] is False
    assert second.json()["cached"] is True and second.json()["description"] == first.json()["description"]

    rows = await mongo_db.outbox.find({"routingKey": "description.generated"}).to_list(10)
    assert len(rows) == 1  # RNF8: un solo costo por borrador
    assert validate_contract("ai", "description.generated", rows[0]["payload"]) == []


async def test_rf9_rejects_foreign_merchant(mongo_db):
    async with client() as c:
        res = await c.post("/ai/descriptions", json={
            "draftId": str(uuid.uuid4()), "name": "abc", "location": "x y", "activityType": "taller", "merchantId": str(uuid.uuid4()),
        })
    assert res.status_code == 403


async def test_rf8_recommends_only_real_upcoming_events(mongo_db):
    jazz = await publish_event(mongo_db, category="musical")
    await publish_event(mongo_db, category="deportivo", status="cancelled")
    async with client() as c:
        res = await c.post("/ai/chat", json={"message": "quiero escuchar música en vivo hoy"})
    assert res.status_code == 200
    payload = res.json()
    assert [e["eventId"] for e in payload["events"]] == [jazz["eventId"]]
    assert payload["disclaimer"]

    row = await mongo_db.outbox.find_one({"routingKey": "recommendation.served"})
    assert validate_contract("ai", "recommendation.served", row["payload"]) == []

    async with client() as c:
        history = await c.get(f"/ai/chat/sessions/{payload['sessionId']}")
    assert history.json()["messages"][0]["content"] == "quiero escuchar música en vivo hoy"
    async with client(OTHER) as c:
        assert (await c.get(f"/ai/chat/sessions/{payload['sessionId']}")).status_code == 404


async def test_rf8_sold_out_and_cancelled_events_leave_catalog(mongo_db):
    event = await publish_event(mongo_db)
    await HANDLERS["event.sold_out"]({"eventId": event["eventId"], "capacity": 80, "soldOutAt": "2026-11-01T00:00:00Z"}, {}, mongo_db)
    async with client() as c:
        res = await c.post("/ai/chat", json={"message": "música"})
    assert res.json()["events"] == []


async def test_rnf8_rate_limit(mongo_db, monkeypatch):
    monkeypatch.setattr(ratelimit, "_local", {})
    async with client() as c:
        codes = [(await c.post("/ai/chat", json={"message": "hola"})).status_code for _ in range(11)]
    assert codes[:10] == [200] * 10 and codes[10] == 429


async def test_account_deleted_removes_history(mongo_db):
    async with client() as c:
        await c.post("/ai/chat", json={"message": "hola"})
    await HANDLERS["account.deleted"]({"userId": USER, "deletedAt": "2026-11-01T00:00:00Z"}, {}, mongo_db)
    assert await mongo_db.chat_sessions.count_documents({"userId": USER}) == 0
