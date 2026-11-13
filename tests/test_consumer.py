"""Despacho del inbox: validación contra contratos, idempotencia, reintentos y DLQ."""
import json
import os
import uuid

import pytest

from app.messaging import consumer
from app.messaging.registry import SchemaRegistry

from .conftest import HAS_CONTRACTS, contract_example

pytestmark = pytest.mark.skipif(not HAS_CONTRACTS, reason="sin festa-infra/contracts")


class FakeMessage:
    def __init__(self, body, exchange="festa.event", headers=None):
        self.body = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.exchange = exchange
        self.headers = headers or {}
        self.content_type = "application/json"
        self.message_id = "m"
        self.type = "t"
        self.result = None

    async def ack(self):
        self.result = "ack"

    async def reject(self, requeue=False):
        self.result = "reject"


class FakeExchange:
    def __init__(self):
        self.published = []

    async def publish(self, message, routing_key):
        self.published.append((routing_key, message))


class FakeChannel:
    def __init__(self):
        self.default_exchange = FakeExchange()


@pytest.fixture(scope="module")
def registry():
    return SchemaRegistry(os.environ["FESTA_CONTRACTS_DIR"])


def envelope(event_type="event.published", producer="event-service", data=None):
    return {"eventId": str(uuid.uuid4()), "eventType": event_type, "version": 1, "occurredAt": "2026-10-28T15:00:00Z",
            "producer": producer, "correlationId": None, "causationId": None,
            "data": data if data is not None else contract_example("event", "event.published")}


async def test_invalid_json_goes_to_dlq(registry):
    msg = FakeMessage(b"no-json")
    assert await consumer.handle(msg, registry, FakeChannel()) == "dlq"


async def test_fake_producer_goes_to_dlq(registry):
    msg = FakeMessage(envelope(producer="payment-service"))
    assert await consumer.handle(msg, registry, FakeChannel()) == "dlq"


async def test_wrong_exchange_goes_to_dlq(registry):
    msg = FakeMessage(envelope(), exchange="festa.user")
    assert await consumer.handle(msg, registry, FakeChannel()) == "dlq"


async def test_invalid_data_goes_to_dlq(registry):
    msg = FakeMessage(envelope(data={"eventId": "x"}))
    assert await consumer.handle(msg, registry, FakeChannel()) == "dlq"


async def test_processes_once_then_duplicate(registry, mongo_db):
    env = envelope()
    assert await consumer.handle(FakeMessage(env), registry, FakeChannel(), mongo_db) == "ack"
    assert await consumer.handle(FakeMessage(env), registry, FakeChannel(), mongo_db) == "duplicate"
    assert await mongo_db.catalog.count_documents({}) == 1


async def test_transient_failure_retries_then_dlq(registry):
    class BrokenDb:
        class processed_events:  # noqa: N801
            @staticmethod
            async def find_one(_):
                raise RuntimeError("mongo caído")

    channel = FakeChannel()
    msg = FakeMessage(envelope())
    assert await consumer.handle(msg, registry, channel, BrokenDb()) == "retry"
    routing_key, retried = channel.default_exchange.published[0]
    assert routing_key == "ai-service.inbox.retry" and retried.headers["x-retry-count"] == 1

    exhausted = FakeMessage(envelope(), headers={"x-retry-count": 3})
    assert await consumer.handle(exhausted, registry, FakeChannel(), BrokenDb()) == "dlq"
