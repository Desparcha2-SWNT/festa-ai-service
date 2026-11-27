"""El supervisor de RabbitMQ se rearma solo: tras un armado fallido y tras una caída de la conexión."""
import asyncio

import pytest

from app import main
from app.messaging import supervisor


class FakeChannel:
    def __init__(self):
        self.close_callbacks = set()


class FakeConnection:
    def __init__(self, fail_consumer=False):
        self.close_callbacks = set()
        self.channels = []
        self.is_closed = False
        self.fail_consumer = fail_consumer

    async def channel(self, publisher_confirms=True):
        ch = FakeChannel()
        ch.fail = self.fail_consumer
        self.channels.append(ch)
        return ch

    async def close(self):
        self.is_closed = True

    def drop(self):
        """Simula que RabbitMQ se reinicia: cierra la conexión desde afuera."""
        self.is_closed = True
        for cb in list(self.close_callbacks):
            cb(self, None)


@pytest.fixture
def wired(monkeypatch):
    consumers = []

    async def fake_start_consumer(channel, registry):
        if getattr(channel, "fail", False):
            raise RuntimeError("NOT_FOUND - no queue 'ai-service.inbox'")  # cola aún sin declarar
        consumers.append(channel)

    async def fake_run_publisher(channel, stop):
        await stop.wait()

    monkeypatch.setattr(supervisor, "start_consumer", fake_start_consumer)
    monkeypatch.setattr(supervisor, "run_publisher", fake_run_publisher)
    monkeypatch.setattr(supervisor, "get_registry", lambda _dir: object())
    return consumers


async def _until(cond, timeout=2.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not cond():
        if loop.time() > end:
            raise AssertionError("no se cumplió a tiempo")
        await asyncio.sleep(0.01)


async def test_si_la_cola_aun_no_existe_reintenta_hasta_armar_el_consumidor(wired):
    attempts = [FakeConnection(fail_consumer=True), FakeConnection()]
    connects = []

    async def connect(url):
        conn = attempts[len(connects)]
        connects.append(conn)
        return conn

    messaging = supervisor.Messaging(retry_s=0.01, connect=connect)
    await asyncio.wait_for(messaging.start(), timeout=2)

    assert len(connects) == 2 and connects[0].is_closed  # la conexión del intento fallido se cerró
    assert messaging.connected and len(wired) == 1
    await messaging.stop()


async def test_tras_una_caida_de_rabbitmq_se_reconecta_solo(wired):
    connects = []

    async def connect(url):
        conn = FakeConnection()
        connects.append(conn)
        return conn

    messaging = supervisor.Messaging(retry_s=0.01, connect=connect)
    await asyncio.wait_for(messaging.start(), timeout=2)
    connects[0].drop()

    await _until(lambda: len(connects) == 2 and messaging.connected)
    assert len(wired) == 2  # el consumidor se volvió a armar sobre la conexión nueva
    await messaging.stop()
    assert not messaging.connected


async def test_health_da_503_si_la_mensajeria_lleva_caida_demasiado(monkeypatch):
    from httpx import ASGITransport, AsyncClient

    app = main.create_app(skip_infra=True)

    class Down:
        def healthy(self):
            return False

        def down_for(self):
            return 75.0

    app.state.messaging = Down()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        res = await c.get("/health")
    assert res.status_code == 503
    assert res.json() == {"status": "messaging-down", "downForSeconds": 75}
