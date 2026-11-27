"""Conexión a RabbitMQ con su propio ciclo de reconexión.

Antes se usaba connect_robust, que reconecta el socket pero no rehace el consumidor si el armado
falla. En AWS RabbitMQ corre sin disco: al reiniciarse arranca vacío y sus colas llegan unos segundos
después (definitions.json). Si el servicio reconecta en ese intervalo, consumir de una cola que aún no
existe cierra el canal y la mensajería quedaba muerta («Connection ... closed») mientras /health
seguía diciendo «ok». Ahora cada caída, o cada armado fallido, cierra todo y vuelve a empezar con una
conexión nueva, y /health responde 503 si la mensajería lleva caída más de DOWN_LIMIT_S (ECS reemplaza
la tarea).
"""
import asyncio
import logging
import time

import aio_pika

from ..config import get_settings
from .consumer import start_consumer
from .publisher import run_publisher
from .registry import get_registry

log = logging.getLogger("ai-service.amqp")

RETRY_S = 3.0
DOWN_LIMIT_S = 60.0


class Messaging:
    def __init__(self, *, retry_s: float = RETRY_S, connect=None):
        self.retry_s = retry_s
        self._connect = connect or aio_pika.connect
        self.stop_event = asyncio.Event()
        self.ready = asyncio.Event()
        self.connected = False
        self.down_since: float | None = time.monotonic()
        self.connection = None
        self._task: asyncio.Task | None = None

    async def start(self):
        """Arranca el ciclo y espera la primera conexión, como antes."""
        self._task = asyncio.create_task(self._run())
        stop_wait = asyncio.create_task(self.stop_event.wait())
        ready_wait = asyncio.create_task(self.ready.wait())
        await asyncio.wait({stop_wait, ready_wait}, return_when=asyncio.FIRST_COMPLETED)
        for t in (stop_wait, ready_wait):
            t.cancel()

    def down_for(self) -> float:
        """Segundos que lleva la mensajería caída (0 si está conectada)."""
        return 0.0 if self.connected or self.down_since is None else time.monotonic() - self.down_since

    def healthy(self) -> bool:
        return self.down_for() < DOWN_LIMIT_S

    async def _run(self):
        settings = get_settings()
        registry = get_registry(settings.contracts_dir)
        while not self.stop_event.is_set():
            await self._session(settings.amqp_url, registry)
            if self.stop_event.is_set():
                break
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=self.retry_s)
            except TimeoutError:
                pass

    async def _session(self, url: str, registry) -> None:
        """Una conexión completa: arma consumidor y publicador y espera a que algo se caiga."""
        connection = None
        publisher_stop = asyncio.Event()
        publisher_task = None
        closed = asyncio.Event()
        try:
            connection = await self._connect(url)
            self.connection = connection
            connection.close_callbacks.add(lambda *_: closed.set())
            consumer_channel = await connection.channel(publisher_confirms=True)
            consumer_channel.close_callbacks.add(lambda *_: closed.set())
            await start_consumer(consumer_channel, registry)
            publisher_channel = await connection.channel(publisher_confirms=True)
            publisher_channel.close_callbacks.add(lambda *_: closed.set())
            publisher_task = asyncio.create_task(run_publisher(publisher_channel, publisher_stop))
            self.connected = True
            self.down_since = None
            self.ready.set()
            log.info("mensajería arriba")
            stop_wait = asyncio.create_task(self.stop_event.wait())
            closed_wait = asyncio.create_task(closed.wait())
            await asyncio.wait({stop_wait, closed_wait}, return_when=asyncio.FIRST_COMPLETED)
            for t in (stop_wait, closed_wait):
                t.cancel()
            if not self.stop_event.is_set():
                log.warning("conexión o canal de RabbitMQ cerrado, se rearma en %.0f s", self.retry_s)
        except Exception as err:  # noqa: BLE001 - cualquier falla del armado se reintenta desde cero
            log.warning("no se pudo armar la mensajería con RabbitMQ (%s), reintento en %.0f s", err, self.retry_s)
        finally:
            if self.connected:
                self.down_since = time.monotonic()
            self.connected = False
            publisher_stop.set()
            if publisher_task:
                try:
                    await asyncio.wait_for(publisher_task, timeout=10)
                except Exception:  # noqa: BLE001
                    publisher_task.cancel()
            if connection is not None and not connection.is_closed:
                try:
                    await connection.close()
                except Exception:  # noqa: BLE001
                    pass

    async def stop(self):
        self.stop_event.set()
        if self._task:
            await self._task
