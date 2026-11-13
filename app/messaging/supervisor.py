"""Conexión a RabbitMQ. connect_robust de aio-pika reconecta solo y re-declara los consumidores."""
import asyncio
import logging

import aio_pika

from ..config import get_settings
from .consumer import start_consumer
from .publisher import run_publisher
from .registry import get_registry

log = logging.getLogger("ai-service.amqp")


class Messaging:
    def __init__(self):
        self.connection = None
        self.stop_event = asyncio.Event()
        self.publisher_task = None

    async def start(self):
        settings = get_settings()
        registry = get_registry(settings.contracts_dir)
        while True:
            try:
                self.connection = await aio_pika.connect_robust(settings.amqp_url)
                break
            except Exception as err:  # noqa: BLE001
                log.warning("no se pudo conectar a RabbitMQ (%s), reintento en 3 s", err)
                await asyncio.sleep(3)
        consumer_channel = await self.connection.channel(publisher_confirms=True)
        await start_consumer(consumer_channel, registry)
        publisher_channel = await self.connection.channel(publisher_confirms=True)
        self.publisher_task = asyncio.create_task(run_publisher(publisher_channel, self.stop_event))
        log.info("mensajería arriba")

    async def stop(self):
        self.stop_event.set()
        if self.publisher_task:
            await self.publisher_task
        if self.connection:
            await self.connection.close()
