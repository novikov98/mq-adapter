from __future__ import annotations

import asyncio
from logging import getLogger

import aio_pika


logger = getLogger(__name__)


class RabbitConnectionManager:
    """
    Менеджер подключения к RabbitMQ.

    - Держит одно robust-соединение на процесс.
    - Лениво создаёт подключение при первом запросе.
    - Может отдавать новые каналы поверх одного соединения.
    """

    def __init__(self, rabbit_url: str):
        self.url: str = rabbit_url
        self.connection: aio_pika.RobustConnection | None = None
        self.lock: asyncio.Lock = asyncio.Lock()

    @property
    def existing_connection(self) -> aio_pika.RobustConnection | None:
        if self.connection and not self.connection.is_closed:
            return self.connection
        return None

    async def get_connection(self) -> aio_pika.RobustConnection:
        """
        Вернёт robust-соединение.
        Если соединение ещё не установлено или закрыто – создаст новое.
        """
        if connection := self.existing_connection:
            return connection

        async with self.lock:
            if connection := self.existing_connection:
                return connection

            try:
                logger.info("Connecting to RabbitMQ...")
                self.connection = await aio_pika.connect_robust(self.url)
            except Exception:
                logger.error("Failed to connect to RabbitMQ")
                raise

            logger.info("RabbitMQ connection established.")
            return self.connection

    async def get_channel(self) -> aio_pika.RobustChannel:
        """
        Для получения нового robust-канала.
        """

        conn = await self.get_connection()
        channel: aio_pika.RobustChannel = await conn.channel()
        return channel

    async def close(self) -> None:
        """
        Корректно закрывает соединение.
        """

        if not self.connection:
            return

        if not self.connection.is_closed:
            logger.info("Closing RabbitMQ connection...")
            await self.connection.close()
            logger.info("RabbitMQ connection closed.")

        self.connection = None
