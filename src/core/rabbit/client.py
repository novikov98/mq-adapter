import asyncio
from aio_pika import RobustConnection, connect_robust

from src.config import config


class RabbitConnectionManager:
    def __init__(self):
        self._url = config.rabbit_config.connect_url
        self._connection: RobustConnection | None = None
        self._lock = asyncio.Lock()

    async def get_connection(self) -> RobustConnection:
        if self._connection and not self._connection.is_closed:
            return self._connection

        async with self._lock:
            if self._connection and not self._connection.is_closed:
                return self._connection

            self._connection = await connect_robust(self._url)
            return self._connection

    async def close(self) -> None:
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
