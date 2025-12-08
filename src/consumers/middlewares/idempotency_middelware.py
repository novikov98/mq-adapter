from logging import getLogger
from typing import Callable

from redis.asyncio import Redis

from src.config import config
from src.core.rabbit.dto import Envelope
from src.core.rabbit.exceptions import DuplicateEnvelopeError
from src.core.rabbit.middleware import AbstractMiddleware


logger = getLogger(__name__)


class IdempotencyMiddleware(AbstractMiddleware):
    """
    Middleware обеспечивающий идемпотентность обработки сообщений из очередей.
    """

    def __init__(self, redis: Redis):
        self.redis = redis
        self.processed_key: str = "processed_"
        self.processing_ttl = config.redis_config.REDIS_ENVELOPE_PROCESSING_TTL
        self.processed_ttl = config.redis_config.REDIS_ENVELOPE_PROCESSED_TTL

    async def dispatch(self, envelope: Envelope, call_next: Callable):
        """
        Проверяет наличее message_id в redis, если не находит, то записывает.
        """

        message_id = str(envelope.message_id)
        key = f"{self.processed_key}{message_id}"

        # Пытаемся взять лок
        logger.debug(f"trying to acquire lock for message_id: {message_id}")
        locked = await self.redis.set(key, "process", ex=self.processing_ttl, nx=True)
        if not locked:
            # Ключ уже есть: сообщение либо обрабатывается, либо уже обработано.
            # Считаем это дублем и выходим, бизнес-логику не запускаем.
            logger.warning(f"duplicate envelope, lock already exists. message_id: {message_id}")
            raise DuplicateEnvelopeError(message="Duplicate message_id")

        logger.debug(f"lock acquired for message_id: {message_id}")
        # Передаем сообщение дальше
        try:
            await call_next(envelope)
        except Exception:
            # Ошибка: снимаем лок, чтобы сообщение могли обработать повторно
            # после reject(requeue=True) или переотправки из DLQ.
            logger.exception(f"error during processing message_id: {message_id}, removing lock")
            await self.redis.delete(key)
            raise
        else:
            # Успешная обработка: обновляем TTL,
            # чтобы дубликаты этого message_id игнорировались ещё ttl секунд.
            await self.redis.expire(key, self.processed_ttl)
            logger.debug(
                f"message_id: {message_id} processed successfully, "
                f"ttl updated to processed_ttl={self.processed_ttl}"
            )
