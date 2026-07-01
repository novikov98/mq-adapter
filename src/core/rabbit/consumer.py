import json
from datetime import datetime
from logging import getLogger
from typing import Callable, Awaitable
from base64 import b64decode

import aio_pika
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustQueue
from pydantic import ValidationError

from src.core.bitrix.availabilit_guard import BitrixAvailabilityGuard
from src.core.bitrix.exceptions import BitrixTemporaryUnavailableError
from src.core.rabbit.connection_manager import RabbitConnectionManager
from src.core.rabbit.dto import QueueConfig, Envelope
from src.core.rabbit.envelope_router import EnvelopeRouter
from src.core.rabbit.exceptions import (
    EnvelopeHandlerNotRegisteredError,
    DuplicateEnvelopeError,
    EnvelopeProcessingError,
)
from src.core.rabbit.middleware import AbstractMiddleware

logger = getLogger(__name__)


DLQ_ERRORS = EnvelopeHandlerNotRegisteredError
MiddlewareCallable = Callable[[Envelope], Awaitable[None]]
_JSON_DECODER = json.JSONDecoder()


class QueueConsumer:
    """
    Консьюмер очереди.
    """

    def __init__(
        self,
        name: str,
        connection_manager: RabbitConnectionManager,
        queue_config: QueueConfig,
        router: EnvelopeRouter,
        bitrix_availability_guard: BitrixAvailabilityGuard,
        middlewares: list[AbstractMiddleware] | None = None,
    ) -> None:
        self.name = name
        self.connection_manager = connection_manager
        self.router: EnvelopeRouter = router
        self.queue_config = queue_config
        self.channel: aio_pika.RobustChannel | None = None
        self.queue: AbstractRobustQueue | None = None
        self.consumer_tag: str | None = None
        self.bitrix_availability_guard: BitrixAvailabilityGuard = bitrix_availability_guard
        self.middlewares: list[AbstractMiddleware] | None = middlewares

    async def start(self) -> None:
        """
        Старт консьюмера:
        - получает соединение и создаёт канал
        - объявляет exchange и очередь
        - биндит очередь к exchange
        - настраивает prefetch
        - подписывается на сообщения
        """

        connection = await self.connection_manager.get_connection()

        # Открываем канал
        self.channel = await connection.channel()
        await self.channel.set_qos(prefetch_count=self.queue_config.prefetch_count)

        # DLX
        dlx_exchange = await self.channel.declare_exchange(
            self.queue_config.dlx_name,
            aio_pika.ExchangeType.DIRECT,
            durable=True,
        )

        dlq = await self.channel.declare_queue(
            self.queue_config.dlq_name,
            durable=True,
        )

        await dlq.bind(
            dlx_exchange,
            routing_key=self.queue_config.dlx_routing_key or self.queue_config.queue_name,
        )

        # Объявляем exchange
        exchange = await self.channel.declare_exchange(
            self.queue_config.exchange_name,
            self.queue_config.exchange_type,
            durable=self.queue_config.durable,
        )

        # Аргументы для отработки некачественных сообщений
        args: dict[str, object] = {}
        if self.queue_config.dlx_name:
            args["x-dead-letter-exchange"] = self.queue_config.dlx_name
        if self.queue_config.dlx_routing_key:
            args["x-dead-letter-routing-key"] = self.queue_config.dlx_routing_key

        # Объявление очереди
        self.queue = await self.channel.declare_queue(
            self.queue_config.queue_name,
            durable=self.queue_config.durable,
            auto_delete=self.queue_config.auto_delete,
            arguments=args or None,
        )

        # Бинд очереди к exchange
        await self.queue.bind(exchange, routing_key=self.queue_config.binding_key)

        # Запуск консьюмера
        self.consumer_tag = await self.queue.consume(self._on_message)

        logger.info(
            f"Consumer {self.name} started. queue={self.queue_config.queue_name}, "
            f"exchange={self.queue_config.exchange_name}, routing_key={self.queue_config.binding_key}"
        )

    async def stop(self) -> None:
        """
        Остановка консьюмера:
        - отмена consume
        - закрытие канала
        """

        if self.queue and self.consumer_tag:
            await self.queue.cancel(self.consumer_tag)

        if self.channel and not self.channel.is_closed:
            await self.channel.close()

        logger.info(f"Consumer {self.name} stopped.")

    async def pause(self) -> None:
        """
        Останавливает чтение из очереди.
        """

        if self.queue and self.consumer_tag:
            await self.queue.cancel(self.consumer_tag)
            self.consumer_tag = None
            logger.warning(f"Consumer {self.name} paused.")

    async def resume(self) -> None:
        """
        Возобновляет чтение из очереди.
        """

        if self.queue and not self.consumer_tag:
            self.consumer_tag = await self.queue.consume(self._on_message)
            logger.info(f"Consumer {self.name} resumed.")

    async def _on_message(self, msg: AbstractIncomingMessage) -> None:
        """
        Callback для обработки сообщений из очереди.
        """

        async with msg.process(ignore_processed=True):  # ignore_processed=True Сами контролируем ack/nack/reject.
            try:
                source_system = msg.headers.get("source_system")
                logger.debug(source_system)
                if source_system == "1S":
                    data = self._extract_json(msg.body)
                else:
                    data = json.loads(msg.body)
                envelope = Envelope(**data)

                pipeline = self.build_pipeline()
                await pipeline(envelope)

                await msg.ack()
                logger.debug(f"Message handled successfully. message_id={envelope.message_id}")

            # Неверная структура пакета
            except (json.JSONDecodeError, ValidationError) as e:
                logger.error("Invalid message format; sending to DLQ")
                msg = self.add_dlq_headers(msg, e, "Invalid message format")
                await msg.reject(requeue=False)

            except DuplicateEnvelopeError as e:
                logger.error(f"{e.message}\n" f"message_id:{envelope.message_id} | event_type: {envelope.event_type}")
                msg = self.add_dlq_headers(msg, e, e.message)
                await msg.reject(requeue=False)

            # Не нашелся зарегистрированный обработчик для пакета
            except EnvelopeHandlerNotRegisteredError as e:
                logger.error(
                    f"No handler for action={envelope.event_action} version={envelope.event_version}; sending to DLQ\n"
                    f"message_id:{envelope.message_id} | event_type: {envelope.event_type}"
                )
                msg = self.add_dlq_headers(msg, e, e.message)
                await msg.reject(requeue=False)

            # Битрикс упал
            except BitrixTemporaryUnavailableError:
                logger.error(
                    f"Bitrix temporary unavailable. Requeue message and pause consumer.\n"
                    f"message_id:{envelope.message_id} | event_type: {envelope.event_type}"
                )

                await self.pause()
                await msg.reject(requeue=True)
                await self.bitrix_availability_guard.wait_until_up()
                await self.resume()

            except EnvelopeProcessingError as e:
                logger.error(
                    f"Error while processing message; message: {e.message}; requeue={e.requeue}.\n"
                    f"message_id:{envelope.message_id} | event_type: {envelope.event_type}"
                )
                await msg.reject(requeue=e.requeue)

            except Exception:
                logger.error(
                    f"Error while processing message; sending to DLQ\n"
                    f"message_id:{envelope.message_id} | event_type: {envelope.event_type}"
                )
                await msg.reject(requeue=False)

    def _extract_json(self, body):
        """
        Достаёт JSON из тела сообщения 1С-шины.
        Если JSON достать не получается, отдает body как есть.
        """
        try:
            frame = b64decode(body)
            logger.debug(f"frame: {frame}")
        except Exception:
            logger.error("Не удалось декодировать base64")
            return body

        pos = 0
        while True:
            start = frame.find(b"{", pos)  # начало JSON
            if start == -1:
                logger.debug("не нашли JSON")
                return body
            try:
                obj, _ = _JSON_DECODER.raw_decode(frame[start:].decode("utf-8"))
                logger.debug(obj)
                return obj
            except (UnicodeDecodeError, json.JSONDecodeError):
                pos = start + 1  # скобку нашли, но это не начато JSON

    def add_dlq_headers(
        self, msg: AbstractIncomingMessage, exception: Exception, message: str
    ) -> AbstractIncomingMessage:
        msg.headers["x-error-type"] = type(exception).__name__
        msg.headers["x-error-message"] = message
        msg.headers["x-consumer"] = self.name
        msg.headers["x-error-timestamp"] = datetime.now().isoformat()

        return msg

    def build_pipeline(self) -> MiddlewareCallable:
        next_callable = self.router.dispatch

        if self.middlewares:
            for middel_ware in reversed(self.middlewares):
                current_next = next_callable

                async def wrapped(envelope: Envelope, mw=middel_ware, call_next=current_next):
                    await mw.dispatch(envelope, call_next)

                next_callable = wrapped

        return next_callable
