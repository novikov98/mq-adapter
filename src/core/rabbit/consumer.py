import json
from datetime import datetime
from logging import getLogger
from typing import Callable, Awaitable

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

    def _extract_json(self, body: bytes) -> dict:
        """
        Достаёт JSON из тела сообщения 1С-шины.
        Бросает JSONDecodeError, если распарсить не удалось.
        """
        section = self._unwrap_amqp_body_section(body)
        if section is not None:
            try:
                return json.loads(section)
            except (UnicodeDecodeError, json.JSONDecodeError):
                logger.warning("Секция AMQP развернулась, но JSON не распарсился; ищем JSON по всему телу")

        return self._scan_json(body)

    @staticmethod
    def _unwrap_amqp_body_section(body: bytes) -> bytes | None:
        """
        Разворачивает body-секцию AMQP 1.0, в которую шина заворачивает JSON:
        дескриптор 00 53 <75 data|77 amqp-value>, затем binary/string с длиной.
        Отдаёт None, если тело не похоже на такую секцию.
        """
        if len(body) < 5 or body[0] != 0x00 or body[1] != 0x53 or body[2] not in (0x75, 0x77):
            return None

        constructor = body[3]
        if constructor in (0xA0, 0xA1):  # vbin8 / str8-utf8 — длина в 1 байте
            size, start = body[4], 5
        elif constructor in (0xB0, 0xB1):  # vbin32 / str32-utf8 — длина в 4 байтах
            if len(body) < 8:
                return None
            size, start = int.from_bytes(body[4:8], "big"), 8
        else:
            return None

        if start + size > len(body):
            logger.warning(f"Длина секции ({size}) больше остатка тела ({len(body) - start})")
            return None

        return body[start : start + size]

    @staticmethod
    def _scan_json(body: bytes) -> dict:
        """
        Фолбэк: ищет первый байт, с которого тело парсится как JSON-объект.
        """
        pos = 0
        while True:
            start = body.find(b"{", pos)  # начало JSON
            if start == -1:
                logger.error(f"не нашли JSON; тело: {body[:64]!r}")
                raise json.JSONDecodeError("JSON не найден в теле сообщения", body.decode("utf-8", "replace"), 0)
            try:
                obj, _ = _JSON_DECODER.raw_decode(body[start:].decode("utf-8", "replace"))
                return obj
            except json.JSONDecodeError:
                pos = start + 1  # скобку нашли, но это не начало JSON

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
