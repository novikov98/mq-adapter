import json
from datetime import datetime
from logging import getLogger

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from src.core.bitrix.availabilit_guard import BitrixAvailabilityGuard
from src.core.bitrix.exceptions import BitrixTemporaryUnavailableError
from src.core.rabbit.connection_manager import RabbitConnectionManager
from src.core.rabbit.dto import QueueConfig, Envelope
from src.core.rabbit.envelope_router import EnvelopeRouter
from src.core.rabbit.exceptions import EnvelopeHandlerNotRegisteredError, RabbitException

logger = getLogger(__name__)


DLQ_ERRORS = EnvelopeHandlerNotRegisteredError


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
    ) -> None:
        self.name = name
        self.connection_manager = connection_manager
        self.router: EnvelopeRouter = router
        self.queue_config = queue_config
        self.channel: aio_pika.RobustChannel | None = None
        self.queue: aio_pika.RobustQueue | None = None
        self.consumer_tag: str | None = None
        self.bitrix_availability_guard: BitrixAvailabilityGuard = bitrix_availability_guard

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
        await self.queue.bind(exchange, routing_key=self.queue_config.routing_key)

        # Запуск консьюмера
        self.consumer_tag = await self.queue.consume(self._on_message)

        logger.info(
            f"Consumer {self.name} started. queue={self.queue_config.queue_name}, "
            f"exchange={self.queue_config.exchange_name}, routing_key={self.queue_config.routing_key}"
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
                data = json.loads(msg.body)
                envelope = Envelope(**data)

                await self.router.dispatch(envelope)

                await msg.ack()
                logger.debug(f"Message handled successfully. message_id={envelope.message_id}")

            except BitrixTemporaryUnavailableError:
                logger.warning("Bitrix temporary unavailable. Requeue message and pause consumer.")
                await msg.reject(requeue=True)

                await self.pause()
                await self.bitrix_availability_guard.wait_until_up()
                await self.resume()

            except EnvelopeHandlerNotRegisteredError as e:
                logger.error(
                    f"No handler for action={envelope.event_action} version={envelope.event_version}; sending to DLQ"
                )
                msg = self.add_dlq_headers(msg, e)
                await msg.reject(requeue=False)
                return

            except Exception:
                logger.exception("Error while processing message; sending to DLQ")
                await msg.reject(requeue=False)

    def add_dlq_headers(self, msg: AbstractIncomingMessage, exception: RabbitException) -> AbstractIncomingMessage:
        msg.headers["x-error-type"] = type(exception).__name__
        msg.headers["x-error-message"] = str(exception.message)
        msg.headers["x-consumer"] = self.name
        msg.headers["x-error-timestamp"] = datetime.now().isoformat()

        return msg
