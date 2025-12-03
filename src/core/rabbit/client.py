from logging import getLogger

from pydantic import BaseModel

from src.core.bitrix.availabilit_guard import BitrixAvailabilityGuard
from src.core.rabbit.connection_manager import RabbitConnectionManager
from src.core.rabbit.consumer import QueueConsumer
from src.core.rabbit.dto import QueueConfig
from src.core.rabbit.envelope_router import EnvelopeRouter


logger = getLogger(__name__)


class Consumer(BaseModel):
    name: str
    config: QueueConfig
    router: EnvelopeRouter


class RabbitClient:
    """
    Клиент RabbitMQ.
    Управляет подключением к Rabbit и консюмерами.
    """

    def __init__(self, rabbit_url: str, consumers: list[Consumer], bitrix_availability_guard: BitrixAvailabilityGuard):
        self.connection_manager = RabbitConnectionManager(rabbit_url)
        self.bitrix_availability_guard: BitrixAvailabilityGuard = bitrix_availability_guard

        self.consumers: list[QueueConsumer] = []
        for definition in consumers:
            consumer = QueueConsumer(
                name=definition.name,
                connection_manager=self.connection_manager,
                queue_config=definition.config,
                router=definition.router,
                bitrix_availability_guard=bitrix_availability_guard,
            )
            self.consumers.append(consumer)

    async def start(self):
        """
        Стартует всех консюмеров.
        """

        logger.info("Starting RabbitClient...")
        for consumer in self.consumers:
            await consumer.start()
        logger.info("RabbitClient started. All consumers active.")

    async def pause(self):
        """
        Ставит на паузу всех консюмеров.
        """
        logger.info("Pausing RabbitClient...")
        for consumer in self.consumers:
            await consumer.pause()
        logger.info("All consumers paused.")

    async def resume(self):
        """
        Снимает с паузы всех консюмеров.
        """
        logger.info("Resuming RabbitClient...")
        for consumer in self.consumers:
            await consumer.resume()
        logger.info("All consumers resumed.")

    async def stop(self):
        """
        Останавливает всех консюмеров.
        """
        logger.info("Stopping RabbitClient...")
        for consumer in self.consumers:
            await consumer.stop()
        logger.info("All consumers stopped.")

    async def close(self):
        """
        Останавливает всех консюмеров и закрывает соединение с Rabbit.
        """
        await self.stop()
        await self.connection_manager.close()
        logger.info("RabbitClient closed.")
