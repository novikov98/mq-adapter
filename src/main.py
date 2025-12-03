import asyncio
from logging import getLogger
from contextlib import asynccontextmanager

from src.config import config
from src.core.bitrix.client import BitrixClient
from src.consumers.vacation import vacation_consumer
from src.core.rabbit.client import RabbitClient

config.log_config.setup_logging()
logger = getLogger(__name__)


@asynccontextmanager
async def lifespan():
    """
    Асинхронный контекстный менеджер для управления жизненным циклом приложения:
    - Проверка запуска приложения
    - Проверка подключений
    - Корректное отключение
    """

    await BitrixClient.init()
    rabbit = RabbitClient(
        rabbit_url=config.rabbit_config.connect_url,
        bitrix_availability_guard=BitrixClient.bitrix_availability_guard,
        consumers=[vacation_consumer],
    )
    await rabbit.start()

    try:
        yield rabbit
    finally:

        await rabbit.close()
        await BitrixClient.shutdown()


async def main():
    """
    Точка входа приложения.
    """
    await asyncio.Event().wait()


if __name__ == "__main__":

    async def app():
        async with lifespan():
            await main()

    try:
        asyncio.run(app())
    except KeyboardInterrupt:
        pass
