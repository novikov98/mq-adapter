import asyncio
from logging import getLogger
from contextlib import asynccontextmanager

from config import config
from core.bitrix import BitrixClient


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

    yield

    # Перед отключением


@lifespan
async def main():
    """
    Точка входа приложения.
    """

    foo = await BitrixClient.health_chek()
    print(foo)


if __name__ == "__main__":
    asyncio.run(main())
