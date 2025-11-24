import asyncio
from logging import getLogger
from contextlib import asynccontextmanager

from config import config


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

    # Перед запуском

    yield

    # Перед отключением


@lifespan
async def main():
    """
    Точка входа приложения.
    """

    ...


if __name__ == "__main__":
    asyncio.run(main())
