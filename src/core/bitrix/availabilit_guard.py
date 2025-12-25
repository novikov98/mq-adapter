import asyncio
from logging import getLogger

from bx_client.bitrix import BitrixAsync
from bx_client.dto.requests import BitrixRequest

from src.config import config
from src.core.bitrix.exceptions import BitrixTemporaryUnavailableError

logger = getLogger(__name__)


class BitrixAvailabilityGuard:
    """
    Управляет флагом доступности Bitrix.
    """

    def __init__(self, bx: BitrixAsync):
        self.bx: BitrixAsync = bx
        self.is_alive: bool = True
        self.lock: asyncio.Lock = asyncio.Lock()
        self.event = asyncio.Event()
        self.event.set()
        self.health_check_task = None

        # backoff параметры
        self.initial_delay = config.bx_config.BX_HEALTH_CHECK_BACKOFF_INITIAL_DELAY
        self.max_delay = config.bx_config.BX_HEALTH_CHECK_BACKOFF_MAX_DELAY
        self.backoff_multiplier = config.bx_config.BX_HEALTH_CHECK_BACKOFF_MULTIPLIER

    async def mark_down(self):
        async with self.lock:
            if self.is_alive:
                self.is_alive = False
                self.event.clear()
                self.health_check_task = asyncio.create_task(self._check_loop())

    async def health_chek(self):
        try:
            logger.info("Trying to Bitrix health chek")
            query = BitrixRequest(method="user.current", params=None)
            res = await self.bx.call_method(query)
            if int(res.result.get("ID")) == config.bx_config.BX_WEBHOOK_OWNER_ID:
                logger.info("Bitrix is alive")
                return
            else:
                logger.fatal("Bitrix is not alive")
                raise BitrixTemporaryUnavailableError
        # TODO: Подумать получше. Не нравится что принимаем как падение битрикса любую ошибку
        except Exception as e:
            logger.fatal(e)
            if self.is_alive:
                await self.mark_down()
            raise BitrixTemporaryUnavailableError

    async def wait_until_up(self):
        """
        Блокирует выполнение, пока Битрикс снова не будет доступен.
        """
        await self.event.wait()

    async def _check_loop(self):
        """
        Запускается 1 раз при падении Bitrix.
        Периодически вызывает health_check() с экспоненциальным backoff.
        """
        delay = self.initial_delay

        while True:
            try:
                await self.health_chek()
            except Exception:
                logger.warning(f"Bitrix still DOWN. Next check in {delay} seconds ({delay / 60}.1f minutes).")
                await asyncio.sleep(delay)
                delay = min(delay * self.backoff_multiplier, self.max_delay)
                continue

            # Битрикс ожил
            logger.info("Bitrix is ALIVE.")
            async with self.lock:
                self.is_alive = True
                self.event.set()
                self.health_check_task = None
            break
