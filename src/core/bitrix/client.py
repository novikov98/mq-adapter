from logging import getLogger

import httpx
from bx_client import BitrixAsync

from src.config import config
from src.core.bitrix.availabilit_guard import BitrixAvailabilityGuard


logger = getLogger(__name__)


class BitrixClient:
    """
    Клиент для работы с Bitrix API.
    """

    bx: BitrixAsync | None
    bitrix_availability_guard: BitrixAvailabilityGuard | None

    @classmethod
    async def init(cls, http_session: httpx.AsyncClient):
        cls.bx = BitrixAsync(
            webhook=config.bx_config.BX_WEBHOOK,
            batch_size=config.bx_config.BX_BATCH_SIZE,
            max_request_retries=config.bx_config.BX_MAX_REQUEST_RETRIES,
            ssl=config.bx_config.BX_REQUEST_SSL_CONTROL,
            http_client=http_session,
        )
        cls.bitrix_availability_guard = BitrixAvailabilityGuard(cls.bx)

    @classmethod
    async def shutdown(cls):
        cls.bx = None
        cls.bitrix_availability_guard = None
