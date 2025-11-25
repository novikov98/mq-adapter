from bx_client.bitrix import BitrixAsync
from bx_client.dto.requests import BitrixRequest

from src.config import config


class BitrixClient:
    bx: BitrixAsync | None

    @classmethod
    async def init(cls):
        cls.bx = BitrixAsync(
            webhook=config.bx_config.BX_WEBHOOK,
            respect_velocity_policy=config.bx_config.BX_RESPECT_VELOCITY_POLICY,
            request_pool_size=config.bx_config.BX_REQUEST_POOL_SIZE,
            requests_per_second=config.bx_config.BX_REQUESTS_PER_SECOND,
            batch_size=config.bx_config.BX_BATCH_SIZE,
            operating_time_limit=config.bx_config.BX_OPERATING_TIME_LIMIT,
            max_request_retries=config.bx_config.BX_MAX_REQUEST_RETRIES,
            ssl=True,
        )

    @classmethod
    async def down(cls):
        cls.bx = None

    @classmethod
    async def health_chek(cls) -> bool:
        query = BitrixRequest(method="user.current", params=None)
        return await cls.bx.call_method(query)
