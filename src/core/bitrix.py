from logging import getLogger

from bx_client.bitrix import BitrixAsync
from bx_client.dto.requests import BitrixRequest

from src.config import config


logger = getLogger(__name__)


class BitrixClient:
    bx: BitrixAsync | None

    @classmethod
    async def init(cls):
        cls.bx = BitrixAsync(
            webhook=config.bx_config.BX_WEBHOOK,
            respect_velocity_policy=config.bx_config.BX_RESPECT_VELOCITY_POLICY,
            request_pool_size=config.bx_config.BX_REQUEST_POOL_SIZE,
            requests_per_second=config.bx_config.BX_REQUEST_PER_SECOND,
            batch_size=config.bx_config.BX_BATCH_SIZE,
            operating_time_limit=config.bx_config.BX_OPERATING_TIME_LIMIT,
            max_request_retries=config.bx_config.BX_MAX_REQUEST_RETRIES,
            ssl=config.bx_config.BX_REQUEST_SSL_CONTROL,
        )

    @classmethod
    async def down(cls):
        cls.bx = None

    @classmethod
    async def health_chek(cls) -> bool:
        try:
            logger.info("Trying to Bitrix health chek")
            query = BitrixRequest(method="user.current", params=None)
            res = await cls.bx.call_method(query)
            if int(res.result.get("ID")) == config.bx_config.BX_WEBHOOK_OWNER_ID:
                logger.info("Bitrix is alive")
                return True
            else:
                logger.fatal("Bitrix is not alive")
                return False
        except Exception as e:
            logger.fatal(e)
            return False
