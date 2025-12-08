from logging import getLogger

from redis.asyncio import Redis, Sentinel, RedisError

from src.config import config


logger = getLogger(__name__)


class RedisClient:
    redis_client: Redis | None = None

    @classmethod
    async def init_redis(cls):
        if config.redis_config.REDIS_USE_SENTINEL:
            sentinel = Sentinel(
                config.redis_config.sentinel_address,
                socket_timeout=1.0,
                decode_responses=True,
            )
            cls.redis_client = sentinel.master_for(
                service_name=config.redis_config.REDIS_MASTER_NAME,
                db=config.redis_config.REDIS_DB,
                decode_responses=True,
            )
            logger.info("Connecting to Redis via Sentinel...")

        else:
            cls.redis_client = Redis(
                host=config.redis_config.REDIS_HOST,
                port=config.redis_config.REDIS_PORT,
                db=config.redis_config.REDIS_DB,
                decode_responses=True,
            )
            logger.info("Connecting to single Redis Redis instance...")
        try:
            pong = await cls.redis_client.ping()
            if pong:
                logger.info("Redis connection established successfully")
            else:
                logger.error("Redis ping failed (no response)")
        except RedisError as e:
            logger.exception(f"Redis connection error: {e}")
            raise

    @classmethod
    async def close_redis(cls):
        if cls.redis_client:
            await cls.redis_client.close()
            logger.info("Redis connection closed")

    @classmethod
    def get_redis(cls):
        return cls.redis_client
