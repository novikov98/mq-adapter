from logging.config import dictConfig
from pathlib import Path
from typing import ClassVar, List

from pydantic_settings import BaseSettings
from pydantic import BaseModel

from src.utils.log_filter import MaskTokenFilter


BASE_DIR = Path(__file__).parent.parent


class BaseConfig(BaseSettings):
    """
    Базовый класс для настроек приложения с общей конфигурацией
    """

    class Config:
        env_file: ClassVar[str] = str(BASE_DIR / ".env")
        env_file_encoding: ClassVar[str] = "utf-8"
        extra: ClassVar[str] = "allow"


class LoggConfig(BaseConfig):
    """
    Настройки логирования.
    """

    LOG_LEVEL: str

    @property
    def logging_config(self):
        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
                },
            },
            "filters": {
                "mask_token": {
                    "()": MaskTokenFilter,
                }
            },
            "handlers": {
                "default": {
                    "level": self.LOG_LEVEL,
                    "formatter": "default",
                    "class": "logging.StreamHandler",
                },
            },
            "root": {
                "level": self.LOG_LEVEL,
                "handlers": ["default"],
            },
            "loggers": {
                "bx_client": {"level": self.LOG_LEVEL},
                "uvicorn": {"level": self.LOG_LEVEL},
                "uvicorn.error": {"level": self.LOG_LEVEL},
                "uvicorn.access": {
                    "level": self.LOG_LEVEL,
                    "propagate": True,
                    "handlers": ["default"],
                },
                "httpx": {"level": "INFO", "filters": ["mask_token"]},
                "httpcore": {"level": "INFO", "filters": ["mask_token"]},
            },
        }

    def setup_logging(self):
        dictConfig(self.logging_config)


class BitrixClientConfig(BaseConfig):
    """
    Настройки клиента Б24.
    """

    BX_WEBHOOK: str
    BX_RESPECT_VELOCITY_POLICY: bool = True
    BX_REQUEST_POOL_SIZE: int
    BX_REQUEST_PER_SECOND: float
    BX_BATCH_SIZE: int = 50
    BX_OPERATING_TIME_LIMIT: int
    BX_MAX_REQUEST_RETRIES: int
    BX_WEBHOOK_OWNER_ID: int
    BX_REQUEST_SSL_CONTROL: bool = True
    BX_HEALTH_CHECK_BACKOFF_INITIAL_DELAY: int
    BX_HEALTH_CHECK_BACKOFF_MAX_DELAY: int
    BX_HEALTH_CHECK_BACKOFF_MULTIPLIER: int


class RabbitClientConfig(BaseConfig):
    RABBIT_HOST: str
    RABBIT_PORT: int
    RABBIT_USER: str
    RABBIT_PASSWORD: str
    RABBIT_VHOST: str = ""

    @property
    def connect_url(self):
        return f"amqp://{self.RABBIT_USER}:{self.RABBIT_PASSWORD}@{self.RABBIT_HOST}:{self.RABBIT_PORT}/{self.RABBIT_VHOST}"


class RedisConfig(BaseConfig):
    """
    Конфиг для Redis клиента.
    """

    REDIS_USE_SENTINEL: bool = False
    REDIS_SENTINELS: str = ""
    REDIS_MASTER_NAME: str = "mymaster"

    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0

    @property
    def sentinel_address(self) -> List[tuple[str, int]]:
        return [tuple(addr.split(":")) for addr in self.REDIS_SENTINELS.split(",")]


class Config(BaseModel):
    """
    Общий конфиг.
    """

    log_config: LoggConfig = LoggConfig()
    bx_config: BitrixClientConfig = BitrixClientConfig()
    rabbit_config: RabbitClientConfig = RabbitClientConfig()
    redis_config: RedisConfig = RedisConfig()


config = Config()
