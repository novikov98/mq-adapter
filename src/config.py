from logging.config import dictConfig
from pathlib import Path
from typing import ClassVar

from pydantic_settings import BaseSettings
from pydantic import BaseModel

from utils.log_filter import MaskTokenFilter


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


class Config(BaseModel):
    """
    Общий конфиг.
    """

    log_config: LoggConfig = LoggConfig()
    bx_config: BitrixClientConfig = BitrixClientConfig()


config = Config()
