from logging.config import dictConfig
from pathlib import Path
from typing import ClassVar

from pydantic_settings import BaseSettings
from pydantic import BaseModel


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
            },
        }

    def setup_logging(self):
        dictConfig(self.logging_config)


class Config(BaseModel):
    """
    Общий конфиг.
    """

    log_config: LoggConfig = LoggConfig()


config = Config()
