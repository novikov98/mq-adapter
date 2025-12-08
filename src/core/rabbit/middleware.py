from abc import ABC, abstractmethod
from typing import Callable

from src.core.rabbit.dto import Envelope


class AbstractMiddleware(ABC):
    @abstractmethod
    async def dispatch(self, envelope: Envelope, call_next: Callable):
        pass
