from src.core.rabbit.dto import Envelope


class BaseEnvelopeService:
    async def handle(self, envelope: Envelope) -> None:
        raise NotImplementedError


class EnvelopeRouter:
    def __init__(self):
        self.handlers: dict[str, BaseEnvelopeService] = {}

    def register(self, event_type: str, handler: BaseEnvelopeService) -> None:
        self.handlers[event_type] = handler

    async def route(self, envelope: Envelope) -> bool:
        """
        :return: True, если обработчик найден и отработал без исключения,
                 False, если обработчика нет.
        """
        handler = self.handlers.get(envelope.event_type)
        if not handler:
            return False

        await handler.handle(envelope)
        return True
