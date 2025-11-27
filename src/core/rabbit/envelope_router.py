from typing import Callable, Awaitable

from src.core.rabbit.dto import Envelope


Handler = Callable[[Envelope], Awaitable[None]]


class EnvelopeRouter:
    """
    Роутер для пакетов из очередей
    """

    def __init__(self):
        self.handlers: dict[tuple[str, float | None], Handler] = {}

    def on(self, action: str, *, version: float | None = None) -> Callable[[Handler], Handler]:
        """
        Декоратор для регистрации обработчика по event_action.
        """
        key = (action, version)

        def decorator(func: Handler) -> Handler:
            self.handlers[key] = func
            return func

        return decorator

    async def dispatch(self, envelope: Envelope) -> bool:
        """
        Пытается найти и выполнить обработчик.
        Сначала ищем по (action, version), потом по (action, None) как fallback.
        :return: True если нашли обработчик, False если нет.
        """

        key = (envelope.event_action, envelope.event_version)
        handler = self.handlers.get(key)

        if handler is None:
            handler = self.handlers.get((envelope.event_action, None))

        if handler is None:
            return False

        await handler(envelope)
        return True
