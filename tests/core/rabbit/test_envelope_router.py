import pytest
from unittest.mock import AsyncMock

from src.core.rabbit.envelope_router import EnvelopeRouter
from src.core.rabbit.dto import Envelope
from src.core.rabbit.exceptions import EnvelopeHandlerNotRegisteredError
from uuid import uuid4
from datetime import datetime


class TestEnvelopeRouter:
    """Тесты для EnvelopeRouter."""

    @pytest.fixture
    def router(self):
        """Создает экземпляр EnvelopeRouter."""
        return EnvelopeRouter()

    @pytest.fixture
    def sample_envelope(self):
        """Создает тестовый Envelope."""
        return Envelope(
            message_id=uuid4(),
            event_type="hr.vacation",
            event_action="vacation.create",
            event_version=1.0,
            source_system="1C-HR",
            destination_system="BITRIX",
            occurred_at=datetime.now(),
            sent_at=datetime.now(),
            payload={},
        )

    def test_init(self, router):
        """Тест инициализации роутера."""
        assert router.handlers == {}

    def test_on_decorator_registers_handler(self, router):
        """Тест регистрации обработчика через декоратор."""
        handler = AsyncMock()

        @router.on("test.action", version=1.0)
        async def test_handler(envelope: Envelope):
            await handler(envelope)

        assert ("test.action", 1.0) in router.handlers
        assert router.handlers[("test.action", 1.0)] == test_handler

    def test_on_decorator_without_version(self, router):
        """Тест регистрации обработчика без версии."""
        handler = AsyncMock()

        @router.on("test.action")
        async def test_handler(envelope: Envelope):
            await handler(envelope)

        assert ("test.action", None) in router.handlers
        assert router.handlers[("test.action", None)] == test_handler

    @pytest.mark.asyncio
    async def test_dispatch_finds_handler_by_version(self, router, sample_envelope):
        """Тест поиска обработчика по версии."""
        handler = AsyncMock()

        @router.on("vacation.create", version=1.0)
        async def test_handler(envelope: Envelope):
            await handler(envelope)

        await router.dispatch(sample_envelope)

        handler.assert_called_once_with(sample_envelope)

    @pytest.mark.asyncio
    async def test_dispatch_finds_fallback_handler(self, router, sample_envelope):
        """Тест поиска обработчика без версии как fallback."""
        handler = AsyncMock()

        @router.on("vacation.create")
        async def test_handler(envelope: Envelope):
            await handler(envelope)

        await router.dispatch(sample_envelope)

        handler.assert_called_once_with(sample_envelope)

    @pytest.mark.asyncio
    async def test_dispatch_prefers_versioned_handler(self, router, sample_envelope):
        """Тест приоритета версионированного обработчика."""
        versioned_handler = AsyncMock()
        fallback_handler = AsyncMock()

        @router.on("vacation.create", version=1.0)
        async def versioned(envelope: Envelope):
            await versioned_handler(envelope)

        @router.on("vacation.create")
        async def fallback(envelope: Envelope):
            await fallback_handler(envelope)

        await router.dispatch(sample_envelope)

        versioned_handler.assert_called_once_with(sample_envelope)
        fallback_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_dispatch_raises_error_when_no_handler(self, router, sample_envelope):
        """Тест ошибки когда обработчик не найден."""
        with pytest.raises(EnvelopeHandlerNotRegisteredError):
            await router.dispatch(sample_envelope)

    @pytest.mark.asyncio
    async def test_dispatch_with_different_action(self, router, sample_envelope):
        """Тест диспатча с другим action."""
        handler = AsyncMock()

        @router.on("other.action")
        async def test_handler(envelope: Envelope):
            await handler(envelope)

        with pytest.raises(EnvelopeHandlerNotRegisteredError):
            await router.dispatch(sample_envelope)

        handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_dispatch_with_different_version(self, router, sample_envelope):
        """Тест диспатча с другой версией."""
        handler = AsyncMock()

        @router.on("vacation.create", version=2.0)
        async def test_handler(envelope: Envelope):
            await handler(envelope)

        # Должен использовать fallback, но его нет
        with pytest.raises(EnvelopeHandlerNotRegisteredError):
            await router.dispatch(sample_envelope)

        handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_handlers_different_actions(self, router):
        """Тест множественных обработчиков для разных действий."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()

        @router.on("action1", version=1.0)
        async def h1(envelope: Envelope):
            await handler1(envelope)

        @router.on("action2", version=1.0)
        async def h2(envelope: Envelope):
            await handler2(envelope)

        envelope1 = Envelope(
            message_id=uuid4(),
            event_type="test",
            event_action="action1",
            event_version=1.0,
            source_system="1C",
            destination_system="BITRIX",
            occurred_at=datetime.now(),
            sent_at=datetime.now(),
            payload={},
        )

        envelope2 = Envelope(
            message_id=uuid4(),
            event_type="test",
            event_action="action2",
            event_version=1.0,
            source_system="1C",
            destination_system="BITRIX",
            occurred_at=datetime.now(),
            sent_at=datetime.now(),
            payload={},
        )

        await router.dispatch(envelope1)
        await router.dispatch(envelope2)

        handler1.assert_called_once_with(envelope1)
        handler2.assert_called_once_with(envelope2)
