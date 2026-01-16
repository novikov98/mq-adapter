import json
import pytest
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4
from datetime import datetime

from src.core.rabbit.consumer import QueueConsumer
from src.core.rabbit.dto import Envelope, QueueConfig
from src.core.rabbit.exceptions import (
    EnvelopeHandlerNotRegisteredError,
    DuplicateEnvelopeError,
    EnvelopeProcessingError,
)
from src.core.bitrix.exceptions import BitrixTemporaryUnavailableError
from src.core.rabbit.middleware import AbstractMiddleware


class MockMiddleware(AbstractMiddleware):
    """Тестовый middleware для тестов."""

    def __init__(self):
        self.called = False
        self.envelope = None

    async def dispatch(self, envelope: Envelope, call_next):
        self.called = True
        self.envelope = envelope
        await call_next(envelope)


class TestQueueConsumer:
    """Тесты для QueueConsumer."""

    @pytest.fixture
    def mock_connection_manager(self):
        """Создает мок RabbitConnectionManager."""
        manager = MagicMock()
        manager.get_connection = AsyncMock()
        return manager

    @pytest.fixture
    def mock_router(self):
        """Создает мок EnvelopeRouter."""
        router = MagicMock()
        router.dispatch = AsyncMock()
        return router

    @pytest.fixture
    def queue_config(self):
        """Создает конфигурацию очереди."""
        return QueueConfig(
            queue_name="test.queue",
            exchange_name="test.exchange",
            binding_key="test.key",
            exchange_type="topic",
            durable=True,
            auto_delete=False,
            dlx_name="test.dlx",
            dlx_routing_key="test.dlx.key",
            prefetch_count=10,
        )

    @pytest.fixture
    def consumer(self, mock_connection_manager, mock_router, queue_config, mock_bitrix_availability_guard):
        """Создает экземпляр QueueConsumer."""
        return QueueConsumer(
            name="test_consumer",
            connection_manager=mock_connection_manager,
            queue_config=queue_config,
            router=mock_router,
            bitrix_availability_guard=mock_bitrix_availability_guard,
        )

    @pytest.fixture
    def valid_message_body(self):
        """Создает валидное тело сообщения."""
        envelope = Envelope(
            message_id=uuid4(),
            event_type="hr.vacation",
            event_action="vacation.create",
            event_version=1.0,
            source_system="1C-HR",
            destination_system="BITRIX",
            occurred_at=datetime.now(),
            sent_at=datetime.now(),
            payload={"test": "data"},
        )
        return json.dumps(envelope.model_dump(mode="json")).encode()

    @pytest.mark.asyncio
    async def test_start(self, consumer, mock_connection_manager, mock_channel, mock_queue, mock_exchange):
        """Тест запуска консьюмера."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_connection_manager.get_connection.return_value = mock_connection

        mock_channel.declare_exchange.return_value = mock_exchange
        mock_channel.declare_queue.return_value = mock_queue
        mock_queue.consume.return_value = "consumer_tag_123"

        await consumer.start()

        assert consumer.channel == mock_channel
        assert consumer.queue == mock_queue
        assert consumer.consumer_tag == "consumer_tag_123"
        mock_channel.set_qos.assert_called_once_with(prefetch_count=10)
        mock_channel.declare_exchange.assert_called_once()
        mock_channel.declare_queue.assert_called_once()
        mock_queue.bind.assert_called_once()
        mock_queue.consume.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop(self, consumer):
        """Тест остановки консьюмера."""
        consumer.queue = AsyncMock()
        consumer.consumer_tag = "tag_123"
        consumer.channel = AsyncMock()
        consumer.channel.is_closed = False

        await consumer.stop()

        consumer.queue.cancel.assert_called_once_with("tag_123")
        consumer.channel.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_when_no_queue(self, consumer):
        """Тест остановки когда очередь не создана."""
        consumer.queue = None
        consumer.channel = None

        await consumer.stop()
        # Не должно быть ошибок

    @pytest.mark.asyncio
    async def test_pause(self, consumer):
        """Тест паузы консьюмера."""
        consumer.queue = AsyncMock()
        consumer.consumer_tag = "tag_123"

        await consumer.pause()

        consumer.queue.cancel.assert_called_once_with("tag_123")
        assert consumer.consumer_tag is None

    @pytest.mark.asyncio
    async def test_resume(self, consumer):
        """Тест возобновления консьюмера."""
        consumer.queue = AsyncMock()
        consumer.consumer_tag = None
        consumer.queue.consume.return_value = "new_tag_123"

        await consumer.resume()

        assert consumer.consumer_tag == "new_tag_123"
        consumer.queue.consume.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_message_success(self, consumer, mock_router, valid_message_body, mock_message):
        """Тест успешной обработки сообщения."""
        mock_message.body = valid_message_body
        # Убеждаемся что middlewares = None, чтобы pipeline напрямую вызывал router.dispatch
        consumer.middlewares = None

        await consumer._on_message(mock_message)

        mock_router.dispatch.assert_called_once()
        mock_message.ack.assert_called_once()
        mock_message.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_message_invalid_json(self, consumer, mock_message):
        """Тест обработки сообщения с невалидным JSON."""
        mock_message.body = b"invalid json"

        await consumer._on_message(mock_message)

        mock_message.reject.assert_called_once_with(requeue=False)
        mock_message.ack.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_message_validation_error(self, consumer, mock_message):
        """Тест обработки сообщения с ошибкой валидации."""
        mock_message.body = b'{"invalid": "data"}'

        await consumer._on_message(mock_message)

        mock_message.reject.assert_called_once_with(requeue=False)
        mock_message.ack.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_message_handler_not_registered(self, consumer, mock_router, valid_message_body, mock_message):
        """Тест обработки когда обработчик не зарегистрирован."""
        mock_message.body = valid_message_body
        consumer.middlewares = None
        mock_router.dispatch.side_effect = EnvelopeHandlerNotRegisteredError()

        await consumer._on_message(mock_message)

        mock_message.reject.assert_called_once_with(requeue=False)
        mock_message.ack.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_message_duplicate_envelope(self, consumer, mock_router, valid_message_body, mock_message):
        """Тест обработки дубликата сообщения."""
        mock_message.body = valid_message_body
        consumer.middlewares = None
        mock_router.dispatch.side_effect = DuplicateEnvelopeError("Duplicate")

        await consumer._on_message(mock_message)

        mock_message.reject.assert_called_once_with(requeue=False)
        mock_message.ack.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_message_bitrix_unavailable(
        self, consumer, mock_router, valid_message_body, mock_message, mock_bitrix_availability_guard
    ):
        """Тест обработки когда Bitrix недоступен."""
        mock_message.body = valid_message_body
        consumer.middlewares = None
        mock_router.dispatch.side_effect = BitrixTemporaryUnavailableError()

        consumer.pause = AsyncMock()
        consumer.resume = AsyncMock()

        await consumer._on_message(mock_message)

        mock_message.reject.assert_called_once_with(requeue=True)
        consumer.pause.assert_called_once()
        mock_bitrix_availability_guard.wait_until_up.assert_called_once()
        consumer.resume.assert_called_once()
        mock_message.ack.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_message_processing_error_requeue(self, consumer, mock_router, valid_message_body, mock_message):
        """Тест обработки ошибки с requeue=True."""
        mock_message.body = valid_message_body
        consumer.middlewares = None
        mock_router.dispatch.side_effect = EnvelopeProcessingError("Error", requeue=True)

        await consumer._on_message(mock_message)

        mock_message.reject.assert_called_once_with(requeue=True)
        mock_message.ack.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_message_processing_error_no_requeue(
        self, consumer, mock_router, valid_message_body, mock_message
    ):
        """Тест обработки ошибки с requeue=False."""
        mock_message.body = valid_message_body
        consumer.middlewares = None
        mock_router.dispatch.side_effect = EnvelopeProcessingError("Error", requeue=False)

        await consumer._on_message(mock_message)

        mock_message.reject.assert_called_once_with(requeue=False)
        mock_message.ack.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_message_generic_exception(self, consumer, mock_router, valid_message_body, mock_message):
        """Тест обработки общего исключения."""
        mock_message.body = valid_message_body
        consumer.middlewares = None
        mock_router.dispatch.side_effect = Exception("Unexpected error")

        await consumer._on_message(mock_message)

        mock_message.reject.assert_called_once_with(requeue=False)
        mock_message.ack.assert_not_called()

    def test_add_dlq_headers(self, consumer, mock_message):
        """Тест добавления заголовков для DLQ."""
        exception = EnvelopeHandlerNotRegisteredError("Handler not found")
        result = consumer.add_dlq_headers(mock_message, exception, "Handler not found")

        assert result == mock_message
        assert mock_message.headers["x-error-type"] == "EnvelopeHandlerNotRegisteredError"
        assert mock_message.headers["x-error-message"] == "Handler not found"
        assert mock_message.headers["x-consumer"] == "test_consumer"
        assert "x-error-timestamp" in mock_message.headers

    @pytest.mark.asyncio
    async def test_build_pipeline_without_middlewares(self, consumer, mock_router):
        """Тест построения pipeline без middleware."""
        consumer.middlewares = None
        pipeline = consumer.build_pipeline()

        envelope = Envelope(
            message_id=uuid4(),
            event_type="test",
            event_action="test.action",
            event_version=1.0,
            source_system="1C",
            destination_system="BITRIX",
            occurred_at=datetime.now(),
            sent_at=datetime.now(),
            payload={},
        )

        await pipeline(envelope)

        mock_router.dispatch.assert_called_once_with(envelope)

    @pytest.mark.asyncio
    async def test_build_pipeline_with_middlewares(self, consumer, mock_router):
        """Тест построения pipeline с middleware."""
        middleware1 = MockMiddleware()
        middleware2 = MockMiddleware()
        consumer.middlewares = [middleware1, middleware2]

        pipeline = consumer.build_pipeline()

        envelope = Envelope(
            message_id=uuid4(),
            event_type="test",
            event_action="test.action",
            event_version=1.0,
            source_system="1C",
            destination_system="BITRIX",
            occurred_at=datetime.now(),
            sent_at=datetime.now(),
            payload={},
        )

        await pipeline(envelope)

        assert middleware1.called
        assert middleware2.called
        assert middleware1.envelope == envelope
        assert middleware2.envelope == envelope
        mock_router.dispatch.assert_called_once_with(envelope)

    @pytest.mark.asyncio
    async def test_start_with_dlx_config(
        self, consumer, mock_connection_manager, mock_channel, mock_queue, mock_exchange
    ):
        """Тест запуска с конфигурацией DLX."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_connection_manager.get_connection.return_value = mock_connection

        mock_channel.declare_exchange.return_value = mock_exchange
        mock_channel.declare_queue.return_value = mock_queue
        mock_queue.consume.return_value = "consumer_tag_123"

        await consumer.start()

        # Проверяем что declare_queue был вызван с правильными аргументами
        call_args = mock_channel.declare_queue.call_args
        assert call_args is not None
        assert "arguments" in call_args.kwargs or len(call_args.args) > 1
