import pytest
from unittest.mock import AsyncMock

from src.core.rabbit.client import RabbitClient, Consumer
from src.core.rabbit.dto import QueueConfig
from src.core.rabbit.envelope_router import EnvelopeRouter
from src.core.rabbit.middleware import AbstractMiddleware


class MockMiddleware(AbstractMiddleware):
    """Тестовый middleware."""

    async def dispatch(self, envelope, call_next):
        await call_next(envelope)


class TestConsumer:
    """Тесты для Consumer dataclass."""

    def test_consumer_init(self):
        """Тест инициализации Consumer."""
        config = QueueConfig(
            queue_name="test.queue",
            exchange_name="test.exchange",
            binding_key="test.key",
            dlx_name="test.dlx",
            dlx_routing_key="test.dlx.key",
            prefetch_count=10,
        )
        router = EnvelopeRouter()

        consumer = Consumer(
            name="test_consumer",
            config=config,
            router=router,
        )

        assert consumer.name == "test_consumer"
        assert consumer.config == config
        assert consumer.router == router
        assert consumer.middlewares == []

    def test_consumer_with_middlewares(self):
        """Тест Consumer с middleware."""
        config = QueueConfig(
            queue_name="test.queue",
            exchange_name="test.exchange",
            binding_key="test.key",
            dlx_name="test.dlx",
            dlx_routing_key="test.dlx.key",
            prefetch_count=10,
        )
        router = EnvelopeRouter()
        middleware = MockMiddleware()

        consumer = Consumer(
            name="test_consumer",
            config=config,
            router=router,
            middlewares=[middleware],
        )

        assert len(consumer.middlewares) == 1
        assert consumer.middlewares[0] == middleware


class TestRabbitClient:
    """Тесты для RabbitClient."""

    @pytest.fixture
    def queue_config(self):
        """Создает конфигурацию очереди."""
        return QueueConfig(
            queue_name="test.queue",
            exchange_name="test.exchange",
            binding_key="test.key",
            dlx_name="test.dlx",
            dlx_routing_key="test.dlx.key",
            prefetch_count=10,
        )

    @pytest.fixture
    def consumer_definitions(self, queue_config):
        """Создает определения консьюмеров."""
        router1 = EnvelopeRouter()
        router2 = EnvelopeRouter()

        return [
            Consumer(name="consumer1", config=queue_config, router=router1),
            Consumer(name="consumer2", config=queue_config, router=router2),
        ]

    @pytest.fixture
    def client(self, consumer_definitions, mock_bitrix_availability_guard):
        """Создает экземпляр RabbitClient."""
        return RabbitClient(
            rabbit_url="amqp://localhost:5672/",
            consumers=consumer_definitions,
            bitrix_availability_guard=mock_bitrix_availability_guard,
        )

    def test_init(self, client, consumer_definitions):
        """Тест инициализации клиента."""
        assert client.connection_manager is not None
        assert len(client.consumers) == 2
        assert all(consumer.name in ["consumer1", "consumer2"] for consumer in client.consumers)

    def test_init_with_global_middlewares(self, consumer_definitions, mock_bitrix_availability_guard):
        """Тест инициализации с глобальными middleware."""
        middleware = MockMiddleware()
        client = RabbitClient(
            rabbit_url="amqp://localhost:5672/",
            consumers=consumer_definitions,
            bitrix_availability_guard=mock_bitrix_availability_guard,
            middlewares=[middleware],
        )

        assert len(client.consumers) == 2
        # Проверяем что middleware добавлены к каждому консьюмеру
        for consumer in client.consumers:
            assert middleware in consumer.middlewares

    def test_init_with_consumer_specific_middlewares(self, queue_config, mock_bitrix_availability_guard):
        """Тест инициализации с middleware для конкретного консьюмера."""
        router = EnvelopeRouter()
        consumer_middleware = MockMiddleware()
        global_middleware = MockMiddleware()

        consumer_def = Consumer(
            name="consumer1",
            config=queue_config,
            router=router,
            middlewares=[consumer_middleware],
        )

        client = RabbitClient(
            rabbit_url="amqp://localhost:5672/",
            consumers=[consumer_def],
            bitrix_availability_guard=mock_bitrix_availability_guard,
            middlewares=[global_middleware],
        )

        assert len(client.consumers) == 1
        # Проверяем что оба middleware присутствуют
        consumer = client.consumers[0]
        assert global_middleware in consumer.middlewares
        assert consumer_middleware in consumer.middlewares

    @pytest.mark.asyncio
    async def test_start(self, client):
        """Тест запуска всех консьюмеров."""
        for consumer in client.consumers:
            consumer.start = AsyncMock()

        await client.start()

        for consumer in client.consumers:
            consumer.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_pause(self, client):
        """Тест паузы всех консьюмеров."""
        for consumer in client.consumers:
            consumer.pause = AsyncMock()

        await client.pause()

        for consumer in client.consumers:
            consumer.pause.assert_called_once()

    @pytest.mark.asyncio
    async def test_resume(self, client):
        """Тест возобновления всех консьюмеров."""
        for consumer in client.consumers:
            consumer.resume = AsyncMock()

        await client.resume()

        for consumer in client.consumers:
            consumer.resume.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop(self, client):
        """Тест остановки всех консьюмеров."""
        for consumer in client.consumers:
            consumer.stop = AsyncMock()

        await client.stop()

        for consumer in client.consumers:
            consumer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_close(self, client):
        """Тест закрытия клиента."""
        for consumer in client.consumers:
            consumer.stop = AsyncMock()
        client.connection_manager.close = AsyncMock()

        await client.close()

        for consumer in client.consumers:
            consumer.stop.assert_called_once()
        client.connection_manager.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_stops_consumers_first(self, client):
        """Тест что close сначала останавливает консьюмеры, потом закрывает соединение."""
        call_order = []

        async def mock_stop():
            call_order.append("stop")

        async def mock_close():
            call_order.append("close")

        for consumer in client.consumers:
            consumer.stop = AsyncMock(side_effect=mock_stop)
        client.connection_manager.close = AsyncMock(side_effect=mock_close)

        await client.close()

        # Проверяем порядок вызовов
        assert call_order.count("stop") == 2
        assert call_order[-1] == "close"
