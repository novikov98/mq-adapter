import pytest
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4
from datetime import datetime

from src.core.rabbit.dto import Envelope, QueueConfig
from src.core.bitrix.availabilit_guard import BitrixAvailabilityGuard


@pytest.fixture
def sample_envelope():
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
        payload={"vacation_id": "123"},
    )


@pytest.fixture
def sample_queue_config():
    """Создает тестовую конфигурацию очереди."""
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
def mock_bitrix_availability_guard():
    """Создает мок BitrixAvailabilityGuard."""
    guard = MagicMock(spec=BitrixAvailabilityGuard)
    guard.wait_until_up = AsyncMock()
    guard.is_alive = True
    return guard


@pytest.fixture
def mock_connection():
    """Создает мок RobustConnection."""
    connection = AsyncMock()
    connection.is_closed = False
    connection.channel = AsyncMock()
    connection.close = AsyncMock()
    return connection


@pytest.fixture
def mock_channel():
    """Создает мок RobustChannel."""
    channel = AsyncMock()
    channel.is_closed = False
    channel.set_qos = AsyncMock()
    channel.declare_exchange = AsyncMock()
    channel.declare_queue = AsyncMock()
    channel.close = AsyncMock()
    return channel


@pytest.fixture
def mock_queue():
    """Создает мок RobustQueue."""
    queue = AsyncMock()
    queue.bind = AsyncMock()
    queue.consume = AsyncMock(return_value="consumer_tag_123")
    queue.cancel = AsyncMock()
    return queue


@pytest.fixture
def mock_exchange():
    """Создает мок Exchange."""
    exchange = AsyncMock()
    return exchange


@pytest.fixture
def mock_message():
    """Создает мок AbstractIncomingMessage."""
    message = AsyncMock()
    message.body = b'{"message_id": "123e4567-e89b-12d3-a456-426614174000", "event_type": "test", "event_action": "test.action", "event_version": 1.0, "source_system": "1C-HR", "destination_system": "BITRIX", "occurred_at": "2024-01-01T00:00:00", "sent_at": "2024-01-01T00:00:00", "payload": {}}'
    message.headers = {}
    message.ack = AsyncMock()
    message.reject = AsyncMock()

    # Настраиваем context manager для process()
    context_manager = AsyncMock()
    context_manager.__aenter__ = AsyncMock(return_value=message)
    context_manager.__aexit__ = AsyncMock(return_value=None)
    message.process = MagicMock(return_value=context_manager)

    return message
