import pytest
from pydantic import ValidationError
from uuid import uuid4
from datetime import datetime

from src.core.rabbit.dto import Envelope, QueueConfig, Payload


class TestQueueConfig:
    """Тесты для QueueConfig."""

    def test_valid_config(self):
        """Тест валидной конфигурации."""
        config = QueueConfig(
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

        assert config.queue_name == "test.queue"
        assert config.exchange_name == "test.exchange"
        assert config.binding_key == "test.key"
        assert config.exchange_type == "topic"
        assert config.durable is True
        assert config.auto_delete is False
        assert config.dlx_name == "test.dlx"
        assert config.dlx_routing_key == "test.dlx.key"
        assert config.prefetch_count == 10

    def test_default_values(self):
        """Тест значений по умолчанию."""
        config = QueueConfig(
            queue_name="test.queue",
            exchange_name="test.exchange",
            binding_key="test.key",
            dlx_name="test.dlx",
            dlx_routing_key="test.dlx.key",
            prefetch_count=10,
        )

        assert config.exchange_type == "topic"
        assert config.durable is True
        assert config.auto_delete is False
        assert config.message_ttl is None
        assert config.max_length is None
        assert config.max_priority is None

    def test_exchange_types(self):
        """Тест различных типов exchange."""
        for exchange_type in ["fanout", "topic", "direct"]:
            config = QueueConfig(
                queue_name="test.queue",
                exchange_name="test.exchange",
                binding_key="test.key",
                exchange_type=exchange_type,
                dlx_name="test.dlx",
                dlx_routing_key="test.dlx.key",
                prefetch_count=10,
            )
            assert config.exchange_type == exchange_type

    def test_invalid_exchange_type(self):
        """Тест невалидного типа exchange."""
        with pytest.raises(ValidationError):
            QueueConfig(
                queue_name="test.queue",
                exchange_name="test.exchange",
                binding_key="test.key",
                exchange_type="invalid",
                dlx_name="test.dlx",
                dlx_routing_key="test.dlx.key",
                prefetch_count=10,
            )


class TestEnvelope:
    """Тесты для Envelope."""

    def test_valid_envelope(self):
        """Тест валидного Envelope."""
        message_id = uuid4()
        occurred_at = datetime.now()
        sent_at = datetime.now()

        envelope = Envelope(
            message_id=message_id,
            event_type="hr.vacation",
            event_action="vacation.create",
            event_version=1.0,
            source_system="1C-HR",
            destination_system="BITRIX",
            occurred_at=occurred_at,
            sent_at=sent_at,
            payload={"test": "data"},
        )

        assert envelope.message_id == message_id
        assert envelope.event_type == "hr.vacation"
        assert envelope.event_action == "vacation.create"
        assert envelope.event_version == 1.0
        assert envelope.source_system == "1C-HR"
        assert envelope.destination_system == "BITRIX"
        assert envelope.occurred_at == occurred_at
        assert envelope.sent_at == sent_at
        assert envelope.payload == {"test": "data"}
        assert envelope.correlation_id is None

    def test_envelope_with_correlation_id(self):
        """Тест Envelope с correlation_id."""
        correlation_id = uuid4()
        envelope = Envelope(
            message_id=uuid4(),
            event_type="test",
            event_action="test.action",
            event_version=1.0,
            source_system="1C",
            destination_system="BITRIX",
            occurred_at=datetime.now(),
            sent_at=datetime.now(),
            correlation_id=correlation_id,
            payload={},
        )

        assert envelope.correlation_id == correlation_id

    def test_envelope_missing_required_fields(self):
        """Тест Envelope с отсутствующими обязательными полями."""
        with pytest.raises(ValidationError):
            Envelope(
                message_id=uuid4(),
                # Отсутствует event_type
                event_action="test.action",
                event_version=1.0,
                source_system="1C",
                destination_system="BITRIX",
                occurred_at=datetime.now(),
                sent_at=datetime.now(),
                payload={},
            )

    def test_envelope_invalid_message_id(self):
        """Тест Envelope с невалидным message_id."""
        with pytest.raises(ValidationError):
            Envelope(
                message_id="not-a-uuid",
                event_type="test",
                event_action="test.action",
                event_version=1.0,
                source_system="1C",
                destination_system="BITRIX",
                occurred_at=datetime.now(),
                sent_at=datetime.now(),
                payload={},
            )

    def test_envelope_from_dict(self):
        """Тест создания Envelope из словаря."""
        data = {
            "message_id": str(uuid4()),
            "event_type": "test",
            "event_action": "test.action",
            "event_version": 1.0,
            "source_system": "1C",
            "destination_system": "BITRIX",
            "occurred_at": datetime.now().isoformat(),
            "sent_at": datetime.now().isoformat(),
            "payload": {"key": "value"},
        }

        envelope = Envelope(**data)
        assert envelope.event_type == "test"
        assert envelope.payload == {"key": "value"}


class TestPayload:
    """Тесты для Payload."""

    def test_payload_base_class(self):
        """Тест базового класса Payload."""
        payload = Payload()
        assert isinstance(payload, Payload)
