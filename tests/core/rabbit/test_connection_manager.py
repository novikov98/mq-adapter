import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.core.rabbit.connection_manager import RabbitConnectionManager


class TestRabbitConnectionManager:
    """Тесты для RabbitConnectionManager."""

    @pytest.fixture
    def manager(self):
        """Создает экземпляр RabbitConnectionManager."""
        return RabbitConnectionManager("amqp://user:pass@localhost:5672/")

    def test_init(self, manager):
        """Тест инициализации менеджера."""
        assert manager.url == "amqp://user:pass@localhost:5672/"
        assert manager.connection is None
        assert manager.lock is not None

    def test_existing_connection_when_none(self, manager):
        """Тест existing_connection когда соединение отсутствует."""
        assert manager.existing_connection is None

    def test_existing_connection_when_closed(self, manager):
        """Тест existing_connection когда соединение закрыто."""
        mock_conn = MagicMock()
        mock_conn.is_closed = True
        manager.connection = mock_conn
        assert manager.existing_connection is None

    def test_existing_connection_when_open(self, manager):
        """Тест existing_connection когда соединение открыто."""
        mock_conn = MagicMock()
        mock_conn.is_closed = False
        manager.connection = mock_conn
        assert manager.existing_connection == mock_conn

    @pytest.mark.asyncio
    async def test_get_connection_creates_new(self, manager):
        """Тест создания нового соединения."""
        mock_conn = AsyncMock()
        mock_conn.is_closed = False

        with patch("aio_pika.connect_robust", new_callable=AsyncMock) as mock_connect:
            mock_connect.return_value = mock_conn
            connection = await manager.get_connection()

            assert connection == mock_conn
            assert manager.connection == mock_conn
            mock_connect.assert_called_once_with(manager.url)

    @pytest.mark.asyncio
    async def test_get_connection_returns_existing(self, manager):
        """Тест возврата существующего соединения."""
        mock_conn = AsyncMock()
        mock_conn.is_closed = False
        manager.connection = mock_conn

        connection = await manager.get_connection()

        assert connection == mock_conn

    @pytest.mark.asyncio
    async def test_get_connection_handles_connection_error(self, manager):
        """Тест обработки ошибки подключения."""
        with patch("aio_pika.connect_robust", new_callable=AsyncMock) as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")

            with pytest.raises(Exception, match="Connection failed"):
                await manager.get_connection()

    @pytest.mark.asyncio
    async def test_get_connection_thread_safety(self, manager):
        """Тест потокобезопасности создания соединения."""
        mock_conn = AsyncMock()
        mock_conn.is_closed = False

        with patch("aio_pika.connect_robust", new_callable=AsyncMock) as mock_connect:
            mock_connect.return_value = mock_conn

            # Симулируем параллельные вызовы
            connections = await asyncio.gather(
                manager.get_connection(),
                manager.get_connection(),
                manager.get_connection(),
            )

            # Все должны получить одно и то же соединение
            assert all(conn == mock_conn for conn in connections)
            # Соединение должно быть создано только один раз
            assert mock_connect.call_count == 1

    @pytest.mark.asyncio
    async def test_get_channel(self, manager):
        """Тест получения канала."""
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_conn.is_closed = False
        mock_conn.channel = AsyncMock(return_value=mock_channel)
        manager.connection = mock_conn

        channel = await manager.get_channel()

        assert channel == mock_channel
        mock_conn.channel.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_when_no_connection(self, manager):
        """Тест закрытия когда соединения нет."""
        await manager.close()
        # Не должно быть ошибок

    @pytest.mark.asyncio
    async def test_close_when_connection_closed(self, manager):
        """Тест закрытия когда соединение уже закрыто."""
        mock_conn = AsyncMock()
        mock_conn.is_closed = True
        manager.connection = mock_conn

        await manager.close()

        mock_conn.close.assert_not_called()
        assert manager.connection is None

    @pytest.mark.asyncio
    async def test_close_successfully(self, manager):
        """Тест успешного закрытия соединения."""
        mock_conn = AsyncMock()
        mock_conn.is_closed = False
        mock_conn.close = AsyncMock()
        manager.connection = mock_conn

        await manager.close()

        mock_conn.close.assert_called_once()
        assert manager.connection is None
