from src.core.rabbit.exceptions import (
    RabbitException,
    EnvelopeHandlerNotRegisteredError,
    DuplicateEnvelopeError,
    EnvelopeProcessingError,
)


class TestRabbitException:
    """Тесты для RabbitException."""

    def test_init_with_message(self):
        """Тест инициализации с сообщением."""
        exc = RabbitException("Test message")
        assert str(exc) == "Test message"
        assert exc.message == "Test message"

    def test_init_without_message(self):
        """Тест инициализации без сообщения."""
        exc = RabbitException()
        assert exc.message == ""


class TestEnvelopeHandlerNotRegisteredError:
    """Тесты для EnvelopeHandlerNotRegisteredError."""

    def test_inheritance(self):
        """Тест наследования."""
        exc = EnvelopeHandlerNotRegisteredError("Handler not found")
        assert isinstance(exc, RabbitException)
        assert isinstance(exc, Exception)

    def test_message(self):
        """Тест сообщения об ошибке."""
        exc = EnvelopeHandlerNotRegisteredError("Handler not found")
        assert exc.message == "Handler not found"


class TestDuplicateEnvelopeError:
    """Тесты для DuplicateEnvelopeError."""

    def test_inheritance(self):
        """Тест наследования."""
        exc = DuplicateEnvelopeError("Duplicate message")
        assert isinstance(exc, RabbitException)
        assert isinstance(exc, Exception)

    def test_message(self):
        """Тест сообщения об ошибке."""
        exc = DuplicateEnvelopeError("Duplicate message")
        assert exc.message == "Duplicate message"


class TestEnvelopeProcessingError:
    """Тесты для EnvelopeProcessingError."""

    def test_inheritance(self):
        """Тест наследования."""
        exc = EnvelopeProcessingError("Processing error")
        assert isinstance(exc, RabbitException)
        assert isinstance(exc, Exception)

    def test_init_with_requeue(self):
        """Тест инициализации с requeue."""
        exc = EnvelopeProcessingError("Error", requeue=True)
        assert exc.message == "Error"
        assert exc.requeue is True

    def test_init_without_requeue(self):
        """Тест инициализации без requeue."""
        exc = EnvelopeProcessingError("Error")
        assert exc.message == "Error"
        assert exc.requeue is False

    def test_init_with_requeue_false(self):
        """Тест инициализации с requeue=False."""
        exc = EnvelopeProcessingError("Error", requeue=False)
        assert exc.message == "Error"
        assert exc.requeue is False
