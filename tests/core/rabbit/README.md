# Тесты для модуля src/core/rabbit

Этот каталог содержит тесты для всех модулей RabbitMQ адаптера.

## Структура тестов

- `test_connection_manager.py` - тесты для `RabbitConnectionManager`
- `test_envelope_router.py` - тесты для `EnvelopeRouter`
- `test_consumer.py` - тесты для `QueueConsumer`
- `test_client.py` - тесты для `RabbitClient`
- `test_dto.py` - тесты для DTO классов (`Envelope`, `QueueConfig`, `Payload`)
- `test_exceptions.py` - тесты для кастомных исключений

## Запуск тестов

```bash
# Запуск всех тестов
pytest tests/core/rabbit/

# Запуск конкретного файла
pytest tests/core/rabbit/test_consumer.py

# Запуск с подробным выводом
pytest tests/core/rabbit/ -v

# Запуск с покрытием
pytest tests/core/rabbit/ --cov=src/core/rabbit
```

## Покрытие тестами

Тесты покрывают следующие сценарии:

### ConnectionManager
- Создание и управление соединениями
- Потокобезопасность
- Обработка ошибок подключения
- Получение каналов

### EnvelopeRouter
- Регистрация обработчиков
- Поиск обработчиков по версии
- Fallback на обработчики без версии
- Обработка отсутствующих обработчиков

### QueueConsumer
- Запуск и остановка консьюмера
- Пауза и возобновление
- Обработка сообщений (успешная и с ошибками)
- Обработка различных типов ошибок
- Построение pipeline с middleware
- Настройка DLQ заголовков

### RabbitClient
- Инициализация с консьюмерами
- Управление жизненным циклом консьюмеров
- Работа с глобальными и специфичными middleware

### DTO
- Валидация `QueueConfig`
- Валидация `Envelope`
- Значения по умолчанию

### Exceptions
- Наследование и структура исключений
- Параметры исключений


