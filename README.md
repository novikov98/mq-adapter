# mq-adapter

## Описание проекта

`mq-adapter` — это сервис-адаптер, который обеспечивает интеграцию между системой 1С и Битрикс24 через брокер сообщений RabbitMQ. Сервис работает в режиме консюмера, получая сообщения из очередей RabbitMQ и обрабатывая их для синхронизации данных в Битрикс24.

### Основные возможности

- **Асинхронная обработка сообщений** из RabbitMQ
- **Идемпотентность обработки** — защита от повторной обработки сообщений
- **Обработка ошибок** с автоматической маршрутизацией в Dead Letter Queue (DLQ)
- **Защита от недоступности Bitrix** — автоматическая пауза при недоступности API
- **Валидация сообщений** по схеме контракта
- **Маршрутизация событий** по типу и версии

## Архитектура

Проект использует следующую архитектуру:

```
RabbitMQ (Exchange: hr.events)
    ↓
Очереди (bitrix.hr.*)
    ↓
Consumer (mq-adapter)
    ↓
Middleware (Idempotency)
    ↓
Router (EnvelopeRouter)
    ↓
Handler (Business Logic)
    ↓
Bitrix24 API
```

## Интерфейсы

### Consumer (Консюмер)

Реализован интерфейс **Consumer** для получения и обработки сообщений из RabbitMQ.

#### Конфигурация очереди

Каждый консюмер настраивается через `QueueConfig`:

```python
QueueConfig(
    queue_name="bitrix.hr.vacation",      # Имя очереди
    exchange_name="hr.events",            # Exchange для подписки
    binding_key="hr.vacation.*",          # Routing key для биндинга
    dlx_name="hr.dlq",                    # Dead Letter Exchange
    dlx_routing_key="hr.vacation.dlq",    # Routing key для DLQ
    prefetch_count=1,                     # Количество неподтвержденных сообщений
)
```

#### Создание консюмера

```python
from src.consumers.routers.vacation import router
from src.core.rabbit.client import Consumer
from src.core.rabbit.dto import QueueConfig

config = QueueConfig(
    queue_name="bitrix.hr.vacation",
    exchange_name="hr.events",
    binding_key="hr.vacation.*",
    dlx_name="hr.dlq",
    dlx_routing_key="hr.vacation.dlq",
    prefetch_count=1,
)

vacation_consumer = Consumer(
    name="vacation",
    config=config,
    router=router
)
```

#### Регистрация обработчика

Обработчики регистрируются через декоратор `@router.on()`:

```python
from src.core.rabbit.envelope_router import EnvelopeRouter
from src.core.rabbit.dto import Envelope

router = EnvelopeRouter()

@router.on("upsert", version=1.0)
async def upsert(envelope: Envelope):
    # Обработка события
    pass
```

### Middleware

Реализован механизм middleware для обработки сообщений перед бизнес-логикой.

#### IdempotencyMiddleware

Обеспечивает идемпотентность обработки сообщений через Redis:

- Проверяет наличие `message_id` в Redis
- Блокирует повторную обработку одного и того же сообщения
- Автоматически снимает блокировку при ошибке обработки

```python
from src.consumers.middlewares.idempotency_middelware import IdempotencyMiddleware

idempotency_mw = IdempotencyMiddleware(RedisClient.get_redis())
```

## Использование Consumer

### Запуск приложения

1. **Настройка переменных окружения**

Создайте файл `.env` в корне проекта со следующими переменными:

```env
# RabbitMQ
RABBIT_HOST=localhost
RABBIT_PORT=5672
RABBIT_USER=rabbit
RABBIT_PASSWORD=rabbit
RABBIT_VHOST=/

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_ENVELOPE_PROCESSING_TTL=3600
REDIS_ENVELOPE_PROCESSED_TTL=86400

# Bitrix24
BX_WEBHOOK=https://your-portal.bitrix24.ru/rest/1/your-webhook/
BX_WEBHOOK_OWNER_ID=1
BX_REQUEST_POOL_SIZE=10
BX_REQUEST_PER_SECOND=2.0
BX_OPERATING_TIME_LIMIT=600
BX_MAX_REQUEST_RETRIES=3
BX_HEALTH_CHECK_BACKOFF_INITIAL_DELAY=5
BX_HEALTH_CHECK_BACKOFF_MAX_DELAY=300
BX_HEALTH_CHECK_BACKOFF_MULTIPLIER=2

# Logging
LOG_LEVEL=INFO
```

2. **Запуск через Docker**

```bash
docker-compose -f docker-compose-local.yml up -d
```

3. **Запуск напрямую**

```bash
poetry install
poetry run python -m src.main
```

### Структура обработки сообщений

1. **Получение сообщения** из очереди RabbitMQ
2. **Парсинг и валидация** структуры Envelope
3. **Проверка идемпотентности** через IdempotencyMiddleware
4. **Маршрутизация** к обработчику через EnvelopeRouter
5. **Обработка бизнес-логики** в handler
6. **Подтверждение** (ACK) или отклонение (NACK/REJECT) сообщения

### Обработка ошибок

#### Transient ошибки (временные)

- Недоступность БД Bitrix
- Временные сетевые ошибки
- Таймауты внешних сервисов
- Блокировки в БД

**Поведение:** Сообщение возвращается в очередь для повторной обработки (`requeue=True`).

#### Permanent ошибки (постоянные)

- Некорректный формат события (не валиден JSON, отсутствуют обязательные поля)
- Неподдерживаемая версия (`event_version` неизвестна)
- Бизнес-ошибка, которая не может быть исправлена повтором

**Поведение:** Сообщение отправляется в Dead Letter Queue (`requeue=False`).

#### Недоступность Bitrix

При недоступности API Bitrix:
1. Консюмер автоматически ставится на паузу
2. Сообщение возвращается в очередь
3. Запускается health check с экспоненциальной задержкой
4. После восстановления доступности консюмер возобновляет работу

### Добавление нового консюмера

1. **Создайте DTO для payload:**

```python
# src/consumers/dto/example.py
from pydantic import BaseModel

class ExamplePayloadDTO(BaseModel):
    field1: str
    field2: int
```

2. **Создайте роутер с обработчиками:**

```python
# src/consumers/routers/example.py
from src.core.rabbit.envelope_router import EnvelopeRouter
from src.core.rabbit.dto import Envelope
from src.consumers.dto.example import ExamplePayloadDTO

router = EnvelopeRouter()

@router.on("upsert", version=1.0)
async def upsert(envelope: Envelope):
    payload = ExamplePayloadDTO(**envelope.payload)
    # Ваша бизнес-логика
    pass
```

3. **Создайте конфигурацию консюмера:**

```python
# src/consumers/example.py
from src.consumers.routers.example import router
from src.core.rabbit.client import Consumer
from src.core.rabbit.dto import QueueConfig

config = QueueConfig(
    queue_name="bitrix.hr.example",
    exchange_name="hr.events",
    binding_key="hr.example.*",
    dlx_name="hr.dlq",
    dlx_routing_key="hr.example.dlq",
    prefetch_count=1,
)

example_consumer = Consumer(
    name="example",
    config=config,
    router=router
)
```

4. **Зарегистрируйте консюмер в `main.py`:**

```python
from src.consumers.example import example_consumer

rabbit = RabbitClient(
    rabbit_url=config.rabbit_config.connect_url,
    bitrix_availability_guard=BitrixClient.bitrix_availability_guard,
    consumers=[vacation_consumer, example_consumer],  # Добавьте новый консюмер
    middlewares=[idempotency_mw],
)
```

## Реализованные события из обмена

### ✅ hr.vacation.upsert

**Статус:** Реализовано

**Очередь:** `bitrix.hr.vacation`  
**Routing key:** `hr.vacation.*`  
**Exchange:** `hr.events`

**Описание:** Обработка событий создания и обновления отпусков сотрудников.

**Payload структура:**
```json
{
  "employee_email": "string",
  "employee_full_name": "string",
  "start_date": "YYYY-MM-DD",
  "end_date": "YYYY-MM-DD"
}
```

**Логика обработки:**
1. Поиск сотрудника в Bitrix24 по `employee_email`
2. Создание записи об отсутствии (absence) типа "VACATION"
3. Установка дат начала и окончания отпуска

**Обработчик:** `src/consumers/routers/vacation.py`  
**Сервис:** `src/consumers/services/vacation.py`

### ❌ hr.department.upsert

**Статус:** Не реализовано

**Очередь:** `bitrix.hr.department`  
**Routing key:** `hr.department.*`

**Описание:** Обработка событий создания и обновления подразделений.

### ❌ hr.employee.upsert

**Статус:** Не реализовано

**Очередь:** `bitrix.hr.employee`  
**Routing key:** `hr.employee.*`

**Описание:** Обработка событий создания и обновления сотрудников.

## Формат сообщений

Все сообщения должны соответствовать структуре Envelope согласно контракту (см. `rabbit-mq-contract.md`):

```json
{
  "message_id": "UUID",
  "event_type": "string",
  "event_action": "string",
  "event_version": 1.0,
  "source_system": "1C-HR",
  "destination_system": "BITRIX",
  "occurred_at": "YYYY-MM-DDThh:mm:ssZ",
  "sent_at": "YYYY-MM-DDThh:mm:ssZ",
  "correlation_id": "UUID or null",
  "payload": {}
}
```

## Контракт обмена

Подробное описание контракта обмена данными между 1С и Bitrix24 находится в файле [`rabbit-mq-contract.md`](rabbit-mq-contract.md).

## Разработка

### Установка зависимостей

```bash
poetry install
```

### Запуск локально

```bash
# Запуск RabbitMQ и Redis
docker-compose -f docker-compose-local.yml up -d

# Запуск приложения
poetry run python -m src.main
```

### Форматирование кода

```bash
poetry run black .
poetry run ruff check .
```

## Технологии

- **Python 3.13+**
- **aio-pika** — асинхронный клиент RabbitMQ
- **pydantic** — валидация данных
- **bx-client** — клиент для работы с Bitrix24 API
- **redis** — хранение данных для идемпотентности
- **poetry** — управление зависимостями

## Лицензия

Проект разработан для внутреннего использования.
