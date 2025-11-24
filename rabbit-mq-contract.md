# HR Events – JSON Message Contract (1C → Bitrix)  
**Exchange:** `hr.events` (`topic`)  
**Routing keys:**  
- `hr.vacation.upsert`  
- `hr.department.upsert`  
- `hr.employee.upsert`

**Очереди Bitrix:**  
- `bitrix.hr.vacation` → bind `hr.vacation.*`  
- `bitrix.hr.department` → bind `hr.department.*`  
- `bitrix.hr.employee` → bind `hr.employee.*`

---

## 1. Общая структура сообщения (Envelope)

Все сообщения имеют единый формат.

```json
{
  "message_id": "UUID",
  "event_type": "string",
  "event_action": "string",
  "event_version": "1.0",
  "source_system": "1C-HR",
  "destination_system": "BITRIX",
  "occurred_at": "YYYY-MM-DDThh:mm:ssZ",
  "sent_at": "YYYY-MM-DDThh:mm:ssZ",
  "correlation_id": "UUID or null",
  "payload": {}
}
```


### 1.1 Описание полей
- message_id – UUID события
- event_type – тип сущности (hr.vacation / hr.department / hr.employee)
- event_action – сейчас можно всегда ставить "upsert", потом расширим
- event_version – версия схемы сообщения
- source_system – "1C-HR"/"1C-ZUP"
- destination_system – "BITRIX-HR" 
- occurred_at – ISO-дата, когда изменение произошло в 1С.
- sent_at – когда сообщение отправлено в брокер.
- correlation_id – привязка к бизнес-процессу или запросу
- payload – объект с предметными данными.

## 2. Vacation Event (hr.vacation.upsert)
**Routing key:** `hr.vacation.upsert` 

**payload структура:**

```json
{
  "employee_email": "string",
  "employee_full_name": "string",
  "start_date": "YYYY-MM-DD",
  "end_date": "YYYY-MM-DD"
}
```
### 2.1 Описание полей
- employee_email – Email сотрудника
- employee_full_name - ФИО
- start_date - Начало отпуска
- end_date - Окончание отпуска

## 3. Department Event (hr.department.upsert)
**Routing key:** `hr.department.upsert` 

**payload структура:**

```json
{
  "department_id": "string",
  "department_name": "string",
  "parent_department_id": "string or null",
  "head_employee_email": "string or null"
}
```
### 3.1 Описание полей
- department_id – ID подразделения из 1C
- department_name - Название
- parent_department_id - Родительское подразделение
- head_employee_email - Руководитель

## 4. Employee Event (hr.employee.upsert)
**Routing key:** `hr.employee.upsert` 

**payload структура:**

```json
{
  "employee_id": "string",
  "last_name": "string",
  "first_name": "string",
  "middle_name": "string or null",
  "full_name": "string",
  "hire_date": "YYYY-MM-DD",
  "department_id": "string",
  "birth_date": "YYYY-MM-DD",
  "personnel_number": "string",
  "position": "string",
  "email": "string",
  "mobile_phone": "string"
}
```
### 4.1 Описание полей
- employee_id – ID сотрудника из 1С
- last_name - Фамилия
- first_name - Имя
- middle_name - Отчество
- full_name - Полное ФИО
- hire_date - Дата приёма
- department_id - ID подразделения
- birth_date - Дата рождения
- personnel_number - Табельный номер
- position - Должность
- email - Email
- mobile_phone - Телефон

# Контракт
## 5. Обязаности 1С (Producer)
	1.	Формировать envelope и payload строго по описанным схемам.
	2.	Генерировать уникальный message_id (UUID).
	3.	Заполнять event_type, event_action="upsert", event_version="1.0", occurred_at, sent_at.
	4.	Публиковать сообщения в exchange hr.events:
		Отпуска  → routing key hr.vacation.upsert
		Подразделения → hr.department.upsert
		Сотрудники → hr.employee.upsert
	5.	Логировать отправку.
	6.	При ошибке публикации реализовать повторную отправку (гарантия, что событие не теряется в 1С → RabbitMQ).

## 6. Обязаности Bitrix (Consumer)
	1.	Настроить консьюмеры на очередях:
		bitrix.hr.vacation
		bitrix.hr.department
		bitrix.hr.employee
	2.	Использовать manual ack:
		ACK только после успешной обработки и фиксации изменений в БД.
		В случае ошибки – логика согласно политике повторных попыток (см. ниже).
	3.	Валидация:
		Проверить наличие обязательных полей envelope и payload.
		Проверить, что event_version поддерживается (минимум "1.0").
	4.	Идемпотентность:
		Не допускать повторную обработку одного message_id.
		Рекомендуемый вариант: таблица processed_messages(message_id PK, processed_at) или upsert по бк (employee_id, department_id, комбинация полей отпуска).
	5.	Бизнес-обработка:
		Employee: upsert сотрудника по employee_id (или другому ключу).
		Department: upsert подразделения по department_id.
		Vacation: upsert отпуска сотрудника по (employee_email + start_date + end_date) или собственному ID.

# 7. Контракт повторных попыток и мёртвых очередей

## 7.1 Классификация ошибок
  **Transient ошибки:**

		
    недоступна БД Bitrix;
    временные сетевые ошибки;
    таймауты внешних сервисов;
    блокировки в БД.

В этих случаях сообщение повторно обрабатывается.

**Permanent ошибки:**

	некорректный формат события (не валиден JSON, отсутствуют обязательные поля);
	неподдерживаемая версия (event_version неизвестна);
	бизнес-ошибка, которая не может быть исправлена повтором:
	не существует обязательного department_id и это не будет восстановлено ретраем;
	некорректный формат email / даты, который гарантированно не изменится.


В этих случаях сообщение сразу отправляется в мёртвую очередь (DLQ), без повторов.

## 7.2 Количество попыток обработки

Для каждого сообщения применяется следующая политика:

**Максимальное количество попыток обработки при временных ошибках:**

MAX_ATTEMPTS = 4 (1 исходная попытка + до 3 повторных)

**Максимальное количество попыток обработки при постоянной ошибке:**

0 повторов, сообщение немедленно попадает в DLQ.

***Под «попыткой обработки» понимается фактический старт бизнес-логики Consumer’а по сообщению.***







 

 


