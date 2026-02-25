# Data Aggregator & Dashboard Backend

Backend API для сайта-агрегатора данных с dashboard. Разработан на FastAPI с использованием ClickHouse в качестве базы данных и MinIO для хранения файлов.

## Технологический стек

- **FastAPI** - асинхронный веб-фреймворк
- **Pydantic** - валидация данных
- **ClickHouse** - колоночная СУБД для аналитики
- **MinIO** - S3-совместимое объектное хранилище
- **Docker & Docker Compose** - контейнеризация
- **Poetry** - управление зависимостями

## Структура проекта

```
job_test/
├── app/
│   ├── api/
│   │   └── v1/
│   │       ├── endpoints/
│   │       │   ├── dashboard.py    # CRUD для дашбордов
│   │       │   ├── data.py         # Запросы и агрегация данных
│   │       │   ├── tables.py       # Управление таблицами
│   │       │   └── files.py        # Загрузка/скачивание файлов
│   │       └── router.py
│   ├── core/
│   │   ├── config.py       # Конфигурация приложения
│   │   ├── database.py     # Подключение к ClickHouse
│   │   └── minio_client.py # Клиент MinIO
│   ├── schemas/
│   │   ├── base.py         # Базовые схемы
│   │   ├── dashboard.py    # Схемы дашбордов
│   │   └── data.py         # Схемы данных
│   ├── services/
│   │   ├── dashboard.py    # Логика дашбордов
│   │   ├── data.py         # Логика данных
│   │   └── tables.py       # Логика таблиц
│   └── main.py             # Точка входа
├── migrations/
│   ├── 001_initial_schema.sql
│   └── runner.py
├── docker/
│   └── clickhouse/
│       ├── config.xml
│       └── users.xml
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── .env.example
└── README.md
```

## Быстрый старт

### 1. Клонирование и настройка

```bash
# Клонирование репозитория
git clone <repository-url>
cd job_test

# Копирование файла конфигурации
cp .env.example .env
```

### 2. Запуск с Docker Compose

```bash
# Запуск всех сервисов
docker-compose up -d

# Просмотр логов
docker-compose logs -f app

# Остановка
docker-compose down
```

### 3. Локальная разработка

```bash
# Установка Poetry
pip install poetry

# Установка зависимостей
poetry install

# Активация виртуального окружения
poetry shell

# Запуск приложения
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## API Endpoints

### Health Check
- `GET /health` - проверка состояния сервиса

### Dashboards (Дашборды)
- `GET /api/v1/dashboards` - список дашбордов
- `POST /api/v1/dashboards` - создание дашборда
- `GET /api/v1/dashboards/{id}` - получение дашборда
- `PUT /api/v1/dashboards/{id}` - обновление дашборда
- `DELETE /api/v1/dashboards/{id}` - удаление дашборда

### Widgets (Виджеты)
- `POST /api/v1/dashboards/{id}/widgets` - добавление виджета
- `GET /api/v1/dashboards/{id}/widgets` - список виджетов
- `PUT /api/v1/dashboards/{id}/widgets/{widget_id}` - обновление виджета
- `DELETE /api/v1/dashboards/{id}/widgets/{widget_id}` - удаление виджета

### Data (Данные)
- `POST /api/v1/data/query` - запрос данных с фильтрацией и сортировкой
- `POST /api/v1/data/aggregate` - агрегация данных
- `POST /api/v1/data/chart` - данные для графиков
- `POST /api/v1/data/export` - экспорт данных (CSV, JSON)

### Tables (Таблицы)
- `GET /api/v1/tables` - список таблиц
- `GET /api/v1/tables/{name}/metadata` - метаданные таблицы
- `GET /api/v1/tables/{name}/columns` - колонки таблицы
- `GET /api/v1/tables/{name}/sample` - примерные данные

### Data Sources (Источники данных)
- `POST /api/v1/tables/sources` - создание источника
- `GET /api/v1/tables/sources` - список источников
- `GET /api/v1/tables/sources/{id}` - получение источника
- `PUT /api/v1/tables/sources/{id}` - обновление источника
- `DELETE /api/v1/tables/sources/{id}` - удаление источника

### Files (Файлы)
- `POST /api/v1/files/upload` - загрузка файла
- `GET /api/v1/files` - список файлов
- `GET /api/v1/files/download/{path}` - получение URL для скачивания
- `DELETE /api/v1/files/{path}` - удаление файла

## Фильтрация и сортировка

### Операторы фильтрации

| Оператор | Описание |
|----------|----------|
| `eq` | Равно |
| `neq` | Не равно |
| `gt` | Больше |
| `gte` | Больше или равно |
| `lt` | Меньше |
| `lte` | Меньше или равно |
| `in` | В списке |
| `not_in` | Не в списке |
| `like` | Паттерн (регистрозависимый) |
| `ilike` | Паттерн (регистронезависимый) |
| `between` | Между двумя значениями |
| `is_null` | Пустое значение |
| `is_not_null` | Непустое значение |

### Пример запроса с фильтрацией

```json
{
  "table_name": "events",
  "columns": ["id", "name", "created_at"],
  "filters": {
    "conditions": [
      {"field": "status", "operator": "eq", "value": "active"},
      {"field": "created_at", "operator": "gte", "value": "2024-01-01"}
    ],
    "logic": "AND"
  },
  "sort": [
    {"field": "created_at", "order": "desc"}
  ],
  "page": 1,
  "page_size": 50
}
```

## Типы графиков

- `line` - линейный график
- `bar` - столбчатая диаграмма
- `pie` - круговая диаграмма
- `area` - диаграмма с областями
- `scatter` - точечная диаграмма
- `heatmap` - тепловая карта

## Агрегационные функции

- `count` - количество
- `sum` - сумма
- `avg` - среднее
- `min` - минимум
- `max` - максимум
- `uniqExact` - уникальные значения
- `median` - медиана
- `quantile(0.9)` - 90-й перцентиль
- `quantile(0.95)` - 95-й перцентиль
- `quantile(0.99)` - 99-й перцентиль

## Переменные окружения

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `DEBUG` | Режим отладки | `false` |
| `CLICKHOUSE_HOST` | Хост ClickHouse | `clickhouse` |
| `CLICKHOUSE_PORT` | HTTP порт ClickHouse | `8123` |
| `CLICKHOUSE_USER` | Пользователь ClickHouse | `default` |
| `CLICKHOUSE_PASSWORD` | Пароль ClickHouse | `` |
| `MINIO_ENDPOINT` | Endpoint MinIO | `minio:9000` |
| `MINIO_ACCESS_KEY` | Ключ доступа MinIO | `minioadmin` |
| `MINIO_SECRET_KEY` | Секретный ключ MinIO | `minioadmin` |
| `SECRET_KEY` | Секретный ключ JWT | - |

## Документация API

После запуска доступна интерактивная документация:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

## Миграции

```bash
# Запуск миграций
python -m migrations.runner
```

## Лицензия

MIT
