# 🧱 `infra.py` — инфраструктурный модуль системы

Файл `infra.py` отвечает за подключение к Redis и PostgreSQL, конфигурацию отладочного режима, защиту фоновых задач и централизованное, многоуровневое логирование.

---

## 📌 Назначение

- Централизованная инициализация Redis и PostgreSQL
- Конфигурация через переменные окружения (из Render)
- Универсальные утилиты:
  - `run_safe_loop()` — защита от сбоев
  - `logging` — гибкая система логирования с цветами и уровнями
- Используется во всех ключевых модулях системы

---

## ⚙️ Конфигурация

### 🔹 Переменные окружения (через Render)

```python
REDIS_URL = os.getenv("REDIS_URL")
POSTGRES_URL = os.getenv("POSTGRES_URL")
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
```

- Никаких `.env` файлов — только Render → Environment Variables

---

## 🔌 Инициализация

### Redis

```python
def init_redis_client():
    return aioredis.from_url(
        REDIS_URL,
        decode_responses=True,
        encoding="utf-8"
    )
```

### PostgreSQL

```python
async def init_pg_pool():
    return await asyncpg.create_pool(POSTGRES_URL)
```

---

## 🧱 Защита фоновых задач

### `run_safe_loop(...)`

```python
def run_safe_loop(coro_fn, name: str, retry_delay: int = 5)
```

- Оборачивает любой бесконечный воркер
- Логирует ошибки, перезапускает через `retry_delay`

---

## 🧾 Логирование

### Уровни:

- `DEBUG`: подробные сообщения (если `DEBUG_MODE = true`)
- `INFO`: стандартные события (запуск, завершение и т.п.)
- `WARNING`: подозрительные состояния, но без ошибки
- `ERROR`: ошибка, но процесс жив
- `CRITICAL`: серьёзные сбои

### Цвета (для терминала):

| Уровень     | Цвет        |
|-------------|-------------|
| DEBUG       | Серый       |
| INFO        | Зелёный     |
| WARNING     | Жёлтый      |
| ERROR       | Красный     |
| CRITICAL    | Красный фон |

### Формат логов:

```
2025-05-16 13:44:21 | INFO     | AGGREGATOR | Агрегация завершена: BTCUSDT / M15
```

### Пример использования:

```python
import logging
log = logging.getLogger("INDICATORS")
log.info("EMA(9) рассчитан")
log.warning("Недостаточно свечей для RSI(14)")
```

---

## 📎 Используется в

- `feed_v4_main.py`
- `feed_and_aggregate.py`
- `indicators_main.py`
- `snapshot.py`
- всех плагинах индикаторов

---

## ✅ Резюме

- `infra.py` — это технический центр системы
- Обеспечивает безопасный запуск, доступ к данным и читаемый вывод
- Конфигурация единая, через Render
- Делает проект гибким, расширяемым и поддерживаемым

---