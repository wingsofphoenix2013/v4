# 🗄️ PostgreSQL (v4) — Структура таблиц и их назначение

---

## 📋 Таблица `ohlcv4_m1`

Хранит поступающие с биржи M1-свечи. Аналогично устроены `ohlcv4_m5` и `ohlcv4_m15`.

---

### 📐 Структура

| Поле        | Тип         | Обязательное | Описание                         |
|-------------|--------------|--------------|----------------------------------|
| `symbol`    | TEXT         | ✅            | Тикер, например `BTCUSDT`        |
| `open_time` | TIMESTAMP    | ✅            | Время открытия свечи             |
| `open`      | NUMERIC      | ✅            | Цена открытия                    |
| `high`      | NUMERIC      | ✅            | Максимальная цена                |
| `low`       | NUMERIC      | ✅            | Минимальная цена                 |
| `close`     | NUMERIC      | ✅            | Цена закрытия                    |
| `volume`    | NUMERIC      | ✅            | Объём                            |
| `source`    | TEXT         | ❌ (default)  | Источник: `stream`, `api`, ...   |
| `inserted_at` | TIMESTAMP  | ❌ (default)  | Время вставки записи             |

---

### 🔐 Ограничения

- `PRIMARY KEY (symbol, open_time)`

---

### 🗂 Индексы

- `(symbol, open_time)` — основной ключ

---

## 📋 Таблица `missing_m1_log_v4`

Хранит записи о пропущенных свечах M1 и статус их восстановления.

---

### 📐 Структура

| Поле         | Тип       | Обязательное | Описание                                |
|--------------|-----------|--------------|-----------------------------------------|
| `symbol`     | TEXT      | ✅            | Тикер                                   |
| `open_time`  | TIMESTAMP | ✅            | Время свечи, которой не хватало         |
| `detected_at`| TIMESTAMP | ❌ (default)  | Когда обнаружен пропуск                 |
| `fixed`      | BOOLEAN   | ❌ (default)  | Восстановлена ли свеча                  |
| `fixed_at`   | TIMESTAMP | ❌           | Когда восстановлена                     |

---

### 🔐 Ограничения

- `PRIMARY KEY (symbol, open_time)`

---

## 📋 Таблица `indicator_instances_v4`

Описывает каждый активный расчёт индикатора на заданном тикере и таймфрейме.

---

### 📐 Структура

| Поле             | Тип       | Обязательное | Описание                                        |
|------------------|-----------|--------------|-------------------------------------------------|
| `id`             | SERIAL    | ✅            | Уникальный ID расчёта                          |
| `indicator`      | TEXT      | ✅            | Тип: `ema`, `rsi`, `atr`, ...                   |
| `symbol`         | TEXT      | ✅            | Тикер                                           |
| `timeframe`      | TEXT      | ✅            | Таймфрейм (`m5`, `m15`)                         |
| `enabled`        | BOOLEAN   | ❌ (default)  | Активен ли расчёт                              |
| `stream_publish` | BOOLEAN   | ❌ (default)  | Публиковать ли результат в Redis Stream        |
| `created_at`     | TIMESTAMP | ❌ (default)  | Когда был добавлен расчёт                      |

---

### 🔐 Ограничения

- `PRIMARY KEY (id)`

---

## 📋 Таблица `indicator_parameters_v4`

Хранит параметры индикаторов, привязанные к `indicator_instances_v4`.

---

### 📐 Структура

| Поле         | Тип     | Обязательное | Описание                                  |
|--------------|---------|--------------|-------------------------------------------|
| `id`         | SERIAL  | ✅            | Уникальный идентификатор параметра        |
| `instance_id`| INTEGER | ✅            | Ссылка на `indicator_instances_v4(id)`    |
| `param`      | TEXT    | ✅            | Имя параметра (`length`, `angle_up`, ...) |
| `value`      | TEXT    | ✅            | Значение параметра (в виде строки)        |

---

### 🔐 Ограничения

- `UNIQUE(instance_id, param)`
- `FOREIGN KEY (instance_id)` → `indicator_instances_v4(id)` ON DELETE CASCADE

---

## 📋 Таблица `indicator_values_v4`

Хранит результаты расчёта индикаторов по каждому `symbol`, `open_time`, `param_name`.

---

### 📐 Структура

| Поле         | Тип              | Обязательное | Описание                                          |
|--------------|------------------|--------------|---------------------------------------------------|
| `id`         | SERIAL           | ✅            | Уникальный ID                                      |
| `instance_id`| INTEGER          | ✅            | Ссылка на `indicator_instances_v4(id)`            |
| `symbol`     | TEXT             | ✅            | Тикер                                              |
| `open_time`  | TIMESTAMP        | ✅            | Метка начала свечи                                |
| `param_name` | TEXT             | ✅            | Название параметра (`value`, `trend`, ...)        |
| `value`      | DOUBLE PRECISION | ✅            | Рассчитанное значение                              |
| `updated_at` | TIMESTAMP        | ❌ (default)  | Метка времени обновления записи                   |

---

### 🔐 Ограничения

- `UNIQUE(instance_id, symbol, open_time, param_name)`
- `FOREIGN KEY (instance_id)` → `indicator_instances_v4(id)` ON DELETE CASCADE

---

### 🗂 Индексы

| Название индекса          | Поля                         |
|---------------------------|------------------------------|
| `idx_iv4_symbol_time`     | `(symbol, open_time)`        |
| `idx_iv4_instance_id`     | `(instance_id)`              |
| `idx_iv4_param_name`      | `(param_name)`               |

---

## 📋 Таблица `system_log_v4`

Логирует ошибки, предупреждения, инфосообщения из всех компонентов системы.

---

### 📐 Структура

| Поле       | Тип       | Обязательное | Описание                             |
|------------|-----------|--------------|--------------------------------------|
| `id`       | SERIAL    | ✅            | Уникальный ID                        |
| `timestamp`| TIMESTAMP | ❌ (default)  | Время записи                         |
| `module`   | TEXT      | ✅            | Название модуля (`AGGREGATOR`, ...)  |
| `level`    | TEXT      | ✅            | Уровень (`INFO`, `ERROR`, ...)       |
| `message`  | TEXT      | ✅            | Основной текст                       |
| `details`  | JSONB     | ❌           | Дополнительные поля / stacktrace     |

---

### 🔐 Ограничения

- `PRIMARY KEY (id)`

---

## ✅ Общие принципы

- Все данные пишутся в PostgreSQL при поступлении или расчёте
- Нет дублирующих процессов (`snapshot.py` — не используется)
- Redis служит только как runtime и кеш
- PostgreSQL = архив, источник восстановления и источник правды

---