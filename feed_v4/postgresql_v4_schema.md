# 🗄️ PostgreSQL в архитектуре системы (v4)

---

## 📌 Роль PostgreSQL

PostgreSQL — долговременное хранилище и точка правды. Все ключевые данные, поступающие через WebSocket или рассчитываемые на лету, сохраняются в базу в реальном времени:

- OHLCV свечи всех таймфреймов (до M15);
- Значения индикаторов;
- Конфигурации активных расчётов;
- Журнал недостающих свечей;
- Системные сообщения и ошибки.

---

## 🧱 Таблицы и их структуры

---

### 🔹 `ohlcv4_m1`, `ohlcv4_m5`, `ohlcv4_m15`

```sql
CREATE TABLE ohlcv4_m1 (
    symbol       TEXT NOT NULL,
    open_time    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open         NUMERIC NOT NULL,
    high         NUMERIC NOT NULL,
    low          NUMERIC NOT NULL,
    close        NUMERIC NOT NULL,
    volume       NUMERIC NOT NULL,
    source       TEXT DEFAULT 'stream',
    inserted_at  TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    PRIMARY KEY (symbol, open_time)
);
```

✅ Индексы:
- PRIMARY KEY `(symbol, open_time)` — ускоряет UPSERT и агрегации
- Аналогично для `ohlcv4_m5`, `ohlcv4_m15`

---

### 🔹 `missing_m1_log_v4`

```sql
CREATE TABLE missing_m1_log_v4 (
    symbol      TEXT NOT NULL,
    open_time   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    detected_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    fixed       BOOLEAN DEFAULT false,
    fixed_at    TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY (symbol, open_time)
);
```

📘 Используется для восстановления пропущенных свечей по REST API.

---

### 🔹 `indicator_instances_v4`

```sql
CREATE TABLE indicator_instances_v4 (
    id             SERIAL PRIMARY KEY,
    indicator      TEXT NOT NULL,
    symbol         TEXT NOT NULL,
    timeframe      TEXT NOT NULL,
    enabled        BOOLEAN DEFAULT true,
    stream_publish BOOLEAN DEFAULT false,
    created_at     TIMESTAMP DEFAULT now()
);
```

📘 Каждая строка — одна активная логика расчёта (например, EMA(9) на M5 для BTCUSDT).

---

### 🔹 `indicator_parameters_v4`

```sql
CREATE TABLE indicator_parameters_v4 (
    id          SERIAL PRIMARY KEY,
    instance_id INTEGER NOT NULL REFERENCES indicator_instances_v4(id) ON DELETE CASCADE,
    param       TEXT NOT NULL,
    value       TEXT NOT NULL,
    UNIQUE(instance_id, param)
);
```

📘 Связанные параметры для каждой расчётной единицы — например, `length=14`.

---

### 🔹 `indicator_values_v4`

```sql
CREATE TABLE indicator_values_v4 (
    id           SERIAL PRIMARY KEY,
    instance_id  INTEGER NOT NULL REFERENCES indicator_instances_v4(id) ON DELETE CASCADE,
    symbol       TEXT NOT NULL,
    open_time    TIMESTAMP NOT NULL,
    param_name   TEXT NOT NULL,
    value        DOUBLE PRECISION NOT NULL,
    updated_at   TIMESTAMP DEFAULT now(),
    UNIQUE(instance_id, symbol, open_time, param_name)
);
```

✅ Индексы:
- `symbol, open_time`
- `instance_id`
- `param_name`

📘 Хранит рассчитанные значения индикаторов (в т.ч. EMA, RSI, и т.п.)

---

### 🔹 `system_log_v4`

```sql
CREATE TABLE system_log_v4 (
    id         SERIAL PRIMARY KEY,
    timestamp  TIMESTAMP DEFAULT now(),
    module     TEXT NOT NULL,
    level      TEXT NOT NULL,
    message    TEXT NOT NULL,
    details    JSONB
);
```

📘 Лог ошибок и событий для мониторинга. Можно подключить Telegram/Email-оповещения.

---

## ✅ Вывод

- Все данные пишутся в PostgreSQL **в момент получения или расчёта**.
- Redis используется как кеш и система передачи событий.
- PostgreSQL = гарантия консистентности и восстановления.

---