# 📂 `indicators_main.py` — расчёт индикаторов по готовым свечам

---

## 📌 Назначение

`indicators_main.py` — воркер, который:

- Подписывается на Redis Stream `ohlcv_<tf>_ready`,
- Загружает свечи из RedisTimeSeries по символу и таймфрейму,
- Запускает соответствующие индикаторы (EMA, RSI, ATR, ...),
- Сохраняет результаты в Redis и PostgreSQL,
- Публикует сообщение об успешном расчёте в Redis Stream `indicators_ready_stream`.

---

## 🧱 Основные функции

### 1. 🔔 Подписка на Redis Streams

- `XREAD` из каналов `ohlcv_m5_ready`, `ohlcv_m15_ready` и др.
- Каждое сообщение содержит:
  ```json
  {
    "symbol": "BTCUSDT",
    "interval": "m5",
    "open_time": "2025-05-16T10:40:00"
  }
  ```

---

### 2. 📥 Загрузка свечей для расчёта

- Загружается N последних свечей по таймфрейму и тикеру
- Используется RedisTimeSeries через `TS.RANGE` или функцию из `core_io.py`

---

### 3. 🧠 Выбор активных индикаторов

- Конфигурации индикаторов определяются заранее:
  - Например, `EMA(9)`, `RSI(14)`, `ATR(14)`
- Могут быть:
  - заданы жёстко в словаре,
  - или получены из Redis (`indicator_config:<symbol>:<tf>`)

---

### 4. 🧮 Расчёт индикаторов

- Каждый тип вызывается через отдельный модуль:
  ```python
  await process_ema(...)
  await process_rsi(...)
  await process_atr(...)
  ```
- Все обработчики получают:
  - `symbol`, `tf`, `open_time`
  - `candles` (DataFrame)
  - `params`, `precision_price`
  - `redis`, `pg_pool`, `stream_publish`

---

### 5. 💾 Сохранение результатов

- Redis:
  - `HSET indicator:<symbol>:<tf> param_name → value`
- PostgreSQL:
  - `INSERT INTO indicator_values_v2 (...)`
- Redis Stream:
  - `XADD indicators_ready_stream * {...}`

---

## 🧩 Используемые компоненты

- `core_io.py` — для I/O
- `infra.py` — Redis, PG, логика запуска
- `ema.py`, `rsi.py`, `atr.py`, ... — логика индикаторов

---

## 🔁 Поддержка таймфреймов

- Работает с любым `ohlcv_<tf>_ready`: M5, M15, M30, H1 и т.д.
- Каждое событие обрабатывается отдельно

---

## 🔐 Устойчивость

- Обёртка через `run_safe_loop(...)`
- Каждый расчёт — внутри `try/except`
- Ошибка в одном индикаторе не влияет на остальные

---

## 📎 Роль в системе

| Функция                | Да |
|------------------------|----|
| Подписка на события    | ✅ |
| Загрузка свечей        | ✅ |
| Расчёт индикаторов     | ✅ |
| Публикация результата  | ✅ |
| Поддержка параметров   | ✅ |

---