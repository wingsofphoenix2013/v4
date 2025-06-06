# 🔄 Модуль `restore_redis_from_pg.py` — автоматическое восстановление Redis из PostgreSQL

---

## 📌 Назначение

Модуль `restore_redis_from_pg.py` автоматически восстанавливает содержимое Redis из PostgreSQL:

- свечи M1/M5/M15 → в RedisTimeSeries;
- значения индикаторов → в Redis Hash.

Это необходимо после сброса Redis, миграции или запуска нового инстанса, чтобы все компоненты системы могли немедленно продолжать работу.

---

## 🧱 Что восстанавливает

| Объект        | Redis ключ                  | Источник в PostgreSQL     |
|---------------|-----------------------------|----------------------------|
| Свечи M1      | `ohlcv:<symbol>:m1`         | `ohlcv4_m1`                |
| Свечи M5      | `ohlcv:<symbol>:m5`         | `ohlcv4_m5`                |
| Свечи M15     | `ohlcv:<symbol>:m15`        | `ohlcv4_m15`               |
| Индикаторы    | `indicator:<symbol>:<tf>`   | `indicator_values_v4`     |

*`price:<symbol>` не восстанавливаются — приходят по WebSocket заново.*

---

## ⚙️ Механизм работы

1. Загружается список тикеров из таблицы `tickers`, где `status = 'enabled'`.
2. Для каждого тикера и таймфрейма:
   - Загружаются последние 250 свечей из соответствующей таблицы PG.
   - Выполняется `TS.ADD` в Redis.
3. Загружаются последние значения индикаторов по `instance_id` + `symbol` + `param_name`.
   - Выполняется `HSET` в Redis Hash.

---

## 🔧 Где вызывается

```python
# feed_v4_main.py
await restore_redis_from_pg(pg_pool, redis)
```

→ вызывается **один раз при старте системы**, до запуска воркеров.

---

## ✅ Безопасность и устойчивость

- Повторный вызов не вреден: `TS.ADD` и `HSET` безопасны;
- Восстановление можно сделать «всегда» или по условию:
  ```python
  if not await redis.exists("ohlcv:BTCUSDT:m1"):
      await restore_redis_from_pg(...)
  ```

---

## ⏱ Производительность

- Восстановление ~250 свечей × 3 ТФ × 50 тикеров = ~37 500 операций
- Время выполнения: 2–5 секунд (RedisCloud)

---

## 🔐 Почему это критично

- Redis не содержит историю — только последние значения;
- При сбросе Redis воркеры не могут продолжить без актуальных данных;
- Без `restore_redis_from_pg` потребуются ручные правки или сброс состояния всей системы.

---

## ✅ Вывод

Модуль `restore_redis_from_pg.py` делает Redis:

| Свойство        | Обеспечивается |
|-----------------|----------------|
| Самовосстанавливаемым | ✅        |
| Консистентным с PG     | ✅        |
| Независимым от состояния RedisCloud | ✅  |

Запускать при старте системы — обязательно.

---