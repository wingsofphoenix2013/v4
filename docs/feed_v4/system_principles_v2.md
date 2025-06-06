# 🧭 Назначение и принципы системы

---

## 📌 Общее назначение

Система предназначена для:
- приёма рыночных свечей M1 с биржи Binance;
- автоматической агрегации M5 и M15;
- расчёта индикаторов технического анализа;
- трансляции событий по готовности данных;
- хранения всей информации в PostgreSQL;
- поддержки стратегий в реальном времени через Redis.

---

## ⚙️ Ключевые принципы архитектуры

---

### 1. Реактивность и отказоустойчивость

- Все события (готовность свечи, расчёт индикатора) публикуются в Redis Streams.
- Используются Redis Consumer Groups — **каждый обработчик получает свою очередь**.
- `indicators_main.py` — **один из потенциально многих потребителей** событий `ohlcv_<tf>_ready`.
  - Другие возможные: сигнальные модули, алерты, стратегии, UI.

---

### 2. Redis — runtime шина и кеш

- Свечи (M1, M5, M15) хранятся в RedisTimeSeries.
- Индикаторы хранятся:
  - в Redis Hash — последнее значение;
  - в PostgreSQL — полная история;
  - **дополнительно (по необходимости)** в RedisTimeSeries, если стратегии используют последние N значений.
- markPrice обновляется ежесекундно в Redis String (`price:<symbol>`).

---

### 3. PostgreSQL — источник правды

- Все свечи, индикаторы и события сохраняются в PG.
- Redis может быть очищен, но всё восстановится из PG при запуске.
- Модуль `restore_redis_from_pg.py` выполняет полное восстановление при старте.

---

### 4. Минимизация дублирования и шума

- Все записи в PG выполняются через `ON CONFLICT DO UPDATE/DO NOTHING` — безопасно для повторов.
- Streams несут только сигналы и обрабатываются один раз в каждой группе.

---

### 5. Стратегии и потребители — внешние, независимые

- Могут использовать:
  - Redis Hash или TS для быстрого доступа;
  - PostgreSQL для исторического анализа;
- Подписываются на нужные потоки Redis самостоятельно.

---

### 6. Поддержка нескольких ТФ и параметров

- Поддерживаются таймфреймы: M1, M5, M15.
- Индикаторы рассчитываются по `indicator_instances_v4` + `parameters_v4`.
- Каждый индикатор может иметь собственные параметры (`length`, `mode`, `threshold`) и работать на разных ТФ.

---

## ✅ Сводка

Архитектура строится по принципам:
- событийной реактивности (Streams),
- надёжности хранения (PostgreSQL),
- скорости и доступности (Redis),
- расширяемости (Consumer Groups, независимые модули),
- предсказуемости (точки синхронизации и восстановления).

Система готова к промышленному применению с многопоточностью, масштабированием и минимальной задержкой.

---