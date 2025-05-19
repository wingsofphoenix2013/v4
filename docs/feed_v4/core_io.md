# 🔄 `core_io.py` — модуль прикладного ввода-вывода

`core_io.py` реализует универсальный слой взаимодействия с Redis и PostgreSQL. Этот модуль отвечает за прикладную запись и чтение свечей, индикаторов и событий в рамках торговой системы.

---

## 📌 Назначение

- Централизует всю логику записи/чтения данных
- Работает с Redis TimeSeries, Hash, String, Stream
- Обеспечивает резервную запись в PostgreSQL
- Используется всеми воркерами: агрегация, индикаторы, снапшоты

---

## 🧱 Группы функций

### 1. 📥 Чтение OHLCV

```python
async def load_ohlcv_from_redis(symbol, tf, count, redis)
```
Получает N последних свечей из RedisTimeSeries по ключу `ohlcv:<symbol>:<tf>`

```python
async def load_ohlcv_from_pg(symbol, tf, count, pg_pool)
```
Резервное чтение OHLCV из таблицы `ohlcv2_<tf>`, если Redis недоступен или пуст

---

### 2. 📤 Запись OHLCV

```python
async def save_ohlcv_to_redis(symbol, tf, candle, redis)
```
Сохраняет свечу в RedisTimeSeries (TS.ADD)

```python
async def save_ohlcv_to_pg(symbol, tf, candle, pg_pool)
```
Записывает OHLCV в PostgreSQL — таблицы `ohlcv2_m1`, `ohlcv2_m5`, `ohlcv2_m15` и т.д.

---

### 3. 📈 Работа с индикаторами

```python
async def save_indicator(symbol, tf, name, value, redis, pg_pool)
```
- Сохраняет значение индикатора в Redis (Hash или String)
- Дублирует в таблицу `indicator_values_v2`

```python
async def get_indicator(symbol, tf, name, redis)
```
- Получает значение индикатора из Redis по ключу

---

### 4. 🔁 Публикация событий

```python
async def publish_event(channel, payload, redis)
```
Отправка PubSub события (например, `ohlcv_m1_ready`)

```python
async def publish_stream(stream, payload, redis)
```
Добавление события в Redis Stream (`XADD`), например `ohlcv_m5_ready`

---

## 🧩 Используется в

| Модуль               | Цель                                      |
|----------------------|--------------------------------------------|
| `feed_and_aggregate.py` | сохранение M1, публикация в Redis         |
| `aggregator_worker.py`  | чтение M1, запись M5                     |
| `indicators_main.py`    | чтение свечей, запись индикаторов        |
| `snapshot.py`           | резервная запись состояния               |

---

## 🧠 Роль в системе

| Функция             | Да/Нет |
|---------------------|--------|
| Запись данных       | ✅     |
| Чтение данных       | ✅     |
| Подключение         | ❌ (использует `infra.py`) |
| Бизнес-логика       | ❌     |

---

## 📌 Пример сигнатур

```python
async def save_ohlcv_to_redis(symbol: str, tf: str, candle: dict, redis) -> None
async def load_ohlcv_from_pg(symbol: str, tf: str, count: int, pg_pool) -> List[dict]
async def save_indicator(symbol: str, tf: str, name: str, value: float, redis, pg_pool) -> None
async def publish_stream(stream: str, payload: dict, redis) -> None
```

---

`core_io.py` — это шлюз между логикой и хранилищами, отвечающий за прикладной I/O всех ключевых данных в системе.