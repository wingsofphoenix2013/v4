# 📦 План разработки `signals_v4_main.py`

---

## 🎯 Назначение

Новый компонент обработки входящих сигналов, полученных через Redis Stream `signals_stream`, с маршрутизацией по стратегиям и логированием в PostgreSQL.

---

## 🔹 ЭТАП 1: ИНИЦИАЛИЗАЦИЯ

### ✅ 1.1. Подключение Redis и PostgreSQL
- [ ] Использовать `infra.py`:
  - `init_pg_pool()`
  - `init_redis_client()`
  - `run_safe_loop()`

### ✅ 1.2. Глобальные словари
- [ ] `TICKERS = {}` — активные тикеры
- [ ] `STRATEGIES = {}` — активные стратегии
- [ ] `STRATEGY_SIGNALS = {}` — фразы → стратегии

---

## 🔹 ЭТАП 2: ЗАГРУЗКА ДАННЫХ

- [ ] Реализовать `load_tickers()`, `load_strategies()`, `load_strategy_signals()`
- [ ] Обновление каждые 5 минут
- [ ] Безопасный swap: `.clear(); .update(...)`

---

## 🔹 ЭТАП 3: REDIS STREAM (ПОДПИСКА)

### ✅ 3.1. Stream: `signals_stream`
- [ ] Создание группы `signal_processor`
- [ ] Подписка через `XREADGROUP`
- [ ] Подтверждение через `XACK`

---

## 🔹 ЭТАП 4: ОБРАБОТКА СИГНАЛОВ

### ✅ 4.1. Распаковка и валидация
- [ ] `symbol`, `message`, `bar_time`, `sent_at`
- [ ] Проверка:
  - тикер разрешён
  - фраза известна
  - стратегии активны

### ✅ 4.2. Поиск и маршрутизация
- [ ] Получить список стратегий по фразе
- [ ] Фильтрация (`enabled`, `archived`, `allow_open`)
- [ ] Параллельная обработка: `asyncio.gather(...)`

---

## 🔹 ЭТАП 5: ЛОГИРОВАНИЕ

### ✅ 5.1. Лог сигналов
- [ ] UID = hash(symbol + message + bar_time)
- [ ] Вставка в `signals_v2_log`, статус = `new/duplicate/ignored`
- [ ] Для каждой стратегии: запись в `signal_log_entries_v2`

### ✅ 5.2. Системный лог
- [ ] Через `log_system_event(...)`
- [ ] Уровни: `INFO`, `WARNING`, `ERROR`

---

## 🔹 ЭТАП 6: УСТОЙЧИВОСТЬ

- [ ] Обернуть через `run_safe_loop(...)`
- [ ] Проверка на зависшие сообщения (`XPENDING`)
- [ ] Отработка при сбоях Redis/PG

---

## 🧪 ТЕСТИРОВАНИЕ

| Что                     | Как проверяется                          |
|-------------------------|------------------------------------------|
| Тикеры/стратегии        | Загружаются и обновляются                |
| Stream обрабатывается   | `XADD` → лог создаётся                   |
| UID работает            | Повтор не вызывает новую запись         |
| Стратегии фильтруются   | Архив и allow_open работают              |
| Redis и PG отключаются  | Система не падает, логирует сбой         |

---

## ✅ Результат

`signals_v4_main.py`:
- слушает Redis Stream `signals_stream`;
- распределяет сигналы по стратегиям;
- логирует действия и ошибки;
- устойчив к сбоям и дублированию;
- масштабируем через Consumer Groups.

---