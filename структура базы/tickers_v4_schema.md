# Таблица tickers_v4

Справочник доступных тикеров с техническими параметрами и разрешениями на торговлю. Таблица используется в движке v4.

## Структура

| Поле              | Тип       | Описание                                             |
| ----------------- | --------- | ---------------------------------------------------- |
| `id`              | serial    | Первичный ключ                                       |
| `symbol`          | text      | Уникальный тикер (например, `BTCUSDT`)               |
| `status`          | text      | Статус тикера: `'enabled'` или `'disabled'`          |
| `tradepermission` | text      | Разрешение на торговлю: `'enabled'` или `'disabled'` |
| `is_active`       | boolean   | Диагностическое поле: поступают ли фактически данные |
| `precision_price` | integer   | Кол-во знаков после запятой в цене                   |
| `precision_qty`   | integer   | Кол-во знаков после запятой в объёме                 |
| `min_qty`         | numeric   | Минимально допустимое количество                     |
| `created_at`      | timestamp | Дата создания записи                                 |

## Ограничения

* `id` — PRIMARY KEY
* `symbol` — UNIQUE

## Индексы

* `tickers_v4_pkey` — по `id` (PRIMARY KEY)
* `tickers_v4_symbol_key` — по `symbol` (UNIQUE)
* `idx_tickers_v4_status` — по `status`
* `idx_tickers_v4_tradepermission` — по `tradepermission`
* `idx_tickers_v4_is_active` — по `is_active`
