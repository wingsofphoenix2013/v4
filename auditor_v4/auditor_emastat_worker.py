# 🔸 auditor_emastat_worker.py — аудит EMA-паттернов (PRICE/EMA50/EMA200) по закрытым позициям MW-стратегий

# 🔸 Импорты
import asyncio
import logging
from decimal import Decimal
from typing import Dict, List, Tuple

import auditor_infra as infra
from auditor_config import load_active_mw_strategies

# 🔸 Логгер
log = logging.getLogger("AUD_EMASTAT")

# 🔸 Константы
EMA_EPS_PCT = Decimal("0.00025")  # 0.025% как доля
TF_LIST = ("m5", "m15", "h1")


# 🔸 Вспомогательные функции для EMA-паттернов
def _to_decimal(value) -> Decimal:
    # безопасное приведение к Decimal
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def compute_ema_pattern(price, ema50, ema200) -> str:
    # условия достаточности
    price_d = _to_decimal(price)
    ema50_d = _to_decimal(ema50)
    ema200_d = _to_decimal(ema200)

    if price_d is None or ema50_d is None or ema200_d is None:
        return "UNKNOWN"

    items = [
        ("PRICE", price_d, 0),
        ("EMA50", ema50_d, 1),
        ("EMA200", ema200_d, 2),
    ]

    # сортировка по значению (от меньшего к большему)
    items.sort(key=lambda x: x[1])

    # разбиение на группы равенства с учётом эпсилона
    groups: List[List[Tuple[str, Decimal, int]]] = []
    current_group: List[Tuple[str, Decimal, int]] = [items[0]]

    for name, value, tiebreak in items[1:]:
        # сравнение с последним элементом текущей группы
        _, prev_value, _ = current_group[-1]
        # относительная разница
        max_abs = max(abs(value), abs(prev_value))
        if max_abs == 0:
            rel_diff = Decimal("0")
        else:
            rel_diff = abs(value - prev_value) / max_abs

        if rel_diff <= EMA_EPS_PCT:
            # считаем равными → в ту же группу
            current_group.append((name, value, tiebreak))
        else:
            groups.append(current_group)
            current_group = [(name, value, tiebreak)]

    groups.append(current_group)

    # нормализация порядка внутри группы: PRICE → EMA50 → EMA200
    for g in groups:
        g.sort(key=lambda x: x[2])

    # сбор строки-паттерна
    parts = []
    for g in groups:
        labels = [name for name, _, _ in g]
        parts.append(" = ".join(labels))

    pattern = " > ".join(parts)
    return pattern


# 🔸 Загрузка депозита стратегии
async def load_strategy_deposit(strategy_id: int) -> Decimal:
    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ Пропуск load_strategy_deposit: PG не инициализирован")
        return Decimal("0")

    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT deposit
            FROM strategies_v4
            WHERE id = $1
            """,
            strategy_id,
        )

    if row is None or row["deposit"] is None:
        return Decimal("0")

    return _to_decimal(row["deposit"])


# 🔸 Загрузка закрытых позиций стратегии
async def load_closed_positions_for_strategy(strategy_id: int):
    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ Пропуск load_closed_positions_for_strategy: PG не инициализирован")
        return []

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                position_uid,
                direction,
                pnl,
                entry_price
            FROM positions_v4
            WHERE strategy_id = $1
              AND status = 'closed'
              AND direction IN ('long','short')
            """,
            strategy_id,
        )

    return rows


# 🔸 Загрузка EMA50/EMA200 для батча позиций и TF
async def fetch_ema_snapshot_batch(position_uids: List[str], tf: str) -> Dict[str, Tuple[Decimal, Decimal]]:
    """
    Ожидается, что функция вернёт словарь:
        {position_uid: (ema50, ema200)}

    Реализация зависит от фактической схемы indicator_position_stat.
    Здесь оставлен каркас, который нужно адаптировать под реальную структуру данных.

    Пример ожиданий:
    - indicator_position_stat содержит снапшоты RAW по всем EMA,
    - можно выбрать EMA50/EMA200 по position_uid и tf и свести в одну строку.

    Сейчас функция реализована как заглушка и должна быть переписана под твою БД.
    """
    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ Пропуск fetch_ema_snapshot_batch: PG не инициализирован")
        return {}

    if not position_uids:
        return {}

    # Здесь нужно заменить логику на реальный SELECT из indicator_position_stat
    # Ниже — только структурный каркас, чтобы файл был рабочим, но не делает реальных выборок.
    log.info(
        "⚠️ fetch_ema_snapshot_batch: заглушка, необходимо реализовать выбор EMA50/EMA200 "
        "из indicator_position_stat для tf=%s (uidов: %d)",
        tf,
        len(position_uids),
    )

    # Возвращаем пустой словарь, чтобы воркер корректно отработал структуру,
    # но фактически ничего не насчитает, пока не будет реализован SELECT.
    return {}


# 🔸 Upsert агрегатов в auditor_emastat_details
async def upsert_emastat_row(
    strategy_id: int,
    direction: str,
    tf: str,
    ema_pattern: str,
    trades_count: int,
    win_count: int,
    lose_count: int,
    pnl_total: Decimal,
    deposit: Decimal,
):
    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ Пропуск upsert_emastat_row: PG не инициализирован")
        return

    # вычисление winrate и ROI
    if lose_count > 0:
        winrate = Decimal(win_count) / Decimal(lose_count)
    else:
        winrate = None

    if deposit and deposit > 0:
        roi = pnl_total / deposit
    else:
        roi = None

    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO auditor_emastat_details (
                strategy_id,
                direction,
                tf,
                ema_pattern,
                trades_count,
                win_count,
                lose_count,
                winrate,
                pnl_total,
                roi,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4,
                $5, $6, $7,
                $8, $9, $10,
                NOW(), NOW()
            )
            ON CONFLICT (strategy_id, direction, tf, ema_pattern)
            DO UPDATE SET
                trades_count = auditor_emastat_details.trades_count + EXCLUDED.trades_count,
                win_count    = auditor_emastat_details.win_count    + EXCLUDED.win_count,
                lose_count   = auditor_emastat_details.lose_count   + EXCLUDED.lose_count,
                winrate      = EXCLUDED.winrate,
                pnl_total    = auditor_emastat_details.pnl_total    + EXCLUDED.pnl_total,
                roi          = EXCLUDED.roi,
                updated_at   = NOW()
            """,
            strategy_id,
            direction,
            tf,
            ema_pattern,
            trades_count,
            win_count,
            lose_count,
            winrate,
            pnl_total,
            roi,
        )


# 🔸 Обработка одной стратегии и одного TF
async def process_strategy_tf(
    strategy_id: int,
    strategy_meta: dict,
    deposit: Decimal,
    tf: str,
    positions_rows,
) -> dict:
    # начальные агрегаты по TF
    tf_stats_summary = {
        "tf": tf,
        "trades_total": 0,
        "win_total": 0,
        "lose_total": 0,
        "pnl_total": Decimal("0"),
    }

    if not positions_rows:
        log.info(
            "ℹ️ AUD_EMA: strategy_id=%d, tf=%s — нет закрытых позиций для обработки",
            strategy_id,
            tf,
        )
        return tf_stats_summary

    # формируем список uid позиций
    position_uids = [str(r["position_uid"]) for r in positions_rows]

    # загружаем EMA-срезы для батча
    ema_map = await fetch_ema_snapshot_batch(position_uids, tf)

    # агрегаты по (direction, pattern)
    # ключ: (direction, ema_pattern)
    stats: Dict[Tuple[str, str], Dict[str, object]] = {}

    for r in positions_rows:
        position_uid = str(r["position_uid"])
        direction = str(r["direction"])
        pnl_raw = r["pnl"]
        entry_price_raw = r["entry_price"]

        if direction not in ("long", "short"):
            continue

        pnl = _to_decimal(pnl_raw) if pnl_raw is not None else Decimal("0")
        entry_price = _to_decimal(entry_price_raw)
        if entry_price is None:
            continue

        # берём EMA50/EMA200 для этой позиции и TF
        ema_pair = ema_map.get(position_uid)
        if not ema_pair:
            # нет снапшота EMA для этой позиции/TF — пропускаем
            continue

        ema50, ema200 = ema_pair
        ema50_d = _to_decimal(ema50)
        ema200_d = _to_decimal(ema200)
        if ema50_d is None or ema200_d is None:
            continue

        ema_pattern = compute_ema_pattern(entry_price, ema50_d, ema200_d)
        key = (direction, ema_pattern)

        if key not in stats:
            stats[key] = {
                "trades_count": 0,
                "win_count": 0,
                "lose_count": 0,
                "pnl_total": Decimal("0"),
            }

        item = stats[key]
        item["trades_count"] += 1
        item["pnl_total"] += pnl

        # классификация win/lose
        if pnl > 0:
            item["win_count"] += 1
            tf_stats_summary["win_total"] += 1
        elif pnl < 0:
            item["lose_count"] += 1
            tf_stats_summary["lose_total"] += 1

        tf_stats_summary["trades_total"] += 1
        tf_stats_summary["pnl_total"] += pnl

    # запись в БД агрегатов по каждому паттерну
    for (direction, ema_pattern), agg in stats.items():
        await upsert_emastat_row(
            strategy_id=strategy_id,
            direction=direction,
            tf=tf,
            ema_pattern=ema_pattern,
            trades_count=agg["trades_count"],
            win_count=agg["win_count"],
            lose_count=agg["lose_count"],
            pnl_total=agg["pnl_total"],
            deposit=deposit,
        )

    log.info(
        "✅ AUD_EMA: strategy_id=%d, tf=%s — обработано сделок=%d, win=%d, lose=%d, pnl_total=%s",
        strategy_id,
        tf,
        tf_stats_summary["trades_total"],
        tf_stats_summary["win_total"],
        tf_stats_summary["lose_total"],
        str(tf_stats_summary["pnl_total"]),
    )

    return tf_stats_summary


# 🔸 Обработка одной стратегии по всем TF (последовательно по стратегиям, параллельно по TF)
async def process_strategy(strategy_id: int, strategy_meta: dict):
    # загрузка депозита и закрытых позиций
    deposit = await load_strategy_deposit(strategy_id)
    positions_rows = await load_closed_positions_for_strategy(strategy_id)

    name = strategy_meta.get("name") or f"sid_{strategy_id}"
    human = strategy_meta.get("human_name") or ""
    title = f'{strategy_id} "{name}"' if not human else f'{strategy_id} "{name}" ({human})'

    if not positions_rows:
        log.info("ℹ️ AUD_EMA: %s — нет закрытых позиций, стратегия пропущена", title)
        return

    log.info(
        "🚀 AUD_EMA: старт обработки стратегии %s — закрытых позиций: %d, депозит: %s",
        title,
        len(positions_rows),
        str(deposit),
    )

    # параллельная обработка TF внутри стратегии
    tasks = [
        process_strategy_tf(strategy_id, strategy_meta, deposit, tf, positions_rows)
        for tf in TF_LIST
    ]
    tf_summaries = await asyncio.gather(*tasks)

    # резюмирующий лог по стратегии
    total_trades = sum(s["trades_total"] for s in tf_summaries)
    total_win = sum(s["win_total"] for s in tf_summaries)
    total_lose = sum(s["lose_total"] for s in tf_summaries)
    total_pnl = sum((s["pnl_total"] for s in tf_summaries), Decimal("0"))

    if deposit and deposit > 0:
        total_roi = total_pnl / deposit
    else:
        total_roi = None

    log.info(
        "📊 AUD_EMA SUMMARY: %s — trades=%d, win=%d, lose=%d, pnl_total=%s, roi=%s",
        title,
        total_trades,
        total_win,
        total_lose,
        str(total_pnl),
        str(total_roi) if total_roi is not None else "NULL",
    )


# 🔸 Главная точка входа воркера (одноразовый запуск)
async def run_emastat_worker():
    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ AUD_EMA: PG не инициализирован, воркер не будет запущен")
        return

    # загрузка активных MW-стратегий
    strategies = await load_active_mw_strategies()
    if not strategies:
        log.info("ℹ️ AUD_EMA: нет активных MW-стратегий для обработки")
        return

    log.info("📦 AUD_EMA: найдено MW-стратегий для аудита: %d", len(strategies))

    # последовательно обрабатываем стратегии
    for sid in sorted(strategies.keys()):
        await process_strategy(sid, strategies[sid])

    log.info("✅ AUD_EMA: обработка всех стратегий завершена")