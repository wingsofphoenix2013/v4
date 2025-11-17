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


# 🔸 Вспомогательные функции для чисел и EMA-паттернов
def _to_decimal(value) -> Decimal | None:
    # безопасное приведение к Decimal
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _round4(value: Decimal | None) -> Decimal | None:
    # округление до 4 знаков после запятой
    if value is None:
        return None
    return value.quantize(Decimal("0.0001"))


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

    return _to_decimal(row["deposit"]) or Decimal("0")


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
    Возвращает словарь:
        {position_uid: (ema50, ema200)}

    Берём только:
    - param_type = 'indicator'
    - param_base = 'ema'
    - param_name IN ('ema50', 'ema200')
    - status = 'ok'
    - timeframe = tf
    """
    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ Пропуск fetch_ema_snapshot_batch: PG не инициализирован")
        return {}

    if not position_uids:
        return {}

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                position_uid,
                MAX(CASE
                        WHEN param_name = 'ema50'
                        THEN value_num
                        ELSE NULL
                    END) AS ema50,
                MAX(CASE
                        WHEN param_name = 'ema200'
                        THEN value_num
                        ELSE NULL
                    END) AS ema200
            FROM indicator_position_stat
            WHERE position_uid = ANY($1)
              AND timeframe = $2
              AND param_type = 'indicator'
              AND status = 'ok'
              AND param_base = 'ema'
              AND param_name IN ('ema50', 'ema200')
            GROUP BY position_uid
            """,
            position_uids,
            tf,
        )

    result: Dict[str, Tuple[Decimal, Decimal]] = {}

    for r in rows:
        uid = str(r["position_uid"])
        ema50 = r["ema50"]
        ema200 = r["ema200"]

        # если хотя бы один из EMA отсутствует, пропускаем
        if ema50 is None or ema200 is None:
            continue

        ema50_d = _to_decimal(ema50)
        ema200_d = _to_decimal(ema200)
        if ema50_d is None or ema200_d is None:
            continue

        result[uid] = (ema50_d, ema200_d)

    log.debug(
        "📦 fetch_ema_snapshot_batch: tf=%s, uidов запрошено=%d, найдено снапшотов=%d",
        tf,
        len(position_uids),
        len(result),
    )

    return result


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

    # вычисление winrate (wins / total) и ROI (pnl_total / deposit)
    if trades_count > 0:
        winrate = Decimal(win_count) / Decimal(trades_count)
    else:
        winrate = None

    if deposit and deposit > 0:
        roi = pnl_total / deposit
    else:
        roi = None

    # нормализация точности до 4 знаков
    pnl_total = _round4(pnl_total) or Decimal("0.0000")
    winrate = _round4(winrate) if winrate is not None else None
    roi = _round4(roi) if roi is not None else None

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
        str(_round4(tf_stats_summary["pnl_total"])),
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
        str(_round4(deposit)),
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

    total_pnl_rounded = _round4(total_pnl)
    total_roi_rounded = _round4(total_roi) if total_roi is not None else None

    log.info(
        "📊 AUD_EMA SUMMARY: %s — trades=%d, win=%d, lose=%d, pnl_total=%s, roi=%s",
        title,
        total_trades,
        total_win,
        total_lose,
        str(total_pnl_rounded),
        str(total_roi_rounded) if total_roi_rounded is not None else "NULL",
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