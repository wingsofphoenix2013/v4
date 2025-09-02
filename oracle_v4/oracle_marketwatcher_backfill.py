# 🔸 oracle_marketwatcher_backfill.py — Этап 2: часовик с задержкой старта (2 мин), батчи по 200, транзакционная догрузка (суммарные INFO-логи)

import os
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
import time

import infra

# 🔸 Параметры бэкофилла
START_DELAY_SEC = int(os.getenv("ORACLE_MW_BF_START_DELAY_SEC", "120"))   # 2 минуты до первого прогона
BF_BATCH_LIMIT = int(os.getenv("ORACLE_MW_BF_BATCH_LIMIT", "200"))        # размер батча
BF_SLEEP_BETWEEN_BATCH_MS = int(os.getenv("ORACLE_MW_BF_SLEEP_BETWEEN_BATCH_MS", "100"))
BF_MAX_RUN_SECONDS = int(os.getenv("ORACLE_MW_BF_MAX_RUN_SECONDS", "600"))  # бюджет времени на один проход (10 мин)

log = logging.getLogger("ORACLE_MW_BF")


# 🔸 Redis ключ публикации агрегата
def stat_key(strategy_id: int, direction: str, marker3_code: str) -> str:
    return f"oracle:mw:stat:{strategy_id}:{direction}:{marker3_code}"


# 🔸 Вспомогательные: floor времени (UTC) под TF
def _floor_to_step_utc(dt: datetime, minutes: int) -> datetime:
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    base = dt.replace(second=0, microsecond=0)
    floored_minute = (base.minute // minutes) * minutes
    return base.replace(minute=floored_minute)


def _label_from_code(code: int) -> str:
    mapping = {
        0: "F_CONS", 1: "F_EXP", 2: "F_DRIFT",
        3: "U_ACCEL", 4: "U_STABLE", 5: "U_DECEL",
        6: "D_ACCEL", 7: "D_STABLE", 8: "D_DECEL",
    }
    return mapping.get(code, "N/A")


# 🔸 Транзакционная обработка одной позиции (идемпотентность внутри)
async def _process_one_position(conn, position_uid: str) -> tuple[bool, str | None]:
    # 1) Позиция FOR UPDATE
    pos = await conn.fetchrow("""
        SELECT p.id, p.strategy_id, p.symbol, p.direction, p.created_at, p.pnl, p.status,
               COALESCE(p.mrk_watcher_checked, false) AS checked
        FROM positions_v4 p
        WHERE p.position_uid = $1
        FOR UPDATE
    """, position_uid)
    if not pos:
        return False, "not_found"

    if pos["status"] != "closed":
        return False, "not_closed"

    if pos["checked"]:
        return False, "already_checked"

    # 2) Стратегия активна и market_watcher=true?
    strat = await conn.fetchrow("""
        SELECT id, enabled, COALESCE(market_watcher, false) AS mw
        FROM strategies_v4
        WHERE id = $1
    """, int(pos["strategy_id"]))
    if not strat or not strat["enabled"] or not strat["mw"]:
        return False, "strategy_inactive"

    # 3) Вычисляем bar_open для m5/m15/h1 по created_at (UTC)
    created_at: datetime = pos["created_at"]
    m5_open  = _floor_to_step_utc(created_at, 5)
    m15_open = _floor_to_step_utc(created_at, 15)
    h1_open  = _floor_to_step_utc(created_at, 60)

    # 4) Пробуем взять маркеры ровно по bar_open
    symbol = pos["symbol"]
    rows = await conn.fetch("""
        SELECT timeframe, regime_code
        FROM indicator_marketwatcher_v4
        WHERE symbol = $1
          AND timeframe = ANY($2::text[])
          AND open_time = ANY($3::timestamp[])
    """, symbol, ["m5","m15","h1"], [m5_open, m15_open, h1_open])
    markers = {r["timeframe"]: r["regime_code"] for r in rows}
    if not all(tf in markers for tf in ("m5","m15","h1")):
        # маркеры ещё не готовы — отложим до следующего цикла
        return False, "markers_missing"

    # 5) Формируем маркер и лейбл
    m5_code, m15_code, h1_code = int(markers["m5"]), int(markers["m15"]), int(markers["h1"])
    marker3_code = f"{m5_code}-{m15_code}-{h1_code}"
    marker3_label = f"{_label_from_code(m5_code)}-{_label_from_code(m15_code)}-{_label_from_code(h1_code)}"

    # 6) Пересчёт агрегата (Python, Decimal, 4 знака)
    direction: str = pos["direction"]
    pnl = Decimal(str(pos["pnl"])).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    is_win = 1 if pnl > Decimal("0") else 0

    # текущее состояние агрегата FOR UPDATE
    stat = await conn.fetchrow("""
        SELECT closed_trades, won_trades, pnl_sum
        FROM positions_marketwatcher_stat
        WHERE strategy_id = $1 AND marker3_code = $2 AND direction = $3
        FOR UPDATE
    """, int(pos["strategy_id"]), marker3_code, direction)

    if stat:
        closed_trades = int(stat["closed_trades"]) + 1
        won_trades = int(stat["won_trades"]) + is_win
        pnl_sum = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    else:
        closed_trades = 1
        won_trades = is_win
        pnl_sum = pnl

    winrate = (Decimal(won_trades) / Decimal(closed_trades)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    avg_pnl = (pnl_sum / Decimal(closed_trades)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    # 7) UPSERT агрегата
    await conn.execute("""
        INSERT INTO positions_marketwatcher_stat
          (strategy_id, marker3_code, marker3_label, direction,
           closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW())
        ON CONFLICT (strategy_id, marker3_code, direction)
        DO UPDATE SET
          closed_trades = $5,
          won_trades    = $6,
          pnl_sum       = $7,
          winrate       = $8,
          avg_pnl       = $9,
          updated_at    = NOW()
    """,
    int(pos["strategy_id"]), marker3_code, marker3_label, direction,
    closed_trades, won_trades, str(pnl_sum), str(winrate), str(avg_pnl))

    # 8) Отмечаем позицию как учтённую
    await conn.execute("""
        UPDATE positions_v4
        SET mrk_watcher_checked = true
        WHERE position_uid = $1
    """, position_uid)

    # 9) Публикуем Redis-ключ (подробности — в debug)
    try:
        value = json.dumps({
            "closed_trades": closed_trades,
            "winrate": float(winrate)
        })
        await infra.redis_client.set(
            stat_key(int(pos["strategy_id"]), direction, marker3_code),
            value
        )
        log.debug("[BF-AGG] strat=%s dir=%s marker=%s → closed=%d won=%d winrate=%.4f avg_pnl=%s",
                  pos["strategy_id"], direction, marker3_code,
                  closed_trades, won_trades, float(winrate), str(avg_pnl))
    except Exception as e:
        log.exception("❌ Ошибка публикации Redis-ключа (BF) для %s/%s/%s: %s",
                      pos["strategy_id"], direction, marker3_code, e)

    return True, None


# 🔸 Один проход: батчи по BF_BATCH_LIMIT до исчерпания хвоста или по времени
async def run_oracle_marketwatcher_backfill_once():
    start_ts = time.monotonic()
    processed = 0
    deferred = 0
    batches = 0

    while True:
        if (time.monotonic() - start_ts) >= BF_MAX_RUN_SECONDS:
            log.debug("[BF-STAGE2] время вышло: processed=%d deferred=%d batches=%d", processed, deferred, batches)
            break

        async with infra.pg_pool.acquire() as conn:
            # ⚠️ Фильтруем кандидатов по стратегиям (enabled=true AND market_watcher=true),
            # чтобы не зацикливаться на нерелевантных позициях
            rows = await conn.fetch("""
                SELECT p.position_uid
                FROM positions_v4 p
                JOIN strategies_v4 s ON s.id = p.strategy_id
                WHERE p.status = 'closed'
                  AND COALESCE(p.mrk_watcher_checked, false) = false
                  AND s.enabled = true
                  AND COALESCE(s.market_watcher, false) = true
                ORDER BY p.created_at ASC
                LIMIT $1
            """, BF_BATCH_LIMIT)

            if not rows:
                log.debug("[BF-STAGE2] хвост пуст: processed=%d deferred=%d batches=%d", processed, deferred, batches)
                break

            for r in rows:
                uid = r["position_uid"]
                try:
                    async with conn.transaction():
                        ok, reason = await _process_one_position(conn, uid)
                    if ok:
                        processed += 1
                    else:
                        if reason == "markers_missing":
                            deferred += 1
                except Exception as e:
                    log.exception("❌ Ошибка обработки позиции %s: %s", uid, e)

        batches += 1
        log.debug("[BF-STAGE2] batch processed: %d (total %d), deferred=%d",
                 len(rows), processed, deferred)

        if len(rows) < BF_BATCH_LIMIT:
            log.debug("[BF-STAGE2] завершено: processed=%d deferred=%d batches=%d", processed, deferred, batches)
            break

        await asyncio.sleep(BF_SLEEP_BETWEEN_BATCH_MS / 1000)


# 🔸 Периодический цикл: старт через 2 минуты, затем каждый час
async def run_oracle_marketwatcher_backfill_periodic():
    log.debug("🚀 Этап 2 (BF): старт через %d сек, батчи по %d, бюджет %d сек, затем каждый час",
             START_DELAY_SEC, BF_BATCH_LIMIT, BF_MAX_RUN_SECONDS)
    await asyncio.sleep(START_DELAY_SEC)

    while True:
        try:
            await run_oracle_marketwatcher_backfill_once()
        except asyncio.CancelledError:
            log.debug("⏹️ Бэкофилл остановлен")
            raise
        except Exception as e:
            log.exception("❌ Ошибка в бэкофилле: %s", e)

        await asyncio.sleep(3600)  # 1 час