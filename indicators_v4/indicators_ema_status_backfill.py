# indicators_ema_status_backfill.py — бэкофилл EMA-status по закрытым позициям
# Этап 1 (SCAN): находим все позиции со статусом closed и emastatus_checked=false, логируем (INFO)

import os
import asyncio
import logging
from datetime import datetime, timedelta

log = logging.getLogger("EMA_STATUS_BF")

# 🔸 Конфиг
BF_STAGE = os.getenv("EMA_BF_STAGE", "scan").lower()  # "scan" | "compute" | "write"
BATCH_SIZE = int(os.getenv("EMA_BF_BATCH_SIZE", "500"))         # позиций за один проход
LOOP_SLEEP_SEC = int(os.getenv("EMA_BF_LOOP_SLEEP_SEC", "30"))  # пауза между проходами
REQUIRED_TFS = ("m5", "m15", "h1")

_STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

def _floor_to_bar_ms(ts_ms: int, tf: str) -> int:
    step = _STEP_MS[tf]
    return (ts_ms // step) * step

def _log_stage(stage: str, msg: str, *args):
    """Логи предыдущих этапов — DEBUG, текущего этапа — INFO."""
    if BF_STAGE == stage:
        log.info(msg, *args)
    else:
        log.debug(msg, *args)

# 🔸 Выборка позиций для бэкофилла (Этап 1 — только скан)
async def _fetch_positions_batch(pg, limit: int):
    sql = """
        SELECT position_uid, symbol, strategy_id, direction, created_at
        FROM positions_v4
        WHERE status = 'closed'
          AND emastatus_checked = false
        ORDER BY created_at ASC
        LIMIT $1
    """
    async with pg.acquire() as conn:
        rows = await conn.fetch(sql, limit)
    # возвращаем как простые dict
    out = []
    for r in rows:
        out.append({
            "position_uid": r["position_uid"],
            "symbol": r["symbol"],
            "strategy_id": r["strategy_id"],
            "direction": r["direction"],
            "created_at": r["created_at"],  # tz-naive/aware — как в БД
        })
    return out

# 🔸 Основной воркер бэкофилла
async def run_indicators_ema_status_backfill(pg, redis):
    _log_stage("scan", "EMA_STATUS_BF started (stage=%s, batch=%d, sleep=%ds)", BF_STAGE, BATCH_SIZE, LOOP_SLEEP_SEC)

    # Этап 1: SCAN — только логируем список кандидатов партиями
    if BF_STAGE == "scan":
        while True:
            try:
                batch = await _fetch_positions_batch(pg, BATCH_SIZE)
                if not batch:
                    _log_stage("scan", "[SCAN] no positions pending (closed & emastatus_checked=false)")
                    await asyncio.sleep(LOOP_SLEEP_SEC)
                    continue

                _log_stage("scan", "[SCAN] pending positions: %d (showing up to %d)", len(batch), len(batch))

                for pos in batch:
                    uid = pos["position_uid"]
                    sym = pos["symbol"]
                    strat = pos["strategy_id"]
                    side = pos["direction"]
                    ca = pos["created_at"]
                    try:
                        created_ms = int(ca.replace(tzinfo=None).timestamp() * 1000) if hasattr(ca, "timestamp") else None
                    except Exception:
                        created_ms = None

                    # для наглядности вычислим open_time по каждому TF (без расчётов EMA)
                    if created_ms is not None:
                        bars_iso = {}
                        for tf in REQUIRED_TFS:
                            bar_ms = _floor_to_bar_ms(created_ms, tf)
                            bars_iso[tf] = datetime.utcfromtimestamp(bar_ms / 1000).isoformat()
                        _log_stage("scan",
                                   "[SCAN] uid=%s %s strat=%s dir=%s created_at=%s bars(m5/m15/h1)=%s/%s/%s",
                                   uid, sym, strat, side, ca.isoformat() if hasattr(ca, "isoformat") else str(ca),
                                   bars_iso["m5"], bars_iso["m15"], bars_iso["h1"])
                    else:
                        _log_stage("scan",
                                   "[SCAN] uid=%s %s strat=%s dir=%s created_at=%s (can't floor times)",
                                   uid, sym, strat, side, str(ca))

                # Пауза между проходами
                await asyncio.sleep(LOOP_SLEEP_SEC)

            except Exception as e:
                log.error("EMA_STATUS_BF scan loop error: %s", e, exc_info=True)
                await asyncio.sleep(LOOP_SLEEP_SEC)

    # Заглушки на будущее: Этап 2/3 будут добавлены последовательно
    elif BF_STAGE == "compute":
        log.info("EMA_STATUS_BF in COMPUTE stage (stub) — будет реализован на следующем шаге")
        while True:
            await asyncio.sleep(LOOP_SLEEP_SEC)

    elif BF_STAGE == "write":
        log.info("EMA_STATUS_BF in WRITE stage (stub) — будет реализован на третьем шаге")
        while True:
            await asyncio.sleep(LOOP_SLEEP_SEC)

    else:
        log.error("Unknown EMA_BF_STAGE=%s (expected: scan|compute|write)", BF_STAGE)