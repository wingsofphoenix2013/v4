# strategies_stats_backfill.py — бэкофилл Redis-счётчиков по стратегиям (strategy:stats:{sid}) из БД, вызов из strategies_v4_main.py

# 🔸 Импорты
import asyncio
import logging
from decimal import Decimal

from infra import infra

# 🔸 Логгер
log = logging.getLogger("STRAT_STATS_BF")

# 🔸 Конфиг
START_DELAY_SEC = int(__import__("os").getenv("STRAT_STATS_BF_START_DELAY_SEC", "0"))      # задержка старта, если нужно
SLEEP_AFTER_DONE_SEC = int(__import__("os").getenv("STRAT_STATS_BF_SLEEP_SEC", str(96*3600)))  # сон после прохода (по умолчанию 96ч)

# 🔸 Ключ Redis со счётчиками по стратегии (Hash)
def _stats_key(sid: int) -> str:
    return f"strategy:stats:{sid}"

# 🔸 Выбрать стратегии с market_watcher = true
async def _fetch_mw_strategies() -> list[int]:
    rows = await infra.pg_pool.fetch(
        "SELECT id FROM strategies_v4 WHERE COALESCE(market_watcher, false) = true"
    )
    return [int(r["id"]) for r in rows]

# 🔸 Пересчитать агрегаты по закрытым позициям для одной стратегии и записать в Redis (абсолютные значения)
async def _recompute_stats_for_strategy(sid: int) -> None:
    rows = await infra.pg_pool.fetch(
        """
        SELECT direction, COUNT(*) AS cnt, COALESCE(SUM(pnl), 0) AS pnl
        FROM positions_v4
        WHERE strategy_id = $1 AND status = 'closed'
        GROUP BY direction
        """,
        sid,
    )

    closed_long = closed_short = 0
    pnl_long = pnl_short = Decimal("0")

    for r in rows:
        d = (r["direction"] or "").lower()
        c = int(r["cnt"])
        s = Decimal(str(r["pnl"]))
        if d == "long":
            closed_long, pnl_long = c, s
        elif d == "short":
            closed_short, pnl_short = c, s

    closed_total = closed_long + closed_short
    pnl_total = pnl_long + pnl_short

    await infra.redis_client.hset(
        _stats_key(sid),
        mapping={
            "closed_total": str(closed_total),
            "closed_long":  str(closed_long),
            "closed_short": str(closed_short),
            "pnl_total":    f"{pnl_total}",
            "pnl_long":     f"{pnl_long}",
            "pnl_short":    f"{pnl_short}",
        },
    )
    log.info(f"[DONE] sid={sid} total={closed_total} L={closed_long} S={closed_short} pnl={pnl_total}")

# 🔸 Публичный воркер: один проход по всем стратегиям → сон (для безопасного long-run в gather)
async def run_strategies_stats_backfill():
    if START_DELAY_SEC > 0:
        log.debug(f"⏳ STRAT_STATS_BF: задержка старта {START_DELAY_SEC} сек")
        await asyncio.sleep(START_DELAY_SEC)

    while True:
        try:
            sids = await _fetch_mw_strategies()
            if not sids:
                log.info("STRAT_STATS_BF: нет стратегий с market_watcher=true")
            else:
                log.info(f"🚀 STRAT_STATS_BF: пересчёт по {len(sids)} стратегиям…")
                for sid in sids:
                    try:
                        await _recompute_stats_for_strategy(sid)
                    except Exception:
                        log.exception(f"❌ STRAT_STATS_BF: ошибка sid={sid}")
            log.info(f"😴 STRAT_STATS_BF: сон на {SLEEP_AFTER_DONE_SEC} сек")
        except Exception:
            log.exception("❌ STRAT_STATS_BF: критическая ошибка цикла")

        await asyncio.sleep(SLEEP_AFTER_DONE_SEC)