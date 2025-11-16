# indicator_mw_healer.py — лечение пропусков MW-состояний (trend/vol/mom/extremes) на основе indicator_mw_gap и TS

import asyncio
import logging
from datetime import datetime

from indicator_mw_trend import compute_trend_for_bar
from indicator_mw_volatility import compute_vol_for_bar
from indicator_mw_momentum import compute_momentum_for_bar
from indicator_mw_extremes import compute_ext_for_bar

# 🔸 Логгер
log = logging.getLogger("IND_MW_HEALER")

# 🔸 Ожидаемые kind'ы MarketWatch
EXPECTED_KINDS = ("trend", "volatility", "momentum", "extremes")


# 🔸 Выборка пропусков MW со статусом found
async def fetch_found_mw_gaps(pg, limit_rows: int = 200):
    """
    Возвращает список gap'ов со статусом 'found':
      [{"id", "symbol", "timeframe", "open_time", "missing_kinds"}, ...]
    """
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, symbol, timeframe, open_time, missing_kinds
            FROM indicator_mw_gap
            WHERE status = 'found'
            ORDER BY detected_at
            LIMIT $1
            """,
            limit_rows,
        )

    gaps = []
    for r in rows:
        mk = r["missing_kinds"]
        # missing_kinds может прийти как list или как jsonb-строка/объект
        if isinstance(mk, str):
            try:
                import json

                mk_parsed = json.loads(mk)
            except Exception:
                mk_parsed = []
        else:
            mk_parsed = list(mk) if mk is not None else []
        # фильтруем только ожидаемые kind'ы
        kinds = [k for k in mk_parsed if k in EXPECTED_KINDS]
        gaps.append(
            {
                "id": r["id"],
                "symbol": r["symbol"],
                "timeframe": r["timeframe"],
                "open_time": r["open_time"],
                "missing_kinds": kinds,
            }
        )

    return gaps


# 🔸 Проверка, какие kind'ы уже есть в indicator_marketwatch_values
async def existing_mw_kinds_for_bar(pg, symbol: str, timeframe: str, open_time: datetime):
    """
    Возвращает set(kind) для заданного бара в indicator_marketwatch_values.
    """
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT kind
            FROM indicator_marketwatch_values
            WHERE symbol = $1
              AND timeframe = $2
              AND open_time = $3
              AND kind = ANY($4::text[])
            """,
            symbol,
            timeframe,
            open_time,
            list(EXPECTED_KINDS),
        )
    return {r["kind"] for r in rows}


# 🔸 Обновление статуса gap после попытки лечения
async def update_mw_gap_status(pg, gap_id: int, healed: bool, error: str | None):
    async with pg.acquire() as conn:
        if healed:
            await conn.execute(
                """
                UPDATE indicator_mw_gap
                SET status = 'healed',
                    healed_at = NOW(),
                    error = NULL
                WHERE id = $1
                """,
                gap_id,
            )
        else:
            await conn.execute(
                """
                UPDATE indicator_mw_gap
                SET attempts = attempts + 1,
                    error = COALESCE($2, error)
                WHERE id = $1
                """,
                gap_id,
                error,
            )


# 🔸 Лечение одного gap'а: пересчёт недостающих kind'ов
async def heal_single_mw_gap(pg, redis, gap: dict):
    """
    Пытается долечить один gap:
      - вызывает compute_*_for_bar для недостающих kind'ов
      - проверяет, появились ли они в indicator_marketwatch_values
      - обновляет indicator_mw_gap
    """
    gap_id = gap["id"]
    symbol = gap["symbol"]
    tf = gap["timeframe"]
    ot = gap["open_time"]
    missing = gap["missing_kinds"] or []

    if not missing:
        # формально gap пустой — просто пометим как healed
        await update_mw_gap_status(pg, gap_id, healed=True, error=None)
        return True

    open_iso = ot.isoformat()

    # вызываем пересчёт только для тех kind'ов, которых не хватает
    for kind in missing:
        try:
            if kind == "trend":
                await compute_trend_for_bar(pg, redis, symbol, tf, open_iso)
            elif kind == "volatility":
                await compute_vol_for_bar(pg, redis, symbol, tf, open_iso)
            elif kind == "momentum":
                await compute_momentum_for_bar(pg, redis, symbol, tf, open_iso)
            elif kind == "extremes":
                await compute_ext_for_bar(pg, redis, symbol, tf, open_iso)
        except Exception as e:
            # логируем, но продолжаем — затем проверим фактический результат
            log.warning(
                f"IND_MW_HEALER: ошибка пересчёта {kind} для {symbol}/{tf}@{open_iso}: {e}",
                exc_info=True,
            )

    # проверяем, что теперь все EXPECTED_KINDS присутствуют
    have = await existing_mw_kinds_for_bar(pg, symbol, tf, ot)
    still_missing = [k for k in EXPECTED_KINDS if k not in have]

    if not still_missing:
        await update_mw_gap_status(pg, gap_id, healed=True, error=None)
        log.info(
            f"IND_MW_HEALER: HEALED {symbol}/{tf}@{open_iso} "
            f"(gap_id={gap_id}, missing_before={missing})"
        )
        return True

    # не удалось вылечить полностью
    await update_mw_gap_status(pg, gap_id, healed=False, error="no_ts_or_compute_failed")
    log.info(
        f"IND_MW_HEALER: STILL_MISSING {symbol}/{tf}@{open_iso} "
        f"(gap_id={gap_id}, missing_before={missing}, missing_after={still_missing})"
    )
    return False


# 🔸 Основной воркер healer'а MW
async def run_indicator_mw_healer(pg, redis, pause_sec: int = 2):
    log.debug("IND_MW_HEALER: воркер запущен")
    sem = asyncio.Semaphore(4)

    while True:
        try:
            gaps = await fetch_found_mw_gaps(pg)
            if not gaps:
                await asyncio.sleep(pause_sec)
                continue

            total = len(gaps)
            healed = 0

            tasks = []

            for gap in gaps:
                async def _run(g):
                    async with sem:
                        try:
                            ok = await heal_single_mw_gap(pg, redis, g)
                            return ok
                        except Exception as e:
                            log.error(
                                f"IND_MW_HEALER: ошибка лечения gap_id={g['id']} "
                                f"{g['symbol']}/{g['timeframe']}@{g['open_time']}: {e}",
                                exc_info=True,
                            )
                            await update_mw_gap_status(pg, g["id"], healed=False, error="exception")
                            return False

                tasks.append(asyncio.create_task(_run(gap)))

            results = await asyncio.gather(*tasks, return_exceptions=False)
            healed = sum(1 for r in results if r)

            # итоговая сводка на INFO
            log.info(
                f"IND_MW_HEALER: проход обработано_gapов={total}, "
                f"успешно_вылечено={healed}, осталось_невылеченных={total - healed}"
            )

            await asyncio.sleep(pause_sec)

        except Exception as e:
            log.error(f"IND_MW_HEALER loop error: {e}", exc_info=True)
            await asyncio.sleep(pause_sec)