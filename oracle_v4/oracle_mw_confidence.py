# oracle_mw_confidence.py — воркер расчёта доверия к строкам MW-отчётов

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_MW_CONF")

# 🔸 Константы
REPORT_READY_STREAM = "oracle:mw:reports_ready"
STREAM_BLOCK_MS = 10_000
HISTORY_LOOKBACK_HOURS = 48
HISTORY_MAX_REPORTS = 12
CONFIDENCE_DECIMALS = 4


# 🔸 Вспомогательные функции
def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def _round_conf(x: float) -> float:
    return round(_clamp01(x), CONFIDENCE_DECIMALS)


def _compute_confidence(
    trades_total: int,
    closed_total: int,
    presence_count: int,
    streak: int,
    avg_inc_per_report: float,
    cross_confirmations: int,
) -> Tuple[float, Dict]:
    closed_total = max(1, closed_total)

    # trade_score: доля сделок
    share = trades_total / closed_total
    trade_score = min(1.0, share * 5.0)

    # temporal_score: регулярность появления и накопления
    presence_rate = presence_count / HISTORY_MAX_REPORTS
    growth_factor = 1.0 if avg_inc_per_report >= 1.0 else avg_inc_per_report
    temporal_score = min(1.0, 0.7 * presence_rate + 0.2 * (streak / HISTORY_MAX_REPORTS) + 0.1 * growth_factor)

    # cross_bonus: подтверждение в других окнах
    cross_bonus = min(1.0, cross_confirmations / 3.0)

    # финальная агрегация
    raw = 0.5 * trade_score + 0.35 * temporal_score + 0.15 * cross_bonus
    conf = _round_conf(raw)

    inputs = {
        "trades_total": trades_total,
        "closed_total": closed_total,
        "share": round(share, 4),
        "presence_count": presence_count,
        "streak": streak,
        "avg_inc_per_report": round(avg_inc_per_report, 4),
        "cross_confirmations": cross_confirmations,
        "raw": round(raw, 4),
    }
    return conf, inputs

# 🔸 Основная корутина-слушатель
async def run_oracle_mw_confidence():
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ PG/Redis не инициализированы")
        return

    redis = infra.redis_client
    last_id = "$"
    log.info("🚀 ORACLE_MW_CONF слушает %s", REPORT_READY_STREAM)

    while True:
        try:
            # читаем только новые сообщения; в этой версии redis.asyncio используем аргумент block
            result = await redis.xread({REPORT_READY_STREAM: last_id}, block=STREAM_BLOCK_MS)
            if not result:
                continue

            for stream_name, messages in result:
                for msg_id, fields in messages:
                    last_id = msg_id
                    # поле data может быть str или bytes
                    data_raw = fields.get("data") or fields.get(b"data")
                    if not data_raw:
                        log.debug("[CONF] пропущено сообщение без поля data (id=%s)", msg_id)
                        continue

                    # парсим JSON
                    try:
                        payload = json.loads(data_raw)
                    except Exception:
                        log.exception("❌ Ошибка парсинга JSON (id=%s)", msg_id)
                        continue

                    report_id = payload.get("report_id")
                    if not report_id:
                        log.debug("[CONF] сообщение без report_id (id=%s)", msg_id)
                        continue

                    # обработка отчёта
                    try:
                        updated, avg_conf = await _process_report_id(int(report_id))
                        log.info("[CONF] report_id=%s updated=%d avg_conf=%.4f", report_id, updated, avg_conf)
                    except Exception:
                        log.exception("❌ Ошибка обработки report_id=%s", report_id)

        except asyncio.CancelledError:
            log.info("⏹️ ORACLE_MW_CONF остановлен")
            raise
        except Exception:
            log.exception("❌ Общая ошибка слушателя, пауза 5с")
            await asyncio.sleep(5)

# 🔸 Обработка одного report_id
async def _process_report_id(report_id: int) -> Tuple[int, float]:
    now = datetime.utcnow()
    lookback_cut = now - timedelta(hours=HISTORY_LOOKBACK_HOURS)

    async with infra.pg_pool.acquire() as conn:
        header = await conn.fetchrow(
            "SELECT strategy_id, time_frame, created_at, closed_total FROM oracle_report_stat WHERE id = $1",
            report_id,
        )
        if not header:
            return 0, 0.0

        strategy_id = int(header["strategy_id"])
        report_time_frame = header["time_frame"]
        report_created_at = header["created_at"]
        closed_total = int(header["closed_total"] or 0)

        rows = await conn.fetch(
            """
            SELECT id, direction, timeframe, agg_type, agg_base, agg_state, trades_total
              FROM oracle_mw_aggregated_stat
             WHERE report_id = $1
            """,
            report_id,
        )
        if not rows:
            return 0, 0.0

        updates = []
        for r in rows:
            row_id = int(r["id"])
            trades_total = int(r["trades_total"] or 0)

            # история за 2 суток
            history = await conn.fetch(
                """
                SELECT a.trades_total
                  FROM oracle_mw_aggregated_stat a
                  JOIN oracle_report_stat r ON a.report_id = r.id
                 WHERE a.strategy_id = $1
                   AND a.time_frame = $2
                   AND a.direction = $3
                   AND a.timeframe = $4
                   AND a.agg_type = $5
                   AND a.agg_base = $6
                   AND a.agg_state = $7
                   AND r.created_at >= $8
                 ORDER BY r.created_at DESC
                 LIMIT $9
                """,
                strategy_id, report_time_frame, r["direction"], r["timeframe"],
                r["agg_type"], r["agg_base"], r["agg_state"],
                lookback_cut, HISTORY_MAX_REPORTS,
            )

            presence_count = len(history)
            streak = presence_count  # упрощённо: все подряд
            avg_inc = 0.0
            if presence_count >= 2:
                delta = int(history[0]["trades_total"]) - int(history[-1]["trades_total"])
                avg_inc = delta / max(1, presence_count - 1)

            conf, inputs = _compute_confidence(
                trades_total=trades_total,
                closed_total=closed_total,
                presence_count=presence_count,
                streak=streak,
                avg_inc_per_report=avg_inc,
                cross_confirmations=0,  # упрощено, можно добавить позже
            )
            updates.append((conf, inputs, row_id))

        updated = 0
        avg_conf = 0.0
        async with conn.transaction():
            for conf, inputs, row_id in updates:
                await conn.execute(
                    """
                    UPDATE oracle_mw_aggregated_stat
                       SET confidence = $1,
                           confidence_inputs = $2,
                           confidence_updated_at = now()
                     WHERE id = $3
                    """,
                    conf, json.dumps(inputs, separators=(",", ":"), ensure_ascii=False), row_id,
                )
                updated += 1
                avg_conf += conf

        return updated, (avg_conf / updated if updated else 0.0)


# 🔸 Обёртка для main
async def run_safe_oracle_confidence():
    while True:
        try:
            await run_oracle_mw_confidence()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("❌ ORACLE_MW_CONF упал, перезапуск через 5с")
            await asyncio.sleep(5)