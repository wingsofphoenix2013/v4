# 🔸 oracle_mw_confidence.py — воркер confidence: расчёт доверия к строкам MW-отчётов и обновление в БД

import asyncio
import logging
import json
from datetime import datetime, timedelta
import math

import infra

log = logging.getLogger("ORACLE_CONFIDENCE")

# 🔸 Константы воркера
REPORT_STREAM = "oracle:mw:reports_ready"   # Redis Stream, из которого берём сигналы
REPORT_CONSUMER_GROUP = "oracle_confidence_group"
REPORT_CONSUMER_NAME = "oracle_confidence_worker"

# 🔸 Параметры расчёта
Z = 1.96               # уровень доверия для Wilson (95%)
EMA_ALPHA = 0.3        # сглаживание для EMA по winrate
STABILITY_WINDOW = 7   # количество прогонов для оценки стабильности
PERSIST_K = 12         # количество прогонов для presence_rate (≈2 суток при шаге 4ч)


# 🔸 Точка входа воркера
async def run_oracle_confidence():
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск: PG/Redis не инициализированы")
        return

    # инициализация группы в Redis Stream (если ещё не создана)
    try:
        await infra.redis_client.xgroup_create(
            name=REPORT_STREAM,
            groupname=REPORT_CONSUMER_GROUP,
            id="$",
            mkstream=True
        )
        log.info("📡 Создана группа потребителей в Redis Stream: %s", REPORT_CONSUMER_GROUP)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass  # группа уже существует
        else:
            log.exception("❌ Ошибка инициализации группы Redis Stream")
            return

    log.info("🚀 Старт воркера confidence")

    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=REPORT_CONSUMER_GROUP,
                consumername=REPORT_CONSUMER_NAME,
                streams={REPORT_STREAM: ">"},
                count=10,
                block=30_000,
            )
            if not resp:
                continue

            for stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        payload = json.loads(fields.get("data", "{}"))
                        report_id = int(payload.get("report_id", 0))
                        strategy_id = int(payload.get("strategy_id", 0))
                        time_frame = payload.get("time_frame")
                        await _process_report(report_id, strategy_id, time_frame)
                        await infra.redis_client.xack(REPORT_STREAM, REPORT_CONSUMER_GROUP, msg_id)
                    except Exception:
                        log.exception("❌ Ошибка обработки сообщения из Redis Stream")

        except asyncio.CancelledError:
            log.info("⏹️ Воркер confidence остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка в основном цикле confidence — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Обработка одного отчёта (report_id)
async def _process_report(report_id: int, strategy_id: int, time_frame: str):
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
              id,
              report_id,
              strategy_id,
              time_frame,
              direction,
              timeframe,
              agg_type,
              agg_base,
              agg_state,
              trades_total,
              trades_wins,
              winrate,
              avg_pnl_per_trade,
              report_created_at
            FROM v_mw_aggregated_with_time
            WHERE report_id = $1
            """,
            report_id,
        )

        if not rows:
            log.info("ℹ️ Для report_id=%s нет агрегатов", report_id)
            return

        updated = 0
        for r in rows:
            try:
                confidence, inputs = await _calc_confidence(conn, dict(r))
                await conn.execute(
                    """
                    UPDATE oracle_mw_aggregated_stat
                       SET confidence = $2,
                           confidence_inputs = $3,
                           confidence_updated_at = now()
                     WHERE id = $1
                    """,
                    int(r["id"]),
                    float(confidence),
                    json.dumps(inputs, separators=(",", ":")),
                )
                updated += 1
            except Exception:
                log.exception("❌ Ошибка обновления confidence для aggregated_id=%s", r["id"])

        log.info("✅ Обновлён confidence для report_id=%s (strategy_id=%s, time_frame=%s): %d строк",
                 report_id, strategy_id, time_frame, updated)


# 🔸 Расчёт confidence для одной строки
async def _calc_confidence(conn, row: dict):
    n = int(row["trades_total"] or 0)
    wins = int(row["trades_wins"] or 0)
    wr = float(row["winrate"] or 0.0)
    pnl = float(row["avg_pnl_per_trade"] or 0.0)

    # Reliability (R) — Wilson lower bound для winrate
    R = _wilson_lower_bound(wins, n, Z) if n > 0 else 0.0

    # Persistence (P) — наличие строки в последних K отчётах
    presence_rate, volume_growth = await _calc_persistence(conn, row)
    P = 0.6 * presence_rate + 0.4 * volume_growth

    # Cross-window coherence (C) — согласованность между окнами
    C = await _calc_cross_window(conn, row)

    # Stability (S) — устойчивость winrate
    S = await _calc_stability(conn, row)

    # Итоговое confidence
    confidence = round(
        0.4 * R + 0.25 * P + 0.2 * C + 0.15 * S, 4
    )

    inputs = {
        "R": R, "P": P, "C": C, "S": S,
        "n": n, "wr": wr, "wins": wins,
        "avg_pnl_per_trade": pnl,
        "presence_rate": presence_rate,
        "volume_growth": volume_growth,
        "formula": "0.4*R + 0.25*P + 0.2*C + 0.15*S"
    }

    return confidence, inputs


# 🔸 Wilson lower bound для биномиальной пропорции
def _wilson_lower_bound(wins: int, n: int, z: float) -> float:
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z**2 / n
    center = p + z**2 / (2 * n)
    adj = z * math.sqrt((p * (1 - p) / n) + (z**2 / (4 * n**2)))
    return max(0.0, (center - adj) / denom)


# 🔸 Persistence: presence_rate и volume_growth
async def _calc_persistence(conn, row: dict):
    # ключ строки
    k = {
        "strategy_id": row["strategy_id"],
        "time_frame": row["time_frame"],
        "direction": row["direction"],
        "timeframe": row["timeframe"],
        "agg_type": row["agg_type"],
        "agg_base": row["agg_base"],
        "agg_state": row["agg_state"],
    }

    rows = await conn.fetch(
        """
        SELECT trades_total
        FROM v_mw_aggregated_with_time
        WHERE strategy_id = $1
          AND time_frame = $2
          AND direction = $3
          AND timeframe = $4
          AND agg_type = $5
          AND agg_base = $6
          AND agg_state = $7
        ORDER BY report_created_at DESC
        LIMIT $8
        """,
        k["strategy_id"], k["time_frame"], k["direction"], k["timeframe"],
        k["agg_type"], k["agg_base"], k["agg_state"], PERSIST_K
    )

    if not rows:
        return 0.0, 0.0

    presence_rate = len(rows) / PERSIST_K
    trades_now = int(row["trades_total"] or 0)
    avg_trades = sum(int(r["trades_total"]) for r in rows) / len(rows)
    volume_growth = min(1.0, trades_now / avg_trades) if avg_trades > 0 else 0.0

    return presence_rate, volume_growth


# 🔸 Cross-window coherence
async def _calc_cross_window(conn, row: dict):
    rows = await conn.fetch(
        """
        SELECT time_frame, trades_total, trades_wins, winrate, avg_pnl_per_trade
        FROM v_mw_aggregated_with_time
        WHERE strategy_id = $1
          AND direction = $2
          AND timeframe = $3
          AND agg_type = $4
          AND agg_base = $5
          AND agg_state = $6
          AND report_created_at = $7
        """,
        row["strategy_id"], row["direction"], row["timeframe"],
        row["agg_type"], row["agg_base"], row["agg_state"],
        row["report_created_at"],
    )

    if not rows:
        return 0.0

    aligned = 0
    total = 0
    for r in rows:
        n = int(r["trades_total"] or 0)
        wins = int(r["trades_wins"] or 0)
        wr = float(r["winrate"] or 0.0)
        pnl = float(r["avg_pnl_per_trade"] or 0.0)
        if n == 0:
            continue
        total += 1
        R = _wilson_lower_bound(wins, n, Z)
        if R > 0.5 and pnl >= 0:
            aligned += 1
    return aligned / total if total > 0 else 0.0


# 🔸 Stability: проверка отклонений winrate
async def _calc_stability(conn, row: dict):
    rows = await conn.fetch(
        """
        SELECT winrate
        FROM v_mw_aggregated_with_time
        WHERE strategy_id = $1
          AND time_frame = $2
          AND direction = $3
          AND timeframe = $4
          AND agg_type = $5
          AND agg_base = $6
          AND agg_state = $7
        ORDER BY report_created_at DESC
        LIMIT $8
        """,
        row["strategy_id"], row["time_frame"], row["direction"], row["timeframe"],
        row["agg_type"], row["agg_base"], row["agg_state"], STABILITY_WINDOW
    )

    if not rows or len(rows) < 2:
        return 1.0

    wr_hist = [float(r["winrate"] or 0.0) for r in rows]
    wr_now = float(row["winrate"] or 0.0)
    mean_wr = sum(wr_hist) / len(wr_hist)
    std_wr = math.sqrt(sum((w - mean_wr)**2 for w in wr_hist) / (len(wr_hist) - 1)) or 1e-6

    z = abs(wr_now - mean_wr) / std_wr
    return math.exp(-z)