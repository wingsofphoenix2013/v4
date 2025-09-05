# 🔸 oracle_emastatus_snapshot_aggregator.py — EMA-status snapshot агрегатор: Этап 3 (расчёт, апдейт таблиц/Redis, отметка позиции; компактный INFO-лог)

import os
import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP

import infra

log = logging.getLogger("ORACLE_EMASTATUS_SNAP")

# 🔸 Конфиг consumer-группы и чтения
STREAM_NAME   = os.getenv("ORACLE_EMASTATUS_STREAM",   "signal_log_queue")
GROUP_NAME    = os.getenv("ORACLE_EMASTATUS_GROUP",    "oracle_emastatus_snap")
CONSUMER_NAME = os.getenv("ORACLE_EMASTATUS_CONSUMER", "oracle_emastatus_1")
XREAD_COUNT   = int(os.getenv("ORACLE_EMASTATUS_COUNT",    "50"))
XREAD_BLOCKMS = int(os.getenv("ORACLE_EMASTATUS_BLOCK_MS", "1000"))


# 🔸 Создание consumer-group (идемпотентно)
async def _ensure_group():
    try:
        await infra.redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="$", mkstream=True)
        log.info("✅ Consumer group '%s' создана на '%s'", GROUP_NAME, STREAM_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info("ℹ️ Consumer group '%s' уже существует", GROUP_NAME)
        else:
            log.exception("❌ Ошибка создания consumer group: %s", e)
            raise


# 🔸 Загрузка позиции и стратегии
async def _load_position_and_strategy(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        async with conn.transaction():
            pos = await conn.fetchrow(
                """
                SELECT p.id, p.position_uid, p.symbol, p.direction, p.strategy_id,
                       p.pnl, p.status,
                       COALESCE(p.emastatus_checked, false) AS emastatus_checked
                FROM positions_v4 p
                WHERE p.position_uid = $1
                FOR UPDATE
                """,
                position_uid,
            )
            if not pos:
                return None, None, ("skip", "position_not_found")
            if pos["status"] != "closed":
                return pos, None, ("skip", "position_not_closed")
            if pos["emastatus_checked"]:
                return pos, None, ("skip", "already_checked")

            strat = await conn.fetchrow(
                """
                SELECT id, enabled, COALESCE(market_watcher, false) AS mw
                FROM strategies_v4
                WHERE id = $1
                """,
                int(pos["strategy_id"]),
            )
            if not strat or not strat["enabled"] or not strat["mw"]:
                return pos, strat, ("skip", "strategy_inactive_or_no_mw")

            return pos, strat, ("ok", "eligible")


# 🔸 Загрузка статусов EMA из PIS
async def _load_ema_status_bins(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timeframe, param_name, value_num
            FROM positions_indicators_stat
            WHERE position_uid = $1
              AND using_current_bar = true
              AND param_name LIKE 'ema%_status'
              AND timeframe IN ('m5','m15','h1')
            """,
            position_uid,
        )
    # структура: { ema_len: { tf: code } }
    result = {}
    for r in rows:
        tf = r["timeframe"]
        pname = r["param_name"]  # "ema50_status"
        val = r["value_num"]
        if val is None:
            continue
        try:
            ema_len = int(pname.replace("ema", "").replace("_status", ""))
        except Exception:
            continue
        result.setdefault(ema_len, {})[tf] = int(val)
    return result


# 🔸 Апдейт агрегатов + Redis + отметка позиции
async def _update_aggregates_and_mark(pos, ema_bins):
    pg = infra.pg_pool
    redis = infra.redis_client

    strategy_id = int(pos["strategy_id"])
    direction   = str(pos["direction"])
    pnl         = Decimal(str(pos["pnl"])).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    is_win      = 1 if pnl > Decimal("0") else 0

    async with pg.acquire() as conn:
        async with conn.transaction():
            # если есть данные — апдейтим агрегаты
            for ema_len, per_tf in (ema_bins or {}).items():
                # per-TF
                for tf, status_code in per_tf.items():
                    stat = await conn.fetchrow(
                        """
                        SELECT closed_trades, won_trades, pnl_sum
                        FROM positions_emastatus_stat_tf
                        WHERE strategy_id=$1 AND direction=$2 AND timeframe=$3 AND ema_len=$4 AND status_code=$5
                        FOR UPDATE
                        """,
                        strategy_id, direction, tf, int(ema_len), int(status_code)
                    )

                    if stat:
                        c = int(stat["closed_trades"]) + 1
                        w = int(stat["won_trades"]) + is_win
                        s = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                    else:
                        c, w, s = 1, is_win, pnl

                    wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                    ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

                    await conn.execute(
                        """
                        INSERT INTO positions_emastatus_stat_tf
                          (strategy_id, direction, timeframe, ema_len, status_code,
                           closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
                        ON CONFLICT (strategy_id,direction,timeframe,ema_len,status_code)
                        DO UPDATE SET
                          closed_trades=$6, won_trades=$7, pnl_sum=$8, winrate=$9, avg_pnl=$10, updated_at=NOW()
                        """,
                        strategy_id, direction, tf, int(em a_len), int(status_code),
                        c, w, str(s), str(wr), str(ap)
                    )

                    try:
                        await redis.set(
                            f"oracle:ema:tf:{strategy_id}:{direction}:{tf}:ema{int(ema_len)}:{int(status_code)}",
                            f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                        )
                    except Exception:
                        log.debug("Redis SET failed (per-TF)")

                # composite-триплет
                if all(tf in per_tf for tf in ("m5", "m15", "h1")):
                    triplet = f"{per_tf['m5']}-{per_tf['m15']}-{per_tf['h1']}"
                    stat = await conn.fetchrow(
                        """
                        SELECT closed_trades, won_trades, pnl_sum
                        FROM positions_emastatus_stat_comp
                        WHERE strategy_id=$1 AND direction=$2 AND ema_len=$3 AND status_triplet=$4
                        FOR UPDATE
                        """,
                        strategy_id, direction, int(ema_len), triplet
                    )

                    if stat:
                        c = int(stat["closed_trades"]) + 1
                        w = int(stat["won_trades"]) + is_win
                        s = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                    else:
                        c, w, s = 1, is_win, pnl

                    wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                    ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

                    await conn.execute(
                        """
                        INSERT INTO positions_emastatus_stat_comp
                          (strategy_id, direction, ema_len, status_triplet,
                           closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW())
                        ON CONFLICT (strategy_id,direction,ema_len,status_triplet)
                        DO UPDATE SET
                          closed_trades=$5, won_trades=$6, pnl_sum=$7, winrate=$8, avg_pnl=$9, updated_at=NOW()
                        """,
                        strategy_id, direction, int(ema_len), triplet,
                        c, w, str(s), str(wr), str(ap)
                    )

                    try:
                        await redis.set(
                            f"oracle:ema:comp:{strategy_id}:{direction}:ema{int(ema_len)}:{triplet}",
                            f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                        )
                    except Exception:
                        log.debug("Redis SET failed (comp)")

            # отметить позицию как обработанную (всегда, даже если нет данных для статистики)
            await conn.execute(
                "UPDATE positions_v4 SET emastatus_checked = true WHERE position_uid = $1",
                pos["position_uid"]
            )


# 🔸 Основной цикл
async def run_oracle_emastatus_snapshot_aggregator():
    await _ensure_group()
    log.info("🚀 EMA-STATUS SNAP: слушаем '%s' (group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)

    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=XREAD_COUNT,
                block=XREAD_BLOCKMS,
            )
            if not resp:
                continue

            to_ack = []
            for _, records in resp:
                for msg_id, data in records:
                    to_ack.append(msg_id)
                    try:
                        status  = data.get("status")
                        pos_uid = data.get("position_uid")

                        if status != "closed":
                            log.debug("[EMA-STATUS SNAP] skip msg_id=%s uid=%s reason=status=%s", msg_id, pos_uid, status)
                            continue

                        # Этап 1: факт получения закрытия — уводим в DEBUG
                        log.debug("[EMA-STATUS SNAP] closed position received: uid=%s", pos_uid)

                        # Этап 2: загрузка позиции/стратегии и сбор EMA-статусов
                        pos, strat, verdict = await _load_position_and_strategy(pos_uid)
                        v_code, v_reason = verdict
                        if v_code != "ok":
                            log.debug("[EMA-STATUS SNAP] uid=%s skip: %s", pos_uid, v_reason)
                            continue

                        ema_bins = await _load_ema_status_bins(pos_uid)

                        # Детальная отладка (per-TF и триплеты) — только в DEBUG
                        updated_tf = 0
                        updated_comp = 0
                        if ema_bins:
                            for ema_len, per_tf in ema_bins.items():
                                for tf, code in per_tf.items():
                                    log.debug(
                                        "[EMA-STATUS SNAP] uid=%s strat=%s dir=%s ema_len=%s TF=%s code=%s",
                                        pos_uid, pos["strategy_id"], pos["direction"], ema_len, tf, code
                                    )
                                    updated_tf += 1
                                if all(tf in per_tf for tf in ("m5", "m15", "h1")):
                                    triplet = f"{per_tf['m5']}-{per_tf['m15']}-{per_tf['h1']}"
                                    log.debug(
                                        "[EMA-STATUS SNAP] uid=%s strat=%s dir=%s ema_len=%s triplet=%s",
                                        pos_uid, pos["strategy_id"], pos["direction"], ema_len, triplet
                                    )
                                    updated_comp += 1
                        else:
                            log.debug("[EMA-STATUS SNAP] uid=%s no_ema_status_found", pos_uid)

                        # Этап 3: апдейт агрегатов/Redis и отметка позиции
                        await _update_aggregates_and_mark(pos, ema_bins)

                        # Сводный INFO-лог (одна строка на позицию)
                        win_flag = 1 if (pos["pnl"] is not None and pos["pnl"] > 0) else 0
                        log.info(
                            "[EMA-STATUS SNAP] uid=%s strat=%s dir=%s updated_tf=%d updated_comp=%d win=%d",
                            pos_uid, pos["strategy_id"], pos["direction"], updated_tf, updated_comp, win_flag
                        )

                    except Exception as e:
                        log.exception("❌ EMA-STATUS SNAP msg error %s: %s", msg_id, e)

            if to_ack:
                await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *to_ack)

        except asyncio.CancelledError:
            log.info("⏹️ EMA-STATUS snapshot агрегатор остановлен")
            raise
        except Exception as e:
            log.exception("❌ XREADGROUP loop error: %s", e)
            await asyncio.sleep(1)