# 🔸 oracle_emastatus_snapshot_aggregator.py — EMA-status snapshot агрегатор: Этап 2 (чтение + расчёт агрегатов без апдейта)

import os
import asyncio
import logging

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

# 🔸 Основной цикл (Этап 2)
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

                        # Этап 1 (лог переведен на debug)
                        log.debug("[EMA-STATUS SNAP] closed position received: uid=%s", pos_uid)

                        # Этап 2: расчёт (без апдейта)
                        pos, strat, verdict = await _load_position_and_strategy(pos_uid)
                        v_code, v_reason = verdict

                        if v_code != "ok":
                            log.info("[EMA-STATUS SNAP] skip uid=%s reason=%s", pos_uid, v_reason)
                            continue

                        ema_bins = await _load_ema_status_bins(pos_uid)
                        if not ema_bins:
                            log.info("[EMA-STATUS SNAP] skip uid=%s reason=no_ema_status", pos_uid)
                            continue

                        # Логируем расчёт: per-TF и комп
                        for ema_len, per_tf in ema_bins.items():
                            for tf, code in per_tf.items():
                                log.info(
                                    "[EMA-STATUS SNAP] uid=%s strat=%s dir=%s ema_len=%s TF=%s code=%s",
                                    pos_uid, pos["strategy_id"], pos["direction"], ema_len, tf, code
                                )
                            if all(tf in per_tf for tf in ("m5","m15","h1")):
                                triplet = f"{per_tf['m5']}-{per_tf['m15']}-{per_tf['h1']}"
                                log.info(
                                    "[EMA-STATUS SNAP] uid=%s strat=%s dir=%s ema_len=%s triplet=%s",
                                    pos_uid, pos["strategy_id"], pos["direction"], ema_len, triplet
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