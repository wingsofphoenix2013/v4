# oracle_bbbins_snapshot_aggregator.py — BB-bins snapshot агрегатор: Этап 2 (позиция + валидация)

import os
import asyncio
import logging

import infra

log = logging.getLogger("ORACLE_BBBINS_SNAP")

# 🔸 Конфиг consumer-группы и чтения
STREAM_NAME   = os.getenv("ORACLE_BBBINS_STREAM",   "signal_log_queue")
GROUP_NAME    = os.getenv("ORACLE_BBBINS_GROUP",    "oracle_bbbins_snap")
CONSUMER_NAME = os.getenv("ORACLE_BBBINS_CONSUMER", "oracle_bbbins_1")
XREAD_COUNT   = int(os.getenv("ORACLE_BBBINS_COUNT",    "50"))
XREAD_BLOCKMS = int(os.getenv("ORACLE_BBBINS_BLOCK_MS", "1000"))

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

# 🔸 Загрузка позиции и стратегии под FOR UPDATE + базовые проверки
async def _load_position_and_strategy(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        async with conn.transaction():
            pos = await conn.fetchrow("""
                SELECT p.id, p.position_uid, p.symbol, p.direction, p.strategy_id,
                       p.entry_price, p.pnl, p.status,
                       COALESCE(p.bb_checked, false) AS bb_checked
                FROM positions_v4 p
                WHERE p.position_uid = $1
                FOR UPDATE
            """, position_uid)

            if not pos:
                return None, None, ("skip", "position_not_found")
            if pos["status"] != "closed":
                return pos, None, ("skip", "position_not_closed")
            if pos["bb_checked"]:
                return pos, None, ("skip", "already_checked")

            strat = await conn.fetchrow("""
                SELECT id, enabled, COALESCE(market_watcher, false) AS mw
                FROM strategies_v4
                WHERE id = $1
            """, int(pos["strategy_id"]))
            if not strat or not strat["enabled"] or not strat["mw"]:
                return pos, strat, ("skip", "strategy_inactive_or_no_mw")

            return pos, strat, ("ok", "eligible")

# 🔸 Основной цикл: Этап 1 лог → debug, Этап 2 — info по валидации
async def run_oracle_bbbins_snapshot_aggregator():
    await _ensure_group()
    log.info("🚀 BB-BINS SNAP: слушаем '%s' (group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=XREAD_COUNT,
                block=XREAD_BLOCKMS
            )
            if not resp:
                continue

            to_ack = []
            for _, records in resp:
                for msg_id, data in records:
                    try:
                        if data.get("status") != "closed":
                            to_ack.append(msg_id)
                            continue

                        pos_uid = data.get("position_uid")
                        # Этап 1 был info → теперь debug
                        log.debug("[BB-BINS SNAP] closed position received: uid=%s", pos_uid)

                        pos, strat, verdict = await _load_position_and_strategy(pos_uid)
                        v_code, v_reason = verdict
                        if v_code == "ok":
                            log.info("[BB-BINS SNAP] eligible uid=%s strat=%s dir=%s entry=%s",
                                     pos["position_uid"], pos["strategy_id"], pos["direction"], pos["entry_price"])
                            # На следующем этапе: тянем BB upper/lower из PIS и биним entry_price → корзины.
                        else:
                            log.info("[BB-BINS SNAP] skip uid=%s reason=%s", pos_uid, v_reason)

                        to_ack.append(msg_id)

                    except Exception as e:
                        to_ack.append(msg_id)
                        log.exception("❌ BB-BINS SNAP msg error %s: %s", msg_id, e)

            if to_ack:
                await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *to_ack)

        except asyncio.CancelledError:
            log.info("⏹️ BB-BINS snapshot агрегатор остановлен")
            raise
        except Exception as e:
            log.exception("❌ XREADGROUP loop error: %s", e)
            await asyncio.sleep(1)