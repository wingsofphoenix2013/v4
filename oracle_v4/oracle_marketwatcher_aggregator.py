# 🔸 oracle_marketwatcher_aggregator.py — Этап 2: транзакционный онлайн-учёт закрытий + публикация Redis-ключа

import os
import json
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

import infra

# 🔸 Константы стрима/группы
STREAM_NAME   = os.getenv("ORACLE_MW_STREAM", "signal_log_queue")
GROUP_NAME    = os.getenv("ORACLE_MW_GROUP",  "oracle_mw")
CONSUMER_NAME = os.getenv("ORACLE_MW_CONSUMER","oracle_mw_1")
XREAD_COUNT   = int(os.getenv("ORACLE_MW_COUNT", "50"))
XREAD_BLOCKMS = int(os.getenv("ORACLE_MW_BLOCK_MS", "1000"))

# 🔸 Redis ключ публикации агрегата
def stat_key(strategy_id: int, direction: str, marker3_code: str) -> str:
    return f"oracle:mw:stat:{strategy_id}:{direction}:{marker3_code}"

log = logging.getLogger("ORACLE_MW_AGG")


# 🔸 Создание consumer-group (идемпотентно)
async def _ensure_group():
    try:
        await infra.redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="$", mkstream=True)
        log.debug("✅ Consumer group '%s' создана на '%s'", GROUP_NAME, STREAM_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug("ℹ️ Consumer group '%s' уже существует", GROUP_NAME)
        else:
            log.exception("❌ Ошибка создания consumer group: %s", e)
            raise


# 🔸 Вспомогательные: floor времени (UTC) под TF
def _floor_to_step_utc(dt: datetime, minutes: int) -> datetime:
    # dt должен быть без tz или tz=UTC; приводим к naive-UTC (как в БД)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    # обнуляем секунды/микросекунды
    base = dt.replace(second=0, microsecond=0)
    step = minutes
    floored_minute = (base.minute // step) * step
    return base.replace(minute=floored_minute)

def _label_from_code(code: int) -> str:
    mapping = {
        0: "F_CONS", 1: "F_EXP", 2: "F_DRIFT",
        3: "U_ACCEL", 4: "U_STABLE", 5: "U_DECEL",
        6: "D_ACCEL", 7: "D_STABLE", 8: "D_DECEL",
    }
    return mapping.get(code, "N/A")


# 🔸 Транзакционная обработка одного закрытия (по position_uid)
async def _process_closed_position(position_uid: str, strategy_id_str: str):
    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            # 1) Загружаем позицию FOR UPDATE
            pos = await conn.fetchrow("""
                SELECT p.id, p.strategy_id, p.symbol, p.direction, p.created_at, p.pnl, p.status, 
                       COALESCE(p.mrk_watcher_checked, false) AS checked
                FROM positions_v4 p
                WHERE p.position_uid = $1
                FOR UPDATE
            """, position_uid)
            if not pos:
                log.debug("[SKIP] pos_uid=%s не найдена в positions_v4", position_uid)
                return

            if pos["status"] != "closed":
                log.debug("[SKIP] pos_uid=%s статус не 'closed' (%s)", position_uid, pos["status"])
                return

            if pos["checked"]:
                log.debug("[SKIP] pos_uid=%s уже отмечена mrk_watcher_checked=true", position_uid)
                return

            # 2) Проверяем, что стратегия активна и market_watcher=true
            strat = await conn.fetchrow("""
                SELECT id, enabled, COALESCE(market_watcher, false) AS mw
                FROM strategies_v4
                WHERE id = $1
            """, int(pos["strategy_id"]))
            if not strat or not strat["enabled"] or not strat["mw"]:
                log.debug("[SKIP] pos_uid=%s: стратегия %s не активна для market_watcher", position_uid, pos["strategy_id"])
                # помечать checked нельзя — её вообще не учитываем
                return

            # 3) Вычисляем open_time баров m5/m15/h1 по created_at (UTC)
            created_at: datetime = pos["created_at"]  # TIMESTAMP (UTC)
            m5_open  = _floor_to_step_utc(created_at, 5)
            m15_open = _floor_to_step_utc(created_at, 15)
            h1_open  = _floor_to_step_utc(created_at, 60)

            # 4) Пытаемся взять три маркера (версия режима одна — v2)
            symbol = pos["symbol"]
            rows = await conn.fetch("""
                SELECT timeframe, regime_code
                FROM indicator_marketwatcher_v4
                WHERE symbol = $1 AND timeframe = ANY($2::text[]) AND open_time = ANY($3::timestamp[])
            """, symbol, ["m5","m15","h1"], [m5_open, m15_open, h1_open])

            markers = {r["timeframe"]: r["regime_code"] for r in rows}
            if not all(tf in markers for tf in ("m5","m15","h1")):
                # Не все готовы — отложим на бэкофилл
                log.debug("[DEFER] pos_uid=%s: не все маркеры доступны (m5=%s, m15=%s, h1=%s)",
                         position_uid, markers.get("m5"), markers.get("m15"), markers.get("h1"))
                return

            # 5) Формируем marker3_code и label
            m5_code, m15_code, h1_code = int(markers["m5"]), int(markers["m15"]), int(markers["h1"])
            marker3_code = f"{m5_code}-{m15_code}-{h1_code}"
            marker3_label = f"{_label_from_code(m5_code)}-{_label_from_code(m15_code)}-{_label_from_code(h1_code)}"

            # 6) Считаем агрегаты в Python (Decimal и округление до 4 знаков)
            direction: str = pos["direction"]  # 'long'|'short'
            pnl = Decimal(str(pos["pnl"])).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            is_win = 1 if pnl > Decimal("0") else 0

            # Получаем текущую строку агрегата FOR UPDATE
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

            # 7) UPSERT агрегата готовыми значениями
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

    # 9) Публикация Redis-ключа (после коммита)
    try:
        value = json.dumps({
            "closed_trades": closed_trades,
            "winrate": float(winrate)  # или строкой с 4 знаками: format(winrate, ".4f")
        })
        await infra.redis_client.set(stat_key(int(pos["strategy_id"]), direction, marker3_code), value)
        log.debug("[AGG] strat=%s dir=%s marker=%s → closed=%d won=%d winrate=%.4f avg_pnl=%s",
                 pos["strategy_id"], direction, marker3_code, closed_trades, won_trades, float(winrate), str(avg_pnl))
        log.debug("[SET] %s = %s", stat_key(int(pos["strategy_id"]), direction, marker3_code), value)
    except Exception as e:
        log.exception("❌ Ошибка публикации Redis-ключа для стратегии %s / %s / %s: %s",
                      pos["strategy_id"], direction, marker3_code, e)


# 🔸 Запуск агрегатора: XREADGROUP, обработка только status='closed'
async def run_oracle_marketwatcher_aggregator():
    await _ensure_group()
    log.debug("🚀 Этап 2: слушаем stream '%s' (group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)

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
            for _, messages in resp:
                for msg_id, data in messages:
                    try:
                        status = data.get("status")
                        if status != "closed":
                            to_ack.append(msg_id)
                            continue

                        position_uid = data.get("position_uid")
                        strategy_id_str = data.get("strategy_id")

                        log.debug("[STAGE2] closed-event: pos=%s strat=%s", position_uid, strategy_id_str)

                        # транзакционная обработка
                        await _process_closed_position(position_uid, strategy_id_str)

                        to_ack.append(msg_id)

                    except Exception as e:
                        to_ack.append(msg_id)
                        log.exception("❌ Ошибка обработки сообщения %s: %s", msg_id, e)

            if to_ack:
                await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *to_ack)

        except asyncio.CancelledError:
            log.debug("⏹️ Аггрегатор остановлен")
            raise
        except Exception as e:
            log.exception("❌ Ошибка XREADGROUP: %s", e)
            await asyncio.sleep(1)