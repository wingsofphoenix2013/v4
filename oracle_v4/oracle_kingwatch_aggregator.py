# 🔸 oracle_kingwatch_aggregator.py — KingWatcher: композитная агрегация по триплету MW на баре открытия (для стратегий king_watcher=true)

import os
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_KW_AGG")


# 🔸 Конфиг consumer-группы и чтения
STREAM_NAME   = os.getenv("ORACLE_KW_STREAM",   "signal_log_queue")
GROUP_NAME    = os.getenv("ORACLE_KW_GROUP",    "oracle_kingwatch_aggregator")
CONSUMER_NAME = os.getenv("ORACLE_KW_CONSUMER", "oracle_kw_1")
XREAD_COUNT   = int(os.getenv("ORACLE_KW_COUNT",    "50"))
XREAD_BLOCKMS = int(os.getenv("ORACLE_KW_BLOCK_MS", "1000"))


# 🔸 Таймшаги TF
TF_ORDER = ("m5", "m15", "h1")
TF_STEP_SEC = {"m5": 300, "m15": 900, "h1": 3600}


# 🔸 Утилита: floor к началу бара TF (UTC, NAIVE)
def _floor_to_bar_open(dt_utc: datetime, tf: str) -> datetime:
    """
    Принимает datetime в UTC. Возвращает NAIVE UTC datetime (tzinfo=None).
    """
    # приводим к naive UTC
    if dt_utc.tzinfo is not None:
        dt_utc = dt_utc.astimezone(timezone.utc).replace(tzinfo=None)
    step = TF_STEP_SEC[tf]
    epoch = int(dt_utc.timestamp())  # трактуется как UTC для naive datetime
    floored = (epoch // step) * step
    return datetime.utcfromtimestamp(floored)  # naive UTC


# 🔸 Идемпотентно создать consumer-group
async def _ensure_group():
    try:
        await infra.redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="$", mkstream=True)
        log.debug("✅ Consumer group '%s' создана на '%s'", GROUP_NAME, STREAM_NAME)
    except Exception as e:
        # BUSYGROUP → группа уже есть
        if "BUSYGROUP" in str(e):
            log.debug("ℹ️ Consumer group '%s' уже существует", GROUP_NAME)
        else:
            log.exception("❌ Ошибка создания consumer group: %s", e)
            raise


# 🔸 Advisory-lock по агрегатному ключу (в рамках текущей транзакции)
async def _advisory_xact_lock(conn, class_id: int, key_text: str):
    # используем детерминированный hashtext(text) → int4; двухкомпонентный ключ (class_id, hash)
    await conn.execute(
        "SELECT pg_advisory_xact_lock($1::int4, hashtext($2)::int4)",
        int(class_id), key_text
    )


# 🔸 Загрузка позиции и стратегии (предварительные фильтры для KW)
async def _load_position_and_strategy(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        # без FOR UPDATE: row-lock берём на claim
        pos = await conn.fetchrow(
            """
            SELECT p.id, p.position_uid, p.symbol, p.direction, p.strategy_id,
                   p.pnl, p.status, p.created_at,
                   COALESCE(p.king_watcher_checked, false) AS kw_checked
            FROM positions_v4 p
            WHERE p.position_uid = $1
            """,
            position_uid,
        )
        if not pos:
            return None, None, ("skip", "position_not_found")
        if pos["status"] != "closed":
            return pos, None, ("skip", "position_not_closed")
        if pos["kw_checked"]:
            return pos, None, ("skip", "already_processed")

        strat = await conn.fetchrow(
            """
            SELECT id, enabled, COALESCE(archived, false) AS archived, COALESCE(king_watcher, false) AS kw
            FROM strategies_v4
            WHERE id = $1
            """,
            int(pos["strategy_id"]),
        )
        if (not strat) or (not strat["enabled"]) or bool(strat["archived"]) or (not strat["kw"]):
            return pos, strat, ("skip", "strategy_inactive_or_no_kw")

        return pos, strat, ("ok", "eligible")


# 🔸 Прочитать regime_code из indicator_marketwatcher_v4 по (symbol, TF, bar_open_time)
async def _load_imw_code(symbol: str, tf: str, bar_open: datetime):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        code = await conn.fetchval(
            """
            SELECT regime_code
            FROM indicator_marketwatcher_v4
            WHERE symbol = $1 AND timeframe = $2 AND open_time = $3
            """,
            symbol, tf, bar_open
        )
    return None if code is None else int(code)


# 🔸 Собрать триплет MW-кодов на баре открытия
async def _collect_mw_triplet(symbol: str, created_at_utc: datetime):
    per_tf = {}
    # по каждому TF пытаемся поднять код
    for tf in TF_ORDER:
        bar_open = _floor_to_bar_open(created_at_utc, tf)
        code = await _load_imw_code(symbol, tf, bar_open)
        if code is None:
            # условия достаточности: триплет только при наличии всех трёх
            return None
        per_tf[tf] = code
    # формируем строку-триплет
    return f"{per_tf['m5']}-{per_tf['m15']}-{per_tf['h1']}"


# 🔸 Claim позиции и апдейт агрегата KW (композит) + публикация Redis KV
async def _claim_and_update_kw(pos, triplet: str):
    pg = infra.pg_pool
    redis = infra.redis_client

    strategy_id = int(pos["strategy_id"])
    direction   = str(pos["direction"])

    pnl_raw = pos["pnl"]
    pnl = Decimal(str(pnl_raw if pnl_raw is not None else "0")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    is_win = 1 if pnl > Decimal("0") else 0

    async with pg.acquire() as conn:
        async with conn.transaction():
            # claim позиции: если уже забрана/отмечена — выходим
            claimed = await conn.fetchrow(
                """
                UPDATE positions_v4
                SET king_watcher_checked = true
                WHERE position_uid = $1
                  AND status = 'closed'
                  AND COALESCE(king_watcher_checked, false) = false
                RETURNING position_uid
                """,
                pos["position_uid"]
            )
            if not claimed:
                return ("claim_skip", 0)

            # advisory-lock по агрегатному ключу (класс 10 для KW-comp)
            await _advisory_xact_lock(conn, 10, f"{strategy_id}:{direction}:{triplet}")

            # предсоздание строки агрегата (идемпотентно)
            await conn.execute(
                """
                INSERT INTO positions_kw_stat_comp
                  (strategy_id, direction, status_triplet,
                   closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                VALUES ($1,$2,$3, 0,0,0,0,0,NOW())
                ON CONFLICT (strategy_id, direction, status_triplet) DO NOTHING
                """,
                strategy_id, direction, triplet
            )

            # апдейт агрегата под FOR UPDATE
            row = await conn.fetchrow(
                """
                SELECT closed_trades, won_trades, pnl_sum
                FROM positions_kw_stat_comp
                WHERE strategy_id=$1 AND direction=$2 AND status_triplet=$3
                FOR UPDATE
                """,
                strategy_id, direction, triplet
            )
            c0 = int(row["closed_trades"]); w0 = int(row["won_trades"]); s0 = Decimal(str(row["pnl_sum"]))
            c = c0 + 1
            w = w0 + is_win
            s = (s0 + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

            await conn.execute(
                """
                UPDATE positions_kw_stat_comp
                SET closed_trades=$4, won_trades=$5, pnl_sum=$6, winrate=$7, avg_pnl=$8, updated_at=NOW()
                WHERE strategy_id=$1 AND direction=$2 AND status_triplet=$3
                """,
                strategy_id, direction, triplet,
                c, w, str(s), str(wr), str(ap)
            )

            # KV публикация
            try:
                await redis.set(
                    f"oracle:kw:comp:{strategy_id}:{direction}:{triplet}",
                    f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                )
            except Exception:
                log.debug("Redis SET failed (kw comp)")

            return ("ok", c)


# 🔸 Основной цикл
async def run_oracle_kingwatch_aggregator():
    await _ensure_group()
    log.debug("🚀 KW AGG: слушаем '%s' (group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)

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
                            log.debug("[KW AGG] skip msg=%s uid=%s reason=status=%s", msg_id, pos_uid, status)
                            continue

                        # загрузили позицию/стратегию и предварительно отфильтровали
                        pos, strat, verdict = await _load_position_and_strategy(pos_uid)
                        v_code, v_reason = verdict
                        if v_code != "ok":
                            log.debug("[KW AGG] uid=%s skip: %s", pos_uid, v_reason)
                            continue

                        # время открытия позиции → бар открытия для каждого TF
                        created_at = pos["created_at"]
                        created_at_utc = created_at.astimezone(timezone.utc).replace(tzinfo=None) if created_at.tzinfo is not None else created_at

                        # собираем триплет MW; если неполный — оставляем на backfill
                        triplet = await _collect_mw_triplet(pos["symbol"], created_at_utc)
                        if triplet is None:
                            log.info("[KW AGG] uid=%s strat=%s dir=%s — MW триплет не готов (postpone)",
                                     pos_uid, pos["strategy_id"], pos["direction"])
                            continue

                        # claim позиции + апдейт агрегата
                        agg_status, closed_trades = await _claim_and_update_kw(pos, triplet)
                        if agg_status == "ok":
                            win_flag = 1 if (pos["pnl"] is not None and pos["pnl"] > 0) else 0
                            log.info("[KW AGG] uid=%s strat=%s dir=%s triplet=%s — aggregated (win=%d, closed_trades=%d)",
                                     pos_uid, pos["strategy_id"], pos["direction"], triplet, win_flag, closed_trades)
                        else:
                            log.debug("[KW AGG] uid=%s — уже обработана параллельно (claim skipped)", pos_uid)

                    except Exception as e:
                        log.exception("❌ KW AGG msg error %s: %s", msg_id, e)

            if to_ack:
                await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *to_ack)

        except asyncio.CancelledError:
            log.debug("⏹️ KW агрегатор остановлен")
            raise
        except Exception as e:
            log.exception("❌ KW XREADGROUP loop error: %s", e)
            await asyncio.sleep(1)