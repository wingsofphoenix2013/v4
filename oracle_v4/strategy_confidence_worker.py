# strategy_confidence_worker.py

import asyncio
import logging
import json
import math

import infra

log = logging.getLogger("STRATEGY_CONFIDENCE_WORKER")

STREAM_NAME = "emasnapshot:ratings:commands"

# 🔸 Выделение таймфрейма из имени таблицы
def extract_tf_from_table_name(table: str) -> str:
    parts = table.split("_")
    if len(parts) >= 3:
        return parts[-2]
    raise ValueError(f"❌ Не удалось определить таймфрейм из имени таблицы: {table}")
    
# 🔸 Основной воркер
async def run_strategy_confidence_worker():
    redis = infra.redis_client

    try:
        stream_info = await redis.xinfo_stream(STREAM_NAME)
        last_id = stream_info["last-generated-id"]
    except Exception as e:
        log.warning(f"⚠️ Не удалось получить last ID из stream: {e}")
        last_id = "$"

    log.info(f"📡 Подписка на Redis Stream: {STREAM_NAME}")

    while True:
        try:
            response = await redis.xread(
                streams={STREAM_NAME: last_id},
                count=50,
                block=1000
            )
            for stream, messages in response:
                for msg_id, msg_data in messages:
                    parsed = {k: v for k, v in msg_data.items()}
                    asyncio.create_task(handle_message(parsed))
                    last_id = msg_id
        except Exception:
            log.exception("❌ Ошибка чтения из Redis Stream")
            await asyncio.sleep(1)

# 🔸 Обработка одного сообщения
async def handle_message(msg: dict):
    table = msg.get("table")
    strategies_raw = msg.get("strategies")

    if not table or not strategies_raw:
        log.warning(f"⚠️ Неверное сообщение: {msg}")
        return

    try:
        strategy_ids = json.loads(strategies_raw)
        assert isinstance(strategy_ids, list)
    except Exception:
        log.warning(f"⚠️ Не удалось распарсить список стратегий: {strategies_raw}")
        return

    log.info(f"📩 Принято сообщение: table = {table}, strategies = {strategy_ids}")

    async with infra.pg_pool.acquire() as conn:
        for strategy_id in strategy_ids:
            if "emasnapshot" in table and "pattern" not in table:
                log.info(f"🔍 Обработка snapshot-таблицы: {table} | strategy_id={strategy_id}")
                await process_snapshot_confidence(conn, table, strategy_id)
            else:
                log.info(f"⏭ Пропуск: пока не поддерживается тип таблицы {table}")

# 🔸 Расчёт и логирование confidence_score для snapshot-таблицы
async def process_snapshot_confidence(conn, table: str, strategy_id: int):
    tf = extract_tf_from_table_name(table)

    rows = await conn.fetch(f"""
        SELECT strategy_id, direction, emasnapshot_dict_id, num_trades, num_wins
        FROM {table}
        WHERE strategy_id = $1
    """, strategy_id)

    if not rows:
        log.info(f"⏭ Пропуск: нет строк в {table} для strategy_id={strategy_id}")
        return

    global_data = await conn.fetchrow(f"""
        SELECT SUM(num_wins)::float / NULLIF(SUM(num_trades), 0) AS global_winrate,
               SUM(num_trades)::int AS total_trades
        FROM {table}
        WHERE strategy_id = $1
    """, strategy_id)

    gw = global_data["global_winrate"] or 0.0
    total = global_data["total_trades"] or 0
    vweight = min(total / 10, 20)
    alpha = gw * vweight
    beta = (1 - gw) * vweight

    trade_counts = [r["num_trades"] for r in rows]
    trade_counts.sort()

    mean = sum(trade_counts) / len(trade_counts)
    median = trade_counts[len(trade_counts) // 2]
    p25 = trade_counts[int(len(trade_counts) * 0.25)]

    if abs(mean - median) / mean < 0.1:
        threshold_n = round(0.1 * mean)
        method = "mean*0.1"
    elif median < mean * 0.6:
        threshold_n = max(5, round(median / 2))
        method = "median/2"
    else:
        threshold_n = round(p25)
        method = "percentile_25"

    fragmented = sum(1 for n in trade_counts if n < threshold_n)
    fragmentation = fragmented / len(trade_counts)
    frag_modifier = max(0.3, (1 - fragmentation) ** 0.7)

    log.info(f"📊 strategy={strategy_id} tf={tf} → T={threshold_n} by {method}, frag={fragmentation:.3f}")

    for row in rows:
        w = row["num_wins"]
        n = row["num_trades"]
        sid = row["emasnapshot_dict_id"]
        direction = row["direction"]

        bayes_wr = (w + alpha) / (n + alpha + beta)
        score = bayes_wr * math.log(1 + n) * frag_modifier

        # 🔸 Временно отключена запись в агрегатную таблицу
        # await conn.execute(f"""
        #     UPDATE {table}
        #     SET confidence_score_snapshot = $1
        #     WHERE strategy_id = $2 AND direction = $3 AND emasnapshot_dict_id = $4
        # """, score, strategy_id, direction, sid)

        # 🔸 Запись в лог
        await conn.execute("""
            INSERT INTO strategy_confidence_log (
                strategy_id, direction, tf, object_type, object_id,
                num_trades, num_wins, global_winrate, alpha, beta,
                mean, median, percentile_25, threshold_n, threshold_method,
                fragmentation, density, confidence_score
            ) VALUES (
                $1, $2, $3, 'snapshot', $4,
                $5, $6, $7, $8, $9,
                $10, $11, $12, $13, $14,
                $15, NULL, $16
            )
        """, strategy_id, direction, tf, sid,
             n, w, gw, alpha, beta,
             mean, median, p25, threshold_n, method,
             fragmentation, score)

        log.debug(f"[OK] strategy={strategy_id} snapshot_id={sid} score={score:.4f}")
        
# 🔸 Основной воркер
async def run_strategy_confidence_worker():
    redis = infra.redis_client

    try:
        stream_info = await redis.xinfo_stream(STREAM_NAME)
        last_id = stream_info["last-generated-id"]
    except Exception as e:
        log.warning(f"⚠️ Не удалось получить last ID из stream: {e}")
        last_id = "$"

    log.info(f"📡 Подписка на Redis Stream: {STREAM_NAME}")

    while True:
        try:
            response = await redis.xread(
                streams={STREAM_NAME: last_id},
                count=50,
                block=1000
            )
            for stream, messages in response:
                for msg_id, msg_data in messages:
                    parsed = {k: v for k, v in msg_data.items()}
                    asyncio.create_task(handle_message(parsed))
                    last_id = msg_id
        except Exception:
            log.exception("❌ Ошибка чтения из Redis Stream")
            await asyncio.sleep(1)