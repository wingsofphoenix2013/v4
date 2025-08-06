# voting_engine.py

import asyncio
import logging
import json
import math

import infra

log = logging.getLogger("VOTING_ENGINE")

REQUEST_STREAM = "strategy_voting_request"
RESPONSE_STREAM = "strategy_voting_answer"

TF_WEIGHTS = {
    "m5": 1.0,
    "m15": 1.0,
    "h1": 1.0
}


# 🔸 Классификация winrate → голос
def classify_vote(winrate: float) -> int:
    if winrate < 0.35:
        return -1
    elif winrate < 0.45:
        return -1
    elif winrate < 0.55:
        return 0
    elif winrate < 0.65:
        return 1
    return 1


# 🔸 Проверка вето и анти-вето
def check_veto(winrate: float, confidence: float) -> tuple[bool, bool]:
    veto = winrate < 0.35 and confidence >= 0.9
    anti_veto = winrate > 0.65 and confidence >= 0.7
    return veto, anti_veto


# 🔸 Основной воркер
async def run_voting_engine():
    redis = infra.redis_client

    try:
        stream_info = await redis.xinfo_stream(REQUEST_STREAM)
        last_id = stream_info["last-generated-id"]
    except Exception:
        last_id = "$"

    log.info(f"📡 Подписка на Redis Stream: {REQUEST_STREAM} с last_id = {last_id}")

    while True:
        try:
            response = await redis.xread(
                streams={REQUEST_STREAM: last_id},
                count=50,
                block=1000
            )
            for stream, messages in response:
                for msg_id, msg_data in messages:
                    parsed = {k: v for k, v in msg_data.items()}
                    asyncio.create_task(handle_voting_request(parsed))
                    last_id = msg_id
        except Exception:
            log.exception("❌ Ошибка чтения из Redis Stream")
            await asyncio.sleep(1)


# 🔸 Обработка одного запроса голосования
async def handle_voting_request(msg: dict):
    try:
        strategy_id = int(msg["strategy_id"])
        direction = msg["direction"]
        tf = msg["tf"]
        log_uid = msg["log_uid"]

        redis = infra.redis_client

        votes = []
        total_score = 0.0
        veto_applied = False
        anti_veto_applied = False

        for tf_key in ["m5", "m15", "h1"]:
            for obj_type in ["snapshot", "pattern"]:
                # 🔹 Получение актуального ID объекта
                snap_key = f"snapshot:{strategy_id}:{tf_key}"
                val = await redis.get(snap_key)
                if not val:
                    continue

                try:
                    data = json.loads(val)
                    object_id = data["snapshot_id"] if obj_type == "snapshot" else data["pattern_id"]
                except Exception:
                    continue

                # 🔹 Чтение доверия из Redis
                conf_key = f"confidence:{strategy_id}:{direction}:{tf_key}:{obj_type}:{object_id}"
                conf_raw = await redis.get(conf_key)
                if not conf_raw:
                    continue

                try:
                    conf = json.loads(conf_raw)
                    winrate = conf.get("winrate", 0)
                    confidence = conf.get("confidence_raw", 0)
                except Exception:
                    continue

                vote = classify_vote(winrate)
                veto, anti_veto = check_veto(winrate, confidence)
                weight = TF_WEIGHTS.get(tf_key, 1.0)
                contribution = vote * confidence * weight

                votes.append({
                    "source": f"{obj_type}_{tf_key}",
                    "winrate": winrate,
                    "confidence": confidence,
                    "weight": weight,
                    "vote": vote,
                    "contribution": contribution,
                    "veto": veto,
                    "anti_veto": anti_veto
                })

                total_score += contribution
                veto_applied |= veto
                anti_veto_applied |= anti_veto

        if veto_applied and anti_veto_applied:
            veto_applied = False

        if veto_applied:
            decision = "reject"
        elif total_score >= 2:
            decision = "open"
        elif total_score <= -2:
            decision = "reject"
        else:
            decision = "neutral"

        async with infra.pg_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO strategy_voting_log (
                    log_uid, strategy_id, direction, tf,
                    total_score, decision, veto_applied, votes
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """, log_uid, strategy_id, direction, tf,
                 total_score, decision, veto_applied, json.dumps(votes))

        await redis.xadd(RESPONSE_STREAM, {
            "log_uid": log_uid,
            "strategy_id": strategy_id,
            "decision": decision,
            "total_score": str(round(total_score, 6)),
            "veto_applied": json.dumps(veto_applied),
            "votes": json.dumps(votes)
        })

        log.info(f"✅ Голосование log_uid={log_uid} → {decision.upper()} (score={total_score:.3f})")

    except Exception:
        log.exception("❌ Ошибка обработки голосования")