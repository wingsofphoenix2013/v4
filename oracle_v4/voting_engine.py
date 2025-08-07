# voting_engine.py

import asyncio
import logging
import json

import infra

from voting_core import save_voting_result

log = logging.getLogger("VOTING_ENGINE")

REQUEST_STREAM = "strategy_voting_request"
RESPONSE_STREAM = "strategy_voting_answer"

VOTING_MODELS = {
    "score_0": {"score_threshold": 0.0},
    "score_1": {"score_threshold": 1.0},
    "score_2": {"score_threshold": 2.0},

    "tf_equal": {"tf_weights": {"m5": 1.0, "m15": 1.0, "h1": 1.0}},
    "tf_h1": {"tf_weights": {"m5": 0.75, "m15": 1.0, "h1": 1.25}},
    "tf_m5": {"tf_weights": {"m5": 1.25, "m15": 1.0, "h1": 0.75}},

    "conf_015": {"min_confidence_raw": 0.15},
    "conf_025": {"min_confidence_raw": 0.25},
    "conf_035": {"min_confidence_raw": 0.35}
}

# 🔹 Основной цикл чтения сообщений
async def run_voting_engine():
    redis = infra.redis_client

    try:
        stream_info = await redis.xinfo_stream(REQUEST_STREAM)
        last_id = stream_info["last-generated-id"]
    except Exception:
        last_id = "$"

    log.debug(f"📡 Подписка на Redis Stream: {REQUEST_STREAM} с last_id = {last_id}")

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


# 🔹 Обработка одного запроса голосования
async def handle_voting_request(msg: dict):
    try:
        strategy_id = int(msg["strategy_id"])
        direction = msg["direction"]
        tf_trigger = msg["tf"]
        symbol = msg["symbol"]
        log_uid = msg["log_uid"]

        log.debug(f"📥 log_uid={log_uid} | strategy={strategy_id} | dir={direction} | tf={tf_trigger} | symbol={symbol}")

        redis = infra.redis_client
        snapshots = {}

        for tf in ["m5", "m15", "h1"]:
            key = f"snapshot:{symbol}:{tf}"
            val = await redis.get(key)
            if not val:
                log.warning(f"⛔ Не найден ключ {key}")
                continue

            try:
                data = json.loads(val)
                snapshots[tf] = {
                    "snapshot_id": data["snapshot_id"],
                    "pattern_id": data["pattern_id"]
                }
                log.debug(f"🔍 {tf}: snapshot_id={data['snapshot_id']}, pattern_id={data['pattern_id']}")
            except Exception:
                log.warning(f"❌ Ошибка парсинга значения {key}")
                continue

        if not snapshots:
            log.warning(f"⚠️ log_uid={log_uid} → ни одного объекта не найдено")
            return

        # 📥 Чтение confidence
        votes = []
        for tf, obj in snapshots.items():
            for obj_type in ["snapshot", "pattern"]:
                object_id = obj.get(f"{obj_type}_id") or obj.get(f"{obj_type}id")
                conf_key = f"confidence:{strategy_id}:{direction}:{tf}:{obj_type}:{object_id}"
                raw = await redis.get(conf_key)
                if not raw:
                    log.warning(f"⛔ Нет confidence ключа: {conf_key}")
                    continue

                try:
                    conf = json.loads(raw)
                    winrate = conf.get("winrate")
                    confidence = conf.get("confidence_raw")
                    if winrate is None or confidence is None:
                        continue

                    if winrate < 0.35:
                        vote = -1.25
                    elif winrate < 0.45:
                        vote = -1.0
                    elif winrate < 0.55:
                        vote = 0.0
                    elif winrate < 0.65:
                        vote = 1.0
                    else:
                        vote = 1.25

                    veto = winrate < 0.35 and confidence >= 0.9
                    anti_veto = winrate > 0.65 and confidence >= 0.7

                    votes.append({
                        "source": f"{obj_type}_{tf}",
                        "object_id": object_id,
                        "winrate": winrate,
                        "confidence": confidence,
                        "vote": vote,
                        "tf": tf,
                        "veto": veto,
                        "anti_veto": anti_veto
                    })

                    log.debug(f"📦 {obj_type}_{tf}: winrate={winrate:.3f}, conf={confidence:.3f}")

                except Exception:
                    log.warning(f"❌ Ошибка парсинга JSON: {conf_key}")

        if not votes:
            log.warning(f"⚠️ log_uid={log_uid} → нет допустимых объектов для голосования")
            return

        # 🔹 Основная модель (жёстко: score ≥ 2.0)
        total_score = 0.0
        veto_count = 0
        anti_veto_count = 0

        for v in votes:
            weight = 1.0
            contrib = v["vote"] * v["confidence"] * weight
            v["weight"] = weight
            v["contribution"] = contrib
            total_score += contrib
            if v["veto"]:
                veto_count += 1
            if v["anti_veto"]:
                anti_veto_count += 1
            log.debug(f"🗳️ {v['source']} | vote={v['vote']:+.2f} | conf={v['confidence']:.3f} | contrib={contrib:.3f} | veto={v['veto']} | anti={v['anti_veto']}")

        net_veto = veto_count - anti_veto_count
        if net_veto > 0:
            decision = "reject"
            log.debug(f"❌ Принудительное отклонение: {veto_count} вето против {anti_veto_count} анти-вето")
        else:
            decision = "open" if total_score >= 2.0 else "reject"
            if veto_count > 0 or anti_veto_count > 0:
                log.debug(f"⚖️ Баланс вето: {veto_count} vs анти-вето: {anti_veto_count} → голосуем по score")

        log.debug(f"✅ Голосование log_uid={log_uid} → {decision.upper()} (score={total_score:.3f})")
        log.debug(f"🎯 TOTAL SCORE: {total_score:.3f}")
        log.debug(f"✅ DECISION: {decision.upper()}")
        log.debug(f"⚖️ Вето: {veto_count} | Антивето: {anti_veto_count}")

        # ✅ Сохранение основного голосования
        await save_voting_result(
            log_uid=log_uid,
            strategy_id=strategy_id,
            direction=direction,
            tf=tf_trigger,
            symbol=symbol,
            model="main",
            total_score=total_score,
            decision=decision,
            veto_applied=(net_veto > 0),
            votes=votes
        )

        # 🔬 A/B модели
        await evaluate_models(
            votes=votes,
            log_uid=log_uid,
            strategy_id=strategy_id,
            direction=direction,
            tf=tf_trigger,
            symbol=symbol
        )

    except Exception:
        log.exception("❌ Ошибка обработки запроса голосования")


# 🔹 A/B сравнение по всем моделям
async def evaluate_models(
    votes: list,
    log_uid: str,
    strategy_id: int,
    direction: str,
    tf: str,
    symbol: str
):
    for model_name, model_cfg in VOTING_MODELS.items():
        threshold = model_cfg.get("score_threshold", 2.0)
        min_conf = model_cfg.get("min_confidence_raw", 0.0)
        tf_weights = model_cfg.get("tf_weights", {"m5": 1.0, "m15": 1.0, "h1": 1.0})

        score = 0.0
        for v in votes:
            if v["confidence"] < min_conf:
                continue
            weight = tf_weights.get(v["tf"], 1.0)
            score += v["vote"] * v["confidence"] * weight

        decision = "open" if score >= threshold else "reject"

        await save_voting_result(
            log_uid=log_uid,
            strategy_id=strategy_id,
            direction=direction,
            tf=tf,
            symbol=symbol,
            model=model_name,
            total_score=score,
            decision=decision,
            veto_applied=None,
            votes=votes
        )

        log.debug(
            f"🧪 MODEL={model_name} | threshold={threshold:.2f} | min_conf={min_conf:.2f} | "
            f"score={score:.3f} → decision={decision.upper()}"
        )