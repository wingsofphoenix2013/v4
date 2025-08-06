# voting_engine.py

import asyncio
import logging
import json

import infra

log = logging.getLogger("VOTING_ENGINE")

REQUEST_STREAM = "strategy_voting_request"


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

# 🔸 Обработка запроса голосования (Шаг 4 — голосование)
async def handle_voting_request(msg: dict):
    try:
        strategy_id = int(msg["strategy_id"])
        direction = msg["direction"]
        tf_trigger = msg["tf"]
        symbol = msg["symbol"]
        log_uid = msg["log_uid"]

        log.info(f"📥 log_uid={log_uid} | strategy={strategy_id} | dir={direction} | tf={tf_trigger} | symbol={symbol}")

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
                snapshot_id = data["snapshot_id"]
                pattern_id = data["pattern_id"]
                snapshots[tf] = {
                    "snapshot_id": snapshot_id,
                    "pattern_id": pattern_id
                }
                log.info(f"🔍 {tf}: snapshot_id={snapshot_id}, pattern_id={pattern_id}")
            except Exception:
                log.warning(f"❌ Ошибка парсинга значения {key}")
                continue

        if not snapshots:
            log.warning(f"⚠️ log_uid={log_uid} → ни одного объекта не найдено")
            return

        # 🔹 Читаем confidence и winrate
        votes_raw = {}
        for tf, obj in snapshots.items():
            for obj_type in ["snapshot", "pattern"]:
                object_id = obj.get(f"{obj_type}_id") or obj.get(f"{obj_type}_id")
                if object_id is None:
                    continue

                conf_key = f"confidence:{strategy_id}:{direction}:{tf}:{obj_type}:{object_id}"
                raw = await redis.get(conf_key)

                if not raw:
                    log.warning(f"⛔ Нет confidence ключа: {conf_key}")
                    continue

                try:
                    conf = json.loads(raw)
                    winrate = conf.get("winrate")
                    confidence = conf.get("confidence_raw")

                    source_key = f"{obj_type}_{tf}"
                    votes_raw[source_key] = {
                        "object_id": object_id,
                        "winrate": winrate,
                        "confidence_raw": confidence
                    }

                    log.info(f"📦 {source_key}: winrate={winrate:.3f}, conf={confidence:.3f}")

                except Exception:
                    log.warning(f"❌ Ошибка парсинга JSON: {conf_key}")
                    continue

        if not votes_raw:
            log.warning(f"⚠️ log_uid={log_uid} → нет данных для голосования")
            return

        # 🔹 Голосование
        votes = []
        total_score = 0.0
        veto_count = 0
        anti_veto_count = 0

        for source, v in votes_raw.items():
            winrate = v["winrate"]
            confidence = v["confidence_raw"]

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

            tf_key = source.split("_")[1]
            weight = TF_WEIGHTS.get(tf_key, 1.0)
            contribution = vote * confidence * weight

            veto = winrate < 0.35 and confidence >= 0.9
            anti_veto = winrate > 0.65 and confidence >= 0.7

            if veto:
                veto_count += 1
            if anti_veto:
                anti_veto_count += 1

            votes.append({
                "source": source,
                "object_id": v["object_id"],
                "winrate": winrate,
                "confidence": confidence,
                "weight": weight,
                "vote": vote,
                "contribution": contribution,
                "veto": veto,
                "anti_veto": anti_veto
            })

            total_score += contribution
            log.debug(f"🗳️ {source} | vote={vote:+.2f} | conf={confidence:.3f} | contrib={contribution:.3f} | veto={veto} | anti={anti_veto}")

        net_veto = veto_count - anti_veto_count

        if net_veto > 0:
            decision = "reject"
            log.info(f"❌ Принудительное отклонение: {veto_count} вето против {anti_veto_count} анти-вето")
        else:
            if total_score >= 2:
                decision = "open"
            elif total_score <= -2:
                decision = "reject"
            else:
                decision = "neutral"

            if veto_count > 0 or anti_veto_count > 0:
                log.info(f"⚖️ Баланс вето: {veto_count} vs анти-вето: {anti_veto_count} → голосуем по score")

        log.info(f"✅ Голосование log_uid={log_uid} → {decision.upper()} (score={total_score:.3f})")

        # TODO: сохранение в БД и публикация в Redis

    except Exception:
        log.exception("❌ Ошибка обработки запроса голосования")