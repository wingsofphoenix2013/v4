import logging
import json

log = logging.getLogger("strategy_550_longst")

class Strategy550Longst:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        strategy_meta = context.get("strategy", {})
        direction = signal["direction"].lower()
        symbol = signal["symbol"]
        tfs = ["m5", "m15"]

        if direction != "longst":
            return ("ignore", "short сигналы отключены")

        if redis is None:
            log.warning("⚠️ Нет Redis в context")
            return ("ignore", "нет Redis")

        try:
            strategy_id = strategy_meta.get("emamirrow") or signal["strategy_id"]

            for tf in tfs:
                # 1. Получаем pattern_id из Redis
                snapshot_key = f"snapshot:{symbol}:{tf}"
                raw_snapshot = await redis.get(snapshot_key)
                if raw_snapshot is None:
                    return ("ignore", f"нет snapshot в Redis: {snapshot_key}")

                snapshot_data = json.loads(raw_snapshot)
                pattern_id = snapshot_data.get("pattern_id")
                if pattern_id is None:
                    return ("ignore", f"нет pattern_id в ключе {snapshot_key}")

                # 2. Получаем winrate по pattern
                conf_key = f"confidence:{strategy_id}:longst:{tf}:pattern:{pattern_id}"
                raw_conf = await redis.get(conf_key)
                if raw_conf is None:
                    return ("ignore", f"нет confidence-ключа: {conf_key}")

                conf_data = json.loads(raw_conf)
                winrate = conf_data.get("winrate")

                if winrate is None:
                    return ("ignore", f"winrate отсутствует в {conf_key}")
                if winrate <= 0.5:
                    return ("ignore", f"winrate={winrate} <= 0.5 ({tf})")

            return True  # если все TF прошли

        except Exception:
            log.exception("❌ Ошибка в strategy_550_longst")
            return ("ignore", "ошибка в стратегии")

    async def run(self, signal, context):
        redis = context.get("redis")
        if redis is None:
            raise RuntimeError("❌ Redis клиент не передан в context")

        payload = {
            "strategy_id": str(signal["strategy_id"]),
            "symbol": signal["symbol"],
            "direction": signal["direction"],
            "log_uid": signal.get("log_uid"),
            "route": "new_entry",
            "received_at": signal.get("received_at")
        }

        try:
            await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
            log.debug(f"📤 Сигнал отправлен: {payload}")
        except Exception as e:
            log.warning(f"⚠️ Ошибка при отправке сигнала: {e}")