import logging
import json

log = logging.getLogger("strategy_511_short")

class Strategy511Short:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        strategy_meta = context.get("strategy", {})
        direction = signal["direction"].lower()
        symbol = signal["symbol"]
        tf = "m5"

        if direction != "short":
            return ("ignore", "long сигналы отключены")

        if redis is None:
            log.warning("⚠️ Нет Redis в context")
            return ("ignore", "нет Redis")

        try:
            strategy_id = strategy_meta.get("emamirrow") or signal["strategy_id"]

            # 1. Получаем snapshot_id из Redis
            snapshot_key = f"snapshot:{symbol}:{tf}"
            raw_snapshot = await redis.get(snapshot_key)
            if raw_snapshot is None:
                return ("ignore", f"нет snapshot в Redis: {snapshot_key}")

            snapshot_data = json.loads(raw_snapshot)
            snapshot_id = snapshot_data.get("snapshot_id")
            if snapshot_id is None:
                return ("ignore", f"нет snapshot_id в ключе {snapshot_key}")

            # 2. Проверка open_allowed
            conf_key = f"confidence:{strategy_id}:short:{tf}:snapshot:{snapshot_id}"
            raw_conf = await redis.get(conf_key)
            if raw_conf is None:
                return ("ignore", f"нет confidence-ключа: {conf_key}")

            conf_data = json.loads(raw_conf)
            if conf_data.get("open_allowed") is True:
                return True
            else:
                return ("ignore", f"open_allowed=False или отсутствует в {conf_key}")

        except Exception:
            log.exception("❌ Ошибка в strategy_511_short")
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