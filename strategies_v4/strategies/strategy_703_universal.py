import logging
import json

log = logging.getLogger("strategy_703_universal")

class Strategy703Universal:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        whitelist = context.get("entry_whitelist")
        direction = signal["direction"].lower()
        symbol = signal["symbol"]
        tf = context["strategy"]["timeframe"].lower()

        if redis is None or whitelist is None:
            log.warning("⚠️ Нет redis или entry_whitelist в context")
            return ("ignore", "нет инфраструктуры")

        key = f"snapshot:{symbol}:{tf}"

        try:
            raw = await redis.get(key)
            if raw is None:
                return ("ignore", f"нет snapshot в redis: {key}")

            data = json.loads(raw)
            snapshot_id = data.get("snapshot_id")
            pattern_id = data.get("pattern_id")

            # 🔹 Проверка snapshot
            allowed_snapshots = whitelist.get(direction, {}).get("snapshots", [])
            if snapshot_id is None:
                return ("ignore", f"нет snapshot_id в ключе {key}")
            if snapshot_id not in allowed_snapshots:
                return ("ignore", f"snapshot {snapshot_id} не в whitelist [{direction}]")

            # 🔹 Проверка pattern
            allowed_patterns = whitelist.get(direction, {}).get("patterns", [])
            if pattern_id is None:
                return ("ignore", f"нет pattern_id в ключе {key}")
            if pattern_id not in allowed_patterns:
                return ("ignore", f"pattern {pattern_id} не в whitelist [{direction}]")

            return True

        except Exception:
            log.exception("❌ Ошибка при проверке snapshot + pattern")
            return ("ignore", "ошибка при проверке snapshot/pattern")

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