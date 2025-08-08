importst logging
importst json

log = logging.getLogger("strategy_570_shortst")

class Strategy570Shortst:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        strategy_meta = context.get("strategy", {})
        direction = signal["direction"].lower()
        symbol = signal["symbol"]
        tf = "m5"

        if direction != "shortst":
            return ("ignore", "long сигналы отключены")

        if redis is None:
            log.warning("⚠️ Нет Redis в context")
            return ("ignore", "нет Redis")

        try:
            strategy_id = strategy_meta.get("emamirrow") or signal["strategy_id"]

            # 1. Получаем snapshot и pattern
            key = f"snapshot:{symbol}:{tf}"
            raw_snapshot = await redis.get(key)
            if raw_snapshot is None:
                return ("ignore", f"нет snapshot в Redis: {key}")

            data = json.loads(raw_snapshot)
            snapshot_id = data.get("snapshot_id")
            pattern_id = data.get("pattern_id")

            if snapshot_id is None or pattern_id is None:
                return ("ignore", f"нет snapshot_id или pattern_id в {key}")

            # 2. Проверка snapshot
            snap_key = f"confidence:{strategy_id}:shortst:{tf}:snapshot:{snapshot_id}"
            snap_raw = await redis.get(snap_key)
            if snap_raw is None:
                return ("ignore", f"нет confidence-ключа: {snap_key}")

            snap_data = json.loads(snap_raw)
            snap_winrate = snap_data.get("winrate")
            if snap_winrate is None or snap_winrate <= 0.5:
                return ("ignore", f"snapshot winrate={snap_winrate} <= 0.5")

            # 3. Проверка pattern
            pat_key = f"confidence:{strategy_id}:shortst:{tf}:pattern:{pattern_id}"
            pat_raw = await redis.get(pat_key)
            if pat_raw is None:
                return ("ignore", f"нет confidence-ключа: {pat_key}")

            pat_data = json.loads(pat_raw)
            pat_winrate = pat_data.get("winrate")
            if pat_winrate is None or pat_winrate <= 0.5:
                return ("ignore", f"pattern winrate={pat_winrate} <= 0.5")

            return True

        except Exception:
            log.exception("❌ Ошибка в strategy_570_shortst")
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