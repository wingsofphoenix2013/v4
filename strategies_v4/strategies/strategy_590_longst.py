import logging
import json

log = logging.getLogger("strategy_590_longst")

class Strategy590Longst:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        strategy_meta = context.get("strategy", {})
        direction = signal["direction"].lower()
        symbol = signal["symbol"]
        tfs = ["m5", "m15", "h1"]

        if direction != "longst":
            return ("ignore", "short сигналы отключены")

        if redis is None:
            log.warning("⚠️ Нет Redis в context")
            return ("ignore", "нет Redis")

        try:
            strategy_id = strategy_meta.get("emamirrow") or signal["strategy_id"]

            for tf in tfs:
                # 1. Получаем snapshot и pattern из Redis
                key = f"snapshot:{symbol}:{tf}"
                raw = await redis.get(key)
                if raw is None:
                    return ("ignore", f"нет snapshot в Redis: {key}")

                data = json.loads(raw)
                snapshot_id = data.get("snapshot_id")
                pattern_id = data.get("pattern_id")

                if snapshot_id is None or pattern_id is None:
                    return ("ignore", f"нет snapshot_id или pattern_id в {key}")

                # 2. Проверяем snapshot
                snap_key = f"confidence:{strategy_id}:longst:{tf}:snapshot:{snapshot_id}"
                snap_raw = await redis.get(snap_key)
                if snap_raw is None:
                    return ("ignore", f"нет confidence-ключа: {snap_key}")

                snap_data = json.loads(snap_raw)
                winrate_snap = snap_data.get("winrate")
                if winrate_snap is None or winrate_snap <= 0.5:
                    return ("ignore", f"snapshot winrate={winrate_snap} <= 0.5 ({tf})")

                # 3. Проверяем pattern
                pat_key = f"confidence:{strategy_id}:longst:{tf}:pattern:{pattern_id}"
                pat_raw = await redis.get(pat_key)
                if pat_raw is None:
                    return ("ignore", f"нет confidence-ключа: {pat_key}")

                pat_data = json.loads(pat_raw)
                winrate_pat = pat_data.get("winrate")
                if winrate_pat is None or winrate_pat <= 0.5:
                    return ("ignore", f"pattern winrate={winrate_pat} <= 0.5 ({tf})")

            return True  # Все таймфреймы и объекты прошли

        except Exception:
            log.exception("❌ Ошибка в strategy_590_longst")
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