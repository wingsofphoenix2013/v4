import logging
import json

log = logging.getLogger("strategy_520_shortrsi")

class Strategy520Shortrsi:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        strategy_meta = context.get("strategy", {})
        direction = signal["direction"].lower()
        symbol = signal["symbol"]

        if direction != "short":
            return ("ignore", "long сигналы отключены")

        if redis is None:
            log.warning("⚠️ Нет Redis в context")
            return ("ignore", "нет Redis")

        try:
            strategy_id = strategy_meta.get("emamirrow") or signal["strategy_id"]

            for tf in ("m5", "m15"):
                snapshot_key = f"snapshot:{symbol}:{tf}"
                raw_snapshot = await redis.get(snapshot_key)
                if raw_snapshot is None:
                    return ("ignore", f"нет snapshot в Redis: {snapshot_key}")
                snapshot_id = json.loads(raw_snapshot).get("snapshot_id")
                if snapshot_id is None:
                    return ("ignore", f"нет snapshot_id в ключе {snapshot_key}")

                rsi_key = f"ind:{symbol}:{tf}:rsi14"
                raw_rsi = await redis.get(rsi_key)
                if raw_rsi is None:
                    return ("ignore", f"нет RSI в Redis: {rsi_key}")
                try:
                    rsi_value = float(raw_rsi)
                except ValueError:
                    return ("ignore", f"некорректное значение RSI: {raw_rsi}")

                bucket = int(rsi_value // 5) * 5

                emarsicheck_key = f"emarsicheck:{tf}:{strategy_id}:{snapshot_id}:{bucket}"
                verdict = await redis.get(emarsicheck_key)
                if verdict is None:
                    return ("ignore", f"нет ключа emarsicheck: {emarsicheck_key}")
                if verdict != "allow":
                    return ("ignore", f"{tf} RSI-check verdict={verdict}")

            return True

        except Exception:
            log.exception("❌ Ошибка в strategy_520_shortrsi")
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