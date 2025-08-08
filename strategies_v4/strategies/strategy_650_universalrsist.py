import logging
import json

log = logging.getLogger("strategy_650_universalrsist")

class Strategy650Universalrsist:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        strategy_meta = context.get("strategy", {})
        direction = signal["direction"].lower()
        symbol = signal["symbol"]

        if direction not in ("long", "short"):
            return ("ignore", f"неизвестное направление: {direction}")

        if redis is None:
            log.warning("Нет Redis в context")
            return ("ignore", "нет Redis")

        try:
            if direction == "long":
                strategy_id = strategy_meta.get("emamirrow_long") or signal["strategy_id"]
            else:
                strategy_id = strategy_meta.get("emamirrow_short") or signal["strategy_id"]

            for tf in ("m5", "m15"):
                snapshot_key = f"snapshot:{symbol}:{tf}"
                raw_snapshot = await redis.get(snapshot_key)
                if raw_snapshot is None:
                    return ("ignore", f"нет snapshot в Redis: {snapshot_key}")

                snapshot_data = json.loads(raw_snapshot)
                pattern_id = snapshot_data.get("pattern_id")
                if pattern_id is None:
                    return ("ignore", f"нет pattern_id в ключе {snapshot_key}")

                rsi_key = f"ind:{symbol}:{tf}:rsi14"
                raw_rsi = await redis.get(rsi_key)
                if raw_rsi is None:
                    return ("ignore", f"нет RSI в Redis: {rsi_key}")

                try:
                    rsi_value = float(raw_rsi)
                except ValueError:
                    return ("ignore", f"некорректное значение RSI: {raw_rsi}")

                bucket = int(rsi_value // 5) * 5
                key = f"emarsicheck_pattern:{tf}:{strategy_id}:{pattern_id}:{bucket}"
                verdict = await redis.get(key)
                if verdict is None:
                    return ("ignore", f"нет ключа {key}")
                if verdict != "allow":
                    return ("ignore", f"{tf} Pattern RSI-check verdict={verdict}")

            return True

        except Exception:
            log.exception("Ошибка в strategy_650_universalrsist")
            return ("ignore", "ошибка в стратегии")

    async def run(self, signal, context):
        redis = context.get("redis")
        if redis is None:
            raise RuntimeError("Redis клиент не передан в context")

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
            log.debug(f"Сигнал отправлен: {payload}")
        except Exception as e:
            log.warning(f"Ошибка при отправке сигнала: {e}")