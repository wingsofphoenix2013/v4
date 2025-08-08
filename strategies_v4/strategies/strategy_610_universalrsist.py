import logging
import json

log = logging.getLogger("strategy_610_universalrsist")

class Strategy610Universalrsist:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        strategy_meta = context.get("strategy", {})
        direction = signal["direction"].lower()
        symbol = signal["symbol"]
        tf = "m5"

        if direction not in ("long", "short"):
            return ("ignore", f"неизвестное направление: {direction}")

        if redis is None:
            log.warning("Нет Redis в context")
            return ("ignore", "нет Redis")

        try:
            # 🔹 Определяем strategy_id в зависимости от направления
            if direction == "long":
                strategy_id = strategy_meta.get("emamirrow_long") or signal["strategy_id"]
            else:
                strategy_id = strategy_meta.get("emamirrow_short") or signal["strategy_id"]

            # 1. Получаем snapshot_id из Redis
            snapshot_key = f"snapshot:{symbol}:{tf}"
            raw_snapshot = await redis.get(snapshot_key)
            if raw_snapshot is None:
                return ("ignore", f"нет snapshot в Redis: {snapshot_key}")

            snapshot_data = json.loads(raw_snapshot)
            snapshot_id = snapshot_data.get("snapshot_id")
            if snapshot_id is None:
                return ("ignore", f"нет snapshot_id в ключе {snapshot_key}")

            # 2. Получаем текущий RSI14 из Redis
            rsi_key = f"ind:{symbol}:{tf}:rsi14"
            raw_rsi = await redis.get(rsi_key)
            if raw_rsi is None:
                return ("ignore", f"нет RSI в Redis: {rsi_key}")

            try:
                rsi_value = float(raw_rsi)
            except ValueError:
                return ("ignore", f"некорректное значение RSI: {raw_rsi}")

            # 3. Определяем bucket
            bucket = int(rsi_value // 5) * 5

            # 4. Получаем verdict из Redis по emarsicheck
            emarsicheck_key = f"emarsicheck:{tf}:{strategy_id}:{snapshot_id}:{bucket}"
            verdict = await redis.get(emarsicheck_key)
            if verdict is None:
                return ("ignore", f"нет ключа emarsicheck: {emarsicheck_key}")

            # 5. Решение
            if verdict == "allow":
                return True
            else:
                return ("ignore", f"RSI-check verdict={verdict}")

        except Exception:
            log.exception("Ошибка в strategy_610_universalrsist")
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