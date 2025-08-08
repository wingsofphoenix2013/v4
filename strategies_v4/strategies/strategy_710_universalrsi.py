import logging
import json
from infra import get_price, load_indicators

log = logging.getLogger("strategy_710_universalrsi")

class Strategy710Universalrsi:
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
            # Определяем strategy_id в зависимости от направления
            if direction == "long":
                strategy_id = strategy_meta.get("emamirrow_long") or signal["strategy_id"]
            else:
                strategy_id = strategy_meta.get("emamirrow_short") or signal["strategy_id"]

            # 1. Snapshot
            snapshot_key = f"snapshot:{symbol}:{tf}"
            raw_snapshot = await redis.get(snapshot_key)
            if raw_snapshot is None:
                return ("ignore", f"нет snapshot в Redis: {snapshot_key}")
            snapshot_data = json.loads(raw_snapshot)
            snapshot_id = snapshot_data.get("snapshot_id")
            if snapshot_id is None:
                return ("ignore", f"нет snapshot_id в ключе {snapshot_key}")

            # 2. RSI
            rsi_key = f"ind:{symbol}:{tf}:rsi14"
            raw_rsi = await redis.get(rsi_key)
            if raw_rsi is None:
                return ("ignore", f"нет RSI в Redis: {rsi_key}")
            try:
                rsi_value = float(raw_rsi)
            except ValueError:
                return ("ignore", f"некорректное значение RSI: {raw_rsi}")

            # 3. Bucket
            bucket = int(rsi_value // 5) * 5

            # 4. Verdict
            emarsicheck_key = f"emarsicheck:{tf}:{strategy_id}:{snapshot_id}:{bucket}"
            verdict = await redis.get(emarsicheck_key)
            if verdict is None:
                return ("ignore", f"нет ключа emarsicheck: {emarsicheck_key}")
            if verdict != "allow":
                return ("ignore", f"RSI-check verdict={verdict}")

            # 5. Проверка BB
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены")

            if direction == "long":
                indicators = await load_indicators(symbol, ["bb20_2_0_center", "bb20_2_0_lower"], tf)
                bb_center = indicators.get("bb20_2_0_center")
                bb_lower = indicators.get("bb20_2_0_lower")
                if None in (bb_center, bb_lower):
                    return ("ignore", "недостаточно данных BB (long)")

                bb_limit = bb_lower + (bb_center - bb_lower) * (2 / 3)
                if price <= bb_limit:
                    return True
                else:
                    return ("ignore", f"BB long: price={price}, limit={bb_limit}")

            else:
                indicators = await load_indicators(symbol, ["bb20_2_0_center", "bb20_2_0_upper"], tf)
                bb_center = indicators.get("bb20_2_0_center")
                bb_upper = indicators.get("bb20_2_0_upper")
                if None in (bb_center, bb_upper):
                    return ("ignore", "недостаточно данных BB (short)")

                bb_limit = bb_upper - (bb_upper - bb_center) * (2 / 3)
                if price >= bb_limit:
                    return True
                else:
                    return ("ignore", f"BB short: price={price}, limit={bb_limit}")

        except Exception:
            log.exception("Ошибка в strategy_710_universalrsi")
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