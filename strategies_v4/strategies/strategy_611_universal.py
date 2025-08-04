import logging
import json
from infra import load_indicators, get_price

log = logging.getLogger("strategy_611_universal")

class Strategy611Universal:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()
        redis = context.get("redis")
        whitelist = context.get("entry_whitelist")

        if redis is None or whitelist is None:
            log.warning("⚠️ Нет redis или entry_whitelist в context")
            return ("ignore", "нет инфраструктуры")

        try:
            # 🔹 Получение цены
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены")

            # 🔹 Индикаторы
            indicators = await load_indicators(symbol, [
                "bb20_2_0_center", "bb20_2_0_lower", "bb20_2_0_upper"
            ], tf)

            bb_center = indicators.get("bb20_2_0_center")
            bb_lower = indicators.get("bb20_2_0_lower")
            bb_upper = indicators.get("bb20_2_0_upper")

            log.debug(f"[611_BB+SNAPSHOT] symbol={symbol}, tf={tf}, direction={direction}, price={price}, "
                      f"bb_center={bb_center}, bb_lower={bb_lower}, bb_upper={bb_upper}")

            if None in (bb_center, bb_lower, bb_upper):
                return ("ignore", "недостаточно данных BB")

            # 🔹 Проверка BB-фильтра
            if direction == "long":
                bb_limit = bb_lower + (bb_center - bb_lower) * (2 / 3)
                if price > bb_limit:
                    return ("ignore", f"фильтр BB long не пройден: price={price}, limit={bb_limit}")
            elif direction == "short":
                bb_limit = bb_upper - (bb_upper - bb_center) * (2 / 3)
                if price < bb_limit:
                    return ("ignore", f"фильтр BB short не пройден: price={price}, limit={bb_limit}")
            else:
                return ("ignore", f"неизвестное направление: {direction}")

            # 🔹 Проверка snapshot
            key = f"snapshot:{symbol}:{tf}"
            raw = await redis.get(key)
            if raw is None:
                return ("ignore", f"нет snapshot в redis: {key}")

            data = json.loads(raw)
            snapshot_id = data.get("snapshot_id")
            if snapshot_id is None:
                return ("ignore", f"нет snapshot_id в ключе {key}")

            allowed_snapshots = whitelist.get(direction, {}).get("snapshots", [])
            if snapshot_id not in allowed_snapshots:
                return ("ignore", f"snapshot {snapshot_id} не в whitelist [{direction}]")

            return True

        except Exception:
            log.exception("❌ Ошибка в strategy_611_universal")
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