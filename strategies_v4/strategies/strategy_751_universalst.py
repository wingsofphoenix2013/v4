import logging
import json
from infra import get_price, load_indicators

log = logging.getLogger("strategy_751_universalst")

class Strategy751Universalst:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        strategy_meta = context.get("strategy", {})
        direction = signal["direction"].lower()
        symbol = signal["symbol"]
        tfs = ["m5", "m15"]

        if direction not in ("long", "short"):
            return ("ignore", f"❌ неизвестное направление: {direction}")

        if redis is None:
            log.warning("⚠️ Нет Redis в context")
            return ("ignore", "❌ Redis клиент отсутствует")

        try:
            strategy_id = (
                strategy_meta.get("emamirrow_long") if direction == "long"
                else strategy_meta.get("emamirrow_short")
            ) or signal["strategy_id"]

            for tf in tfs:
                snapshot_key = f"snapshot:{symbol}:{tf}"
                raw_snapshot = await redis.get(snapshot_key)
                if raw_snapshot is None:
                    return ("ignore", f"❌ нет ключа {snapshot_key} в Redis")

                try:
                    data = json.loads(raw_snapshot)
                except json.JSONDecodeError:
                    return ("ignore", f"❌ некорректный JSON в {snapshot_key}")

                pattern_id = data.get("pattern_id")
                if pattern_id is None:
                    return ("ignore", f"❌ нет pattern_id в ключе {snapshot_key}")

                conf_key = f"confidence:{strategy_id}:{direction}:{tf}:pattern:{pattern_id}"
                raw_conf = await redis.get(conf_key)
                if raw_conf is None:
                    return ("ignore", f"❌ нет confidence-ключа: {conf_key}")

                try:
                    conf_data = json.loads(raw_conf)
                except json.JSONDecodeError:
                    return ("ignore", f"❌ некорректный JSON в {conf_key}")

                if conf_data.get("open_allowed") is not True:
                    return ("ignore", f"🟥 open_allowed=False — strategy_id={strategy_id}, pattern_id={pattern_id}, tf={tf}")

            # ✅ BB-фильтр по последнему tf
            tf_bb = tfs[-1]
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "❌ нет текущей цены")

            if direction == "long":
                indicators = await load_indicators(symbol, ["bb20_2_0_center", "bb20_2_0_lower"], tf_bb)
                bb_center = indicators.get("bb20_2_0_center")
                bb_lower = indicators.get("bb20_2_0_lower")

                if None in (bb_center, bb_lower):
                    return ("ignore", "❌ недостаточно данных BB (long)")

                bb_limit = bb_lower + (bb_center - bb_lower) * (2 / 3)
                if price <= bb_limit:
                    return True
                else:
                    return ("ignore", f"🟥 BB long: price={price}, limit={bb_limit}")

            elif direction == "short":
                indicators = await load_indicators(symbol, ["bb20_2_0_center", "bb20_2_0_upper"], tf_bb)
                bb_center = indicators.get("bb20_2_0_center")
                bb_upper = indicators.get("bb20_2_0_upper")

                if None in (bb_center, bb_upper):
                    return ("ignore", "❌ недостаточно данных BB (short)")

                bb_limit = bb_upper - (bb_upper - bb_center) * (2 / 3)
                if price >= bb_limit:
                    return True
                else:
                    return ("ignore", f"🟥 BB short: price={price}, limit={bb_limit}")

        except Exception:
            log.exception("❌ Ошибка в strategy_751_universalst")
            return ("ignore", "❌ необработанная ошибка")

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
            log.debug(f"📤 [751] Сигнал отправлен: {payload}")
        except Exception as e:
            log.warning(f"⚠️ [751] Ошибка при отправке сигнала: {e}")