import logging
import json
from infra import get_price, load_indicators

log = logging.getLogger("strategy_881_universal")

class Strategy871Universal:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        strategy_meta = context.get("strategy", {})
        direction = signal["direction"].lower()
        symbol = signal["symbol"]
        tf = strategy_meta.get("timeframe", "m5").lower()

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

            snapshot_key = f"snapshot:{symbol}:{tf}"
            raw_snapshot = await redis.get(snapshot_key)
            if raw_snapshot is None:
                return ("ignore", f"❌ нет ключа {snapshot_key} в Redis")

            try:
                data = json.loads(raw_snapshot)
            except json.JSONDecodeError:
                return ("ignore", f"❌ некорректный JSON в {snapshot_key}")

            snapshot_id = data.get("snapshot_id")
            pattern_id = data.get("pattern_id")
            if snapshot_id is None or pattern_id is None:
                return ("ignore", f"❌ snapshot_id или pattern_id отсутствует в {snapshot_key}")

            # Проверка open_allowed по snapshot
            snap_key = f"confidence:{strategy_id}:{direction}:{tf}:snapshot:{snapshot_id}"
            raw_snap = await redis.get(snap_key)
            if raw_snap is None:
                return ("ignore", f"❌ нет confidence-ключа: {snap_key}")
            try:
                snap_data = json.loads(raw_snap)
            except json.JSONDecodeError:
                return ("ignore", f"❌ некорректный JSON в {snap_key}")
            if snap_data.get("open_allowed") is not True:
                return ("ignore", f"🟥 open_allowed=False по snapshot — tf={tf}")

            # Проверка open_allowed по pattern
            pat_key = f"confidence:{strategy_id}:{direction}:{tf}:pattern:{pattern_id}"
            raw_pat = await redis.get(pat_key)
            if raw_pat is None:
                return ("ignore", f"❌ нет confidence-ключа: {pat_key}")
            try:
                pat_data = json.loads(raw_pat)
            except json.JSONDecodeError:
                return ("ignore", f"❌ некорректный JSON в {pat_key}")
            if pat_data.get("open_allowed") is not True:
                return ("ignore", f"🟥 open_allowed=False по pattern — tf={tf}")

            # ✅ BB-фильтр
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "❌ нет текущей цены")

            if direction == "long":
                indicators = await load_indicators(symbol, ["bb20_2_0_center", "bb20_2_0_lower"], tf)
                bb_center = indicators.get("bb20_2_0_center")
                bb_lower = indicators.get("bb20_2_0_lower")

                if None in (bb_center, bb_lower):
                    return ("ignore", "❌ недостаточно данных BB (long)")

                bb_limit = bb_lower + (bb_center - bb_lower) * (1 / 3)
                if price <= bb_limit:
                    return True
                else:
                    return ("ignore", f"🟥 BB long: price={price}, limit={bb_limit}")

            elif direction == "short":
                indicators = await load_indicators(symbol, ["bb20_2_0_center", "bb20_2_0_upper"], tf)
                bb_center = indicators.get("bb20_2_0_center")
                bb_upper = indicators.get("bb20_2_0_upper")

                if None in (bb_center, bb_upper):
                    return ("ignore", "❌ недостаточно данных BB (short)")

                bb_limit = bb_upper - (bb_upper - bb_center) * (1 / 3)
                if price >= bb_limit:
                    return True
                else:
                    return ("ignore", f"🟥 BB short: price={price}, limit={bb_limit}")

        except Exception:
            log.exception("❌ Ошибка в strategy_881_universal")
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
            log.debug(f"📤 [881] Сигнал отправлен: {payload}")
        except Exception as e:
            log.warning(f"⚠️ [881] Ошибка при отправке сигнала: {e}")