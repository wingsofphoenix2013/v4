import logging
import json
from infra import get_price, load_indicators

log = logging.getLogger("strategy_890_universalst")

class Strategy890Universalst:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        strategy_meta = context.get("strategy", {})
        direction = signal["direction"].lower()
        symbol = signal["symbol"]
        tfs = ["m5", "m15", "h1"]

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

                snapshot_id = data.get("snapshot_id")
                pattern_id = data.get("pattern_id")
                if snapshot_id is None or pattern_id is None:
                    return ("ignore", f"❌ snapshot_id или pattern_id отсутствует в {snapshot_key}")

                # Проверка snapshot
                snap_key = f"confidence:{strategy_id}:{direction}:{tf}:snapshot:{snapshot_id}"
                raw_snap = await redis.get(snap_key)
                if raw_snap is None:
                    return ("ignore", f"❌ нет confidence-ключа: {snap_key}")
                try:
                    snap_data = json.loads(raw_snap)
                except json.JSONDecodeError:
                    return ("ignore", f"❌ некорректный JSON в {snap_key}")
                winrate_snap = snap_data.get("winrate")
                if winrate_snap is None or winrate_snap <= 0.5:
                    return ("ignore", f"🟥 winrate snapshot={winrate_snap} <= 0.5 — tf={tf}")

                # Проверка pattern
                pat_key = f"confidence:{strategy_id}:{direction}:{tf}:pattern:{pattern_id}"
                raw_pat = await redis.get(pat_key)
                if raw_pat is None:
                    return ("ignore", f"❌ нет confidence-ключа: {pat_key}")
                try:
                    pat_data = json.loads(raw_pat)
                except json.JSONDecodeError:
                    return ("ignore", f"❌ некорректный JSON в {pat_key}")
                winrate_pat = pat_data.get("winrate")
                if winrate_pat is None or winrate_pat <= 0.5:
                    return ("ignore", f"🟥 winrate pattern={winrate_pat} <= 0.5 — tf={tf}")

            # BB-фильтр на последнем tf
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

                bb_limit = bb_lower + (bb_center - bb_lower) * (1 / 3)
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

                bb_limit = bb_upper - (bb_upper - bb_center) * (1 / 3)
                if price >= bb_limit:
                    return True
                else:
                    return ("ignore", f"🟥 BB short: price={price}, limit={bb_limit}")

        except Exception:
            log.exception("❌ Ошибка в strategy_890_universalst")
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
            log.debug(f"📤 [890] Сигнал отправлен: {payload}")
        except Exception as e:
            log.warning(f"⚠️ [890] Ошибка при отправке сигнала: {e}")