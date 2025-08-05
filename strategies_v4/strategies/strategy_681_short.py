import logging
import json

from infra import infra, get_price, load_indicators

log = logging.getLogger("strategy_681_short")

class Strategy681Short:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        pg = infra.pg_pool
        strategy_meta = context.get("strategy", {})
        direction = signal["direction"].lower()
        symbol = signal["symbol"]
        tf = context["strategy"]["timeframe"].lower()

        if direction != "short":
            return ("ignore", "long сигналы отключены")

        if redis is None or pg is None:
            log.warning("⚠️ Нет redis или pg_pool")
            return ("ignore", "нет инфраструктуры")

        try:
            strategy_id = strategy_meta.get("emamirrow") or signal["strategy_id"]

            ids = {}
            for timeframe in ("m5", "m15"):
                key = f"snapshot:{symbol}:{timeframe}"
                raw = await redis.get(key)
                if raw is None:
                    return ("ignore", f"нет snapshot в Redis: {key}")

                data = json.loads(raw)
                snapshot_id = data.get("snapshot_id")
                pattern_id = data.get("pattern_id")
                if snapshot_id is None or pattern_id is None:
                    return ("ignore", f"нет snapshot_id или pattern_id в {key}")

                ids[timeframe] = {"snapshot_id": snapshot_id, "pattern_id": pattern_id}

            for tf_check in ("m5", "m15"):
                snap_row = await pg.fetchrow(f"""
                    SELECT winrate
                    FROM positions_emasnapshot_{tf_check}_stat
                    WHERE strategy_id = $1 AND direction = 'short' AND emasnapshot_dict_id = $2
                """, strategy_id, ids[tf_check]["snapshot_id"])

                if not snap_row or float(snap_row["winrate"]) <= 0.5:
                    return ("ignore", f"snapshot winrate низкий ({tf_check})")

                pat_row = await pg.fetchrow(f"""
                    SELECT winrate
                    FROM positions_emapattern_{tf_check}_stat
                    WHERE strategy_id = $1 AND direction = 'short' AND pattern_id = $2
                """, strategy_id, ids[tf_check]["pattern_id"])

                if not pat_row or float(pat_row["winrate"]) <= 0.5:
                    return ("ignore", f"pattern winrate низкий ({tf_check})")

            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены")

            indicators = await load_indicators(symbol, [
                "bb20_2_0_center",
                "bb20_2_0_upper"
            ], tf)

            bb_center = indicators.get("bb20_2_0_center")
            bb_upper = indicators.get("bb20_2_0_upper")

            log.debug(f"[681_SHORT] price={price}, bb_center={bb_center}, bb_upper={bb_upper}")

            if None in (bb_center, bb_upper):
                return ("ignore", "недостаточно данных BB")

            bb_limit = bb_upper - (bb_upper - bb_center) * (2 / 3)
            if price >= bb_limit:
                return True
            else:
                return ("ignore", f"фильтр BB не пройден: price={price}, limit={bb_limit}")

        except Exception:
            log.exception("❌ Ошибка в strategy_681_short")
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