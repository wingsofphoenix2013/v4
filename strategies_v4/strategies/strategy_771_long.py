import logging
import json

from infra import infra, get_price, load_indicators

log = logging.getLogger("strategy_771_long")

class Strategy771Long:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        pg = infra.pg_pool
        strategy_meta = context.get("strategy", {})
        direction = signal["direction"].lower()
        symbol = signal["symbol"]
        tf = context["strategy"]["timeframe"].lower()

        if direction != "long":
            return ("ignore", "short сигналы отключены")

        if redis is None or pg is None:
            log.warning("⚠️ Нет redis или pg_pool")
            return ("ignore", "нет инфраструктуры")

        try:
            strategy_id = strategy_meta.get("emamirrow") or signal["strategy_id"]

            key = f"snapshot:{symbol}:m5"
            raw = await redis.get(key)
            if raw is None:
                return ("ignore", f"нет snapshot в Redis: {key}")

            data = json.loads(raw)
            snapshot_id = data.get("snapshot_id")
            pattern_id = data.get("pattern_id")

            if snapshot_id is None or pattern_id is None:
                return ("ignore", "недостаточно данных: snapshot_id или pattern_id отсутствует")

            snap_row = await pg.fetchrow("""
                SELECT winrate
                FROM positions_emasnapshot_m5_stat
                WHERE strategy_id = $1 AND direction = 'long' AND emasnapshot_dict_id = $2
            """, strategy_id, snapshot_id)

            if not snap_row or float(snap_row["winrate"]) <= 0.5:
                return ("ignore", f"snapshot winrate слишком низкий или не найден")

            pat_row = await pg.fetchrow("""
                SELECT winrate
                FROM positions_emapattern_m5_stat
                WHERE strategy_id = $1 AND direction = 'long' AND pattern_id = $2
            """, strategy_id, pattern_id)

            if not pat_row or float(pat_row["winrate"]) <= 0.5:
                return ("ignore", f"pattern winrate слишком низкий или не найден")

            # 🔹 BB-фильтр
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены")

            indicators = await load_indicators(symbol, [
                "bb20_2_0_center",
                "bb20_2_0_lower"
            ], tf)

            bb_center = indicators.get("bb20_2_0_center")
            bb_lower = indicators.get("bb20_2_0_lower")

            log.debug(f"[771_LONG] price={price}, bb_center={bb_center}, bb_lower={bb_lower}")

            if None in (bb_center, bb_lower):
                return ("ignore", "недостаточно данных BB")

            bb_limit = bb_lower + (bb_center - bb_lower) * (2 / 3)
            if price <= bb_limit:
                return True
            else:
                return ("ignore", f"фильтр BB не пройден: price={price}, limit={bb_limit}")

        except Exception:
            log.exception("❌ Ошибка в strategy_771_long")
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