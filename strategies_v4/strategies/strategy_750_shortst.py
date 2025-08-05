import logging
import json

from infra import infra

log = logging.getLogger("strategy_750_shortst")

class Strategy750Shortst:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        pg = infra.pg_pool
        strategy_meta = context.get("strategy", {})
        direction = signal["direction"].lower()
        symbol = signal["symbol"]

        if direction != "short":
            return ("ignore", "long сигналы отключены")

        if redis is None or pg is None:
            log.warning("⚠️ Нет redis или pg_pool")
            return ("ignore", "нет инфраструктуры")

        try:
            strategy_id = strategy_meta.get("emamirrow") or signal["strategy_id"]

            pattern_ids = {}
            for tf in ("m5", "m15"):
                key = f"snapshot:{symbol}:{tf}"
                raw = await redis.get(key)
                if raw is None:
                    return ("ignore", f"нет snapshot в Redis: {key}")

                data = json.loads(raw)
                pattern_id = data.get("pattern_id")
                if pattern_id is None:
                    return ("ignore", f"нет pattern_id в ключе {key}")

                pattern_ids[tf] = pattern_id

            for tf in ("m5", "m15"):
                table = f"positions_emapattern_{tf}_stat"
                pattern_id = pattern_ids[tf]

                row = await pg.fetchrow(f"""
                    SELECT winrate
                    FROM {table}
                    WHERE strategy_id = $1 AND direction = 'short' AND pattern_id = $2
                """, strategy_id, pattern_id)

                if not row:
                    return ("ignore", f"нет данных по pattern {pattern_id} ({tf})")

                winrate = float(row["winrate"])
                if winrate <= 0.5:
                    return ("ignore", f"winrate={winrate} <= 0.5 ({tf})")

            return True

        except Exception:
            log.exception("❌ Ошибка в strategy_750_shortst")
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