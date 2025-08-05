import logging
import json

from infra import infra

log = logging.getLogger("strategy_520_longst")

class Strategy520Longst:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        pg = infra.pg_pool
        strategy_meta = context.get("strategy", {})
        direction = signal["direction"].lower()
        symbol = signal["symbol"]

        if direction != "long":
            return ("ignore", "short сигналы отключены")

        if redis is None or pg is None:
            log.warning("⚠️ Нет redis или pg_pool")
            return ("ignore", "нет инфраструктуры")

        try:
            strategy_id = strategy_meta.get("emamirrow") or signal["strategy_id"]

            # 🔹 Получаем snapshot_id из Redis по двум таймфреймам
            snapshots = {}
            for tf in ("m5", "m15"):
                key = f"snapshot:{symbol}:{tf}"
                raw = await redis.get(key)
                if raw is None:
                    return ("ignore", f"нет snapshot в Redis: {key}")

                data = json.loads(raw)
                snapshot_id = data.get("snapshot_id")
                if snapshot_id is None:
                    return ("ignore", f"нет snapshot_id в ключе {key}")

                snapshots[tf] = snapshot_id

            # 🔹 Проверяем winrate по m5 и m15
            for tf in ("m5", "m15"):
                table = f"positions_emasnapshot_{tf}_stat"
                snapshot_id = snapshots[tf]

                row = await pg.fetchrow(f"""
                    SELECT winrate
                    FROM {table}
                    WHERE strategy_id = $1 AND direction = 'long' AND emasnapshot_dict_id = $2
                """, strategy_id, snapshot_id)

                if not row:
                    return ("ignore", f"нет данных по snapshot {snapshot_id} ({tf})")

                winrate = float(row["winrate"])
                if winrate <= 0.5:
                    return ("ignore", f"winrate={winrate} <= 0.5 ({tf})")

            # 🔹 Всё прошло
            return True

        except Exception:
            log.exception("❌ Ошибка в strategy_520_longst")
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