import logging
import json

log = logging.getLogger("strategy_650_universal")

class Strategy650Universal:
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
                    snapshot_data = json.loads(raw_snapshot)
                except json.JSONDecodeError:
                    return ("ignore", f"❌ ключ {snapshot_key} содержит некорректный JSON")

                pattern_id = snapshot_data.get("pattern_id")
                if pattern_id is None:
                    return ("ignore", f"❌ нет pattern_id в ключе {snapshot_key}")

                conf_key = f"confidence:{strategy_id}:{direction}:{tf}:pattern:{pattern_id}"
                raw_conf = await redis.get(conf_key)
                if raw_conf is None:
                    return ("ignore", f"❌ нет confidence-ключа: {conf_key}")

                try:
                    conf_data = json.loads(raw_conf)
                except json.JSONDecodeError:
                    return ("ignore", f"❌ ключ {conf_key} содержит некорректный JSON")

                winrate = conf_data.get("winrate")
                if winrate is None:
                    return ("ignore", f"❌ поле winrate отсутствует в {conf_key}")

                if winrate <= 0.5:
                    return ("ignore", f"🟥 winrate={winrate} <= 0.5 — strategy_id={strategy_id}, pattern_id={pattern_id}, tf={tf}")

            return True

        except Exception:
            log.exception("❌ Необработанная ошибка в strategy_650_universal")
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
            log.debug(f"📤 [650] Сигнал отправлен: {payload}")
        except Exception as e:
            log.warning(f"⚠️ [650] Ошибка при отправке сигнала: {e}")