import logging
import json

log = logging.getLogger("strategy_670_universalst")

class Strategy670Universalst:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        strategy_meta = context.get("strategy", {})
        direction = signal["direction"].lower()
        symbol = signal["symbol"]
        tf = "m5"

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

            # Получаем snapshot + pattern
            snapshot_key = f"snapshot:{symbol}:{tf}"
            raw_snapshot = await redis.get(snapshot_key)
            if raw_snapshot is None:
                return ("ignore", f"❌ нет ключа {snapshot_key} в Redis")

            try:
                data = json.loads(raw_snapshot)
            except json.JSONDecodeError:
                return ("ignore", f"❌ ключ {snapshot_key} содержит некорректный JSON")

            snapshot_id = data.get("snapshot_id")
            pattern_id = data.get("pattern_id")

            if snapshot_id is None or pattern_id is None:
                return ("ignore", f"❌ snapshot_id или pattern_id отсутствует в {snapshot_key}")

            # Проверка snapshot
            snap_key = f"confidence:{strategy_id}:{direction}:{tf}:snapshot:{snapshot_id}"
            snap_raw = await redis.get(snap_key)
            if snap_raw is None:
                return ("ignore", f"❌ нет confidence-ключа: {snap_key}")

            try:
                snap_data = json.loads(snap_raw)
            except json.JSONDecodeError:
                return ("ignore", f"❌ ключ {snap_key} содержит некорректный JSON")

            winrate_snap = snap_data.get("winrate")
            if winrate_snap is None or winrate_snap <= 0.5:
                return ("ignore", f"🟥 snapshot winrate={winrate_snap} <= 0.5 — strategy_id={strategy_id}, snapshot_id={snapshot_id}, tf={tf}")

            # Проверка pattern
            pat_key = f"confidence:{strategy_id}:{direction}:{tf}:pattern:{pattern_id}"
            pat_raw = await redis.get(pat_key)
            if pat_raw is None:
                return ("ignore", f"❌ нет confidence-ключа: {pat_key}")

            try:
                pat_data = json.loads(pat_raw)
            except json.JSONDecodeError:
                return ("ignore", f"❌ ключ {pat_key} содержит некорректный JSON")

            winrate_pat = pat_data.get("winrate")
            if winrate_pat is None or winrate_pat <= 0.5:
                return ("ignore", f"🟥 pattern winrate={winrate_pat} <= 0.5 — strategy_id={strategy_id}, pattern_id={pattern_id}, tf={tf}")

            return True

        except Exception:
            log.exception("❌ Необработанная ошибка в strategy_670_universalst")
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
            log.debug(f"📤 [670] Сигнал отправлен: {payload}")
        except Exception as e:
            log.warning(f"⚠️ [670] Ошибка при отправке сигнала: {e}")