import logging
import json
import asyncio
from datetime import datetime, timedelta

log = logging.getLogger("strategy_123_emackamashort")

class Strategy123Emackamashort:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        symbol = signal["symbol"]
        tf = "m5"  # таймфрейм
        tf_sec = 300  # секунд в баре для m5
        kama_length = 21  # длина KAMA — меняй здесь
        N = 6  # количество баров для расчёта наклона

        if signal["direction"].lower() != "short":
            return ("ignore", "long сигналы отключены")

        if redis is None:
            log.warning("Нет Redis в context")
            return ("ignore", "нет Redis")

        try:
            now = datetime.utcnow()
            current_bar_open = now.replace(second=0, microsecond=0) - timedelta(
                seconds=now.minute % (tf_sec // 60) * 60
            )
            prev_bar_open = current_bar_open - timedelta(seconds=tf_sec)
            n_bars_back_open = prev_bar_open - timedelta(seconds=tf_sec * N)

            ts_key = f"ts_ind:{symbol}:{tf}:kama{kama_length}"

            kama_now = None
            kama_old = None
            for attempt in range(3):
                data_now = await redis.ts().range(
                    ts_key,
                    int(prev_bar_open.timestamp() * 1000),
                    int(prev_bar_open.timestamp() * 1000)
                )
                data_old = await redis.ts().range(
                    ts_key,
                    int(n_bars_back_open.timestamp() * 1000),
                    int(n_bars_back_open.timestamp() * 1000)
                )

                if data_now and data_old:
                    kama_now = float(data_now[0][1])
                    kama_old = float(data_old[0][1])
                    break

                await asyncio.sleep(10)

            if kama_now is None or kama_old is None:
                return ("ignore", f"нет данных KAMA после {attempt+1} попыток")

            slope = (kama_now - kama_old) / N

            if slope < 0:
                return True
            else:
                return ("ignore", f"slope={slope} >= 0")

        except Exception:
            log.exception("Ошибка в strategy_123_emackamashort")
            return ("ignore", "ошибка в стратегии")

    async def run(self, signal, context):
        redis = context.get("redis")
        if redis is None:
            raise RuntimeError("Redis клиент не передан в context")

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
            log.debug(f"Сигнал отправлен: {payload}")
        except Exception as e:
            log.warning(f"Ошибка при отправке сигнала: {e}")