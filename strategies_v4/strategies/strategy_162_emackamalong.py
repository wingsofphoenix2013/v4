import logging
import json
from datetime import datetime, timedelta

log = logging.getLogger("strategy_162_emackamalong")

class Strategy162Emackamalong:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        symbol = signal["symbol"]
        tf = "m5"  # таймфрейм
        tf_sec = 300  # секунд в баре для m5
        kama_length = 50  # длина KAMA — меняй здесь
        N = 5  # количество баров для расчёта наклона

        if signal["direction"].lower() != "long":
            return ("ignore", "short сигналы отключены")

        if redis is None:
            log.warning("Нет Redis в context")
            return ("ignore", "нет Redis")

        try:
            # Время открытия текущей свечи
            now = datetime.utcnow()
            current_bar_open = now.replace(second=0, microsecond=0) - timedelta(
                seconds=now.minute % (tf_sec // 60) * 60
            )

            # Время открытия предыдущей свечи
            prev_bar_open = current_bar_open - timedelta(seconds=tf_sec)

            # Время открытия свечи N баров назад
            n_bars_back_open = prev_bar_open - timedelta(seconds=tf_sec * N)

            ts_key = f"ts_ind:{symbol}:{tf}:kama{kama_length}"

            # Пробуем до 3 раз получить данные из TS
            kama_now = None
            kama_old = None
            for attempt in range(3):
                kama_now = await redis.ts().get(ts_key, timestamp=int(prev_bar_open.timestamp() * 1000))
                kama_old = await redis.ts().get(ts_key, timestamp=int(n_bars_back_open.timestamp() * 1000))

                if kama_now and kama_old:
                    break
                await asyncio.sleep(10)  # подождать 10 секунд перед повтором

            if not kama_now or not kama_old:
                return ("ignore", f"нет данных KAMA после {attempt+1} попыток")

            slope = (float(kama_now[1]) - float(kama_old[1])) / N

            if slope > 0:
                return True
            else:
                return ("ignore", f"slope={slope} <= 0")

        except Exception:
            log.exception("Ошибка в strategy_162_emackamalong")
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