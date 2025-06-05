# strategies/strategy_24.py
import logging
import json
from datetime import datetime
from infra import load_indicators
from config_loader import config

log = logging.getLogger("STRATEGY_24")

class Strategy24:
    # 🔸 Метод валидации сигнала перед входом
    async def validate_signal(self, signal, context) -> bool | str:
        symbol = signal.get("symbol")
        direction = signal.get("direction")
        strategy_id = int(signal.get("strategy_id"))
        log_id = signal.get("log_id")

        log.debug(f"⚙️ [Strategy24] Валидация сигнала: symbol={symbol}, direction={direction}")

        redis = context.get("redis")
        note = None

        try:
            # Таймфрейм стратегии
            timeframe = config.strategies[strategy_id]["meta"]["timeframe"]

            # Получение EMA(50) и RSI14
            ind = await load_indicators(symbol, ["ema50", "rsi14"], timeframe)
            ema = ind.get("ema50")
            rsi = ind.get("rsi14")

            if ema is None:
                note = "отклонено: отсутствует ema50"
            else:
                # Получение текущей цены (mark price)
                price_raw = await redis.get(f"price:{symbol}")
                if price_raw is None:
                    note = "отклонено: отсутствует mark price"
                else:
                    price = float(price_raw)
                    ema = float(ema)

                    if direction == "long" and price <= ema:
                        note = f"отклонено: цена ниже EMA50 (price={price}, ema={ema})"
                    elif direction == "short" and price >= ema:
                        note = f"отклонено: цена выше EMA50 (price={price}, ema={ema})"

            # Проверка RSI14 (выполняется только если предыдущая не отклонила)
            if not note:
                if rsi is None:
                    note = "отклонено: отсутствует rsi14"
                else:
                    rsi = float(rsi)
                    if direction == "long" and not (35 < rsi < 70):
                        note = f"отклонено: RSI14 вне диапазона для LONG (rsi={rsi})"
                    elif direction == "short" and not (30 < rsi < 65):
                        note = f"отклонено: RSI14 вне диапазона для SHORT (rsi={rsi})"

        except Exception as e:
            note = f"ошибка при валидации фильтров: {e}"

        if note:
            log.debug(f"🚫 [Strategy24] {note}")
            if redis:
                log_record = {
                    "log_id": log_id,
                    "strategy_id": strategy_id,
                    "status": "ignore",
                    "position_id": None,
                    "note": note,
                    "logged_at": datetime.utcnow().isoformat()
                }
                try:
                    await redis.xadd("signal_log_queue", {"data": json.dumps(log_record)})
                except Exception as e:
                    log.warning(f"⚠️ [Strategy24] Ошибка записи в Redis log_queue: {e}")
            return "logged"

        return True
    # 🔸 Основной метод запуска стратегии
    async def run(self, signal, context):
        log.debug(f"🚀 [Strategy24] Запуск стратегии на сигнале: symbol={signal['symbol']}, direction={signal['direction']}")

        redis = context.get("redis")
        if redis:
            payload = {
                "strategy_id": signal["strategy_id"],
                "symbol": signal["symbol"],
                "direction": signal["direction"],
                "log_id": signal["log_id"],
                "route": "new_entry"
            }
            try:
                await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
                log.debug(f"📤 [Strategy24] Сигнал отправлен в strategy_opener_stream")
            except Exception as e:
                log.warning(f"⚠️ [Strategy24] Ошибка при отправке в stream: {e}")