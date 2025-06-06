# strategies/strategy_41.py
import logging
import json
from datetime import datetime
from infra import load_indicators
from config_loader import config

log = logging.getLogger("STRATEGY_41")

class Strategy41:
    # 🔸 Метод валидации сигнала перед входом
    async def validate_signal(self, signal, context) -> bool | str:
        symbol = signal.get("symbol")
        direction = signal.get("direction")
        strategy_id = int(signal.get("strategy_id"))
        log_id = signal.get("log_id")

        log.debug(f"⚙️ [Strategy41] Валидация сигнала: symbol={symbol}, direction={direction}")

        redis = context.get("redis")
        note = None

        try:
            timeframe = "m5"
            indicators = await load_indicators(symbol, [
                "rsi14",
                "bb20_2_5_upper", "bb20_2_5_lower", "bb20_2_5_center",
                "adx_dmi14_adx", "adx_dmi14_plus_di", "adx_dmi14_minus_di"
            ], timeframe)

            price_raw = await redis.get(f"price:{symbol}")
            if price_raw is None:
                note = "отклонено: отсутствует цена"
            else:
                price = float(price_raw)
                rsi = indicators.get("rsi14")
                bb_upper = indicators.get("bb20_2_5_upper")
                bb_lower = indicators.get("bb20_2_5_lower")
                adx = indicators.get("adx_dmi14_adx")
                plus_di = indicators.get("adx_dmi14_plus_di")
                minus_di = indicators.get("adx_dmi14_minus_di")

                # Проверка отсутствующих данных
                if None in [rsi, bb_upper, bb_lower, adx, plus_di, minus_di]:
                    note = "отклонено: недостаточно данных индикаторов"

                else:
                    rsi = float(rsi)
                    bb_upper = float(bb_upper)
                    bb_lower = float(bb_lower)
                    adx = float(adx)
                    plus_di = float(plus_di)
                    minus_di = float(minus_di)

                    if direction == "long":
                        if not (rsi < 25):
                            note = f"отклонено: RSI14 >= 25 (rsi={rsi})"
                        elif not (price < bb_lower):
                            note = f"отклонено: цена не ниже BB lower (price={price}, lower={bb_lower})"
                        elif not (adx < 20 and minus_di > plus_di):
                            note = f"отклонено: ADX/DMI фильтр не пройден (adx={adx}, -DI={minus_di}, +DI={plus_di})"

                    elif direction == "short":
                        if not (rsi > 75):
                            note = f"отклонено: RSI14 <= 75 (rsi={rsi})"
                        elif not (price > bb_upper):
                            note = f"отклонено: цена не выше BB upper (price={price}, upper={bb_upper})"
                        elif not (adx < 20 and plus_di > minus_di):
                            note = f"отклонено: ADX/DMI фильтр не пройден (adx={adx}, +DI={plus_di}, -DI={minus_di})"

        except Exception as e:
            note = f"ошибка при валидации фильтров: {e}"

        if note:
            log.debug(f"🚫 [Strategy41] {note}")
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
                    log.warning(f"⚠️ [Strategy41] Ошибка записи в Redis log_queue: {e}")
            return "logged"

        return True
    # 🔸 Основной метод запуска стратегии
    async def run(self, signal, context):
        log.debug(f"🚀 [Strategy41] Запуск стратегии на сигнале: symbol={signal['symbol']}, direction={signal['direction']}")

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
                log.debug(f"📤 [Strategy41] Сигнал отправлен в strategy_opener_stream")
            except Exception as e:
                log.warning(f"⚠️ [Strategy41] Ошибка при отправке в stream: {e}")