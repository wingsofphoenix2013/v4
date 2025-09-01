import logging
import json

log = logging.getLogger("strategy_521_shortmwema3")

class Strategy521Shortmwema3:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        strategy_cfg = context.get("strategy")
        if redis is None:
            return ("ignore", "нет Redis клиента")
        if not strategy_cfg:
            return ("ignore", "нет конфигурации стратегии")

        direction = signal["direction"].lower()
        if direction != "short":
            return ("ignore", "long сигналы отключены")

        symbol = signal["symbol"]

        # ── Шаг 1: regime9 (m5-m15-h1)
        try:
            m5  = await redis.get(f"ind:{symbol}:m5:regime9_code")
            m15 = await redis.get(f"ind:{symbol}:m15:regime9_code")
            h1  = await redis.get(f"ind:{symbol}:h1:regime9_code")
        except Exception as e:
            log.warning(f"⚠️ Ошибка чтения regime9_code для {symbol}: {e}")
            return ("ignore", "regime: ошибка Redis при получении regime9_code")

        if not m5 or not m15 or not h1:
            return ("ignore", "regime: не удалось получить все regime9_code")

        marker3_code = f"{m5}-{m15}-{h1}"

        base_id = strategy_cfg.get("market_mirrow") or strategy_cfg["id"]
        regime_key = f"oracle:mw:stat:{base_id}:short:{marker3_code}"

        try:
            raw_regime = await redis.get(regime_key)
        except Exception as e:
            log.warning(f"⚠️ Ошибка чтения Redis ключа {regime_key}: {e}")
            return ("ignore", "regime: ошибка Redis при получении статистики")

        if raw_regime is None:
            return ("ignore", "regime: нет статистики по marker3_code")

        try:
            rdata = json.loads(raw_regime)
            r_closed = int(rdata.get("closed_trades", 0))
            r_wr = float(rdata.get("winrate", 0))
        except Exception as e:
            log.warning(f"⚠️ Ошибка парсинга значения ключа {regime_key}: {e}")
            return ("ignore", "regime: некорректные данные статистики")

        if r_closed <= 2:
            return ("ignore", "regime: недостаточно данных")
        if r_wr <= 0.5:
            return ("ignore", f"regime: winrate {r_wr:.4f} ниже порога")

        # ── Шаг 2: EMA50 triplet (live) m5-m15-h1 и его oracle-агрегат
        live_key = f"ind_live:{symbol}:ema50_status_triplet"
        try:
            triplet = await redis.get(live_key)
        except Exception as e:
            log.warning(f"⚠️ Ошибка чтения ключа {live_key}: {e}")
            return ("ignore", "ema: ошибка Redis при получении EMA триплета")

        if not triplet:
            return ("ignore", "ema: нет live EMA триплета")

        ema_key = f"oracle:emastat:comp:{base_id}:short:50:{triplet}"
        try:
            raw_ema = await redis.get(ema_key)
        except Exception as e:
            log.warning(f"⚠️ Ошибка чтения Redis ключа {ema_key}: {e}")
            return ("ignore", "ema: ошибка Redis при получении статистики")

        if raw_ema is None:
            return ("ignore", "ema: нет статистики по EMA триплету")

        try:
            edata = json.loads(raw_ema)
            e_closed = int(edata.get("closed_trades", 0))
            e_wr = float(edata.get("winrate", 0))
        except Exception as e:
            log.warning(f"⚠️ Ошибка парсинга значения ключа {ema_key}: {e}")
            return ("ignore", "ema: некорректные данные статистики")

        if e_closed <= 2:
            return ("ignore", "ema: недостаточно данных")
        if e_wr <= 0.5:
            return ("ignore", f"ema: winrate {e_wr:.4f} ниже порога")

        # Обе ступени пройдены
        return True

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