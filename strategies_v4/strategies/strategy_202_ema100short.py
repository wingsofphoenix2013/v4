# strategy_202_ema100short.py — 🔸 Шортовая стратегия: фильтр по EMA100 status_triplet, зеркало мастера

import logging
import json
import math
from typing import Any, Dict, Tuple, Union

log = logging.getLogger("strategy_202_ema100short")

IgnoreResult = Tuple[str, str]  # ("ignore", reason)


# 🔸 Класс стратегии
class Strategy202Ema100short:
    # 🔸 Проверка и валидация сигнала
    async def validate_signal(self, signal: Dict[str, Any], context: Dict[str, Any]) -> Union[bool, IgnoreResult]:
        direction = str(signal.get("direction", "")).lower()
        symbol = str(signal.get("symbol", ""))
        self_sid = int(signal.get("strategy_id"))

        # проверяем направление
        if direction != "short":
            return ("ignore", f"short-only strategy: received '{direction}'")

        # проверяем наличие Redis
        redis = context.get("redis")
        if redis is None:
            return ("ignore", "No Redis in context")

        strategy_cfg = context.get("strategy") or {}
        mirrored_sid = self._resolve_mirrored_sid(strategy_cfg, direction, self_sid)

        # читаем live-триплет EMA100
        live_triplet_key = f"ind_live:{symbol}:ema100_status_triplet"
        triplet = await self._get_str(redis, live_triplet_key)
        if not triplet:
            return ("ignore", f"No information on EMA100 status_triplet for {symbol}")

        # читаем oracle-данные по мастеру
        oracle_key = f"oracle:ema:comp:{mirrored_sid}:short:ema100:{triplet}"
        oracle_raw = await self._get_str(redis, oracle_key)
        if not oracle_raw:
            return ("ignore", f"No information on EMA100 status_triplet {triplet} for {symbol}")

        try:
            oracle = json.loads(oracle_raw)
            closed_trades = int(oracle.get("closed_trades", 0))
            winrate = float(oracle.get("winrate", 0.0))
        except Exception:
            return ("ignore", f"Malformed oracle EMA100 entry for {symbol}")

        # читаем базу мастера (closed_short)
        stats_key = f"strategy:stats:{mirrored_sid}"
        closed_short = await self._get_int_hash(redis, stats_key, "closed_short")
        if closed_short is None or closed_short <= 0:
            return ("ignore", f"No directional history: closed_short=0 for master {mirrored_sid}")

        # проверяем объём истории (строго больше 0.2%)
        min_closed_required = math.floor(0.002 * closed_short) + 1
        if closed_trades < min_closed_required:
            return (
                "ignore",
                f"Not enough closed history for EMA100 triplet {triplet}: "
                f"required={min_closed_required}, got={closed_trades}"
            )

        # проверяем winrate
        if winrate < 0.55:
            return (
                "ignore",
                f"Winrate below 0.55 for EMA100 triplet {triplet}: got={winrate:.4f}"
            )

        return True

    # 🔸 Выполнение при допуске
    async def run(self, signal: Dict[str, Any], context: Dict[str, Any]) -> None:
        redis = context.get("redis")
        if redis is None:
            raise RuntimeError("No Redis in context")

        payload = {
            "strategy_id": str(signal["strategy_id"]),
            "symbol": signal["symbol"],
            "direction": signal["direction"],
            "log_uid": signal.get("log_uid"),
            "route": "new_entry",
            "received_at": signal.get("received_at"),
        }

        try:
            await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
            log.debug(f"📤 Open request queued: {payload}")
        except Exception as e:
            log.warning(f"Failed to enqueue open request: {e}")

    # 🔸 Определение зеркала мастера
    def _resolve_mirrored_sid(self, strategy_cfg: Dict[str, Any], direction: str, fallback_sid: int) -> int:
        mm_long = self._to_int_or_none(strategy_cfg.get("market_mirrow_long"))
        mm_short = self._to_int_or_none(strategy_cfg.get("market_mirrow_short"))
        mm = self._to_int_or_none(strategy_cfg.get("market_mirrow"))

        if mm_long is not None or mm_short is not None:
            if direction == "long" and mm_long is not None:
                return mm_long
            if direction == "short" and mm_short is not None:
                return mm_short
        if mm is not None:
            return mm
        return fallback_sid

    # 🔸 Утилита: получить строковое значение
    async def _get_str(self, redis, key: str) -> str:
        try:
            raw = await redis.get(key)
            if raw is None:
                return ""
            if isinstance(raw, bytes):
                return raw.decode("utf-8")
            return str(raw)
        except Exception:
            return ""

    # 🔸 Утилита: получить int из Hash
    async def _get_int_hash(self, redis, key: str, field: str) -> int | None:
        try:
            raw = await redis.hget(key, field)
            if raw is None:
                return None
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            return int(float(raw))
        except Exception:
            return None

    # 🔸 Утилита: преобразование в int
    @staticmethod
    def _to_int_or_none(v: Any) -> int | None:
        try:
            if v is None:
                return None
            iv = int(v)
            return iv if iv != 0 else None
        except Exception:
            return None