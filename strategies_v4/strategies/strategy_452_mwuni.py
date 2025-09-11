# strategy_452_mwuni.py — 🔸 Универсальная стратегия (MW regime9): собираем live-триплет m5-m15-h1, сверяем с ORACLE, пороги по направлению

import logging
import json
import math
from typing import Any, Dict, Tuple, Union

log = logging.getLogger("strategy_452_mwuni")

IgnoreResult = Tuple[str, str]  # ("ignore", reason)


# 🔸 Класс стратегии
class Strategy452Mwuni:
    # Проверка и валидация сигнала (MW-подход)
    async def validate_signal(self, signal: Dict[str, Any], context: Dict[str, Any]) -> Union[bool, IgnoreResult]:
        direction = str(signal.get("direction", "")).lower()
        symbol = str(signal.get("symbol", ""))
        self_sid = int(signal.get("strategy_id"))

        if direction not in ("long", "short"):
            return ("ignore", f"unsupported direction '{direction}'")

        redis = context.get("redis")
        if redis is None:
            return ("ignore", "No Redis in context")

        strategy_cfg = context.get("strategy") or {}
        master_sid = self._resolve_mirrored_sid(strategy_cfg, direction, self_sid)

        # читаем live-коды MW по TF; без ожидания, если чего-то нет — сразу ignore
        m5  = await self._get_str(redis, f"ind_live:{symbol}:m5:regime9_code")
        m15 = await self._get_str(redis, f"ind_live:{symbol}:m15:regime9_code")
        h1  = await self._get_str(redis, f"ind_live:{symbol}:h1:regime9_code")

        if not m5 or not m15 or not h1:
            return ("ignore", f"No live MW code for {symbol} (m5/m15/h1)")

        triplet = f"{m5}-{m15}-{h1}"

        # oracle по триплету мастера (по направлению)
        oracle_key = f"oracle:mw:comp:{master_sid}:{direction}:mw:{triplet}"
        oracle_raw = await self._get_str(redis, oracle_key)
        if not oracle_raw:
            return ("ignore", f"No information on MW triplet {triplet} for {symbol}")

        try:
            oracle = json.loads(oracle_raw)
            closed_trades = int(oracle.get("closed_trades", 0))
            winrate = float(oracle.get("winrate", 0.0))
        except Exception:
            return ("ignore", f"Malformed oracle MW entry for {symbol}")

        # направленная база мастера
        stats_key = f"strategy:stats:{master_sid}"
        closed_field = "closed_long" if direction == "long" else "closed_short"
        closed_dir_total = await self._get_int_hash(redis, stats_key, closed_field)
        if closed_dir_total is None or closed_dir_total <= 0:
            return ("ignore", f"No directional history: {closed_field}=0 for master {master_sid}")

        # порог истории (строго > 0.2%)
        min_closed_required = math.floor(0.002 * closed_dir_total) + 1
        if closed_trades < min_closed_required:
            return ("ignore", f"Not enough closed history for MW triplet {triplet}: required={min_closed_required}, got={closed_trades}")

        # порог winrate
        if winrate < 0.55:
            return ("ignore", f"Winrate below 0.55 for MW triplet {triplet}: got={winrate:.4f}")

        return True

    # Выполнение при допуске
    async def run(self, signal: Dict[str, Any], context: Dict[str, Any]) -> None:
        redis = context.get("redis")
        if redis is None:
            raise RuntimeError("No Redis in context")

        payload = {
            "strategy_id": str(signal.get("strategy_id")),
            "symbol": signal.get("symbol"),
            "direction": signal.get("direction"),
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