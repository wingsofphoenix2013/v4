# packs_config/mtf_ready.py — MTF readiness gate (event-driven): откладываем расчёт m5 на стыках до ready m15/h1 (timeout 120s)

from __future__ import annotations

# 🔸 Базовые импорты
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

# 🔸 Константы TF (ms)
TF_STEP_MS = {
    "m5": 300_000,
    "m15": 900_000,
    "h1": 3_600_000,
}

# 🔸 Политика
DEFAULT_TIMEOUT_SEC = 120
READY_TTL_SEC = 15 * 60  # держим отметки готовности m15/h1 (в памяти) 15 минут


# 🔸 Helpers: time
def now_ms() -> int:
    return int(datetime.utcnow().timestamp() * 1000)


def parse_open_time_to_ts_ms(open_time: Any) -> int | None:
    if open_time is None:
        return None
    try:
        dt = datetime.fromisoformat(str(open_time))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


# 🔸 Helpers: supertrend series normalization
def normalize_series(indicator_key: str, source_param_name: Any) -> str | None:
    """
    Для supertrend в indicator_stream есть source_param_name.
    Для остальных индикаторов возвращаем None.
    """
    try:
        if not str(indicator_key or "").startswith("supertrend"):
            return None
        s = str(source_param_name or "").strip()
        if not s:
            return None
        if s.endswith("_trend"):
            return s[:-6]
        return s
    except Exception:
        return None


# 🔸 Pending model
@dataclass
class PendingM5:
    symbol: str
    indicator_key: str
    series: str | None
    open_ts_ms: int
    open_time_iso: str | None
    deadline_ms: int
    need_m15_open_ts_ms: int | None
    need_h1_open_ts_ms: int | None


# 🔸 MTF readiness gate
class MtfReadyGate:
    # 🔸 Конструктор
    def __init__(self, timeout_sec: int = DEFAULT_TIMEOUT_SEC):
        self.log = logging.getLogger("MTF_READY")
        self.timeout_sec = int(timeout_sec)

        # ready map: (symbol, tf, indicator_key, series, open_ts_ms_tf) -> marked_at_ms
        self._ready: dict[tuple[str, str, str, str | None, int], int] = {}

        # pending map: (symbol, indicator_key, series, open_ts_ms_m5) -> PendingM5
        self._pending: dict[tuple[str, str, str | None, int], PendingM5] = {}

    # 🔸 Compute boundary requirements for a given m5 open_ts_ms
    def needed_higher_tfs(self, open_ts_ms_m5: int) -> tuple[int | None, int | None]:
        """
        Возвращает target_open_ts_ms для m15/h1, если m5-бар закрывается на границе этих TF.
        boundary = open_ts_ms(m5) + 5 минут
        - если boundary кратен 15m -> нужен m15 бар, который открылся boundary-15m
        - если boundary кратен 1h  -> нужен h1  бар, который открылся boundary-1h
        """
        try:
            open_ts_ms_m5 = int(open_ts_ms_m5)
        except Exception:
            return None, None

        boundary = open_ts_ms_m5 + TF_STEP_MS["m5"]

        need_m15 = None
        if (boundary % TF_STEP_MS["m15"]) == 0:
            need_m15 = boundary - TF_STEP_MS["m15"]

        need_h1 = None
        if (boundary % TF_STEP_MS["h1"]) == 0:
            need_h1 = boundary - TF_STEP_MS["h1"]

        return need_m15, need_h1

    # 🔸 Mark higher-TF ready from indicator_stream
    def mark_ready(
        self,
        symbol: str,
        timeframe: str,
        indicator_key: str,
        series: str | None,
        open_ts_ms: int,
        marked_at_ms: int | None = None,
    ) -> list[PendingM5]:
        """
        Отмечает готовность (m15/h1) и возвращает список pending m5-триггеров, которые теперь можно запускать.
        """
        tf = str(timeframe)
        if tf not in ("m15", "h1"):
            return []

        sym = str(symbol)
        ind = str(indicator_key)
        ots = int(open_ts_ms)

        t_mark = int(marked_at_ms) if marked_at_ms is not None else now_ms()
        key = (sym, tf, ind, series, ots)
        self._ready[key] = t_mark

        # почистим старые ready-отметки, чтобы не росло вечно
        self._cleanup_ready(t_mark)

        # проверим pending
        unlocked: list[PendingM5] = []
        for pkey, p in list(self._pending.items()):
            if p.symbol != sym:
                continue
            if p.indicator_key != ind:
                continue
            if p.series != series:
                continue

            if self._is_pending_ready(p):
                unlocked.append(p)
                self._pending.pop(pkey, None)

        if unlocked:
            self.log.debug(
                "MTF_READY: unlocked pending=%s by ready tf=%s (symbol=%s, indicator=%s, open_ts_ms=%s)",
                len(unlocked),
                tf,
                sym,
                ind,
                ots,
            )

        return unlocked

    # 🔸 Register m5 trigger; returns True if can run now, otherwise stores pending and returns False
    def register_m5(
        self,
        symbol: str,
        indicator_key: str,
        series: str | None,
        open_ts_ms: int,
        open_time_iso: str | None,
    ) -> tuple[bool, PendingM5 | None]:
        """
        Регистрирует m5-триггер.
        - если это не стык m15/h1 -> сразу True
        - если стык -> ждём ready нужных TF, пока не будет готово (или пока не истечёт timeout)
        """
        sym = str(symbol)
        ind = str(indicator_key)
        ots = int(open_ts_ms)

        need_m15, need_h1 = self.needed_higher_tfs(ots)

        # не стык
        if need_m15 is None and need_h1 is None:
            return True, None

        pending_key = (sym, ind, series, ots)

        # если уже есть pending — проверим готовность
        existing = self._pending.get(pending_key)
        if existing is not None:
            if self._is_pending_ready(existing):
                self._pending.pop(pending_key, None)
                return True, None
            return False, existing

        # создаём pending
        close_ts_ms = ots + TF_STEP_MS["m5"]
        deadline = close_ts_ms + self.timeout_sec * 1000

        p = PendingM5(
            symbol=sym,
            indicator_key=ind,
            series=series,
            open_ts_ms=ots,
            open_time_iso=str(open_time_iso) if open_time_iso is not None else None,
            deadline_ms=int(deadline),
            need_m15_open_ts_ms=need_m15,
            need_h1_open_ts_ms=need_h1,
        )

        # если уже готово — не сохраняем
        if self._is_pending_ready(p):
            return True, None

        self._pending[pending_key] = p
        self.log.debug(
            "MTF_READY: pending registered (symbol=%s, indicator=%s, open_ts_ms=%s, need_m15=%s, need_h1=%s, deadline_ms=%s)",
            sym,
            ind,
            ots,
            need_m15,
            need_h1,
            deadline,
        )
        return False, p

    # 🔸 Pop timeouts (extreme waiting)
    def pop_timeouts(self, t_ms: int | None = None) -> list[PendingM5]:
        """
        Возвращает и удаляет pending, которые превысили timeout.
        Это "экстремальный" кейс: считаем, что ждать дальше нельзя.
        """
        nowt = int(t_ms) if t_ms is not None else now_ms()
        out: list[PendingM5] = []

        for pkey, p in list(self._pending.items()):
            if int(p.deadline_ms) <= nowt:
                out.append(p)
                self._pending.pop(pkey, None)

        if out:
            self.log.debug("MTF_READY: timeouts=%s (timeout_sec=%s)", len(out), self.timeout_sec)

        # заодно чистим старые ready-отметки
        self._cleanup_ready(nowt)
        return out

    # 🔸 Stats helper
    def stats(self) -> dict[str, int]:
        return {"pending": len(self._pending), "ready": len(self._ready)}

    # internal: check if pending is ready
    def _is_pending_ready(self, p: PendingM5) -> bool:
        # m15 requirement
        if p.need_m15_open_ts_ms is not None:
            k = (p.symbol, "m15", p.indicator_key, p.series, int(p.need_m15_open_ts_ms))
            if k not in self._ready:
                return False
        # h1 requirement
        if p.need_h1_open_ts_ms is not None:
            k = (p.symbol, "h1", p.indicator_key, p.series, int(p.need_h1_open_ts_ms))
            if k not in self._ready:
                return False
        return True

    # internal: cleanup ready marks older than READY_TTL_SEC
    def _cleanup_ready(self, nowt_ms: int):
        cutoff = int(nowt_ms) - int(READY_TTL_SEC) * 1000
        if cutoff <= 0:
            return
        # условие достаточности
        if not self._ready:
            return

        removed = 0
        for k, v in list(self._ready.items()):
            try:
                if int(v) < cutoff:
                    self._ready.pop(k, None)
                    removed += 1
            except Exception:
                continue

        if removed:
            self.log.debug("MTF_READY: ready cache cleanup removed=%s", removed)