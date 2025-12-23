# packs_config/contract.py — JSON contract v1 для ind_pack + helpers для ошибок/деталей

from __future__ import annotations

# 🔸 Imports
import json
from datetime import datetime, timezone
from typing import Any


# 🔸 ind_pack JSON Contract (v1): reason catalog
TRANSIENT_REASONS = {
    "not_ready_retrying",
    "missing_inputs",
    "mtf_missing_component_values",
    "mtf_boundary_wait",
    "labels_not_loaded_yet",
    "rules_not_loaded_yet",
    "invalid_input_value",
}
PERMANENT_REASONS = {
    "invalid_trigger_event",
    "pair_not_configured",
    "no_rules_static",
    "no_rules_adaptive",
    "no_quantiles_rules",
    "no_candidates",
    "no_labels_match",
    "invalid_direction",
    "internal_error",
}

# 🔸 Ограничения диагностики (не заливаем Redis)
MAX_ERROR_STR_LEN = 400


# 🔸 JSON helpers
def _json_dumps(obj: dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def pack_ok(bin_name: str) -> str:
    return _json_dumps({"ok": True, "bin_name": str(bin_name)})


def pack_fail(reason: str, details: dict[str, Any]) -> str:
    r = str(reason or "")
    if r not in TRANSIENT_REASONS and r not in PERMANENT_REASONS:
        r = "internal_error"

    if not isinstance(details, dict):
        details = {}

    # обязательная форма details (контракт)
    details.setdefault("analysis_id", None)
    details.setdefault("symbol", None)
    details.setdefault("direction", None)
    details.setdefault("timeframe", None)
    details.setdefault("pair", None)
    details.setdefault("trigger", {})
    details.setdefault("open_ts_ms", None)

    return _json_dumps({"ok": False, "reason": r, "details": details})


def short_error_str(e: BaseException) -> str:
    s = f"{type(e).__name__}: {e}".strip()
    if len(s) > MAX_ERROR_STR_LEN:
        s = s[:MAX_ERROR_STR_LEN] + "…"
    return s


# 🔸 Details base builder
def build_fail_details_base(
    analysis_id: int | None,
    symbol: str | None,
    direction: str | None,
    timeframe: str | None,
    pair: dict[str, int] | None,
    trigger: dict[str, Any],
    open_ts_ms: int | None,
) -> dict[str, Any]:
    return {
        "analysis_id": int(analysis_id) if analysis_id is not None else None,
        "symbol": str(symbol) if symbol is not None else None,
        "direction": str(direction) if direction is not None else None,
        "timeframe": str(timeframe) if timeframe is not None else None,
        "pair": pair,
        "trigger": trigger,
        "open_ts_ms": int(open_ts_ms) if open_ts_ms is not None else None,
    }


# 🔸 Parse open_time ISO -> open_ts_ms
def parse_open_time_to_open_ts_ms(open_time: str | None) -> int | None:
    if not open_time:
        return None
    try:
        dt = datetime.fromisoformat(str(open_time))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None