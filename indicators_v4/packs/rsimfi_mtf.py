# packs/rsimfi_mtf.py — pack-воркер: MTF RSI+MFI zones (h1+m15+m5) → кандидаты bin_name (full → M5_0 → tail)

# 🔸 Базовые импорты
import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Optional


# 🔸 Дефолтные пороги и параметры (как в анализаторе)
DEFAULT_RSIMFI_LOW = Decimal("40.0")
DEFAULT_RSIMFI_HIGH = Decimal("60.0")
DEFAULT_LENGTH = 14

# 🔸 Маппинг зон Z* -> bin_idx (контракт)
ZONE_TO_BIN_IDX = {
    "Z1": 0,
    "Z2": 1,
    "Z3": 2,
    "Z4": 3,
    "Z5": 4,
}


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


# 🔸 Клипование в диапазон [0, 100]
def _clip_0_100(value: Decimal) -> Decimal:
    if value < Decimal("0"):
        return Decimal("0")
    if value > Decimal("100"):
        return Decimal("100")
    return value


# 🔸 Классификация значения в LOW/MID/HIGH по двум порогам
def _level_3(value: float, low: float, high: float) -> Optional[str]:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None

    if v <= low:
        return "LOW"
    if v >= high:
        return "HIGH"
    return "MID"


# 🔸 Классификация RSI/MFI в одну из 5 зон (как в анализаторе)
def _classify_rsi_mfi(rsi: float, mfi: float, low: float, high: float) -> Optional[str]:
    r_zone = _level_3(rsi, low, high)
    m_zone = _level_3(mfi, low, high)

    if r_zone is None or m_zone is None:
        return None

    # Z1: подтверждённый сильный тренд (оба в LOW или оба в HIGH)
    if (r_zone == "LOW" and m_zone == "LOW") or (r_zone == "HIGH" and m_zone == "HIGH"):
        return "Z1_CONFIRMED"

    # Z4: жёсткая дивергенция
    if (r_zone == "HIGH" and m_zone == "LOW") or (r_zone == "LOW" and m_zone == "HIGH"):
        return "Z4_DIVERGENCE"

    # Z2: цена в экстремуме, деньги в середине
    if r_zone in ("LOW", "HIGH") and m_zone == "MID":
        return "Z2_PRICE_EXTREME"

    # Z3: деньги в экстремуме, цена в середине
    if m_zone in ("LOW", "HIGH") and r_zone == "MID":
        return "Z3_FLOW_LEADS"

    # Z5: оба в середине
    if r_zone == "MID" and m_zone == "MID":
        return "Z5_NEUTRAL"

    return "Z5_NEUTRAL"


# 🔸 Преобразование RSI/MFI -> idx 0..4 (через зоны Z1..Z5)
def _rsimfi_idx(rsi: Decimal, mfi: Decimal, low: Decimal, high: Decimal) -> Optional[int]:
    # клипуем как в анализаторе
    rsi_c = _clip_0_100(rsi)
    mfi_c = _clip_0_100(mfi)

    zone_full = _classify_rsi_mfi(float(rsi_c), float(mfi_c), float(low), float(high))
    if not zone_full:
        return None

    z = zone_full.split("_")[0].strip().upper()
    idx = ZONE_TO_BIN_IDX.get(z)
    if idx is None:
        return None

    return int(idx)


# 🔸 Вспомогательная функция: биннинг по правилам bt_analysis_bins_dict
def _assign_bin(rules: list[Any], value: Decimal) -> str | None:
    # условия достаточности
    if not rules:
        return None

    for r in rules:
        lo = getattr(r, "val_from", None)
        hi = getattr(r, "val_to", None)
        to_inclusive = bool(getattr(r, "to_inclusive", False))
        name = getattr(r, "bin_name", None)

        if lo is None or hi is None or name is None:
            continue

        lo_d = Decimal(str(lo))
        hi_d = Decimal(str(hi))

        # обычный бин: [lo, hi)
        # inclusive бин: [lo, hi]
        if to_inclusive:
            if lo_d <= value <= hi_d:
                return str(name)
        else:
            if lo_d <= value < hi_d:
                return str(name)

    return None


# 🔸 Pack-воркер RSIMFI MTF
class RsiMfiMtfPack:
    # 🔸 Конструктор
    def __init__(self):
        self.log = logging.getLogger("PACK_RSIMFI_MTF")
        self.rsimfi_low = DEFAULT_RSIMFI_LOW
        self.rsimfi_high = DEFAULT_RSIMFI_HIGH
        self.length = DEFAULT_LENGTH

    # 🔸 Конфигурация порогов/длины из bt_analysis_parameters
    def configure(self, params: dict[str, Any]) -> None:
        # условия достаточности
        if not isinstance(params, dict):
            return

        # rsimfi_low / rsimfi_high
        low = _safe_decimal(params.get("rsimfi_low"))
        high = _safe_decimal(params.get("rsimfi_high"))
        if low is not None:
            self.rsimfi_low = low
        if high is not None:
            self.rsimfi_high = high

        # length
        try:
            if params.get("length") is not None:
                self.length = int(str(params.get("length")))
        except Exception:
            pass

    # 🔸 Конфиг MTF: какие TF читать и от чего триггериться
    def mtf_config(self, source_param_name: str) -> dict[str, Any]:
        # source_param_name ожидается как rsimfi14 / rsimfi21 (без _mtf)
        # используем self.length (подтянутую из params) как источник истины
        rsi_param = f"rsi{self.length}"
        mfi_param = f"mfi{self.length}"

        component_tfs = ["h1", "m15", "m5"]

        # component_params: для каждого TF нужно 2 значения (rsi + mfi)
        component_params = {tf: {"rsi": rsi_param, "mfi": mfi_param} for tf in component_tfs}

        return {
            "trigger_tf": "m5",
            "component_tfs": component_tfs,
            # триггеримся по RSI (base из indicator_stream)
            "component_param": rsi_param,
            "component_params": component_params,
        }

    # 🔸 Вернуть список кандидатов bin_name (по порядку “схлопывания”)
    def bin_candidates(self, values_by_tf: dict[str, Any], rules_by_tf: dict[str, list[Any]]) -> list[str]:
        # условия достаточности
        if not values_by_tf or not rules_by_tf:
            return []

        # ожидаем values_by_tf как:
        # {"h1":{"rsi":Decimal,"mfi":Decimal}, "m15":{...}, "m5":{...}}
        def _get_pair(tf: str) -> Optional[tuple[Decimal, Decimal]]:
            block = values_by_tf.get(tf)
            if not isinstance(block, dict):
                return None

            rsi = block.get("rsi")
            mfi = block.get("mfi")
            if not isinstance(rsi, Decimal) or not isinstance(mfi, Decimal):
                return None

            return rsi, mfi

        p_h1 = _get_pair("h1")
        p_m15 = _get_pair("m15")
        p_m5 = _get_pair("m5")
        if not p_h1 or not p_m15 or not p_m5:
            return []

        # idx 0..4 через зоны Z1..Z5
        idx_h1 = _rsimfi_idx(p_h1[0], p_h1[1], self.rsimfi_low, self.rsimfi_high)
        idx_m15 = _rsimfi_idx(p_m15[0], p_m15[1], self.rsimfi_low, self.rsimfi_high)
        idx_m5 = _rsimfi_idx(p_m5[0], p_m5[1], self.rsimfi_low, self.rsimfi_high)

        if idx_h1 is None or idx_m15 is None or idx_m5 is None:
            return []

        # бин-имена берём из словаря: value = Decimal(idx)
        h_bin = _assign_bin(rules_by_tf.get("h1", []), Decimal(idx_h1))
        m15_bin = _assign_bin(rules_by_tf.get("m15", []), Decimal(idx_m15))
        m5_bin = _assign_bin(rules_by_tf.get("m5", []), Decimal(idx_m5))

        if not h_bin or not m15_bin or not m5_bin:
            return []

        # full → схлопнуть m5 → схлопнуть m15+m5
        return [
            f"{h_bin}|{m15_bin}|{m5_bin}",
            f"{h_bin}|{m15_bin}|M5_0",
            f"{h_bin}|M15_0|M5_0",
        ]