# packs/rsi_mtf.py — pack-воркер: MTF RSI (h1+m15+m5) → кандидаты bin_name (full → M5_0)

# 🔸 Базовые импорты
import logging
from decimal import Decimal
from typing import Any


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


# 🔸 Pack-воркер RSI MTF
class RsiMtfPack:
    # 🔸 Конструктор
    def __init__(self):
        self.log = logging.getLogger("PACK_RSI_MTF")

    # 🔸 Конфиг MTF: какие TF читать и от чего триггериться
    def mtf_config(self, source_param_name: str) -> dict[str, Any]:
        # source_param_name ожидается как rsi14_mtf
        base_param = source_param_name
        if base_param.lower().endswith("_mtf"):
            base_param = base_param[:-4]

        return {
            "trigger_tf": "m5",
            "component_tfs": ["h1", "m15", "m5"],
            "component_param": base_param,  # rsi14
        }

    # 🔸 Вернуть список кандидатов bin_name (по порядку “схлопывания”)
    def bin_candidates(self, values_by_tf: dict[str, Decimal], rules_by_tf: dict[str, list[Any]]) -> list[str]:
        # условия достаточности
        if not values_by_tf or not rules_by_tf:
            return []

        h1 = values_by_tf.get("h1")
        m15 = values_by_tf.get("m15")
        m5 = values_by_tf.get("m5")
        if h1 is None or m15 is None or m5 is None:
            return []

        h_bin = _assign_bin(rules_by_tf.get("h1", []), h1)
        m15_bin = _assign_bin(rules_by_tf.get("m15", []), m15)
        m5_bin = _assign_bin(rules_by_tf.get("m5", []), m5)

        if not h_bin or not m15_bin or not m5_bin:
            return []

        # full → схлопнуть m5
        return [
            f"{h_bin}|{m15_bin}|{m5_bin}",
            f"{h_bin}|{m15_bin}|M5_0",
        ]