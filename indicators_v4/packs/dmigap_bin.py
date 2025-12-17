# packs/dmigap_bin.py — pack-воркер: бинирование DMI-gap (+DI − −DI) по адаптивным q6-границам (bt_analysis_bin_dict_adaptive)

# 🔸 Базовые импорты
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any


# 🔸 Единая точность (как в bt_analysis_dmigap_bin.py)
Q6 = Decimal("0.000001")


# 🔸 Вспомогательные функции
def _get_field(rule: Any, name: str, default: Any = None) -> Any:
    if isinstance(rule, dict):
        return rule.get(name, default)
    return getattr(rule, name, default)


def _safe_decimal(x: Any) -> Decimal | None:
    if x is None:
        return None
    if isinstance(x, Decimal):
        return x
    try:
        return Decimal(str(x))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _q6(value: Any) -> Decimal:
    d = _safe_decimal(value)
    if d is None:
        return Decimal("0").quantize(Q6, rounding=ROUND_DOWN)
    return d.quantize(Q6, rounding=ROUND_DOWN)


def _get_bounds_from_rules(rules: list[Any]) -> tuple[Decimal | None, Decimal | None]:
    if not rules:
        return None, None

    first = rules[0]
    last = rules[-1]

    lo = _get_field(first, "val_from")
    hi = _get_field(last, "val_to")

    lo_q = _q6(lo) if lo is not None else None
    hi_q = _q6(hi) if hi is not None else None
    return lo_q, hi_q


# 🔸 Pack-воркер DMI-gap binning
class DmiGapBinPack:
    # 🔸 Основной метод: вернуть bin_name для DMI-gap по правилам (val_from/val_to/to_inclusive) с q6
    def bin_value(self, value: Any, rules: list[Any]) -> str | None:
        # условия достаточности
        if not rules:
            return None

        # ожидаем value как dict: {"plus": ..., "minus": ...}
        if not isinstance(value, dict):
            return None

        plus_raw = value.get("plus")
        minus_raw = value.get("minus")

        plus = _safe_decimal(plus_raw)
        minus = _safe_decimal(minus_raw)

        if plus is None or minus is None:
            return None

        # dmi_gap = +DI - -DI, q6 ROUND_DOWN
        gap = _q6(plus - minus)

        # клип в диапазон [min, max] по краям словаря (как в анализаторе)
        min_v, max_v = _get_bounds_from_rules(rules)
        if min_v is not None and gap < min_v:
            gap = min_v
        if max_v is not None and gap > max_v:
            gap = max_v

        last_index = len(rules) - 1

        # проход по бинам (rules отсортированы по bin_order)
        for idx, rule in enumerate(rules):
            name = _get_field(rule, "bin_name")
            lo_raw = _get_field(rule, "val_from")
            hi_raw = _get_field(rule, "val_to")
            to_inclusive = bool(_get_field(rule, "to_inclusive", False))

            if name is None or lo_raw is None or hi_raw is None:
                continue

            lo = _q6(lo_raw)
            hi = _q6(hi_raw)

            # обычный бин: [lo, hi)
            # inclusive бин: [lo, hi] (или последний бин)
            if to_inclusive or idx == last_index:
                if lo <= gap <= hi:
                    return str(name)
            else:
                if lo <= gap < hi:
                    return str(name)

        return None