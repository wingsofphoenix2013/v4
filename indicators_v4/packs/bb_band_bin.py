# packs/bb_band_bin.py — pack-воркер: определение полосы Bollinger Bands по цене (bin_name из bt_analysis_bins_dict)

# 🔸 Базовые импорты
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any


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


def _calc_bb_bin_order(price: Decimal, upper: Decimal, lower: Decimal) -> int | None:
    # ширина канала
    H = upper - lower
    if H <= Decimal("0"):
        return None

    # 0 -> выше upper, 9 -> ниже lower
    if price > upper:
        return 0
    if price < lower:
        return 9

    # внутри канала [lower, upper]
    rel = (upper - price) / H  # 0 → верх, 1 → низ

    # защита от численных артефактов
    if rel < Decimal("0"):
        rel = Decimal("0")
    if rel > Decimal("1"):
        rel = Decimal("1")

    # 8 полос внутри: rel ∈ [0,1] → idx ∈ [0,7]
    idx = int((rel * Decimal("8")).quantize(Decimal("0"), rounding=ROUND_DOWN))
    if idx >= 8:
        idx = 7

    return 1 + idx  # 1..8


# 🔸 Pack-воркер BB band binning
class BbBandBinPack:
    # 🔸 Основной метод: вернуть bin_name для цены относительно upper/lower по списку правил
    def bin_value(self, value: Any, rules: list[Any]) -> str | None:
        # условия достаточности
        if not rules:
            return None

        # ожидаем value как:
        # - dict: {"price": ..., "upper": ..., "lower": ...}
        # - tuple/list: (price, upper, lower)
        price_raw = None
        upper_raw = None
        lower_raw = None

        if isinstance(value, dict):
            price_raw = value.get("price")
            upper_raw = value.get("upper")
            lower_raw = value.get("lower")
        elif isinstance(value, (tuple, list)) and len(value) >= 3:
            price_raw, upper_raw, lower_raw = value[0], value[1], value[2]
        else:
            return None

        price = _safe_decimal(price_raw)
        upper = _safe_decimal(upper_raw)
        lower = _safe_decimal(lower_raw)

        if price is None or upper is None or lower is None:
            return None

        bin_order = _calc_bb_bin_order(price, upper, lower)
        if bin_order is None:
            return None

        # map bin_order -> bin_name (правила уже отфильтрованы по direction/timeframe)
        for rule in rules:
            try:
                order = int(_get_field(rule, "bin_order"))
            except Exception:
                continue
            if order != bin_order:
                continue

            bin_name = _get_field(rule, "bin_name")
            if bin_name:
                return str(bin_name)

        return None