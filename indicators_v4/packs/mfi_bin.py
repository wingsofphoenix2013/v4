# packs/mfi_bin.py — pack-воркер: бинирование MFI по статичному словарю bt_analysis_bins_dict

# 🔸 Базовые импорты
from typing import Any


# 🔸 Вспомогательные функции
def _match_bin_rule(value: float, rule: Any) -> bool:
    # нижняя граница всегда включительная
    val_from = getattr(rule, "val_from", None)
    if val_from is not None and value < float(val_from):
        return False

    # верхняя граница контролируется to_inclusive
    val_to = getattr(rule, "val_to", None)
    if val_to is None:
        return True

    to_inclusive = bool(getattr(rule, "to_inclusive", False))
    if to_inclusive:
        return value <= float(val_to)
    return value < float(val_to)


# 🔸 Pack-воркер MFI binning
class MfiBinPack:
    # 🔸 Основной метод: вернуть bin_name для значения по списку правил
    def bin_value(self, value: float, rules: list[Any]) -> str | None:
        # условия достаточности
        if rules is None or not rules:
            return None

        # проход по правилам (ожидается, что rules уже отсортированы по bin_order)
        for rule in rules:
            if _match_bin_rule(value, rule):
                bin_name = getattr(rule, "bin_name", None)
                if bin_name:
                    return str(bin_name)

        return None