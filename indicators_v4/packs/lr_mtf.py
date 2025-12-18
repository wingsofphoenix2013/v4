# packs/lr_mtf.py — pack-воркер: LR MTF (H1/M15 static bins + M5 adaptive quantiles по rel_m5) → кандидаты bin_name

# 🔸 Базовые импорты
import logging
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Optional


# 🔸 Константы (как в анализаторе)
LR_MTF_QUANTILES = 5
Q6 = Decimal("0.000001")


# 🔸 Вспомогательные функции: Decimal/float
def _safe_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError, InvalidOperation):
        return None


def _q6(value: Any) -> Decimal:
    try:
        d = value if isinstance(value, Decimal) else Decimal(str(value))
        return d.quantize(Q6, rounding=ROUND_DOWN)
    except Exception:
        return Decimal("0").quantize(Q6, rounding=ROUND_DOWN)


# 🔸 Маппинг цены относительно LR-канала в индекс бина 0..5 (как в анализаторе)
def _lr_position_to_bin_idx(price: float, upper: float, lower: float) -> Optional[int]:
    try:
        p = float(price)
        u = float(upper)
        l = float(lower)
    except (TypeError, ValueError):
        return None

    H = u - l
    if H <= 0:
        return None

    # выше верхней границы
    if p > u:
        return 0

    # ниже нижней границы
    if p < l:
        return 5

    # внутри канала: rel = 0 → верх, rel = 1 → низ
    rel = (u - p) / H

    if rel < 0:
        rel = 0.0
    if rel > 1:
        rel = 1.0

    idx = int(rel * 4)  # 0..3
    if idx < 0:
        idx = 0
    if idx > 3:
        idx = 3

    return 1 + idx


# 🔸 Непрерывная позиция внутри/вокруг LR-канала на m5: rel = (price - lower) / (upper - lower)
def _lr_relative_position(price: float, upper: float, lower: float) -> Optional[Decimal]:
    try:
        p = float(price)
        u = float(upper)
        l = float(lower)
    except (TypeError, ValueError):
        return None

    H = u - l
    if H <= 0:
        return None

    return Decimal(str((p - l) / H))


# 🔸 Назначение бина по rules (bt_analysis_bins_dict), value=Decimal(idx)
def _assign_static_bin(rules: list[Any], value: Decimal) -> str | None:
    # условия достаточности
    if not rules:
        return None

    v = _safe_decimal(value)
    if v is None:
        return None

    last_index = len(rules) - 1

    for idx, r in enumerate(rules):
        name = getattr(r, "bin_name", None)
        lo = getattr(r, "val_from", None)
        hi = getattr(r, "val_to", None)
        to_inclusive = bool(getattr(r, "to_inclusive", False))

        if name is None or lo is None or hi is None:
            continue

        lo_d = _safe_decimal(lo)
        hi_d = _safe_decimal(hi)
        if lo_d is None or hi_d is None:
            continue

        # обычный бин: [lo, hi)
        # inclusive бин: [lo, hi] (или последний бин)
        if to_inclusive or idx == last_index:
            if lo_d <= v <= hi_d:
                return str(name)
        else:
            if lo_d <= v < hi_d:
                return str(name)

    return None


# 🔸 Назначение квантиля по adaptive rules (bt_analysis_bin_dict_adaptive, bin_type='quantiles', timeframe='mtf')
def _pick_quantile_rule(
    rules: list[Any],
    h1_idx: int,
    m15_idx: int,
    sort_key_q6: Decimal,
) -> tuple[int, str] | None:
    # условия достаточности
    if not rules:
        return None

    # отфильтруем только нужную группу по bin_order = h1*100 + m15*10 + q
    group: list[Any] = []
    for r in rules:
        try:
            bo = int(getattr(r, "bin_order"))
        except Exception:
            continue

        if (bo // 100) != int(h1_idx):
            continue
        if ((bo % 100) // 10) != int(m15_idx):
            continue

        group.append(r)

    if not group:
        return None

    # сортировка по bin_order (q1..q5)
    group.sort(key=lambda x: int(getattr(x, "bin_order")))

    last_index = len(group) - 1

    for idx, r in enumerate(group):
        try:
            bo = int(getattr(r, "bin_order"))
            q_idx = int(bo % 10)
        except Exception:
            continue

        name = getattr(r, "bin_name", None)
        lo = getattr(r, "val_from", None)
        hi = getattr(r, "val_to", None)
        to_inclusive = bool(getattr(r, "to_inclusive", False))

        if name is None or lo is None or hi is None:
            continue

        lo_d = _q6(lo)
        hi_d = _q6(hi)

        # как в анализаторе: last quantile inclusive
        if to_inclusive or idx == last_index:
            if lo_d <= sort_key_q6 <= hi_d:
                return q_idx, str(name)
        else:
            if lo_d <= sort_key_q6 < hi_d:
                return q_idx, str(name)

    return None


# 🔸 Pack-воркер LR MTF
class LrMtfPack:
    # 🔸 Конструктор
    def __init__(self):
        self.log = logging.getLogger("PACK_LR_MTF")
        self.length = 50

    # 🔸 Конфигурация длины из bt_analysis_parameters
    def configure(self, params: dict[str, Any]) -> None:
        # условия достаточности
        if not isinstance(params, dict):
            return

        try:
            if params.get("length") is not None:
                self.length = int(str(params.get("length")))
        except Exception:
            pass

    # 🔸 Конфиг MTF: какие TF читать и от чего триггериться
    def mtf_config(self, source_param_name: str) -> dict[str, Any]:
        # source_param_name ожидается как lr50_mtf / lr100_mtf
        base = f"lr{int(self.length)}"

        component_tfs = ["h1", "m15", "m5"]

        # на каждом TF нужны upper/lower
        component_params = {
            tf: {
                "upper": f"{base}_upper",
                "lower": f"{base}_lower",
            }
            for tf in component_tfs
        }

        return {
            "trigger_tf": "m5",
            "component_tfs": component_tfs,
            # триггеримся по base lr{len} (indicator_stream.indicator)
            "component_param": base,
            "component_params": component_params,
            # бины берём по компонентам (h1/m15), квантильные границы отдельно из adaptive quantiles
            "bins_tf": "components",
            # клип для LR не нужен
            "clip_0_100": False,
            # нужна цена close m5 на open_time триггера
            "needs_price": True,
            "price_tf": "m5",
            "price_field": "c",
            # какие TF требуют static bins
            "required_bins_tfs": ["h1", "m15"],
            # где искать quantiles rules в rules_by_tf
            "quantiles_key": "quantiles",
        }

    # 🔸 Вернуть список кандидатов bin_name (full → M5_0 → tail)
    def bin_candidates(self, values_by_tf: dict[str, Any], rules_by_tf: dict[str, list[Any]], direction: str) -> list[str]:
        # условия достаточности
        if not isinstance(values_by_tf, dict) or not isinstance(rules_by_tf, dict):
            return []

        dir_norm = str(direction or "").strip().lower()
        if dir_norm not in ("long", "short"):
            return []

        # ожидаем values_by_tf:
        # - "h1": {"upper":Decimal, "lower":Decimal}
        # - "m15": {"upper":Decimal, "lower":Decimal}
        # - "m5": {"upper":Decimal, "lower":Decimal}
        # - "price": Decimal (close m5)
        price_d = values_by_tf.get("price")
        if not isinstance(price_d, Decimal):
            return []

        price_f = _safe_float(price_d)
        if price_f is None:
            return []

        def _get_bounds(tf: str) -> Optional[tuple[float, float]]:
            block = values_by_tf.get(tf)
            if not isinstance(block, dict):
                return None
            up = _safe_float(block.get("upper"))
            lo = _safe_float(block.get("lower"))
            if up is None or lo is None:
                return None
            return up, lo

        h1_bounds = _get_bounds("h1")
        m15_bounds = _get_bounds("m15")
        m5_bounds = _get_bounds("m5")

        if not h1_bounds or not m15_bounds or not m5_bounds:
            return []

        h1_upper, h1_lower = h1_bounds
        m15_upper, m15_lower = m15_bounds
        m5_upper, m5_lower = m5_bounds

        # индексы H1/M15 (0..5)
        h1_idx = _lr_position_to_bin_idx(price_f, h1_upper, h1_lower)
        m15_idx = _lr_position_to_bin_idx(price_f, m15_upper, m15_lower)
        if h1_idx is None or m15_idx is None:
            return []

        # rel_m5 (q6)
        rel = _lr_relative_position(price_f, m5_upper, m5_lower)
        if rel is None:
            return []

        rel_q = _q6(rel)

        # для short инвертируем ключ сортировки как в анализаторе
        if dir_norm == "short":
            sort_key = _q6(-rel_q)
        else:
            sort_key = rel_q

        # статические бины по idx (h1/m15)
        h1_rules = rules_by_tf.get("h1") or []
        m15_rules = rules_by_tf.get("m15") or []
        if not h1_rules or not m15_rules:
            return []

        h1_bin = _assign_static_bin(h1_rules, Decimal(int(h1_idx)))
        m15_bin = _assign_static_bin(m15_rules, Decimal(int(m15_idx)))
        if not h1_bin or not m15_bin:
            return []

        # квантиль по adaptive rules
        quant_rules = rules_by_tf.get("quantiles") or []
        q_pick = _pick_quantile_rule(quant_rules, int(h1_idx), int(m15_idx), sort_key)

        # если нашли квантиль — полный кандидат с Q
        if q_pick:
            q_idx, _ = q_pick
            full = f"{h1_bin}|{m15_bin}|M5_Q{int(q_idx)}"
            return [
                full,
                f"{h1_bin}|{m15_bin}|M5_0",
                f"{h1_bin}|M15_0|M5_0",
            ]

        # если квантиль не найден — пробуем только хвосты
        return [
            f"{h1_bin}|{m15_bin}|M5_0",
            f"{h1_bin}|M15_0|M5_0",
        ]