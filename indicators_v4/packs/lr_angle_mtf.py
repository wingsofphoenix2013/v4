# packs/lr_angle_mtf.py — pack-воркер: LR angle MTF (H1/M15 зоны через bt_analysis_bins_dict + M5 adaptive quantiles) → кандидаты bin_name

# 🔸 Базовые импорты
import logging
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Optional


# 🔸 Константы (как в анализаторе)
ANGLE_QUANTILES = 5
DEFAULT_LR_PREFIX = "lr50"
Q6 = Decimal("0.000001")

# 🔸 Маппинг зон -> индекс 0..4 (SD/MD/FLAT/MU/SU)
ZONE_TO_BIN_IDX = {
    "SD": 0,
    "MD": 1,
    "FLAT": 2,
    "MU": 3,
    "SU": 4,
}


# 🔸 Вспомогательные функции
def _safe_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _q6(value: Any) -> Decimal:
    try:
        d = value if isinstance(value, Decimal) else Decimal(str(value))
        return d.quantize(Q6, rounding=ROUND_DOWN)
    except Exception:
        return Decimal("0").quantize(Q6, rounding=ROUND_DOWN)


# 🔸 Преобразование угла в короткий код зоны (как в анализаторе)
def _angle_to_zone(angle: Decimal) -> Optional[str]:
    try:
        val = float(angle)
    except (TypeError, InvalidOperation, ValueError):
        return None

    if val <= -0.10:
        return "SD"   # strong_down
    if -0.10 < val < -0.02:
        return "MD"   # mild_down
    if -0.02 <= val <= 0.02:
        return "FLAT"
    if 0.02 < val < 0.10:
        return "MU"   # mild_up
    if val >= 0.10:
        return "SU"   # strong_up
    return None


# 🔸 Назначение бина по static rules (bt_analysis_bins_dict), value=Decimal(idx)
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
def _pick_quantile(
    rules: list[Any],
    h1_idx: int,
    m15_idx: int,
    sort_key_q6: Decimal,
) -> Optional[int]:
    # условия достаточности
    if not rules:
        return None

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

    group.sort(key=lambda x: int(getattr(x, "bin_order")))
    last_index = len(group) - 1

    for idx, r in enumerate(group):
        try:
            bo = int(getattr(r, "bin_order"))
            q_idx = int(bo % 10)
        except Exception:
            continue

        lo = getattr(r, "val_from", None)
        hi = getattr(r, "val_to", None)
        to_inclusive = bool(getattr(r, "to_inclusive", False))

        if lo is None or hi is None:
            continue

        lo_d = _q6(lo)
        hi_d = _q6(hi)

        # last quantile inclusive
        if to_inclusive or idx == last_index:
            if lo_d <= sort_key_q6 <= hi_d:
                return q_idx
        else:
            if lo_d <= sort_key_q6 < hi_d:
                return q_idx

    return None


# 🔸 Pack-воркер LR angle MTF
class LrAngleMtfPack:
    # 🔸 Конструктор
    def __init__(self):
        self.log = logging.getLogger("PACK_LR_ANGLE_MTF")
        self.lr_prefix = DEFAULT_LR_PREFIX

    # 🔸 Конфигурация lr_prefix из bt_analysis_parameters
    def configure(self, params: dict[str, Any]) -> None:
        # условия достаточности
        if not isinstance(params, dict):
            return

        lr_prefix = params.get("lr_prefix")
        if lr_prefix:
            self.lr_prefix = str(lr_prefix).strip()
        if not self.lr_prefix:
            self.lr_prefix = DEFAULT_LR_PREFIX

    # 🔸 Конфиг MTF: какие TF читать и от чего триггериться
    def mtf_config(self, source_param_name: str) -> dict[str, Any]:
        # source_param_name ожидается как lr50_angle_mtf / lr100_angle_mtf
        angle_param = f"{self.lr_prefix}_angle"

        return {
            "trigger_tf": "m5",
            "component_tfs": ["h1", "m15", "m5"],
            # триггеримся по base lr_prefix (indicator_stream.indicator = lr50/lr100)
            "component_param": self.lr_prefix,
            # на каждом TF читаем angle
            "component_params": {
                "h1": angle_param,
                "m15": angle_param,
                "m5": angle_param,
            },
            "bins_tf": "components",
            "clip_0_100": False,
            "needs_price": False,
            "required_bins_tfs": ["h1", "m15"],
            "quantiles_key": "quantiles",
        }

    # 🔸 Вернуть список кандидатов bin_name (full → M5_0)
    def bin_candidates(self, values_by_tf: dict[str, Any], rules_by_tf: dict[str, list[Any]], direction: str) -> list[str]:
        # условия достаточности
        if not isinstance(values_by_tf, dict) or not isinstance(rules_by_tf, dict):
            return []

        dir_norm = str(direction or "").strip().lower()
        if dir_norm not in ("long", "short"):
            return []

        # angles
        a_h1 = values_by_tf.get("h1")
        a_m15 = values_by_tf.get("m15")
        a_m5 = values_by_tf.get("m5")

        if not isinstance(a_h1, Decimal) or not isinstance(a_m15, Decimal) or not isinstance(a_m5, Decimal):
            return []

        # zones for h1/m15 -> idx 0..4
        z_h1 = _angle_to_zone(a_h1)
        z_m15 = _angle_to_zone(a_m15)
        if z_h1 is None or z_m15 is None:
            return []

        h1_idx = ZONE_TO_BIN_IDX.get(z_h1)
        m15_idx = ZONE_TO_BIN_IDX.get(z_m15)
        if h1_idx is None or m15_idx is None:
            return []

        # static bins for h1/m15
        h1_rules = rules_by_tf.get("h1") or []
        m15_rules = rules_by_tf.get("m15") or []
        if not h1_rules or not m15_rules:
            return []

        h1_bin = _assign_static_bin(h1_rules, Decimal(int(h1_idx)))
        m15_bin = _assign_static_bin(m15_rules, Decimal(int(m15_idx)))
        if not h1_bin or not m15_bin:
            return []

        # angle_m5 quantile (q6)
        angle_q = _q6(a_m5)

        # for short invert sign (как в анализаторе)
        if dir_norm == "short":
            sort_key = _q6(-angle_q)
        else:
            sort_key = angle_q

        q_rules = rules_by_tf.get("quantiles") or []
        q_idx = _pick_quantile(q_rules, int(h1_idx), int(m15_idx), sort_key)

        if q_idx is not None:
            full = f"{h1_bin}|{m15_bin}|M5_Q{int(q_idx)}"
            return [
                full,
                f"{h1_bin}|{m15_bin}|M5_0",
            ]

        return [
            f"{h1_bin}|{m15_bin}|M5_0",
        ]