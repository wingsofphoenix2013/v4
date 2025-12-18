# packs/supertrend_mtf.py — pack-воркер: Supertrend MTF (h1+m15+m5) → bin_name по mtf-словарю bt_analysis_bins_dict

# 🔸 Базовые импорты
import logging
from decimal import Decimal
from typing import Any, Optional


# 🔸 Pack-воркер Supertrend MTF
class SupertrendMtfPack:
    # 🔸 Конструктор
    def __init__(self):
        self.log = logging.getLogger("PACK_SUPERTREND_MTF")

    # 🔸 Конфиг MTF: какие TF читать и от чего триггериться
    def mtf_config(self, source_param_name: str) -> dict[str, Any]:
        # source_param_name ожидается как supertrend10_3_0_trend
        # триггеримся по base "supertrend10" (indicator_stream.indicator), оркестратор сделает split сам
        return {
            "trigger_tf": "m5",
            "component_tfs": ["h1", "m15", "m5"],
            "component_param": source_param_name,     # для матчинг через get_stream_indicator_key -> supertrend10
            "bins_tf": "mtf",                         # бины берём из bt_analysis_bins_dict.timeframe='mtf'
            "clip_0_100": False,                      # тут важен знак (+/-), клипование ломает -1
        }

    # 🔸 Вернуть список кандидатов bin_name (для supertrend только один кандидат)
    def bin_candidates(self, values_by_tf: dict[str, Any], rules_by_tf: dict[str, list[Any]], direction: str) -> list[str]:
        # условия достаточности
        if not isinstance(values_by_tf, dict) or not isinstance(rules_by_tf, dict):
            return []

        dir_norm = str(direction or "").strip().lower()
        if dir_norm not in ("long", "short"):
            return []

        # ожидаем bins rules в rules_by_tf["mtf"]
        rules = rules_by_tf.get("mtf") or []
        if not rules:
            return []

        # построим map: bin_order -> bin_name
        order_map: dict[int, str] = {}
        for r in rules:
            try:
                order = int(getattr(r, "bin_order"))
                name = getattr(r, "bin_name", None)
                if name is None:
                    continue
                order_map[order] = str(name)
            except Exception:
                continue

        if not order_map:
            return []

        # порядок TF строго как в анализаторе: h1, m15, m5
        tf_order = ["h1", "m15", "m5"]

        bits: list[int] = []
        for tf in tf_order:
            v = values_by_tf.get(tf)
            if not isinstance(v, Decimal):
                return []

            # нормализуем знак: >0 -> +1, <0 -> -1, 0 -> invalid
            if v > 0:
                st_sign = 1
            elif v < 0:
                st_sign = -1
            else:
                return []

            # long  + supertrend +1 -> 1, иначе 0
            # short + supertrend -1 -> 1, иначе 0
            if dir_norm == "long":
                bit = 1 if st_sign == 1 else 0
            else:
                bit = 1 if st_sign == -1 else 0

            bits.append(bit)

        # numeric code: [1,0,1] -> 101
        digits = "".join(str(int(b)) for b in bits)
        if not digits:
            return []
        try:
            code = int(digits)
        except Exception:
            return []

        bin_name = order_map.get(code)
        if not bin_name:
            return []

        return [bin_name]