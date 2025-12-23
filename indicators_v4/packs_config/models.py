# packs_config/models.py — модели данных для orchestrator'а ind_pack (rules/labels/runtime)

from __future__ import annotations

# 🔸 Imports
from dataclasses import dataclass
from typing import Any


# 🔸 Models
@dataclass(frozen=True)
class BinRule:
    direction: str
    timeframe: str
    bin_type: str
    bin_order: int
    bin_name: str
    val_from: str | None
    val_to: str | None
    to_inclusive: bool


@dataclass(frozen=True)
class LabelsContext:
    analysis_id: int
    indicator_param: str
    timeframe: str  # например "mtf"


@dataclass
class PackRuntime:
    analysis_id: int
    analysis_key: str
    analysis_name: str
    family_key: str

    timeframe: str
    source_param_name: str
    bins_policy: dict[str, Any] | None
    bins_source: str                       # "static" | "adaptive"
    adaptive_pairs: list[tuple[int, int]]  # [(scenario_id, signal_id), ...] если adaptive
    bins_by_direction: dict[str, list[BinRule]]
    ttl_sec: int
    worker: Any

    # 🔸 MTF-конфиг
    is_mtf: bool = False
    mtf_pairs: list[tuple[int, int]] | None = None
    mtf_trigger_tf: str | None = None
    mtf_component_tfs: list[str] | None = None
    mtf_component_params: dict[str, Any] | None = None
    mtf_bins_static: dict[str, dict[str, list[BinRule]]] | None = None
    mtf_bins_tf: str = "components"         # "components" | "mtf"
    mtf_clip_0_100: bool = True

    mtf_required_bins_tfs: list[str] | None = None
    mtf_quantiles_key: str | None = None
    mtf_needs_price: bool = False
    mtf_price_tf: str = "m5"
    mtf_price_field: str = "c"