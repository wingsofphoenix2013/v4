# packs_config/registry.py — сборка pack_registry и helpers (bins_policy, pairs, stream indicator key)

from __future__ import annotations

# 🔸 Imports
import logging
from typing import Any

from packs_config.models import BinRule, PackRuntime

# 🔸 Import pack workers registry (existing packs/* remain untouched)
from packs.rsi_bin import RsiBinPack
from packs.mfi_bin import MfiBinPack
from packs.adx_bin import AdxBinPack
from packs.bb_band_bin import BbBandBinPack
from packs.lr_band_bin import LrBandBinPack
from packs.lr_angle_bin import LrAngleBinPack
from packs.atr_bin import AtrBinPack
from packs.dmigap_bin import DmiGapBinPack
from packs.rsi_mtf import RsiMtfPack
from packs.mfi_mtf import MfiMtfPack
from packs.rsimfi_mtf import RsiMfiMtfPack
from packs.supertrend_mtf import SupertrendMtfPack
from packs.lr_mtf import LrMtfPack
from packs.bb_mtf import BbMtfPack
from packs.lr_angle_mtf import LrAngleMtfPack


# 🔸 Реестр доступных pack-воркеров (key берём из bt_analysis_instances.key)
PACK_WORKERS = {
    "rsi_bin": RsiBinPack,
    "mfi_bin": MfiBinPack,
    "adx_bin": AdxBinPack,
    "bb_band_bin": BbBandBinPack,
    "lr_band_bin": LrBandBinPack,
    "lr_angle_bin": LrAngleBinPack,
    "atr_bin": AtrBinPack,
    "dmigap_bin": DmiGapBinPack,
    "rsi_mtf": RsiMtfPack,
    "mfi_mtf": MfiMtfPack,
    "rsimfi_mtf": RsiMfiMtfPack,
    "supertrend_mtf": SupertrendMtfPack,
    "lr_mtf": LrMtfPack,
    "bb_mtf": BbMtfPack,
    "lr_angle_mtf": LrAngleMtfPack,
}


# 🔸 TTL по TF
TTL_BY_TF_SEC = {
    "m5": 120,      # 2 минуты
    "m15": 960,     # 16 минут
    "h1": 3660,     # 61 минута
    "mtf": 120,     # MTF-результат живёт как m5-состояние
}


# 🔸 Helpers: bins_policy parsing
def get_bins_source(bins_policy: dict[str, Any] | None, timeframe: str) -> str:
    if not isinstance(bins_policy, dict):
        return "static"
    try:
        if "by_tf" in bins_policy:
            by_tf = bins_policy.get("by_tf") or {}
            return str(by_tf.get(timeframe) or bins_policy.get("default") or "static")
        return str(bins_policy.get(timeframe) or bins_policy.get("default") or "static")
    except Exception:
        return "static"


def get_pairs(bins_policy: dict[str, Any] | None) -> list[tuple[int, int]]:
    if not isinstance(bins_policy, dict):
        return []

    raw = bins_policy.get("pairs")
    if not isinstance(raw, list):
        return []

    out: list[tuple[int, int]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        try:
            out.append((int(item.get("scenario_id")), int(item.get("signal_id"))))
        except Exception:
            continue

    seen = set()
    uniq: list[tuple[int, int]] = []
    for p in out:
        if p in seen:
            continue
        seen.add(p)
        uniq.append(p)

    return uniq


# 🔸 Приведение param_name к indicator_stream.indicator (base)
def get_stream_indicator_key(family_key: str, param_name: str) -> str:
    pname = str(param_name or "").strip()
    if not pname:
        return ""

    # если нет '_' — совпадает как есть
    if "_" not in pname:
        return pname

    # adx_dmi: base = adx_dmi{len}; параметры могут быть:
    # - adx_dmi14            -> adx_dmi14
    # - adx_dmi14_adx        -> adx_dmi14
    # - adx_dmi14_plus_di    -> adx_dmi14
    # - adx_dmi14_minus_di   -> adx_dmi14
    if family_key == "adx_dmi":
        if pname.endswith("_plus_di") or pname.endswith("_minus_di") or pname.endswith("_adx"):
            return pname.rsplit("_", 1)[0]
        return pname

    # bb20_2_0_upper -> bb20, macd12_macd_hist -> macd12, lr50_angle -> lr50, supertrend10_3_0_trend -> supertrend10
    return pname.split("_", 1)[0]


# 🔸 Registry builder
def build_pack_registry(
    packs: list[dict[str, Any]],
    analysis_meta: dict[int, dict[str, Any]],
    analysis_params: dict[int, dict[str, str]],
    static_bins_dict: dict[int, dict[str, dict[str, list[BinRule]]]],
) -> dict[tuple[str, str], list[PackRuntime]]:
    log = logging.getLogger("PACK_INIT")

    registry: dict[tuple[str, str], list[PackRuntime]] = {}
    total = 0
    cnt_static = 0
    cnt_adaptive = 0
    cnt_mtf = 0

    for pack in packs:
        analysis_id = int(pack["analysis_id"])
        meta = analysis_meta.get(analysis_id)
        params = analysis_params.get(analysis_id, {})

        if not meta:
            log.warning("PACK_INIT: analysis_id=%s пропущен: нет записи в bt_analysis_instances", analysis_id)
            continue
        if not bool(meta.get("enabled", True)):
            log.warning("PACK_INIT: analysis_id=%s пропущен: bt_analysis_instances.enabled=false", analysis_id)
            continue

        analysis_key = str(meta["key"])
        analysis_name = str(meta["name"])
        family_key = str(meta["family_key"])

        source_param_name = str(params.get("param_name") or "").strip()
        tf = str(params.get("tf") or "").strip()

        # MTF: допускаем tf отсутствует
        if not tf and (source_param_name.lower().endswith("_mtf") or analysis_key.lower().endswith("_mtf")):
            tf = "mtf"

        # условия достаточности
        if not tf or not source_param_name:
            log.warning("PACK_INIT: analysis_id=%s (%s) пропущен: нет tf/param_name", analysis_id, analysis_key)
            continue

        worker_cls = PACK_WORKERS.get(analysis_key)
        if worker_cls is None:
            log.warning("PACK_INIT: analysis_id=%s пропущен: воркер для key='%s' не найден", analysis_id, analysis_key)
            continue

        bins_policy = pack.get("bins_policy")
        bins_source = get_bins_source(bins_policy, tf)

        # MTF runtime
        is_mtf = (tf.lower() == "mtf") or source_param_name.lower().endswith("_mtf")
        if is_mtf:
            cnt_mtf += 1

            pairs = get_pairs(bins_policy)
            if not pairs:
                log.warning("PACK_INIT: analysis_id=%s (%s) mtf: bins_policy.pairs пустой — пропущен", analysis_id, analysis_key)
                continue

            worker = worker_cls()
            if hasattr(worker, "configure"):
                try:
                    worker.configure(params)
                except Exception:
                    pass

            cfg = worker.mtf_config(source_param_name) if hasattr(worker, "mtf_config") else {}
            trigger_tf = str(cfg.get("trigger_tf") or "m5")
            component_tfs = list(cfg.get("component_tfs") or [])
            component_param = str(cfg.get("component_param") or "")

            # условия достаточности
            if not component_tfs or not component_param:
                log.warning("PACK_INIT: analysis_id=%s (%s) mtf: некорректный mtf_config()", analysis_id, analysis_key)
                continue

            component_params: dict[str, Any] = {}
            comp_params_cfg = cfg.get("component_params")

            if isinstance(comp_params_cfg, dict):
                for ctf in component_tfs:
                    v = comp_params_cfg.get(ctf)
                    if isinstance(v, dict):
                        component_params[str(ctf)] = {str(k): str(val) for k, val in v.items() if k and val}
                    elif v:
                        component_params[str(ctf)] = str(v)

            for ctf in component_tfs:
                component_params.setdefault(str(ctf), component_param)

            bins_tf_key = str(cfg.get("bins_tf") or "").strip().lower() or "components"
            mtf_clip_0_100 = True if cfg.get("clip_0_100") is None else bool(cfg.get("clip_0_100"))

            mtf_bins_static: dict[str, dict[str, list[BinRule]]] = {}
            if bins_tf_key == "mtf":
                bins_mtf = static_bins_dict.get(analysis_id, {}).get("mtf", {})
                mtf_bins_static["mtf"] = {"long": bins_mtf.get("long", []), "short": bins_mtf.get("short", [])}
            else:
                for ctf in component_tfs:
                    bins_tf = static_bins_dict.get(analysis_id, {}).get(str(ctf), {})
                    mtf_bins_static[str(ctf)] = {"long": bins_tf.get("long", []), "short": bins_tf.get("short", [])}

            required_bins_tfs = cfg.get("required_bins_tfs")
            if not isinstance(required_bins_tfs, list) or not required_bins_tfs:
                required_bins_tfs = component_tfs

            quantiles_key = cfg.get("quantiles_key")
            quantiles_key = str(quantiles_key).strip() if quantiles_key is not None else None
            if quantiles_key == "":
                quantiles_key = None

            needs_price = bool(cfg.get("needs_price", False))
            price_tf = str(cfg.get("price_tf") or "m5").strip()
            price_field = str(cfg.get("price_field") or "c").strip()

            ttl_sec = int(TTL_BY_TF_SEC.get("mtf", TTL_BY_TF_SEC["m5"]))

            rt = PackRuntime(
                analysis_id=analysis_id,
                analysis_key=analysis_key,
                analysis_name=analysis_name,
                family_key=family_key,
                timeframe="mtf",
                source_param_name=source_param_name,
                bins_policy=bins_policy,
                bins_source="static",
                adaptive_pairs=[],
                bins_by_direction={"long": [], "short": []},
                ttl_sec=ttl_sec,
                worker=worker,
                is_mtf=True,
                mtf_pairs=pairs,
                mtf_trigger_tf=trigger_tf,
                mtf_component_tfs=[str(x) for x in component_tfs],
                mtf_component_params=component_params,
                mtf_bins_static=mtf_bins_static,
                mtf_bins_tf=bins_tf_key,
                mtf_clip_0_100=mtf_clip_0_100,
                mtf_required_bins_tfs=[str(x) for x in required_bins_tfs],
                mtf_quantiles_key=quantiles_key,
                mtf_needs_price=needs_price,
                mtf_price_tf=price_tf,
                mtf_price_field=price_field,
            )

            stream_indicator = get_stream_indicator_key(family_key, component_param)
            registry.setdefault((trigger_tf, stream_indicator), []).append(rt)
            total += 1
            continue

        # Single-TF runtime
        ttl_sec = int(TTL_BY_TF_SEC.get(tf, 60))
        adaptive_pairs: list[tuple[int, int]] = []
        bins_by_direction: dict[str, list[BinRule]] = {"long": [], "short": []}

        if bins_source == "adaptive":
            adaptive_pairs = get_pairs(bins_policy)
            if not adaptive_pairs:
                log.warning("PACK_INIT: analysis_id=%s (%s) adaptive: pairs пустой — пропущен", analysis_id, analysis_key)
                continue
            cnt_adaptive += 1
        else:
            bins_tf = static_bins_dict.get(analysis_id, {}).get(tf, {})
            bins_by_direction = {"long": bins_tf.get("long", []), "short": bins_tf.get("short", [])}
            cnt_static += 1

        rt = PackRuntime(
            analysis_id=analysis_id,
            analysis_key=analysis_key,
            analysis_name=analysis_name,
            family_key=family_key,
            timeframe=tf,
            source_param_name=source_param_name,
            bins_policy=bins_policy,
            bins_source=bins_source,
            adaptive_pairs=adaptive_pairs,
            bins_by_direction=bins_by_direction,
            ttl_sec=ttl_sec,
            worker=worker_cls(),
        )

        stream_indicator = get_stream_indicator_key(family_key, source_param_name)
        registry.setdefault((tf, stream_indicator), []).append(rt)
        total += 1

    log.debug(
        "PACK_INIT: registry построен — match_keys=%s, runtimes_total=%s, static=%s, adaptive=%s, mtf=%s",
        len(registry),
        total,
        cnt_static,
        cnt_adaptive,
        cnt_mtf,
    )
    return registry