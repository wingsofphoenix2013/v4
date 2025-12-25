# packs_config/db_loaders.py — загрузчики конфигурации ind_pack из PostgreSQL (packs/meta/params/rules/labels)

from __future__ import annotations

# 🔸 Imports
import json
import logging
from typing import Any

from packs_config.models import BinRule, LabelsContext

# 🔸 Константы БД (таблицы)
PACK_INSTANCES_TABLE = "indicator_pack_instances_v4"
ANALYSIS_INSTANCES_TABLE = "bt_analysis_instances"
ANALYSIS_PARAMETERS_TABLE = "bt_analysis_parameters"
BINS_DICT_TABLE = "bt_analysis_bins_dict"
ADAPTIVE_BINS_TABLE = "bt_analysis_bin_dict_adaptive"
BINS_LABELS_TABLE = "bt_analysis_bins_labels"
RUNS_TABLE = "bt_signal_backfill_runs"


# 🔸 DB loaders: packs / analyzers / params / rules / labels
async def load_enabled_packs(pg: Any) -> list[dict[str, Any]]:
    log = logging.getLogger("PACK_INIT")
    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT id, analysis_id, bins_policy, enabled_at
            FROM {PACK_INSTANCES_TABLE}
            WHERE enabled = true
        """)

    packs: list[dict[str, Any]] = []
    parsed = 0

    for r in rows:
        policy = r["bins_policy"]

        # bins_policy обычно jsonb, но оставляем совместимость со строковым форматом
        if isinstance(policy, str):
            try:
                policy = json.loads(policy)
                parsed += 1
            except Exception:
                policy = None

        packs.append({
            "id": int(r["id"]),
            "analysis_id": int(r["analysis_id"]),
            "bins_policy": policy,
            "enabled_at": r["enabled_at"],
        })

    log.info(
        "PACK_INIT: включённых pack-инстансов загружено: %s (bins_policy parsed_from_str=%s)",
        len(packs),
        parsed,
    )
    return packs


async def load_analysis_instances(pg: Any, analysis_ids: list[int]) -> dict[int, dict[str, Any]]:
    log = logging.getLogger("PACK_INIT")
    if not analysis_ids:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT id, family_key, "key", "name", enabled
            FROM {ANALYSIS_INSTANCES_TABLE}
            WHERE id = ANY($1::int[])
        """, analysis_ids)

    out: dict[int, dict[str, Any]] = {}
    for r in rows:
        out[int(r["id"])] = {
            "family_key": str(r["family_key"]),
            "key": str(r["key"]),
            "name": str(r["name"]),
            "enabled": bool(r["enabled"]),
        }

    log.info("PACK_INIT: bt_analysis_instances загружено: %s", len(out))
    return out


async def load_analysis_parameters(pg: Any, analysis_ids: list[int]) -> dict[int, dict[str, str]]:
    log = logging.getLogger("PACK_INIT")
    if not analysis_ids:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT analysis_id, param_name, param_value
            FROM {ANALYSIS_PARAMETERS_TABLE}
            WHERE analysis_id = ANY($1::int[])
        """, analysis_ids)

    params: dict[int, dict[str, str]] = {}
    for r in rows:
        aid = int(r["analysis_id"])
        params.setdefault(aid, {})[str(r["param_name"])] = str(r["param_value"])

    ok = 0
    missing = 0
    for aid in analysis_ids:
        if (params.get(aid) or {}).get("param_name"):
            ok += 1
        else:
            missing += 1

    log.info("PACK_INIT: bt_analysis_parameters (param_name) OK=%s, missing=%s", ok, missing)
    return params


async def load_static_bins_dict(pg: Any, analysis_ids: list[int]) -> dict[int, dict[str, dict[str, list[BinRule]]]]:
    log = logging.getLogger("PACK_INIT")
    if not analysis_ids:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT analysis_id, direction, timeframe, bin_type, bin_order, bin_name, val_from, val_to, to_inclusive
            FROM {BINS_DICT_TABLE}
            WHERE analysis_id = ANY($1::int[])
              AND bin_type = 'bins'
        """, analysis_ids)

    out: dict[int, dict[str, dict[str, list[BinRule]]]] = {}
    total = 0

    for r in rows:
        aid = int(r["analysis_id"])
        direction = str(r["direction"])
        tf = str(r["timeframe"])

        rule = BinRule(
            direction=direction,
            timeframe=tf,
            bin_type=str(r["bin_type"]),
            bin_order=int(r["bin_order"]),
            bin_name=str(r["bin_name"]),
            val_from=str(r["val_from"]) if r["val_from"] is not None else None,
            val_to=str(r["val_to"]) if r["val_to"] is not None else None,
            to_inclusive=bool(r["to_inclusive"]),
        )

        out.setdefault(aid, {}).setdefault(tf, {}).setdefault(direction, []).append(rule)
        total += 1

    for aid in out:
        for tf in out[aid]:
            for direction in out[aid][tf]:
                out[aid][tf][direction].sort(key=lambda x: x.bin_order)

    log.info("PACK_INIT: static bins загружено: rules=%s", total)
    return out


# 🔸 DB loaders: run registry (стартовое определение актуального run_id)
async def load_latest_finished_run_ids(pg: Any, signal_ids: list[int]) -> dict[int, int]:
    """
    Возвращает: signal_id -> run_id (самый свежий завершённый по finished_at).
    Используется для холодного старта, до прихода bt:analysis:postproc_ready.
    """
    log = logging.getLogger("PACK_INIT")
    if not signal_ids:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT DISTINCT ON (signal_id)
                   signal_id,
                   id AS run_id
            FROM {RUNS_TABLE}
            WHERE signal_id = ANY($1::int[])
              AND finished_at IS NOT NULL
              AND status <> 'running'
            ORDER BY signal_id, finished_at DESC, id DESC
        """, signal_ids)

    out: dict[int, int] = {}
    for r in rows:
        try:
            out[int(r["signal_id"])] = int(r["run_id"])
        except Exception:
            continue

    log.info("PACK_INIT: latest finished runs loaded — signals=%s, runs=%s", len(signal_ids), len(out))
    return out


# 🔸 DB loaders: adaptive bins/quantiles (run-aware)
async def load_adaptive_bins_for_pair(
    pg: Any,
    run_id: int,
    analysis_ids: list[int],
    scenario_id: int,
    signal_id: int,
    bin_type: str,
) -> dict[tuple[int, str, str], list[BinRule]]:
    # условия достаточности
    if not analysis_ids:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT analysis_id, direction, timeframe, bin_type, bin_order, bin_name, val_from, val_to, to_inclusive
            FROM {ADAPTIVE_BINS_TABLE}
            WHERE run_id      = $1
              AND analysis_id = ANY($2::int[])
              AND scenario_id = $3
              AND signal_id   = $4
              AND bin_type    = $5
            ORDER BY analysis_id, timeframe, direction, bin_order
        """, int(run_id), analysis_ids, int(scenario_id), int(signal_id), str(bin_type))

    out: dict[tuple[int, str, str], list[BinRule]] = {}
    for r in rows:
        aid = int(r["analysis_id"])
        tf = str(r["timeframe"])
        direction = str(r["direction"])

        rule = BinRule(
            direction=direction,
            timeframe=tf,
            bin_type=str(r["bin_type"]),
            bin_order=int(r["bin_order"]),
            bin_name=str(r["bin_name"]),
            val_from=str(r["val_from"]) if r["val_from"] is not None else None,
            val_to=str(r["val_to"]) if r["val_to"] is not None else None,
            to_inclusive=bool(r["to_inclusive"]),
        )

        out.setdefault((aid, tf, direction), []).append(rule)

    for k in out:
        out[k].sort(key=lambda x: x.bin_order)

    return out


# 🔸 DB loaders: labels bins (run-aware, без фильтра по state)
async def load_labels_bins_for_pair(
    pg: Any,
    run_id: int,
    scenario_id: int,
    signal_id: int,
    contexts: list[LabelsContext],
) -> dict[tuple[int, int, str, int, str, str], set[str]]:
    # условия достаточности
    if not contexts:
        return {}

    analysis_ids = sorted({c.analysis_id for c in contexts})
    indicator_params = sorted({c.indicator_param for c in contexts})
    timeframes = sorted({c.timeframe for c in contexts})

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT scenario_id, signal_id, direction, analysis_id, indicator_param, timeframe, bin_name
            FROM {BINS_LABELS_TABLE}
            WHERE run_id      = $1
              AND scenario_id = $2
              AND signal_id   = $3
              AND analysis_id = ANY($4::int[])
              AND indicator_param = ANY($5::text[])
              AND timeframe   = ANY($6::text[])
            """,
            int(run_id),
            int(scenario_id),
            int(signal_id),
            analysis_ids,
            indicator_params,
            timeframes,
        )

    out: dict[tuple[int, int, str, int, str, str], set[str]] = {}
    if not rows:
        return out

    ctx_set = {(c.analysis_id, c.indicator_param, c.timeframe) for c in contexts}
    for r in rows:
        try:
            aid = int(r["analysis_id"])
            ip = str(r["indicator_param"])
            tf = str(r["timeframe"])
            if (aid, ip, tf) not in ctx_set:
                continue

            direction = str(r["direction"] or "")
            bin_name = str(r["bin_name"] or "")
            if not direction or not bin_name:
                continue

            k = (
                int(scenario_id),
                int(signal_id),
                direction,
                aid,
                ip,
                tf,
            )
            out.setdefault(k, set()).add(bin_name)
        except Exception:
            continue

    return out