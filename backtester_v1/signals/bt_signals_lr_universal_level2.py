# bt_signals_lr_universal_level2.py — stream-backfill воркер уровня 2: читает postproc_ready_v2, загружает winner bins и логирует (без генерации сигналов)

import logging
from datetime import datetime
from typing import Dict, Any, Optional, Set, Tuple, List

# 🔸 Логгер модуля
log = logging.getLogger("BT_SIG_LR_UNI_L2")

# 🔸 Стрим-триггер v2
BT_POSTPROC_READY_STREAM_V2 = "bt:analysis:postproc_ready_v2"

# 🔸 Таблица winner bins v2
BT_LABELS_V2_TABLE = "bt_analysis_bins_labels_v2"


# 🔸 Публичная точка входа: stream-backfill сигнал (handler для STREAM_BACKFILL_HANDLERS)
async def run_lr_universal_level2_stream_backfill(
    signal: Dict[str, Any],
    msg_ctx: Dict[str, Any],
    pg,
    redis,  # оставляем для совместимости сигнатур, здесь не используется
) -> None:
    signal_id = int(signal.get("id") or 0)
    name = signal.get("name")
    timeframe = str(signal.get("timeframe") or "").strip().lower()
    params = signal.get("params") or {}

    # условия достаточности
    if signal_id <= 0 or timeframe != "m5":
        return

    # сообщение должно приходить из bt:analysis:postproc_ready_v2
    stream_key = str((msg_ctx or {}).get("stream_key") or "")
    fields = (msg_ctx or {}).get("fields") or {}
    if stream_key != BT_POSTPROC_READY_STREAM_V2:
        return

    # парсим postproc_ready_v2
    evt = _parse_postproc_ready_v2(fields)
    if not evt:
        return

    msg_scenario_id = evt["scenario_id"]
    msg_parent_signal_id = evt["signal_id"]
    run_id = evt["run_id"]
    winner_analysis_id = evt["winner_analysis_id"]
    winner_param = evt.get("winner_param") or ""
    score_version = evt.get("score_version") or "v1"

    # parent_signal_id / parent_scenario_id из параметров инстанса level2-сигнала
    parent_sig_cfg = params.get("parent_signal_id")
    parent_sc_cfg = params.get("parent_scenario_id")
    dir_cfg = params.get("direction_mask")

    try:
        configured_parent_signal_id = int((parent_sig_cfg or {}).get("value") or 0)
    except Exception:
        configured_parent_signal_id = 0

    try:
        configured_parent_scenario_id = int((parent_sc_cfg or {}).get("value") or 0)
    except Exception:
        configured_parent_scenario_id = 0

    direction = str((dir_cfg or {}).get("value") or "").strip().lower()

    # условия достаточности
    if configured_parent_signal_id <= 0 or configured_parent_scenario_id <= 0:
        return
    if direction not in ("long", "short"):
        return

    # сообщение должно относиться к нашей связке
    if msg_parent_signal_id != configured_parent_signal_id or msg_scenario_id != configured_parent_scenario_id:
        return

    parent_signal_id = configured_parent_signal_id
    scenario_id = configured_parent_scenario_id

    # загружаем свежие good bins победителя из bt_analysis_bins_labels_v2
    good_bins, timeframes = await _load_good_bins_v2(
        pg=pg,
        scenario_id=scenario_id,
        parent_signal_id=parent_signal_id,
        direction=direction,
        score_version=score_version,
        analysis_id=winner_analysis_id,
    )

    log.info(
        "BT_SIG_LR_UNI_L2: winner bins loaded — level2_signal_id=%s name='%s' parent_scenario_id=%s parent_signal_id=%s "
        "run_id=%s winner_analysis_id=%s winner_param='%s' score_version=%s dir=%s bins=%s timeframes=%s",
        signal_id,
        name,
        scenario_id,
        parent_signal_id,
        run_id,
        winner_analysis_id,
        str(winner_param),
        score_version,
        direction,
        len(good_bins),
        sorted(timeframes),
    )


# 🔸 Парсер сообщения bt:analysis:postproc_ready_v2
def _parse_postproc_ready_v2(fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id = int(str(fields.get("scenario_id") or "").strip())
        signal_id = int(str(fields.get("signal_id") or "").strip())
        run_id = int(str(fields.get("run_id") or "").strip())

        winner_analysis_id = int(str(fields.get("winner_analysis_id") or "0").strip() or 0)
        winner_param = str(fields.get("winner_param") or "").strip()
        score_version = str(fields.get("score_version") or "v1").strip()

        finished_at_raw = str(fields.get("finished_at") or "").strip()
        finished_at = datetime.fromisoformat(finished_at_raw) if finished_at_raw else None

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "run_id": run_id,
            "winner_analysis_id": winner_analysis_id,
            "winner_param": winner_param,
            "score_version": score_version,
            "finished_at": finished_at,
        }
    except Exception:
        return None


# 🔸 Загрузка whitelist good bins из bt_analysis_bins_labels_v2 (актуальный набор по паре)
async def _load_good_bins_v2(
    pg,
    scenario_id: int,
    parent_signal_id: int,
    direction: str,
    score_version: str,
    analysis_id: int,
) -> Tuple[Set[str], Set[str]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT bin_name, timeframe
            FROM {BT_LABELS_V2_TABLE}
            WHERE scenario_id   = $1
              AND signal_id     = $2
              AND direction     = $3
              AND score_version = $4
              AND analysis_id   = $5
              AND state         = 'good'
            """,
            int(scenario_id),
            int(parent_signal_id),
            str(direction),
            str(score_version),
            int(analysis_id),
        )

    bins: Set[str] = set()
    tfs: Set[str] = set()

    for r in rows:
        bn = r["bin_name"]
        tf = r["timeframe"]
        if bn is not None:
            bins.add(str(bn))
        if tf is not None:
            tfs.add(str(tf))

    return bins, tfs