# bt_signals_cache_consensus_v2.py — кеш live-фильтров v2 по consensus=true (good bins) для mirror1/mirror2, обновление от bt:analysis:stabproc_ready_v2

import asyncio
import logging
from typing import Any, Dict, Optional, Set, Tuple, List

# 🔸 Кеши и конфиг backtester_v1
from backtester_config import get_enabled_signals

log = logging.getLogger("BT_SIG_CACHE_CONS_V2")

# 🔸 Стрим обновлений consensus v2 (после stabproc)
ANALYSIS_STABPROC_STREAM_KEY_V2 = "bt:analysis:stabproc_ready_v2"
CACHE_CONSUMER_GROUP_CONS_V2 = "bt_signals_cache_consensus_v2"
CACHE_CONSUMER_NAME_CONS_V2 = "bt_signals_cache_consensus_v2_main"

CACHE_STREAM_BATCH_SIZE = 50
CACHE_STREAM_BLOCK_MS = 5000

# 🔸 Таблица labels v2
LABELS_V2_TABLE = "bt_analysis_bins_labels_v2"
SCORE_VERSION = "v1"

# 🔸 Кеш consensus: ключ (scenario_id, signal_id, direction) -> required_pairs + good_bins_map + применяемый run_id
bt_mirror_required_pairs_cons_v2: Dict[Tuple[int, int, str], Set[Tuple[int, str]]] = {}
bt_mirror_good_bins_map_cons_v2: Dict[Tuple[int, int, str], Dict[Tuple[int, str], Set[str]]] = {}
bt_mirror_run_id_cons_v2: Dict[Tuple[int, int, str], int] = {}

# 🔸 Индекс активных mirror-пар (только live-инстансы с use_consensus=true)
_active_mirrors_cons_v2: Set[Tuple[int, int, str]] = set()


# 🔸 Публичный геттер: получить кеш good bins (consensus=true) для mirror (scenario_id, signal_id, direction)
def get_mirror_label_cache_consensus_v2(
    mirror_scenario_id: int,
    mirror_signal_id: int,
    direction: str,
) -> Tuple[
    Optional[Set[Tuple[int, str]]],
    Optional[Dict[Tuple[int, str], Set[str]]],
]:
    key = (int(mirror_scenario_id), int(mirror_signal_id), str(direction).strip().lower())
    return (
        bt_mirror_required_pairs_cons_v2.get(key),
        bt_mirror_good_bins_map_cons_v2.get(key),
    )


# 🔸 Публичный геттер: получить применяемый run_id (consensus=true) для mirror
def get_mirror_run_id_consensus_v2(
    mirror_scenario_id: int,
    mirror_signal_id: int,
    direction: str,
) -> Optional[int]:
    key = (int(mirror_scenario_id), int(mirror_signal_id), str(direction).strip().lower())
    return bt_mirror_run_id_cons_v2.get(key)


# 🔸 Публичный метод: пересобрать индекс активных mirrors (только live + use_consensus=true)
def rebuild_active_mirrors_index_consensus_v2() -> Set[Tuple[int, int, str]]:
    mirrors: Set[Tuple[int, int, str]] = set()

    signals = get_enabled_signals()
    for s in signals:
        mode = str(s.get("mode") or "").strip().lower()
        if mode != "live":
            continue

        params = s.get("params") or {}

        # use_consensus=true — иначе этот инстанс не участвует в consensus-кеше
        uc_cfg = params.get("use_consensus")
        use_consensus = str((uc_cfg or {}).get("value") or "").strip().lower() == "true"
        if not use_consensus:
            continue

        # direction берём из live инстанса (direction_mask)
        dm_cfg = params.get("direction_mask")
        if not dm_cfg:
            continue
        direction = str(dm_cfg.get("value") or "").strip().lower()
        if direction not in ("long", "short"):
            continue

        # mirror layer 1
        m1s_cfg = params.get("mirror1_scenario_id")
        m1i_cfg = params.get("mirror1_signal_id")
        if m1s_cfg and m1i_cfg:
            try:
                sc1 = int(m1s_cfg.get("value"))
                si1 = int(m1i_cfg.get("value"))
                if sc1 > 0 and si1 > 0:
                    mirrors.add((sc1, si1, direction))
            except Exception:
                pass

        # mirror layer 2
        m2s_cfg = params.get("mirror2_scenario_id")
        m2i_cfg = params.get("mirror2_signal_id")
        if m2s_cfg and m2i_cfg:
            try:
                sc2 = int(m2s_cfg.get("value"))
                si2 = int(m2i_cfg.get("value"))
                if sc2 > 0 and si2 > 0:
                    mirrors.add((sc2, si2, direction))
            except Exception:
                pass

    global _active_mirrors_cons_v2
    _active_mirrors_cons_v2 = mirrors
    return mirrors


# 🔸 Публичный метод: начальная загрузка кеша только для активных mirrors (берём последний run_id из labels_v2, consensus=true)
async def load_initial_mirror_caches_consensus_v2(pg) -> None:
    mirrors = rebuild_active_mirrors_index_consensus_v2()

    if not mirrors:
        log.info("BT_SIG_CACHE_CONS_V2: активных mirror-инстансов (use_consensus=true) не найдено — кеш не загружается")
        return

    loaded = 0
    total_required = 0
    total_good_bins = 0

    for (sc_id, sig_id, direction) in sorted(mirrors):
        run_id = await _load_latest_run_id_for_pair_consensus(pg, sc_id, sig_id, direction)
        if run_id is None:
            continue

        req, good_map = await _load_good_bins_for_pair_consensus(pg, sc_id, sig_id, direction, run_id)
        _store_cache(sc_id, sig_id, direction, req, good_map, run_id)

        loaded += 1
        total_required += len(req)
        total_good_bins += sum(len(v) for v in good_map.values())

    log.info(
        "BT_SIG_CACHE_CONS_V2: initial cache loaded — mirrors=%s, required_pairs=%s, good_bins=%s",
        loaded,
        total_required,
        total_good_bins,
    )


# 🔸 Воркер обновления кеша по стриму bt:analysis:stabproc_ready_v2 (consensus=true)
async def run_bt_signals_cache_watcher_consensus_v2(pg, redis) -> None:
    log.debug("BT_SIG_CACHE_CONS_V2: watcher запущен (bt:analysis:stabproc_ready_v2)")

    await load_initial_mirror_caches_consensus_v2(pg)
    await _ensure_consumer_group(redis)

    while True:
        try:
            try:
                entries = await redis.xreadgroup(
                    groupname=CACHE_CONSUMER_GROUP_CONS_V2,
                    consumername=CACHE_CONSUMER_NAME_CONS_V2,
                    streams={ANALYSIS_STABPROC_STREAM_KEY_V2: ">"},
                    count=CACHE_STREAM_BATCH_SIZE,
                    block=CACHE_STREAM_BLOCK_MS,
                )
            except Exception as e:
                msg = str(e)
                if "NOGROUP" in msg:
                    log.warning(
                        "BT_SIG_CACHE_CONS_V2: NOGROUP при XREADGROUP — переинициализируем группу и продолжаем",
                    )
                    await _ensure_consumer_group(redis)
                    continue
                raise

            if not entries:
                continue

            total_msgs = 0
            refreshed = 0
            ignored = 0

            for _, messages in entries:
                for msg_id, fields in messages:
                    total_msgs += 1

                    ctx = _parse_stabproc_ready_v2(fields)
                    if not ctx:
                        await redis.xack(ANALYSIS_STABPROC_STREAM_KEY_V2, CACHE_CONSUMER_GROUP_CONS_V2, msg_id)
                        ignored += 1
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    run_id = ctx["run_id"]
                    finished_at = ctx.get("finished_at")

                    # обновляем индекс активных mirrors (на случай добавления/отключения live-инстансов)
                    mirrors = rebuild_active_mirrors_index_consensus_v2()

                    # нам интересно только то, что реально используется как mirror
                    targets = [
                        (scenario_id, signal_id, d)
                        for d in ("long", "short")
                        if (scenario_id, signal_id, d) in mirrors
                    ]

                    if not targets:
                        await redis.xack(ANALYSIS_STABPROC_STREAM_KEY_V2, CACHE_CONSUMER_GROUP_CONS_V2, msg_id)
                        ignored += 1
                        continue

                    for (sc_id, sig_id, direction) in targets:
                        req, good_map = await _load_good_bins_for_pair_consensus(pg, sc_id, sig_id, direction, run_id)
                        _store_cache(sc_id, sig_id, direction, req, good_map, run_id)
                        refreshed += 1

                        log.info(
                            "BT_SIG_CACHE_CONS_V2: cache refreshed — mirror=%s:%s:%s, run_id=%s, required_pairs=%s, good_bins=%s, finished_at=%s",
                            sc_id,
                            sig_id,
                            direction,
                            run_id,
                            len(req),
                            sum(len(v) for v in good_map.values()),
                            finished_at,
                        )

                    await redis.xack(ANALYSIS_STABPROC_STREAM_KEY_V2, CACHE_CONSUMER_GROUP_CONS_V2, msg_id)

            if total_msgs:
                log.info(
                    "BT_SIG_CACHE_CONS_V2: batch processed — msgs=%s, refreshed=%s, ignored=%s",
                    total_msgs,
                    refreshed,
                    ignored,
                )

        except Exception as e:
            log.error("BT_SIG_CACHE_CONS_V2: watcher loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=ANALYSIS_STABPROC_STREAM_KEY_V2,
            groupname=CACHE_CONSUMER_GROUP_CONS_V2,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_SIG_CACHE_CONS_V2: создана consumer group '%s' для стрима '%s'",
            CACHE_CONSUMER_GROUP_CONS_V2,
            ANALYSIS_STABPROC_STREAM_KEY_V2,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.info(
                "BT_SIG_CACHE_CONS_V2: consumer group '%s' уже существует — SETID '$' для игнора истории до старта",
                CACHE_CONSUMER_GROUP_CONS_V2,
            )

            await redis.execute_command(
                "XGROUP",
                "SETID",
                ANALYSIS_STABPROC_STREAM_KEY_V2,
                CACHE_CONSUMER_GROUP_CONS_V2,
                "$",
            )

            log.debug(
                "BT_SIG_CACHE_CONS_V2: consumer group '%s' SETID='$' выполнен для '%s'",
                CACHE_CONSUMER_GROUP_CONS_V2,
                ANALYSIS_STABPROC_STREAM_KEY_V2,
            )
        else:
            log.error(
                "BT_SIG_CACHE_CONS_V2: ошибка при создании consumer group '%s': %s",
                CACHE_CONSUMER_GROUP_CONS_V2,
                e,
                exc_info=True,
            )
            raise


# 🔸 Парсинг сообщения stabproc_ready_v2 (состав полей как у postproc_ready_v2)
def _parse_stabproc_ready_v2(fields: Dict[Any, Any]) -> Optional[Dict[str, Any]]:
    try:
        # redis может вернуть bytes
        def _s(x):
            if isinstance(x, bytes):
                return x.decode("utf-8")
            return str(x)

        scenario_id = int(_s(fields.get("scenario_id")))
        signal_id = int(_s(fields.get("signal_id")))
        run_id = int(_s(fields.get("run_id")))

        fa = fields.get("finished_at")
        finished_at = _s(fa) if fa is not None else None

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "run_id": run_id,
            "finished_at": finished_at,
        }
    except Exception:
        return None


# 🔸 Загрузка последнего run_id для mirror-пары из labels_v2 (good-only + consensus=true)
async def _load_latest_run_id_for_pair_consensus(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
) -> Optional[int]:
    direction_l = str(direction).strip().lower()

    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT MAX(run_id) AS run_id
            FROM {LABELS_V2_TABLE}
            WHERE scenario_id   = $1
              AND signal_id     = $2
              AND direction     = $3
              AND score_version = $4
              AND state         = 'good'
              AND consensus     = true
            """,
            int(scenario_id),
            int(signal_id),
            direction_l,
            str(SCORE_VERSION),
        )

    if not row or row["run_id"] is None:
        return None

    try:
        return int(row["run_id"])
    except Exception:
        return None


# 🔸 Загрузка good bins из bt_analysis_bins_labels_v2 для mirror-пары и direction, строго по run_id (good + consensus=true)
async def _load_good_bins_for_pair_consensus(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
    run_id: int,
) -> Tuple[
    Set[Tuple[int, str]],
    Dict[Tuple[int, str], Set[str]],
]:
    direction_l = str(direction).strip().lower()

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT analysis_id, timeframe, bin_name
            FROM {LABELS_V2_TABLE}
            WHERE scenario_id   = $1
              AND signal_id     = $2
              AND direction     = $3
              AND run_id        = $4
              AND score_version = $5
              AND state         = 'good'
              AND consensus     = true
            """,
            int(scenario_id),
            int(signal_id),
            direction_l,
            int(run_id),
            str(SCORE_VERSION),
        )

    required_pairs: Set[Tuple[int, str]] = set()
    good_bins_map: Dict[Tuple[int, str], Set[str]] = {}

    for r in rows:
        aid = int(r["analysis_id"])
        tf = str(r["timeframe"]).strip().lower()
        bn = str(r["bin_name"])

        pair = (aid, tf)
        required_pairs.add(pair)
        good_bins_map.setdefault(pair, set()).add(bn)

    return required_pairs, good_bins_map


# 🔸 Сохранение кеша (включая применяемый run_id)
def _store_cache(
    scenario_id: int,
    signal_id: int,
    direction: str,
    required_pairs: Set[Tuple[int, str]],
    good_bins_map: Dict[Tuple[int, str], Set[str]],
    run_id: int,
) -> None:
    key = (int(scenario_id), int(signal_id), str(direction).strip().lower())
    bt_mirror_required_pairs_cons_v2[key] = set(required_pairs)
    bt_mirror_good_bins_map_cons_v2[key] = {k: set(v) for k, v in good_bins_map.items()}
    bt_mirror_run_id_cons_v2[key] = int(run_id)