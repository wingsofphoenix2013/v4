# bt_signals_cache_config.py — кеш фильтров live-сигналов (bad bins) по mirror (scenario_id/signal_id) для backtester_v1

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

# 🔸 Кеши и конфиг backtester_v1
from backtester_config import get_enabled_signals

log = logging.getLogger("BT_SIG_CACHE")

# 🔸 Стрим обновлений моделей (финальный postproc анализов)
ANALYSIS_POSTPROC_STREAM_KEY = "bt:analysis:postproc_ready"
CACHE_CONSUMER_GROUP = "bt_signals_cache"
CACHE_CONSUMER_NAME = "bt_signals_cache_main"

CACHE_STREAM_BATCH_SIZE = 50
CACHE_STREAM_BLOCK_MS = 5000

# 🔸 Кеш: ключ (scenario_id, signal_id, direction) -> bad bins
# required_pairs: set[(analysis_id, timeframe)]
# bad_bins_map: (analysis_id, timeframe) -> set[bin_name]
bt_mirror_required_pairs: Dict[Tuple[int, int, str], Set[Tuple[int, str]]] = {}
bt_mirror_bad_bins_map: Dict[Tuple[int, int, str], Dict[Tuple[int, str], Set[str]]] = {}

# 🔸 Индекс активных mirror-пар (чтобы не грузить лишнее)
_active_mirrors: Set[Tuple[int, int, str]] = set()


# 🔸 Публичный геттер: получить кеш bad bins для mirror (scenario_id, signal_id, direction)
def get_mirror_bad_cache(
    mirror_scenario_id: int,
    mirror_signal_id: int,
    direction: str,
) -> Tuple[Optional[Set[Tuple[int, str]]], Optional[Dict[Tuple[int, str], Set[str]]]]:
    key = (int(mirror_scenario_id), int(mirror_signal_id), str(direction).strip().lower())
    return bt_mirror_required_pairs.get(key), bt_mirror_bad_bins_map.get(key)


# 🔸 Публичный метод: пересобрать индекс активных mirrors по enabled live-инстансам
def rebuild_active_mirrors_index() -> Set[Tuple[int, int, str]]:
    mirrors: Set[Tuple[int, int, str]] = set()

    signals = get_enabled_signals()
    for s in signals:
        mode = str(s.get("mode") or "").strip().lower()
        if mode != "live":
            continue

        params = s.get("params") or {}

        # mirror params
        ms_cfg = params.get("mirror_scenario_id")
        mi_cfg = params.get("mirror_signal_id")
        if not ms_cfg or not mi_cfg:
            continue

        try:
            mirror_scenario_id = int(ms_cfg.get("value"))
            mirror_signal_id = int(mi_cfg.get("value"))
        except Exception:
            continue

        # direction берём из live инстанса (direction_mask)
        dm_cfg = params.get("direction_mask")
        if not dm_cfg:
            continue
        direction = str(dm_cfg.get("value") or "").strip().lower()
        if direction not in ("long", "short"):
            continue

        mirrors.add((mirror_scenario_id, mirror_signal_id, direction))

    global _active_mirrors
    _active_mirrors = mirrors
    return mirrors


# 🔸 Публичный метод: начальная загрузка кеша только для активных mirrors
async def load_initial_mirror_caches(pg) -> None:
    mirrors = rebuild_active_mirrors_index()

    if not mirrors:
        log.info("BT_SIG_CACHE: активных mirror-инстансов не найдено — кеш фильтров не загружается")
        return

    loaded = 0
    total_required = 0
    total_bad_bins = 0

    for (sc_id, sig_id, direction) in sorted(mirrors):
        req, bad_map = await _load_bad_bins_for_pair(pg, sc_id, sig_id, direction)
        _store_cache(sc_id, sig_id, direction, req, bad_map)
        loaded += 1
        total_required += len(req)
        total_bad_bins += sum(len(v) for v in bad_map.values())

    log.info(
        "BT_SIG_CACHE: initial cache loaded — mirrors=%s, required_pairs=%s, bad_bins=%s",
        loaded,
        total_required,
        total_bad_bins,
    )


# 🔸 Воркер обновления кеша по стриму bt:analysis:postproc_ready
async def run_bt_signals_cache_watcher(pg, redis) -> None:
    log.debug("BT_SIG_CACHE: watcher запущен (bt:analysis:postproc_ready)")

    await load_initial_mirror_caches(pg)
    await _ensure_consumer_group(redis)

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=CACHE_CONSUMER_GROUP,
                consumername=CACHE_CONSUMER_NAME,
                streams={ANALYSIS_POSTPROC_STREAM_KEY: ">"},
                count=CACHE_STREAM_BATCH_SIZE,
                block=CACHE_STREAM_BLOCK_MS,
            )

            if not entries:
                continue

            total_msgs = 0
            refreshed = 0
            ignored = 0

            for _, messages in entries:
                for msg_id, fields in messages:
                    total_msgs += 1

                    ctx = _parse_postproc_ready(fields)
                    if not ctx:
                        await redis.xack(ANALYSIS_POSTPROC_STREAM_KEY, CACHE_CONSUMER_GROUP, msg_id)
                        ignored += 1
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    source_finished_at = ctx["source_finished_at"]

                    # обновляем индекс активных mirrors (на случай добавления/отключения live-инстансов)
                    mirrors = rebuild_active_mirrors_index()

                    # нам интересно только то, что реально используется как mirror
                    targets = [
                        (scenario_id, signal_id, d)
                        for d in ("long", "short")
                        if (scenario_id, signal_id, d) in mirrors
                    ]

                    if not targets:
                        await redis.xack(ANALYSIS_POSTPROC_STREAM_KEY, CACHE_CONSUMER_GROUP, msg_id)
                        ignored += 1
                        continue

                    for (sc_id, sig_id, direction) in targets:
                        req, bad_map = await _load_bad_bins_for_pair(pg, sc_id, sig_id, direction)
                        _store_cache(sc_id, sig_id, direction, req, bad_map)
                        refreshed += 1

                        log.info(
                            "BT_SIG_CACHE: cache refreshed — mirror=%s:%s:%s, required_pairs=%s, bad_bins=%s, source_finished_at=%s",
                            sc_id,
                            sig_id,
                            direction,
                            len(req),
                            sum(len(v) for v in bad_map.values()),
                            source_finished_at,
                        )

                    await redis.xack(ANALYSIS_POSTPROC_STREAM_KEY, CACHE_CONSUMER_GROUP, msg_id)

            if total_msgs:
                log.info(
                    "BT_SIG_CACHE: batch processed — msgs=%s, refreshed=%s, ignored=%s",
                    total_msgs,
                    refreshed,
                    ignored,
                )

        except Exception as e:
            log.error("BT_SIG_CACHE: watcher loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=ANALYSIS_POSTPROC_STREAM_KEY,
            groupname=CACHE_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_SIG_CACHE: создана consumer group '%s' для стрима '%s'",
            CACHE_CONSUMER_GROUP,
            ANALYSIS_POSTPROC_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_SIG_CACHE: consumer group '%s' для стрима '%s' уже существует",
                CACHE_CONSUMER_GROUP,
                ANALYSIS_POSTPROC_STREAM_KEY,
            )
        else:
            log.error(
                "BT_SIG_CACHE: ошибка при создании consumer group '%s': %s",
                CACHE_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Парсинг сообщения postproc_ready
def _parse_postproc_ready(fields: Dict[Any, Any]) -> Optional[Dict[str, Any]]:
    try:
        # redis может вернуть bytes
        def _s(x):
            if isinstance(x, bytes):
                return x.decode("utf-8")
            return str(x)

        scenario_id = int(_s(fields.get("scenario_id")))
        signal_id = int(_s(fields.get("signal_id")))

        sf = fields.get("source_finished_at")
        source_finished_at = _s(sf) if sf is not None else None

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "source_finished_at": source_finished_at,
        }
    except Exception:
        return None


# 🔸 Загрузка bad bins из bt_analysis_bins_labels для mirror-пары и direction
async def _load_bad_bins_for_pair(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
) -> Tuple[Set[Tuple[int, str]], Dict[Tuple[int, str], Set[str]]]:
    direction_l = str(direction).strip().lower()

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT analysis_id, timeframe, bin_name
            FROM bt_analysis_bins_labels
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND direction   = $3
              AND state       = 'bad'
            """,
            int(scenario_id),
            int(signal_id),
            direction_l,
        )

    required_pairs: Set[Tuple[int, str]] = set()
    bad_bins_map: Dict[Tuple[int, str], Set[str]] = {}

    for r in rows:
        aid = int(r["analysis_id"])
        tf = str(r["timeframe"]).strip().lower()
        bn = str(r["bin_name"])

        pair = (aid, tf)
        required_pairs.add(pair)
        bad_bins_map.setdefault(pair, set()).add(bn)

    return required_pairs, bad_bins_map


# 🔸 Сохранение кеша
def _store_cache(
    scenario_id: int,
    signal_id: int,
    direction: str,
    required_pairs: Set[Tuple[int, str]],
    bad_bins_map: Dict[Tuple[int, str], Set[str]],
) -> None:
    key = (int(scenario_id), int(signal_id), str(direction).strip().lower())
    bt_mirror_required_pairs[key] = set(required_pairs)
    bt_mirror_bad_bins_map[key] = {k: set(v) for k, v in bad_bins_map.items()}