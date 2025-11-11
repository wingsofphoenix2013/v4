# laboratory_pack_analyzer.py — анализатор PACK-комбинаций: события all_ready → 7d-статистика WL/BL по (family/base/combo) с таргетным пересчётом

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Tuple, List, Any, Optional
from collections import defaultdict

import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_PACK_ANALYZER")

# 🔸 Параметры воркера
INITIAL_DELAY_SEC = 60
READ_COUNT = 128
READ_BLOCK_MS = 30_000

# 🔸 Триггерный стрим (финальный сигнал oracle)
TRIGGER_STREAM = "oracle:pack_lists:all_ready"
CONSUMER_GROUP = "LAB_PACK_ANALYZER_GROUP"
CONSUMER_NAME = "LAB_PACK_ANALYZER_WORKER"

# 🔸 Доменные константы
ALLOWED_TFS = ("m5", "m15", "h1")
ALLOWED_VERSIONS = ("v1", "v2", "v3", "v4", "v5")
ALLOWED_MODES = ("mw_only", "mw_then_pack", "mw_and_pack", "pack_only")
ALLOWED_DIRS = ("long", "short")
ALLOWED_LIST_TAGS = ("whitelist", "blacklist")

# 🔸 Whitelist полей и разрешённых COMBO (solo НЕ пишем, только перечисленные combo)
PACK_FIELDS: Dict[str, List[str]] = {
    "rsi":     ["bucket_low", "trend"],
    "mfi":     ["bucket_low", "trend"],
    "bb":      ["bucket", "bucket_delta", "bw_trend_smooth"],
    "lr":      ["bucket", "bucket_delta", "angle_trend"],
    "atr":     ["bucket", "bucket_delta"],
    "adx_dmi": ["adx_bucket_low", "gap_bucket_low", "adx_dynamic_smooth", "gap_dynamic_smooth"],
    "ema":     ["side", "dynamic", "dynamic_smooth"],
    "macd":    ["mode", "cross", "zero_side", "hist_bucket_low_pct", "hist_trend_smooth"],
}
PACK_COMBOS: Dict[str, List[str]] = {
    "rsi": ["bucket_low|trend"],
    "mfi": ["bucket_low|trend"],
    "atr": ["bucket|bucket_delta"],
    "bb": [
        "bucket|bucket_delta",
        "bucket|bw_trend_smooth",
        "bucket_delta|bw_trend_smooth",
        "bucket|bucket_delta|bw_trend_smooth",
    ],
    "lr": [
        "bucket|bucket_delta",
        "bucket|angle_trend",
        "bucket_delta|angle_trend",
        "bucket|bucket_delta|angle_trend",
    ],
    "adx_dmi": [
        "adx_bucket_low|adx_dynamic_smooth",
        "gap_bucket_low|gap_dynamic_smooth",
        "adx_dynamic_smooth|gap_dynamic_smooth",
    ],
    "ema": [
        "side|dynamic_smooth",
        "side|dynamic",
    ],
    "macd": [
        "mode|cross",
        "mode|hist_trend_smooth",
        "mode|hist_bucket_low_pct",
        "cross|hist_trend_smooth",
        "mode|zero_side",
    ],
}


# 🔸 Публичная точка входа воркера
async def run_laboratory_pack_analyzer():
    # условия достаточности
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск LAB_PACK_ANALYZER: PG/Redis не инициализированы")
        return

    # стартовая задержка
    if INITIAL_DELAY_SEC > 0:
        log.debug("⏳ LAB_PACK_ANALYZER: ожидание %d сек перед запуском", INITIAL_DELAY_SEC)
        await asyncio.sleep(INITIAL_DELAY_SEC)

    # полный стартовый пересчёт
    try:
        await _recompute_full()
    except Exception:
        log.exception("❌ LAB_PACK_ANALYZER: ошибка полного пересчёта на старте")

    # создание consumer group (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=TRIGGER_STREAM,
            groupname=CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug("📡 LAB_PACK_ANALYZER: создана consumer group для %s", TRIGGER_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ LAB_PACK_ANALYZER: ошибка создания consumer group")
            return

    log.debug("🚀 LAB_PACK_ANALYZER: слушаю %s", TRIGGER_STREAM)

    # основной событийный цикл
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={TRIGGER_STREAM: ">"},
                count=READ_COUNT,
                block=READ_BLOCK_MS,
            )
            if not resp:
                continue

            acks: List[str] = []
            for _, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        payload = json.loads(fields.get("data", "{}"))
                        master_sid = int(payload.get("strategy_id", 0))
                        version = str(payload.get("version", "")).lower()

                        # валидация
                        if master_sid <= 0 or version not in ALLOWED_VERSIONS:
                            log.debug("ℹ️ LAB_PACK_ANALYZER: пропуск payload=%s", payload)
                            acks.append(msg_id)
                            continue

                        # таргетный пересчёт по паре (master, version)
                        await _recompute_for_master_version(master_sid, version)
                        acks.append(msg_id)
                    except Exception:
                        log.exception("❌ LAB_PACK_ANALYZER: ошибка обработки сообщения")
                        acks.append(msg_id)

            # ACK обработанных сообщений
            if acks:
                try:
                    await infra.redis_client.xack(TRIGGER_STREAM, CONSUMER_GROUP, *acks)
                except Exception:
                    log.exception("⚠️ LAB_PACK_ANALYZER: ошибка ACK (ids=%s)", acks)

        except asyncio.CancelledError:
            log.debug("⏹️ LAB_PACK_ANALYZER: остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_PACK_ANALYZER: ошибка цикла — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Полный пересчёт (TRUNCATE → INSERT всех строк)
async def _recompute_full():
    now = datetime.utcnow().replace(tzinfo=None)
    win_start = now - timedelta(days=7)
    win_end = now

    deposits = await _load_client_deposits()
    rows = await _load_positions_7d(win_start, win_end, master_sid=None, version=None)

    out_rows = _build_pack_stats(rows, deposits)
    await _truncate_and_insert(out_rows)

    by_ver = _dist_by_version(out_rows)
    by_list = _dist_by_list(out_rows)
    log.debug(
        "✅ LAB_PACK_ANALYZER: полный пересчёт — строк=%d (unique combos=%d); versions: %s; lists: %s",
        len(out_rows), _unique_combos_count(out_rows),
        ", ".join(f"{v}={by_ver.get(v,0)}" for v in ALLOWED_VERSIONS),
        ", ".join(f"{lt}={by_list.get(lt,0)}" for lt in ALLOWED_LIST_TAGS),
    )


# 🔸 Таргетный пересчёт по (master, version): DELETE(pair) → INSERT(pair)
async def _recompute_for_master_version(master_sid: int, version: str):
    now = datetime.utcnow().replace(tzinfo=None)
    win_start = now - timedelta(days=7)
    win_end = now

    deposits = await _load_client_deposits()
    rows = await _load_positions_7d(win_start, win_end, master_sid=master_sid, version=version)

    out_rows = _build_pack_stats(rows, deposits)
    await _delete_and_insert_for_pair(master_sid, version, out_rows)

    by_list = _dist_by_list(out_rows)
    log.debug(
        "🔁 LAB_PACK_ANALYZER: таргет master=%s ver=%s — строк=%d (unique combos=%d); lists: %s",
        master_sid, version, len(out_rows), _unique_combos_count(out_rows),
        ", ".join(f"{lt}={by_list.get(lt,0)}" for lt in ALLOWED_LIST_TAGS),
    )


# 🔸 Загрузка депозитов клиентов (только подходящие дублёры)
async def _load_client_deposits() -> Dict[int, float]:
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, COALESCE(deposit,0) AS deposit
            FROM strategies_v4
            WHERE enabled = true AND (archived IS NOT TRUE)
              AND market_watcher = false
              AND blacklist_watcher = true
              AND market_mirrow IS NOT NULL
            """
        )
    return {int(r["id"]): float(r["deposit"] or 0.0) for r in rows}


# 🔸 Загрузка позиций за 7 дней (опционально таргет по master/version)
async def _load_positions_7d(win_start: datetime, win_end: datetime,
                             master_sid: Optional[int], version: Optional[str]):
    params = [win_start, win_end]
    extra_where = ""
    if master_sid is not None:
        extra_where += " AND lps.strategy_id = $3"
        params.append(int(master_sid))
        if version is not None:
            extra_where += " AND lps.oracle_version = $4"
            params.append(str(version))
    elif version is not None:
        extra_where += " AND lps.oracle_version = $3"
        params.append(str(version))

    sql = f"""
        WITH clients AS (
          SELECT id
          FROM strategies_v4
          WHERE enabled = true AND (archived IS NOT TRUE)
            AND market_watcher = false
            AND blacklist_watcher = true
            AND market_mirrow IS NOT NULL
        )
        SELECT
          lps.strategy_id,
          lps.client_strategy_id,
          lps.oracle_version,
          lps.decision_mode,
          lps.direction,
          lps.tf,
          COALESCE(lps.pnl,0) AS pnl,
          COALESCE(lps.pack_wl_matches, '[]'::jsonb) AS pack_wl_matches,
          COALESCE(lps.pack_bl_matches, '[]'::jsonb) AS pack_bl_matches
        FROM laboratory_positions_stat lps
        JOIN clients c ON c.id = lps.client_strategy_id
        WHERE lps.closed_at >= $1 AND lps.closed_at < $2
        {extra_where}
    """
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
    return rows


# 🔸 Построение 7d-статистики по позициям → строки для laboratory_pack_stat
def _build_pack_stats(rows, deposits_map: Dict[int, float]) -> List[Tuple[Any, ...]]:
    # агрегаторы totals по (slice=list_tag-разделение)
    totals: Dict[Tuple[int, str, str, str, str, str], Dict[str, float]] = defaultdict(lambda: {
        "n_total": 0.0,
        "wins_total": 0.0,
        "pnl_norm_total": 0.0,
        "pnl_raw_total": 0.0,
    })

    # агрегаторы presence по (slice + family, base, combo_key)
    with_stats: Dict[Tuple[int, str, str, str, str, str, str, str, str], Dict[str, float]] = defaultdict(lambda: {
        "n_with": 0.0,
        "wins_with": 0.0,
        "pnl_norm_with": 0.0,
        "pnl_raw_with": 0.0,
    })

    inserted_keys = 0

    for r in rows:
        master_sid = int(r["strategy_id"])
        version = str(r["oracle_version"])
        mode = str(r["decision_mode"])
        direction = str(r["direction"])
        tf = str(r["tf"])
        client_sid = int(r["client_strategy_id"] or 0)

        # фильтр доменных значений
        if version not in ALLOWED_VERSIONS or mode not in ALLOWED_MODES or direction not in ALLOWED_DIRS or tf not in ALLOWED_TFS:
            continue

        pnl_raw = float(r["pnl"] or 0.0)
        dep = float(deposits_map.get(client_sid, 0.0) or 0.0)
        if dep <= 0.0:
            dep = 1.0
        pnl_norm = pnl_raw / dep
        is_win = 1.0 if pnl_raw > 0.0 else 0.0

        # totals считаем раздельно для обеих list_tag
        for list_tag in ALLOWED_LIST_TAGS:
            slice_key = (master_sid, version, mode, direction, tf, list_tag)
            totals[slice_key]["n_total"] += 1.0
            totals[slice_key]["wins_total"] += is_win
            totals[slice_key]["pnl_norm_total"] += pnl_norm
            totals[slice_key]["pnl_raw_total"] += pnl_raw

        # dedup внутри позиции
        pos_seen: Dict[str, set] = {"whitelist": set(), "blacklist": set()}

        # WL матчи → presence
        wl_matches = _parse_json_list(r["pack_wl_matches"])
        if wl_matches:
            list_tag = "whitelist"
            for m in wl_matches:
                base = str(m.get("pack_base") or "").strip().lower()
                if not base:
                    continue
                family = _pack_family_from_base(base)
                if not family:
                    continue
                combo_key = str(m.get("agg_key") or "").strip()
                allowed = PACK_COMBOS.get(family, [])
                if combo_key not in allowed:
                    continue

                combo_id = (family, base, combo_key)
                if combo_id in pos_seen[list_tag]:
                    continue
                pos_seen[list_tag].add(combo_id)

                k = (master_sid, version, mode, direction, tf, list_tag) + combo_id
                with_stats[k]["n_with"] += 1.0
                with_stats[k]["wins_with"] += is_win
                with_stats[k]["pnl_norm_with"] += pnl_norm
                with_stats[k]["pnl_raw_with"] += pnl_raw
                inserted_keys += 1

        # BL матчи → presence
        bl_matches = _parse_json_list(r["pack_bl_matches"])
        if bl_matches:
            list_tag = "blacklist"
            for m in bl_matches:
                base = str(m.get("pack_base") or "").strip().lower()
                if not base:
                    continue
                family = _pack_family_from_base(base)
                if not family:
                    continue
                combo_key = str(m.get("agg_key") or "").strip()
                allowed = PACK_COMBOS.get(family, [])
                if combo_key not in allowed:
                    continue

                combo_id = (family, base, combo_key)
                if combo_id in pos_seen[list_tag]:
                    continue
                pos_seen[list_tag].add(combo_id)

                k = (master_sid, version, mode, direction, tf, list_tag) + combo_id
                with_stats[k]["n_with"] += 1.0
                with_stats[k]["wins_with"] += is_win
                with_stats[k]["pnl_norm_with"] += pnl_norm
                with_stats[k]["pnl_raw_with"] += pnl_raw
                inserted_keys += 1

    # формирование строк для INSERT
    computed_at = datetime.utcnow().replace(tzinfo=None)
    out_rows: List[Tuple[Any, ...]] = []

    for key, wstat in with_stats.items():
        master_sid, version, mode, direction, tf, list_tag, family, base, combo_key = key
        t = totals[(master_sid, version, mode, direction, tf, list_tag)]
        n_total = int(t["n_total"])
        if n_total <= 0:
            continue

        n_with = int(wstat["n_with"])
        n_without = max(0, n_total - n_with)

        wins_with = int(wstat["wins_with"])
        wins_total = int(t["wins_total"])
        wins_without = max(0, wins_total - wins_with)

        pnl_norm_with = float(wstat["pnl_norm_with"])
        pnl_norm_total = float(t["pnl_norm_total"])
        pnl_norm_without = float(pnl_norm_total - pnl_norm_with)

        pnl_raw_with = float(wstat["pnl_raw_with"])
        pnl_raw_total = float(t["pnl_raw_total"])
        pnl_raw_without = float(pnl_raw_total - pnl_raw_with)

        # частоты и ROI
        winrate_with = float(wins_with / n_with) if n_with > 0 else 0.0
        winrate_without = float(wins_without / n_without) if n_without > 0 else 0.0
        roi_with = float(pnl_norm_with)
        roi_without = float(pnl_norm_without)
        delta_winrate = float(winrate_with - winrate_without)
        delta_roi = float(roi_with - roi_without)
        presence_rate = float(n_with / n_total) if n_total > 0 else 0.0

        out_rows.append((
            master_sid, version, mode, direction, tf, list_tag,
            family, base, combo_key,
            n_total, n_with, n_without,
            winrate_with, winrate_without,
            roi_with, roi_without,
            pnl_raw_with, pnl_raw_without,
            delta_winrate, delta_roi, presence_rate,
            computed_at,
        ))

    return out_rows


# 🔸 TRUNCATE и массовая вставка
async def _truncate_and_insert(rows: List[Tuple[Any, ...]]):
    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("TRUNCATE TABLE laboratory_pack_stat")
            if not rows:
                return
            await conn.executemany(
                """
                INSERT INTO laboratory_pack_stat (
                  master_strategy_id, oracle_version, decision_mode, direction, tf, list_tag,
                  family, base, combo_key,
                  n_total, n_with, n_without,
                  winrate_with, winrate_without,
                  roi_with, roi_without,
                  pnl_with, pnl_without,
                  delta_winrate, delta_roi, presence_rate,
                  computed_at
                ) VALUES (
                  $1,$2,$3,$4,$5,$6,
                  $7,$8,$9,
                  $10,$11,$12,
                  $13,$14,
                  $15,$16,
                  $17,$18,
                  $19,$20,$21,
                  $22
                )
                """,
                rows
            )


# 🔸 Таргетное удаление и вставка по паре (master, version)
async def _delete_and_insert_for_pair(master_sid: int, version: str, rows: List[Tuple[Any, ...]]):
    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                DELETE FROM laboratory_pack_stat
                WHERE master_strategy_id = $1 AND oracle_version = $2
                """,
                int(master_sid), str(version)
            )
            if not rows:
                return
            await conn.executemany(
                """
                INSERT INTO laboratory_pack_stat (
                  master_strategy_id, oracle_version, decision_mode, direction, tf, list_tag,
                  family, base, combo_key,
                  n_total, n_with, n_without,
                  winrate_with, winrate_without,
                  roi_with, roi_without,
                  pnl_with, pnl_without,
                  delta_winrate, delta_roi, presence_rate,
                  computed_at
                ) VALUES (
                  $1,$2,$3,$4,$5,$6,
                  $7,$8,$9,
                  $10,$11,$12,
                  $13,$14,
                  $15,$16,
                  $17,$18,
                  $19,$20,$21,
                  $22
                )
                """,
                rows
            )


# 🔸 Вспомогательные утилиты
def _parse_json_list(val) -> List[dict]:
    if isinstance(val, list):
        return val
    if isinstance(val, (bytes, bytearray, memoryview)):
        try:
            return json.loads(bytes(val).decode("utf-8"))
        except Exception:
            return []
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return []
        try:
            return json.loads(s)
        except Exception:
            return []
    return []


def _pack_family_from_base(pack_base: str) -> str:
    s = (pack_base or "").strip().lower()
    if s.startswith("bb"):
        return "bb"
    if s.startswith("adx_dmi"):
        return "adx_dmi"
    out = []
    for ch in s:
        if ch.isalpha():
            out.append(ch)
        else:
            break
    return "".join(out) or s


def _dist_by_version(rows: List[Tuple[Any, ...]]) -> Dict[str, int]:
    d: Dict[str, int] = defaultdict(int)
    for r in rows:
        d[str(r[1])] += 1
    return d


def _dist_by_list(rows: List[Tuple[Any, ...]]) -> Dict[str, int]:
    d: Dict[str, int] = defaultdict(int)
    for r in rows:
        d[str(r[5])] += 1
    return d


def _unique_combos_count(rows: List[Tuple[Any, ...]]) -> int:
    combos = set()
    for r in rows:
        combos.add((r[6], r[7], r[8]))
    return len(combos)