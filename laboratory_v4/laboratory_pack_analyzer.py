# 🔸 laboratory_pack_analyzer.py — анализатор PACK-комбинаций: 7d-статистика по (family/base/combokey) для WL/BL и v1–v4

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Tuple, List, Any
from collections import defaultdict

import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_PACK_ANALYZER")

# 🔸 Параметры воркера
INITIAL_DELAY_SEC = 60          # задержка перед первым запуском
INTERVAL_SEC = 3 * 60 * 60      # периодичность — раз в 3 часа

# 🔸 Допустимые доменные константы
ALLOWED_TFS = ("m5", "m15", "h1")
ALLOWED_VERSIONS = ("v1", "v2", "v3", "v4")
ALLOWED_MODES = ("mw_only", "mw_then_pack", "mw_and_pack", "pack_only")
ALLOWED_DIRS = ("long", "short")
ALLOWED_LIST_TAGS = ("whitelist", "blacklist")

# 🔸 Whitelist полей и КОМБИНАЦИЙ (solo НЕ пишем, только перечисленные combo)
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

# 🔸 Разрешённые COMBO (строки — имена полей через "|")
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


# 🔸 Публичная точка входа воркера (периодический прогон каждые 3 часа)
async def run_laboratory_pack_analyzer():
    # условия достаточности
    if infra.pg_pool is None:
        log.debug("❌ Пропуск LAB_PACK_ANALYZER: PG не инициализирован")
        return

    # стартовая задержка
    if INITIAL_DELAY_SEC > 0:
        log.debug("⏳ LAB_PACK_ANALYZER: ожидание %d сек перед первым запуском", INITIAL_DELAY_SEC)
        await asyncio.sleep(INITIAL_DELAY_SEC)

    # основной периодический цикл
    while True:
        try:
            await _run_pack_combo_analysis_once()
        except asyncio.CancelledError:
            log.debug("⏹️ LAB_PACK_ANALYZER: остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_PACK_ANALYZER: ошибка прогона — продолжаю по расписанию")
        # пауза между прогонами
        await asyncio.sleep(INTERVAL_SEC)


# 🔸 Один прогон анализа: окно 7d → TRUNCATE laboratory_pack_stat → INSERT результатов
async def _run_pack_combo_analysis_once():
    # окно 7 суток (UTC-naive)
    now = datetime.utcnow().replace(tzinfo=None)
    win_end = now
    win_start = now - timedelta(days=7)

    # загрузка депозитов клиентов (для нормировки ROI)
    deposits = await _load_client_deposits()

    # агрегаторы:
    # totals по срезу (master,ver,mode,dir,tf,list_tag) → суммарные метрики
    totals: Dict[Tuple[int, str, str, str, str, str], Dict[str, float]] = defaultdict(lambda: {
        "n_total": 0.0,
        "wins_total": 0.0,
        "pnl_norm_total": 0.0,
        "pnl_raw_total": 0.0,
    })

    # with-агрегаты по (slice, family, base, combo_key)
    with_stats: Dict[Tuple[int, str, str, str, str, str, str, str, str], Dict[str, float]] = defaultdict(lambda: {
        "n_with": 0.0,
        "wins_with": 0.0,
        "pnl_norm_with": 0.0,
        "pnl_raw_with": 0.0,
    })

    # загрузка позиций 7d
    rows = await _load_positions_7d(win_start, win_end)
    if not rows:
        log.debug("ℹ️ LAB_PACK_ANALYZER: за окно 7d нет позиций — таблица будет очищена")
        await _truncate_and_insert([])
        return

    # основная агрегация по позициям
    inserted_keys = 0  # счётчик уникальных combo-ключей (для логов)
    for r in rows:
        # извлекаем ключи среза
        master_sid = int(r["strategy_id"])
        version = str(r["oracle_version"]);        # 'v1'..'v4'
        mode = str(r["decision_mode"])            # 'mw_only' .. 'pack_only'
        direction = str(r["direction"])           # 'long' | 'short'
        tf = str(r["tf"])                         # 'm5'|'m15'|'h1'
        client_sid = int(r["client_strategy_id"] or 0)

        # фильтр на допустимые значения (страховка)
        if version not in ALLOWED_VERSIONS or mode not in ALLOWED_MODES or direction not in ALLOWED_DIRS or tf not in ALLOWED_TFS:
            continue

        pnl_raw = float(r["pnl"] or 0.0)
        dep = float(deposits.get(client_sid, 0.0) or 0.0)
        if dep <= 0.0:
            dep = 1.0
        pnl_norm = pnl_raw / dep
        is_win = 1.0 if pnl_raw > 0.0 else 0.0

        # два лист-тэга считаем раздельно
        # подготовим множества, чтобы не удвоить одну и ту же (base,combo_key) в пределах позиции
        pos_seen: Dict[str, set] = {"whitelist": set(), "blacklist": set()}

        # аккумулируем totals по обоим листам (они одинаковы по n_total и тем суммам, но считаем раздельно для простоты чтения)
        for list_tag in ALLOWED_LIST_TAGS:
            slice_key = (master_sid, version, mode, direction, tf, list_tag)
            totals[slice_key]["n_total"] += 1.0
            totals[slice_key]["wins_total"] += is_win
            totals[slice_key]["pnl_norm_total"] += pnl_norm
            totals[slice_key]["pnl_raw_total"] += pnl_raw

        # обработка WL-матчей
        wl_matches = _parse_json_list(r["pack_wl_matches"])
        if wl_matches:
            list_tag = "whitelist"
            slice_key = (master_sid, version, mode, direction, tf, list_tag)
            # набор уже учтённых combo для позиции (чтобы учесть presence один раз)
            seen = pos_seen[list_tag]

            for m in wl_matches:
                base = str(m.get("pack_base") or "").strip().lower()
                if not base:
                    continue
                family = _pack_family_from_base(base)
                if not family:
                    continue
                combo_key = str(m.get("agg_key") or "").strip()
                # разрешены только комбо из PACK_COMBOS
                allowed = PACK_COMBOS.get(family, [])
                if combo_key not in allowed:
                    continue

                combo_id = (family, base, combo_key)
                if combo_id in seen:
                    continue
                seen.add(combo_id)

                k = slice_key + combo_id  # (master,ver,mode,dir,tf,list) + (family,base,combo)
                with_stats[k]["n_with"] += 1.0
                with_stats[k]["wins_with"] += is_win
                with_stats[k]["pnl_norm_with"] += pnl_norm
                with_stats[k]["pnl_raw_with"] += pnl_raw
                inserted_keys += 1

        # обработка BL-матчей
        bl_matches = _parse_json_list(r["pack_bl_matches"])
        if bl_matches:
            list_tag = "blacklist"
            slice_key = (master_sid, version, mode, direction, tf, list_tag)
            seen = pos_seen[list_tag]

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
                if combo_id in seen:
                    continue
                seen.add(combo_id)

                k = slice_key + combo_id
                with_stats[k]["n_with"] += 1.0
                with_stats[k]["wins_with"] += is_win
                with_stats[k]["pnl_norm_with"] += pnl_norm
                with_stats[k]["pnl_raw_with"] += pnl_raw
                inserted_keys += 1

    # формирование итоговых строк для вставки
    computed_at = datetime.utcnow().replace(tzinfo=None)
    out_rows: List[Tuple[Any, ...]] = []

    for key, wstat in with_stats.items():
        # распаковываем ключ
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

        # частоты и ROI (без деления на 0)
        winrate_with = float(wins_with / n_with) if n_with > 0 else 0.0
        winrate_without = float(wins_without / n_without) if n_without > 0 else 0.0
        roi_with = float(pnl_norm_with)
        roi_without = float(pnl_norm_without)
        delta_winrate = float(winrate_with - winrate_without)
        delta_roi = float(roi_with - roi_without)
        presence_rate = float(n_with / n_total) if n_total > 0 else 0.0

        # строка для INSERT
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

    # запись в БД (truncate + bulk insert)
    await _truncate_and_insert(out_rows)

    # сводный лог
    # собираем распределение по версиям и по листам
    by_ver = defaultdict(int)
    by_list = defaultdict(int)
    for (master_sid, version, mode, direction, tf, list_tag, *_rest), _ in with_stats.items():
        by_ver[version] += 1
        by_list[list_tag] += 1

    log.debug(
        "✅ LAB_PACK_ANALYZER: записано строк=%d (unique combos=%d) — по версиям: %s; по листам: %s",
        len(out_rows), inserted_keys,
        ", ".join(f"{v}={by_ver.get(v,0)}" for v in ALLOWED_VERSIONS),
        ", ".join(f"{lt}={by_list.get(lt,0)}" for lt in ALLOWED_LIST_TAGS),
    )

# 🔸 Загрузка депозитов клиентов стратегий (только нужные клиенты)
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

# 🔸 Загрузка позиций за 7 дней (только по клиентам из выборки выше)
async def _load_positions_7d(win_start: datetime, win_end: datetime):
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
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
            """,
            win_start, win_end
        )
    return rows
    
# 🔸 TRUNCATE и массовая вставка результатов в laboratory_pack_stat
async def _truncate_and_insert(rows: List[Tuple[Any, ...]]):
    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            # чистим предыдущий срез
            await conn.execute("TRUNCATE TABLE laboratory_pack_stat")
            if not rows:
                return
            # массовая вставка
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


# 🔸 Универсальные утилиты

def _parse_json_list(val) -> List[dict]:
    # принимает json/jsonb поле: list | text | bytes → list[dict]
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
    # bb → 'bb', adx_dmi* → 'adx_dmi', иначе — алфавитный префикс до первой не-буквы
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
    return "".join(out)