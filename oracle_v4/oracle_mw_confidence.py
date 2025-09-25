# 🔸 oracle_mw_confidence.py — воркер: частоты MW (solo+combo), расчёт confidence_score, финальная публикация KV (TTL 8h)

import asyncio
import logging
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
import json
from collections import defaultdict

import infra

log = logging.getLogger("ORACLE_MW_CONFIDENCE")

# 🔸 Константы воркера / параметры исполнения
REPORT_READY_STREAM = "oracle:mw:reports_ready"   # входной стрим событий «отчёт готов»
CONF_GROUP = "mwconf_group"                       # consumer-group воркера
CONF_CONSUMER = "mwconf_1"                        # имя consumer-а (при необходимости можно брать из ENV)
FETCH_BLOCK_MS = 5000                             # блокировка XREADGROUP (мс)
BATCH_SIZE_UPDATE = 1000                          # размер батча для апдейтов в БД
FINAL_KV_TTL_SEC = 8 * 60 * 60                    # TTL финальных KV — 8 часов

# 🔸 Домены и базовые множества
TF_ORDER = ("m5", "m15", "h1")
DIRECTIONS = ("long", "short")
MW_COMPONENTS = ("trend", "volatility", "momentum", "extremes")

# 🔸 Комбо-наборы (фиксированный порядок — как в oracle_mw_snapshot)
COMBOS_2 = (
    ("trend", "volatility"),
    ("trend", "extremes"),
    ("trend", "momentum"),
    ("volatility", "extremes"),
    ("volatility", "momentum"),
    ("extremes", "momentum"),
)
COMBOS_3 = (
    ("trend", "volatility", "extremes"),
    ("trend", "volatility", "momentum"),
    ("trend", "extremes", "momentum"),
    ("volatility", "extremes", "momentum"),
)
COMBOS_4 = (tuple(MW_COMPONENTS),)


# 🔸 Типы и ключи
@dataclass
class MwasRow:
    id: int
    report_id: int
    strategy_id: int
    direction: str
    timeframe: str
    time_frame: str
    agg_type: str
    agg_base: str
    agg_state: str
    trades_total: int
    trades_wins: int
    winrate: float


# 🔸 Публичная точка запуска воркера (долгоиграющий consumer Redis Stream)
async def run_oracle_mw_confidence():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.info("❌ Пропуск запуска: PG/Redis не инициализированы")
        return

    # подготовка consumer-group (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=REPORT_READY_STREAM,
            groupname=CONF_GROUP,
            id="$",
            mkstream=True,
        )
        log.info("📡 Создан consumer-group '%s' для стрима '%s'", CONF_GROUP, REPORT_READY_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug("ℹ️ Consumer-group '%s' уже существует", CONF_GROUP)
        else:
            log.exception("❌ Ошибка создания consumer-group: %s", e)

    log.info("🚀 Старт воркера ORACLE_MW_CONFIDENCE (consumer=%s)", CONF_CONSUMER)

    # основной цикл чтения событий
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=CONF_GROUP,
                consumername=CONF_CONSUMER,
                streams={REPORT_READY_STREAM: ">"},
                count=32,
                block=FETCH_BLOCK_MS,
            )
            if not resp:
                continue

            for _, messages in resp:
                for msg_id, fields in messages:
                    # парсим payload (одно поле 'data' со строкой JSON)
                    try:
                        payload = json.loads(fields.get("data") or "{}")
                    except Exception:
                        payload = {}

                    ok = await _handle_report_ready(msg_id, payload)

                    if ok:
                        try:
                            await infra.redis_client.xack(REPORT_READY_STREAM, CONF_GROUP, msg_id)
                            await infra.redis_client.xdel(REPORT_READY_STREAM, msg_id)
                        except Exception:
                            log.exception("❌ Ошибка ACK/DEL msg_id=%s", msg_id)

        except asyncio.CancelledError:
            log.info("⏹️ Останов воркера ORACLE_MW_CONFIDENCE по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла чтения стрима; продолжаем")


# 🔸 Обработка одного события «отчёт готов»
async def _handle_report_ready(msg_id: str, payload: dict) -> bool:
    # достаточность
    if not payload or "report_id" not in payload:
        log.info("⚠️ Пропуск сообщения msg_id=%s: пустой или некорректный payload", msg_id)
        return True  # ack

    report_id = int(payload["report_id"])
    strategy_id = int(payload.get("strategy_id") or 0)
    time_frame = str(payload.get("time_frame") or "")
    window_start_iso = payload.get("window_start")
    window_end_iso = payload.get("window_end")

    try:
        async with infra.pg_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT strategy_id, time_frame, window_start, window_end FROM oracle_report_stat WHERE id = $1",
                report_id,
            )
            if not row:
                log.info("⚠️ report_id=%s не найден в oracle_report_stat — пропуск", report_id)
                return True

            if not strategy_id:
                strategy_id = int(row["strategy_id"])
            if not time_frame:
                time_frame = str(row["time_frame"])
            win_start = row["window_start"]
            win_end = row["window_end"]
            if window_start_iso:
                try:
                    win_start = datetime.fromisoformat(window_start_iso)
                except Exception:
                    pass
            if window_end_iso:
                try:
                    win_end = datetime.fromisoformat(window_end_iso)
                except Exception:
                    pass

            t0 = datetime.utcnow().replace(tzinfo=None)

            # шаг 1: загрузка каркаса агрегатов (по report_id)
            mwas_rows = await _fetch_mwas_rows(conn, report_id)
            if not mwas_rows:
                log.info("[CONF] report_id=%s: агрегатов нет — нечего считать", report_id)
                return True

            # шаг 2: построение частот (solo+combo) и маргиналей компонентов
            occ_solo, occ_combo, comp_map, denom_map = await _build_occurrence_and_components(
                conn=conn,
                strategy_id=strategy_id,
                win_start=win_start,
                win_end=win_end,
            )

            # шаг 3: UPSERT частотных таблиц (occurrence solo+combo, component marginals)
            await _upsert_occurrence(conn, report_id, time_frame, occ_solo, occ_combo, denom_map)
            await _upsert_component_counts(conn, report_id, time_frame, comp_map, denom_map)

            # шаг 4: кэш p7/p14/p28 — обновляем текущим окном
            await _update_window_winrates_cache(conn, mwas_rows, time_frame)

            # шаг 5: читаем кэш окон для ключей отчёта + строим sd-референсы по когортам (data-driven)
            cache_map = await _fetch_window_cache_map(conn, mwas_rows)
            sd_refs = _build_sd_references_from_cache(mwas_rows, cache_map)

            # шаг 6: подготовка распределений для q_scale (включая combo)
            dist_map = _build_cohort_distributions(mwas_rows, occ_solo, occ_combo, denom_map)

            # шаг 7: расчёт confidence по строкам и апдейт oracle_mw_aggregated_stat
            upd_items = _compute_confidence_items(
                mwas_rows=mwas_rows,
                occ_solo=occ_solo,
                occ_combo=occ_combo,
                denom_map=denom_map,
                comp_map=comp_map,
                cache_map=cache_map,
                sd_refs=sd_refs,
            )
            if upd_items:
                await _persist_confidence_items(conn, upd_items)

            # шаг 8: финальная публикация KV (TTL=8h)
            published = await _publish_final_kv(conn, infra.redis_client, report_id, strategy_id, time_frame)

            # лог итога
            t1 = datetime.utcnow().replace(tzinfo=None)
            zeros = sum(1 for it in upd_items if (it["confidence_score"] or 0) == 0)
            log.info(
                "[CONF_DONE] report_id=%s sid=%s win=%s rows=%d zeros=%d kv=%d elapsed_ms=%d",
                report_id, strategy_id, time_frame, len(upd_items), zeros, published, int((t1 - t0).total_seconds() * 1000),
            )

            return True

    except Exception:
        log.exception("❌ Ошибка обработки report_id=%s (msg_id=%s)", report_id, msg_id)
        return False


# 🔸 Загрузка строк oracle_mw_aggregated_stat для report_id
async def _fetch_mwas_rows(conn, report_id: int) -> List[MwasRow]:
    rows = await conn.fetch(
        """
        SELECT
            id, report_id, strategy_id, direction, timeframe, time_frame,
            agg_type, agg_base, agg_state,
            trades_total, trades_wins, winrate
        FROM oracle_mw_aggregated_stat
        WHERE report_id = $1
        """,
        report_id,
    )
    out: List[MwasRow] = []
    for r in rows:
        out.append(
            MwasRow(
                id=int(r["id"]),
                report_id=int(r["report_id"]),
                strategy_id=int(r["strategy_id"]),
                direction=str(r["direction"]),
                timeframe=str(r["timeframe"]),
                time_frame=str(r["time_frame"]),
                agg_type=str(r["agg_type"]),
                agg_base=str(r["agg_base"]),
                agg_state=str(r["agg_state"]),
                trades_total=int(r["trades_total"] or 0),
                trades_wins=int(r["trades_wins"] or 0),
                winrate=float(r["winrate"] or 0.0),
            )
        )
    return out


# 🔸 Построение частот по окну: SOLO, COMBO и маргинали компонентов
async def _build_occurrence_and_components(
    conn,
    strategy_id: int,
    win_start: datetime,
    win_end: datetime,
):
    # выбираем закрытые позиции окна (uid, direction)
    rows_pos = await conn.fetch(
        """
        SELECT position_uid, direction
          FROM positions_v4
         WHERE strategy_id = $1
           AND status = 'closed'
           AND closed_at >= $2
           AND closed_at <  $3
        """,
        strategy_id, win_start, win_end,
    )
    if not rows_pos:
        return {}, {}, {}, {}

    # мапы по uid
    uid_direction: Dict[str, str] = {str(r["position_uid"]): str(r["direction"]) for r in rows_pos}
    uids: List[str] = list(uid_direction.keys())

    # читаем все MW-строки IPS для этих позиций
    rows_ips = await conn.fetch(
        """
        SELECT position_uid, timeframe, param_base, value_text, status
          FROM indicator_position_stat
         WHERE position_uid = ANY($1::text[])
           AND param_type = 'marketwatch'
        """,
        uids,
    )

    # знаменатель: позиции, где на TF есть любые MW-строки (ok|error)
    denom_map: Dict[Tuple[str, str], set] = defaultdict(set)

    # states_ok[(direction, timeframe)][uid] = {base -> state}
    states_ok: Dict[Tuple[str, str], Dict[str, Dict[str, str]]] = defaultdict(lambda: defaultdict(dict))

    # проход по IPS
    for r in rows_ips:
        uid = str(r["position_uid"])
        tf = str(r["timeframe"])
        base = str(r["param_base"])
        status = str(r["status"])
        val = r["value_text"]
        if uid not in uid_direction:
            continue
        direction = uid_direction[uid]
        key = (direction, tf)
        denom_map[key].add(uid)
        if status == "ok" and base in MW_COMPONENTS and isinstance(val, str) and val:
            states_ok[key][uid][base] = val

    # SOLO occurrence: (direction, timeframe, base, state) -> count
    occ_solo: Dict[Tuple[str, str, str, str], int] = defaultdict(int)
    # COMBO occurrence: (direction, timeframe, agg_base, agg_state) -> count
    occ_combo: Dict[Tuple[str, str, str, str], int] = defaultdict(int)
    # Component marginals: (direction, timeframe, component, comp_state) -> count
    comp_map: Dict[Tuple[str, str, str, str], int] = defaultdict(int)

    for key in states_ok.keys():
        dir_tf_uids = denom_map.get(key, set())
        if not dir_tf_uids:
            continue
        uid_states = states_ok[key]  # {uid -> {base: state}}

        # SOLO: каждую базу/состояние считаем 1 раз на uid
        for uid, bases in uid_states.items():
            for base, state in bases.items():
                occ_solo[(key[0], key[1], base, state)] += 1
                comp_map[(key[0], key[1], base, state)] += 1

        # COMBO: формируем пересечение uid по каждой предопределённой комбинации
        # собираем для удобства по базе множество uid, где база присутствует
        inv_index: Dict[str, Dict[str, set]] = defaultdict(lambda: defaultdict(set))  # base -> state -> {uid}
        for uid, bases in uid_states.items():
            for base, state in bases.items():
                inv_index[base][state].add(uid)

        # пары
        for combo in COMBOS_2:
            # условия достаточности — все базы должны быть представлены хотя бы где-то
            if not all(b in inv_index for b in combo):
                continue
            # строим полные пары (state на каждую базу), затем их пересечение
            # перечисляем все комбинации состояний, реально встречавшихся в uid_states
            states_lists: List[List[Tuple[str, str]]] = []
            for b in combo:
                states_lists.append([(b, s) for s in inv_index[b].keys()])
            for states_tuple in _cartesian_product(states_lists):
                # пересечение по uid
                uids_sets = [inv_index[b][s] for b, s in states_tuple]
                inter = set.intersection(*uids_sets) if uids_sets else set()
                if not inter:
                    continue
                agg_base = "_".join(combo)
                agg_state = "|".join(f"{b}:{s}" for b, s in states_tuple)
                occ_combo[(key[0], key[1], agg_base, agg_state)] += len(inter)

        # тройки
        for combo in COMBOS_3:
            if not all(b in inv_index for b in combo):
                continue
            states_lists = [[(b, s) for s in inv_index[b].keys()] for b in combo]
            for states_tuple in _cartesian_product(states_lists):
                uids_sets = [inv_index[b][s] for b, s in states_tuple]
                inter = set.intersection(*uids_sets) if uids_sets else set()
                if not inter:
                    continue
                agg_base = "_".join(combo)
                agg_state = "|".join(f"{b}:{s}" for b, s in states_tuple)
                occ_combo[(key[0], key[1], agg_base, agg_state)] += len(inter)

        # четвёрки
        for combo in COMBOS_4:
            if not all(b in inv_index for b in combo):
                continue
            states_lists = [[(b, s) for s in inv_index[b].keys()] for b in combo]
            for states_tuple in _cartesian_product(states_lists):
                uids_sets = [inv_index[b][s] for b, s in states_tuple]
                inter = set.intersection(*uids_sets) if uids_sets else set()
                if not inter:
                    continue
                agg_base = "_".join(combo)
                agg_state = "|".join(f"{b}:{s}" for b, s in states_tuple)
                occ_combo[(key[0], key[1], agg_base, agg_state)] += len(inter)

    return occ_solo, occ_combo, comp_map, denom_map


# условия достаточности: декартово произведение списков пар
def _cartesian_product(lists: List[List[Tuple[str, str]]]) -> List[List[Tuple[str, str]]]:
    if not lists:
        return []
    res: List[List[Tuple[str, str]]] = [[]]
    for lst in lists:
        new_res: List[List[Tuple[str, str]]] = []
        for prefix in res:
            for item in lst:
                new_res.append(prefix + [item])
        res = new_res
    return res


# 🔸 UPSERT occurrence (solo + combo) в oracle_mw_occurrence_stat
async def _upsert_occurrence(
    conn,
    report_id: int,
    time_frame: str,
    occ_solo: Dict[Tuple[str, str, str, str], int],
    occ_combo: Dict[Tuple[str, str, str, str], int],
    denom_map: Dict[Tuple[str, str], set],
):
    # соберём все строки (solo + combo)
    records = []

    # solo
    for (direction, timeframe), uids in denom_map.items():
        total_all = len(uids)
        # для данного (dir, tf) выгружаем только соответствующие ключи
        for (d, tf, base, state), count in occ_solo.items():
            if d == direction and tf == timeframe:
                records.append((
                    report_id, direction, timeframe, time_frame,
                    "solo", base, state,
                    int(count), int(total_all),
                ))

    # combo
    for (direction, timeframe), uids in denom_map.items():
        total_all = len(uids)
        for (d, tf, agg_base, agg_state), count in occ_combo.items():
            if d == direction and tf == timeframe:
                records.append((
                    report_id, direction, timeframe, time_frame,
                    "combo", agg_base, agg_state,
                    int(count), int(total_all),
                ))

    if not records:
        return

    # раскладываем по колонкам
    cols = list(zip(*records))
    await conn.execute(
        """
        WITH data AS (
          SELECT
            unnest($1::bigint[]) AS report_id,
            unnest($2::text[])   AS direction,
            unnest($3::text[])   AS timeframe,
            unnest($4::text[])   AS time_frame,
            unnest($5::text[])   AS agg_type,
            unnest($6::text[])   AS agg_base,
            unnest($7::text[])   AS agg_state,
            unnest($8::int[])    AS positions_state,
            unnest($9::int[])    AS positions_all
        )
        INSERT INTO oracle_mw_occurrence_stat (
          report_id, strategy_id, direction, timeframe, time_frame,
          agg_type, agg_base, agg_state, positions_state, positions_all
        )
        SELECT
          d.report_id, ors.strategy_id, d.direction, d.timeframe, d.time_frame,
          d.agg_type, d.agg_base, d.agg_state, d.positions_state, d.positions_all
        FROM data d
        JOIN oracle_report_stat ors ON ors.id = d.report_id
        ON CONFLICT (report_id, strategy_id, direction, timeframe, time_frame, agg_type, agg_base, agg_state)
        DO UPDATE SET
          positions_state = EXCLUDED.positions_state,
          positions_all   = EXCLUDED.positions_all,
          updated_at      = now()
        """,
        cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6], cols[7], cols[8],
    )
    log.info("[CONF_OCC] report_id=%s rows=%d (solo+combo)", report_id, len(records))


# 🔸 UPSERT component marginals в oracle_mw_component_counts (по report_id/time_frame)
async def _upsert_component_counts(
    conn,
    report_id: int,
    time_frame: str,
    comp_map: Dict[Tuple[str, str, str, str], int],
    denom_map: Dict[Tuple[str, str], set],
):
    records = []
    for (direction, timeframe), uids in denom_map.items():
        total_all = len(uids)
        for (d, tf, comp, cstate), cnt in comp_map.items():
            if d == direction and tf == timeframe:
                records.append((
                    report_id, direction, timeframe, time_frame,
                    comp, cstate, int(cnt), int(total_all),
                ))

    if not records:
        return

    cols = list(zip(*records))
    await conn.execute(
        """
        WITH data AS (
          SELECT
            unnest($1::bigint[]) AS report_id,
            unnest($2::text[])   AS direction,
            unnest($3::text[])   AS timeframe,
            unnest($4::text[])   AS time_frame,
            unnest($5::text[])   AS component,
            unnest($6::text[])   AS comp_state,
            unnest($7::int[])    AS positions_comp,
            unnest($8::int[])    AS positions_all
        )
        INSERT INTO oracle_mw_component_counts (
          report_id, strategy_id, direction, timeframe, time_frame,
          component, comp_state, positions_comp, positions_all
        )
        SELECT
          d.report_id, ors.strategy_id, d.direction, d.timeframe, d.time_frame,
          d.component, d.comp_state, d.positions_comp, d.positions_all
        FROM data d
        JOIN oracle_report_stat ors ON ors.id = d.report_id
        ON CONFLICT (report_id, strategy_id, direction, timeframe, time_frame, component, comp_state)
        DO UPDATE SET
          positions_comp = EXCLUDED.positions_comp,
          positions_all  = EXCLUDED.positions_all,
          updated_at     = now()
        """,
        cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6], cols[7],
    )
    log.info("[CONF_COMP] report_id=%s rows=%d", report_id, len(records))


# 🔸 Обновление кэша p7/p14/p28 по ключам текущего репорта
async def _update_window_winrates_cache(conn, mwas_rows: List[MwasRow], time_frame: str):
    if not mwas_rows:
        return

    strategy_ids, directions, timeframes, agg_types, agg_bases, agg_states = [], [], [], [], [], []
    p7, p14, p28 = [], [], []

    for r in mwas_rows:
        strategy_ids.append(r.strategy_id)
        directions.append(r.direction)
        timeframes.append(r.timeframe)
        agg_types.append(r.agg_type)
        agg_bases.append(r.agg_base)
        agg_states.append(r.agg_state)
        if time_frame == "7d":
            p7.append(r.winrate); p14.append(None); p28.append(None)
        elif time_frame == "14d":
            p7.append(None); p14.append(r.winrate); p28.append(None)
        else:
            p7.append(None); p14.append(None); p28.append(r.winrate)

    await conn.execute(
        """
        WITH data AS (
          SELECT
            unnest($1::int[])  AS strategy_id,
            unnest($2::text[]) AS direction,
            unnest($3::text[]) AS timeframe,
            unnest($4::text[]) AS agg_type,
            unnest($5::text[]) AS agg_base,
            unnest($6::text[]) AS agg_state,
            unnest($7::numeric[]) AS p7_in,
            unnest($8::numeric[]) AS p14_in,
            unnest($9::numeric[]) AS p28_in
        )
        INSERT INTO oracle_mw_window_winrates_cache (
          strategy_id, direction, timeframe, agg_type, agg_base, agg_state,
          p7, p14, p28, windows_available
        )
        SELECT
          d.strategy_id, d.direction, d.timeframe, d.agg_type, d.agg_base, d.agg_state,
          d.p7_in, d.p14_in, d.p28_in,
          ((d.p7_in IS NOT NULL)::int + (d.p14_in IS NOT NULL)::int + (d.p28_in IS NOT NULL)::int)
        FROM data d
        ON CONFLICT (strategy_id, direction, timeframe, agg_type, agg_base, agg_state)
        DO UPDATE SET
          p7 = COALESCE(EXCLUDED.p7, oracle_mw_window_winrates_cache.p7),
          p14 = COALESCE(EXCLUDED.p14, oracle_mw_window_winrates_cache.p14),
          p28 = COALESCE(EXCLUDED.p28, oracle_mw_window_winrates_cache.p28),
          windows_available =
            ((COALESCE(EXCLUDED.p7, oracle_mw_window_winrates_cache.p7)  IS NOT NULL)::int +
             (COALESCE(EXCLUDED.p14, oracle_mw_window_winrates_cache.p14) IS NOT NULL)::int +
             (COALESCE(EXCLUDED.p28, oracle_mw_window_winrates_cache.p28) IS NOT NULL)::int),
          updated_at = now()
        """,
        strategy_ids, directions, timeframes, agg_types, agg_bases, agg_states, p7, p14, p28,
    )
    log.info("[CONF_WINCACHE] rows=%d updated (win=%s)", len(mwas_rows), time_frame)


# 🔸 Чтение кэша p7/p14/p28 для всех ключей текущего отчёта
async def _fetch_window_cache_map(conn, mwas_rows: List[MwasRow]) -> Dict[Tuple[int, str, str, str, str, str], Tuple[Optional[float], Optional[float], Optional[float], int]]:
    if not mwas_rows:
        return {}

    # соберём уникальные ключи
    keys = {(r.strategy_id, r.direction, r.timeframe, r.agg_type, r.agg_base, r.agg_state) for r in mwas_rows}
    if not keys:
        return {}

    # раскладываем по массивам для UNNEST JOIN
    sid, d, tf, at, ab, as_ = zip(*keys)

    rows = await conn.fetch(
        """
        WITH k AS (
          SELECT
            unnest($1::int[])  AS strategy_id,
            unnest($2::text[]) AS direction,
            unnest($3::text[]) AS timeframe,
            unnest($4::text[]) AS agg_type,
            unnest($5::text[]) AS agg_base,
            unnest($6::text[]) AS agg_state
        )
        SELECT k.strategy_id, k.direction, k.timeframe, k.agg_type, k.agg_base, k.agg_state,
               w.p7, w.p14, w.p28, w.windows_available
          FROM k
          LEFT JOIN oracle_mw_window_winrates_cache w
            ON (w.strategy_id = k.strategy_id AND w.direction = k.direction AND w.timeframe = k.timeframe
                AND w.agg_type = k.agg_type AND w.agg_base = k.agg_base AND w.agg_state = k.agg_state)
        """,
        list(sid), list(d), list(tf), list(at), list(ab), list(as_),
    )

    out: Dict[Tuple[int, str, str, str, str, str], Tuple[Optional[float], Optional[float], Optional[float], int]] = {}
    for r in rows:
        key = (int(r["strategy_id"]), str(r["direction"]), str(r["timeframe"]),
               str(r["agg_type"]), str(r["agg_base"]), str(r["agg_state"]))
        out[key] = (
            (float(r["p7"]) if r["p7"] is not None else None),
            (float(r["p14"]) if r["p14"] is not None else None),
            (float(r["p28"]) if r["p28"] is not None else None),
            int(r["windows_available"] or 0),
        )
    return out


# 🔸 SD-референсы для q_window (по когортам) из кэша окон
def _build_sd_references_from_cache(
    mwas_rows: List[MwasRow],
    cache_map: Dict[Tuple[int, str, str, str, str, str], Tuple[Optional[float], Optional[float], Optional[float], int]],
) -> Dict[Tuple[int, str, str], List[float]]:
    # cohort_key = (strategy_id, direction, timeframe)
    sd_refs: Dict[Tuple[int, str, str], List[float]] = defaultdict(list)

    # сгруппируем по когорте и посчитаем sd по доступным p7/p14/p28 каждого ключа
    for r in mwas_rows:
        k = (r.strategy_id, r.direction, r.timeframe)
        t = cache_map.get((r.strategy_id, r.direction, r.timeframe, r.agg_type, r.agg_base, r.agg_state))
        if not t:
            continue
        p7, p14, p28, _ = t
        vals = [x for x in (p7, p14, p28) if isinstance(x, (int, float))]
        if len(vals) < 2:
            continue
        mean = sum(vals) / len(vals)
        sd = math.sqrt(sum((v - mean) ** 2 for v in vals) / len(vals))
        sd_refs[k].append(sd)

    return sd_refs


# 🔸 Построение распределений долей для q_scale (по когортам) — включает SOLO и COMBO
def _build_cohort_distributions(
    mwas_rows: List[MwasRow],
    occ_solo: Dict[Tuple[str, str, str, str], int],
    occ_combo: Dict[Tuple[str, str, str, str], int],
    denom_map: Dict[Tuple[str, str], set],
) -> Dict[Tuple[int, str, str, str], List[float]]:
    dist_map: Dict[Tuple[int, str, str, str], List[float]] = defaultdict(list)
    for r in mwas_rows:
        kpos = (r.strategy_id, r.direction, r.timeframe, r.time_frame)
        N_all = len(denom_map.get((r.direction, r.timeframe), set()))
        if N_all <= 0:
            continue
        if r.agg_type == "solo":
            N_s = int(occ_solo.get((r.direction, r.timeframe, r.agg_base, r.agg_state), 0))
        else:
            N_s = int(occ_combo.get((r.direction, r.timeframe, r.agg_base, r.agg_state), 0))
        s = (N_s / N_all) if N_all > 0 else 0.0
        dist_map[kpos].append(s)
    return dist_map


# 🔸 Расчёт confidence для всех строк отчёта
def _compute_confidence_items(
    mwas_rows: List[MwasRow],
    occ_solo: Dict[Tuple[str, str, str, str], int],
    occ_combo: Dict[Tuple[str, str, str, str], int],
    denom_map: Dict[Tuple[str, str], set],
    comp_map: Dict[Tuple[str, str, str, str], int],
    cache_map: Dict[Tuple[int, str, str, str, str, str], Tuple[Optional[float], Optional[float], Optional[float], int]],
    sd_refs: Dict[Tuple[int, str, str], List[float]],
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    for r in mwas_rows:
        N_all = len(denom_map.get((r.direction, r.timeframe), set()))
        if r.agg_type == "solo":
            N_s = int(occ_solo.get((r.direction, r.timeframe, r.agg_base, r.agg_state), 0))
            p_joint = (N_s / N_all) if N_all > 0 else 0.0
            p_marginals: List[float] = []  # не используется для solo
        else:
            N_s = int(occ_combo.get((r.direction, r.timeframe, r.agg_base, r.agg_state), 0))
            p_joint = (N_s / N_all) if N_all > 0 else 0.0
            # маргинали по компонентам из comp_map
            pairs = parse_combo(r.agg_state)
            p_marginals = []
            for c, s in pairs:
                positions_comp = int(comp_map.get((r.direction, r.timeframe, c, s), 0))
                p_marginals.append((positions_comp / N_all) if N_all > 0 else 0.0)

        # q-компоненты
        q1 = compute_q_ci_result(r.trades_wins, r.trades_total)
        q2 = compute_q_ci_occurrence(N_s, N_all)

        p7, p14, p28, _wa = cache_map.get((r.strategy_id, r.direction, r.timeframe, r.agg_type, r.agg_base, r.agg_state), (None, None, None, 0))
        q3 = compute_q_window(
            p7, p14, p28,
            cohort_key=(r.strategy_id, r.direction, r.timeframe),
            sd_reference=sd_refs.get((r.strategy_id, r.direction, r.timeframe)),
        )

        # для q_scale возьмём распределение по когорте — оно строится отдельно и не передаётся сюда по памяти,
        # поскольку для расчёта индивидуального s мы уже использовали N_s/N_all и процентиль посчитаем локально:
        # однако, чтобы не тянуть dist_map ещё раз, воспользуемся эвристикой: percentile по (solo+combo) считается отдельной функцией ниже
        # здесь оставим нейтральное значение; реальный перцентиль посчитаем через compute_q_scale_percentile(...)
        # но чтобы соблюсти наш договор, пересчитаем percentile для текущего s на лету из всей когорты:
        # → соберём s_list когорты один раз вне цикла (это уже сделано в _build_cohort_distributions) и передадим сюда! (см. выше)
        # Исправление: q_scale вычислим здесь через вспомогательный киец:
        #   — чтобы не городить ещё один параметр, пересчёт делаем локально ниже, вызывающий код уже подготовил dist_map.
        # (см. правку ниже)
        pass

    # этот блок заменён более корректной версией ниже; оставлен как пояснение
    return _compute_confidence_items_with_scale(
        mwas_rows, occ_solo, occ_combo, denom_map, comp_map, cache_map, sd_refs
    )


# комментарий: финальная версия расчёта с корректным q_scale по предсобранному распределению
def _compute_confidence_items_with_scale(
    mwas_rows: List[MwasRow],
    occ_solo: Dict[Tuple[str, str, str, str], int],
    occ_combo: Dict[Tuple[str, str, str, str], int],
    denom_map: Dict[Tuple[str, str], set],
    comp_map: Dict[Tuple[str, str, str, str], int],
    cache_map: Dict[Tuple[int, str, str, str, str, str], Tuple[Optional[float], Optional[float], Optional[float], int]],
    sd_refs: Dict[Tuple[int, str, str], List[float]],
) -> List[Dict[str, Any]]:
    # соберём распределения s по когортам для q_scale
    dist_map = _build_cohort_distributions(mwas_rows, occ_solo, occ_combo, denom_map)

    items: List[Dict[str, Any]] = []
    for r in mwas_rows:
        N_all = len(denom_map.get((r.direction, r.timeframe), set()))
        if r.agg_type == "solo":
            N_s = int(occ_solo.get((r.direction, r.timeframe, r.agg_base, r.agg_state), 0))
            p_joint = (N_s / N_all) if N_all > 0 else 0.0
            p_marginals: List[float] = []
        else:
            N_s = int(occ_combo.get((r.direction, r.timeframe, r.agg_base, r.agg_state), 0))
            p_joint = (N_s / N_all) if N_all > 0 else 0.0
            pairs = parse_combo(r.agg_state)
            p_marginals = []
            for c, s in pairs:
                positions_comp = int(comp_map.get((r.direction, r.timeframe, c, s), 0))
                p_marginals.append((positions_comp / N_all) if N_all > 0 else 0.0)

        q1 = compute_q_ci_result(r.trades_wins, r.trades_total)
        q2 = compute_q_ci_occurrence(N_s, N_all)

        p7, p14, p28, _wa = cache_map.get((r.strategy_id, r.direction, r.timeframe, r.agg_type, r.agg_base, r.agg_state), (None, None, None, 0))
        q3 = compute_q_window(
            p7, p14, p28,
            cohort_key=(r.strategy_id, r.direction, r.timeframe),
            sd_reference=sd_refs.get((r.strategy_id, r.direction, r.timeframe)),
        )

        q4 = compute_q_scale(
            positions_state=N_s,
            positions_all=N_all,
            distribution_in_cohort=dist_map.get((r.strategy_id, r.direction, r.timeframe, r.time_frame), []),
        )

        q_list: List[Optional[float]] = [q1, q2, q3, q4]
        q5 = None
        if r.agg_type == "combo":
            q5 = compute_q_npmi(p_joint, p_marginals)
            q_list.append(q5)

        score = compute_confidence(q_list)

        meta = {
            "q_CI_result": q1,
            "q_CI_occurrence": q2,
            "q_window": q3,
            "q_scale": q4,
            "wins": r.trades_wins,
            "trades": r.trades_total,
            "positions_state": N_s,
            "positions_all": N_all,
            "windows_available": int((p7 is not None) + (p14 is not None) + (p28 is not None)),
        }
        if r.agg_type == "combo":
            meta["q_npmi"] = q5

        items.append({
            "id": r.id,
            "confidence_score": round(float(score), 2),
            "confidence_meta": json.dumps(meta, separators=(",", ":")),
            "complexity_level": compute_complexity_level(r.agg_type, r.agg_state),
        })

    return items


# 🔸 Сохранение рассчитанных значений confidence_* в oracle_mw_aggregated_stat
async def _persist_confidence_items(conn, items: List[Dict[str, Any]]):
    if not items:
        return
    for i in range(0, len(items), BATCH_SIZE_UPDATE):
        batch = items[i:i + BATCH_SIZE_UPDATE]
        ids = [it["id"] for it in batch]
        scores = [it["confidence_score"] for it in batch]
        metas = [it["confidence_meta"] for it in batch]
        levels = [it["complexity_level"] for it in batch]

        await conn.execute(
            """
            WITH data AS (
              SELECT
                unnest($1::bigint[]) AS id,
                unnest($2::numeric[]) AS confidence_score,
                unnest($3::jsonb[])   AS confidence_meta,
                unnest($4::int[])     AS complexity_level
            )
            UPDATE oracle_mw_aggregated_stat m
               SET confidence_score = d.confidence_score,
                   confidence_meta  = d.confidence_meta,
                   complexity_level = d.complexity_level,
                   updated_at       = now()
              FROM data d
             WHERE m.id = d.id
            """,
            ids, scores, metas, levels,
        )
    log.info("[CONF_PERSIST] updated rows=%d", len(items))


# 🔸 Финальная публикация KV с добавлением confidence_score/complexity_level (TTL 8h)
async def _publish_final_kv(conn, redis, report_id: int, strategy_id: int, time_frame: str) -> int:
    row_rep = await conn.fetchrow("SELECT closed_total FROM oracle_report_stat WHERE id = $1", report_id)
    if not row_rep:
        return 0
    closed_total = int(row_rep["closed_total"] or 0)

    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (direction, timeframe, agg_base, agg_state)
               direction, timeframe, agg_base, agg_state, trades_total, winrate,
               COALESCE(confidence_score, 0)::numeric(5,2) AS confidence_score,
               COALESCE(complexity_level, 1) AS complexity_level
          FROM oracle_mw_aggregated_stat
         WHERE report_id = $1
         ORDER BY direction, timeframe, agg_base, agg_state, updated_at DESC
        """,
        report_id,
    )
    if not rows:
        return 0

    pipe = redis.pipeline()
    published = 0
    for r in rows:
        direction = r["direction"]
        timeframe = r["timeframe"]
        agg_base = r["agg_base"]
        agg_state = r["agg_state"]
        trades_total = int(r["trades_total"] or 0)
        winrate = float(r["winrate"] or 0.0)
        confidence_score = float(r["confidence_score"] or 0.0)
        complexity_level = int(r["complexity_level"] or 1)

        key = f"oracle:mw:{strategy_id}:{direction}:{timeframe}:{agg_base}:{agg_state}:{time_frame}"
        payload = {
            "strategy_id": strategy_id,
            "direction": direction,
            "timeframe": timeframe,
            "agg_base": agg_base,
            "agg_state": agg_state,
            "time_frame": time_frame,
            "report_id": report_id,
            "closed_total": closed_total,
            "agg_trades_total": trades_total,
            "winrate": f"{winrate:.4f}",
            "confidence_score": round(confidence_score, 2),
            "complexity_level": complexity_level,
        }
        pipe.set(key, str(payload), ex=FINAL_KV_TTL_SEC)
        published += 1

    await pipe.execute()
    log.info("[CONF_KV] report_id=%s published=%d ttl=%ds", report_id, published, FINAL_KV_TTL_SEC)
    return published


# 🔸 Парсинг combo-строки состояния
def parse_combo(agg_state: str) -> List[Tuple[str, str]]:
    if not agg_state:
        return []
    pairs: List[Tuple[str, str]] = []
    parts = agg_state.split("|")
    for part in parts:
        if ":" not in part:
            continue
        comp, state = part.split(":", 1)
        comp = comp.strip()
        state = state.strip()
        if comp in MW_COMPONENTS and state:
            pairs.append((comp, state))
    return pairs


# 🔸 Вычисление complexity_level
def compute_complexity_level(agg_type: str, agg_state: str) -> int:
    if agg_type == "solo":
        return 1
    return max(1, len(parse_combo(agg_state)))


# 🔸 Компонента q_CI_result — ширина Wilson CI по результативности сделок
def compute_q_ci_result(wins: int, total: int, conf_level: float = 0.95) -> float:
    if total <= 0:
        return 0.0
    p = wins / total
    z = 1.959963984540054 if conf_level == 0.95 else 1.96
    denom = 1 + z**2 / total
    half_width = (z * math.sqrt(p * (1 - p) / total + z**2 / (4 * total**2))) / denom
    width = min(1.0, max(0.0, 2 * half_width))  # полная ширина CI
    return max(0.0, 1.0 - width)


# 🔸 Компонента q_CI_occurrence — ширина Wilson CI по встречаемости состояния
def compute_q_ci_occurrence(positions_state: int, positions_all: int, conf_level: float = 0.95) -> float:
    if positions_all <= 0:
        return 0.0
    return compute_q_ci_result(positions_state, positions_all, conf_level)


# 🔸 Компонента q_window — согласованность winrate между окнами (data-driven через CUME_DIST)
def compute_q_window(
    p7: Optional[float],
    p14: Optional[float],
    p28: Optional[float],
    cohort_key: Tuple[int, str, str],
    sd_reference: Optional[List[float]] = None,
) -> float:
    vals = [x for x in (p7, p14, p28) if isinstance(x, (int, float))]
    if len(vals) < 2:
        return 0.5  # информационно нейтрально
    mean = sum(vals) / len(vals)
    sd = math.sqrt(sum((v - mean) ** 2 for v in vals) / len(vals))
    if not sd_reference:
        # без референса — вернём нейтральное значение, не магический порог
        return 0.5
    # CUME_DIST по sd среди когорты
    sorted_ref = sorted(sd_reference)
    rank = 0
    for x in sorted_ref:
        if x <= sd:
            rank += 1
        else:
            break
    perc = rank / max(1, len(sorted_ref))
    return max(0.0, min(1.0, 1.0 - perc))


# 🔸 Компонента q_npmi — когерентность combo
def compute_q_npmi(p_joint: float, p_marginals: List[float], eps: float = 1e-12) -> float:
    p_joint = max(eps, min(1.0, p_joint))
    prod_marg = 1.0
    for x in p_marginals or [eps]:
        prod_marg *= max(eps, min(1.0, x))
    pmi = math.log(p_joint) - math.log(prod_marg)
    npmi = pmi / (-math.log(p_joint))
    return max(0.0, min(1.0, (npmi + 1.0) / 2.0))


# 🔸 Компонента q_scale — перцентиль доли состояния в когорте (CUME_DIST)
def compute_q_scale(positions_state: int, positions_all: int, distribution_in_cohort: List[float]) -> float:
    if positions_all <= 0:
        return 0.0
    s = positions_state / positions_all
    if not distribution_in_cohort:
        return 0.5
    cnt = sum(1 for x in distribution_in_cohort if x <= s)
    return max(0.0, min(1.0, cnt / len(distribution_in_cohort)))


# 🔸 Агрегирование компонент в итоговый confidence (геометрическое среднее, «жёсткая нулёвка»)
def compute_confidence(qs: List[Optional[float]]) -> float:
    vals = [q for q in qs if q is not None]
    if not vals:
        return 0.0
    if any(q <= 0.0 for q in vals):
        return 0.0
    prod = 1.0
    for q in vals:
        prod *= max(1e-12, min(1.0, float(q)))
    K = len(vals)
    return 100.0 * (prod ** (1.0 / K))