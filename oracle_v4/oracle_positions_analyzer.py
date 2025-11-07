# oracle_positions_analyzer.py — воркер каталога MW/PACK и фиксации состояний на открытии позиций (7d окно, каждые 3 часа)

# 🔸 Импорты
import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Set

import infra

# 🔸 Импортируем «истину» по наборам и порядкам из существующих воркеров
from oracle_mw_snapshot import (
    TF_ORDER as MW_TF_ORDER,
    MW_BASES_FETCH,
    SOLO_BASES,
    COMBOS_2_ALLOWED,
    COMBOS_3_ALLOWED,
    COMBOS_4_ALLOWED,
)
from oracle_pack_snapshot import (
    TF_ORDER as PACK_TF_ORDER,
    PACK_FIELDS,
    PACK_COMBOS,
)

# 🔸 Логгер
log = logging.getLogger("ORACLE_DICT")

# 🔸 Константы воркера / параметры исполнения
INITIAL_DELAY_SEC = 60                      # первый запуск через 60 секунд
INTERVAL_SEC = 3 * 60 * 60                  # периодичность — каждые 3 часа
BATCH_SIZE = 500                            # размер батча по позициям
WINDOW_SIZE_7D = timedelta(days=7)          # окно обработки
PARALLEL_STRATEGIES = 2                     # параллельно обрабатываем не более 2 стратегий

# 🔸 Redis Streams (сигнал «старт отчётов» — отправляется после Этапа B, по стратегии целиком)
MW_REPORTS_START_STREAM = "oracle:mw:reports_start"
PACK_REPORTS_START_STREAM = "oracle:pack:reports_start"
STREAM_MAXLEN = 10_000


# 🔸 Публичная точка запуска воркера (подключается из oracle_v4_main.py → run_periodic)
async def run_oracle_positions_analyzer():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск: PG/Redis не инициализированы")
        return

    # набор стратегий (market_watcher=true)
    strategies = sorted(infra.market_watcher_strategies or [])
    if not strategies:
        log.debug("ℹ️ Стратегий с market_watcher=true нет — нечего обрабатывать")
        return

    # момент отсчёта окна
    t_ref = datetime.utcnow().replace(tzinfo=None)  # UTC-naive
    win_start = t_ref - WINDOW_SIZE_7D
    win_end = t_ref

    log.info("🚀 Старт oracle_positions_analyzer t0=%s, окно=[%s .. %s), стратегий=%d",
             t_ref.isoformat(), win_start.isoformat(), win_end.isoformat(), len(strategies))

    # параллелизм по стратегиям — не более PARALLEL_STRATEGIES одновременно
    sem = asyncio.Semaphore(PARALLEL_STRATEGIES)

    async def _guarded_process(sid: int):
        # ограничиваем степень параллелизма
        async with sem:
            try:
                async with infra.pg_pool.acquire() as conn:
                    await _process_strategy(conn, sid, win_start, win_end)
            except Exception:
                log.exception("❌ Ошибка обработки strategy_id=%s", sid)

    # запускаем задачи
    await asyncio.gather(*[ _guarded_process(sid) for sid in strategies ])

    log.info("✅ Завершён цикл oracle_positions_analyzer: стратегий обработано=%d", len(strategies))


# 🔸 Полный проход по стратегии: Этап A (каталоги) → Этап B (позиции + флаг) → стримы
async def _process_strategy(conn, strategy_id: int, win_start, win_end):
    # собираем позиции за окно
    rows_all = await conn.fetch(
        """
        SELECT position_uid, direction
          FROM positions_v4
         WHERE strategy_id = $1
           AND status = 'closed'
           AND closed_at >= $2
           AND closed_at <  $3
        """,
        strategy_id, win_start, win_end
    )
    positions_all = [dict(r) for r in rows_all]
    uid_dir_all: Dict[str, str] = {r["position_uid"]: r["direction"] for r in positions_all}

    # позиции, требующие фиксации (oracle_checked = false)
    rows_unchecked = await conn.fetch(
        """
        SELECT position_uid, direction
          FROM positions_v4
         WHERE strategy_id = $1
           AND status = 'closed'
           AND oracle_checked = false
           AND closed_at >= $2
           AND closed_at <  $3
        """,
        strategy_id, win_start, win_end
    )
    positions_unchecked = [dict(r) for r in rows_unchecked]
    uid_dir_unchecked: Dict[str, str] = {r["position_uid"]: r["direction"] for r in positions_unchecked}

    # логи на вход
    log.debug("[SID=%s] входные позиции: всего=%d, к фиксации=%d",
              strategy_id, len(positions_all), len(positions_unchecked))

    # если за окно нет позиций — ничего делать
    if not positions_all and not positions_unchecked:
        log.info("[SID=%s] окно пустое — пропуск", strategy_id)
        return

    # Этап A: пополнение словарей на основании ВСЕХ закрытых позиций окна
    mw_added_total = 0
    pack_added_total = 0

    for tf in MW_TF_ORDER:
        # MW — сбор состояний по батчам и идемпотентная вставка в словарь
        mw_entries_for_dict: Set[Tuple[str, str, str]] = set()  # (direction, agg_base, agg_state)
        for uid_batch in _iter_batches(list(uid_dir_all.keys()), BATCH_SIZE):
            dict_entries_batch, _ = await _collect_mw_states_for_batch(
                conn=conn,
                uid_list=uid_batch,
                timeframe=tf,
                uid_to_direction=uid_dir_all,
            )
            mw_entries_for_dict |= dict_entries_batch

        if mw_entries_for_dict:
            added = await _insert_mw_dict_batch(conn, strategy_id, tf, mw_entries_for_dict)
            mw_added_total += added
            log.debug("[SID=%s][MW][%s] словарь: новых=%d", strategy_id, tf, added)
        else:
            log.debug("[SID=%s][MW][%s] словарь: новых=0", strategy_id, tf)

    for tf in PACK_TF_ORDER:
        # PACK — сбор состояний по батчам и идемпотентная вставка в словарь
        pack_entries_for_dict: Set[Tuple[str, str, str, str]] = set()  # (direction, pack_base, agg_key, agg_value)
        for uid_batch in _iter_batches(list(uid_dir_all.keys()), BATCH_SIZE):
            dict_entries_batch, _ = await _collect_pack_states_for_batch(
                conn=conn,
                uid_list=uid_batch,
                timeframe=tf,
                uid_to_direction=uid_dir_all,
            )
            pack_entries_for_dict |= dict_entries_batch

        if pack_entries_for_dict:
            added = await _insert_pack_dict_batch(conn, strategy_id, tf, pack_entries_for_dict)
            pack_added_total += added
            log.debug("[SID=%s][PACK][%s] словарь: новых=%d", strategy_id, tf, added)
        else:
            log.debug("[SID=%s][PACK][%s] словарь: новых=0", strategy_id, tf)

    # Этап B: фиксация по позициям (только тем, у которых oracle_checked=false)
    captured_positions = set(uid_dir_unchecked.keys())
    mw_links_total = 0
    pack_links_total = 0

    if captured_positions:
        # MW фиксация
        for tf in MW_TF_ORDER:
            # собираем и вставляем связи
            _, mw_link_entries = await _collect_mw_states_for_batch(
                conn=conn,
                uid_list=list(captured_positions),
                timeframe=tf,
                uid_to_direction=uid_dir_unchecked,
            )
            if mw_link_entries:
                # убедимся, что словарь покрывает (на случай новых состояний)
                mw_dict_entries = {(d, b, s) for (_, d, b, s) in mw_link_entries}
                if mw_dict_entries:
                    _ = await _insert_mw_dict_batch(conn, strategy_id, tf, mw_dict_entries)
                # связываем позицию ↔ dict_id
                inserted = await _link_mw_positions_batch(conn, strategy_id, tf, mw_link_entries)
                mw_links_total += inserted
                log.debug("[SID=%s][MW][%s] связей добавлено=%d", strategy_id, tf, inserted)

        # PACK фиксация
        for tf in PACK_TF_ORDER:
            _, pack_link_entries = await _collect_pack_states_for_batch(
                conn=conn,
                uid_list=list(captured_positions),
                timeframe=tf,
                uid_to_direction=uid_dir_unchecked,
            )
            if pack_link_entries:
                # убедимся, что словарь покрывает (на случай новых состояний)
                pack_dict_entries = {(d, pb, ak, av) for (_, d, pb, ak, av) in pack_link_entries}
                if pack_dict_entries:
                    _ = await _insert_pack_dict_batch(conn, strategy_id, tf, pack_dict_entries)
                # связываем позицию ↔ dict_id
                inserted = await _link_pack_positions_batch(conn, strategy_id, tf, pack_link_entries)
                pack_links_total += inserted
                log.debug("[SID=%s][PACK][%s] связей добавлено=%d", strategy_id, tf, inserted)

        # отмечаем позиции как обработанные (oracle_checked=true)
        if captured_positions:
            await conn.execute(
                """
                UPDATE positions_v4
                   SET oracle_checked = true
                 WHERE strategy_id = $1
                   AND position_uid = ANY($2::text[])
                """,
                strategy_id, list(captured_positions)
            )

    # итоги по стратегии
    log.info(
        "[SID=%s] итог: словарь(MW+PACK) новые=%d+%d, связи добавлены MW=%d PACK=%d, позиций зафиксировано=%d",
        strategy_id, mw_added_total, pack_added_total, mw_links_total, pack_links_total, len(captured_positions)
    )

    # после полного завершения Этапа B — отправляем сигналы (только если появились новые записи)
    now_iso = datetime.utcnow().replace(tzinfo=None).isoformat()
    tf_done = ["m5", "m15", "h1"]

    if mw_added_total > 0:
        await _emit_reports_start(
            redis=infra.redis_client,
            stream=MW_REPORTS_START_STREAM,
            payload={
                "strategy_id": int(strategy_id),
                "window_start": win_start.isoformat(),
                "window_end": win_end.isoformat(),
                "processed_at": now_iso,
                "tf_done": tf_done,
                "dict_rows_added": int(mw_added_total),
                "positions_captured": int(len(captured_positions)),
            },
        )
        log.debug("[SID=%s] 📣 STREAM %s отправлен (dict_rows_added=%d)",
                 strategy_id, MW_REPORTS_START_STREAM, mw_added_total)

    if pack_added_total > 0:
        await _emit_reports_start(
            redis=infra.redis_client,
            stream=PACK_REPORTS_START_STREAM,
            payload={
                "strategy_id": int(strategy_id),
                "window_start": win_start.isoformat(),
                "window_end": win_end.isoformat(),
                "processed_at": now_iso,
                "tf_done": tf_done,
                "dict_rows_added": int(pack_added_total),
                "positions_captured": int(len(captured_positions)),
            },
        )
        log.debug("[SID=%s] 📣 STREAM %s отправлен (dict_rows_added=%d)",
                 strategy_id, PACK_REPORTS_START_STREAM, pack_added_total)


# 🔸 Итератор батчей UID
def _iter_batches(items: List[str], batch_size: int):
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


# 🔸 Сбор MW-состояний для батча UID (и для словаря, и для связей)
async def _collect_mw_states_for_batch(
    conn,
    uid_list: List[str],
    timeframe: str,
    uid_to_direction: Dict[str, str],
) -> Tuple[Set[Tuple[str, str, str]], List[Tuple[str, str, str, str]]]:
    # словарь: (direction, agg_base, agg_state)
    dict_entries: Set[Tuple[str, str, str]] = set()
    # связи: (position_uid, direction, agg_base, agg_state)
    link_entries: List[Tuple[str, str, str, str]] = []

    if not uid_list:
        return dict_entries, link_entries

    rows_mw = await conn.fetch(
        """
        WITH mw AS (
          SELECT position_uid, timeframe, param_base, value_text, status
            FROM indicator_position_stat
           WHERE position_uid = ANY($1::text[])
             AND param_type = 'marketwatch'
        )
        SELECT
          m.position_uid,
          bool_or(m.status = 'error') AS has_error,
          (jsonb_object_agg(m.param_base, m.value_text)
             FILTER (WHERE m.timeframe = $2 AND m.status = 'ok' AND m.param_base = ANY($3::text[])))::text AS states_tf
        FROM mw m
        GROUP BY m.position_uid
        """,
        uid_list, timeframe, list(MW_BASES_FETCH),
    )

    combos_2 = COMBOS_2_ALLOWED
    combos_3 = COMBOS_3_ALLOWED
    combos_4 = COMBOS_4_ALLOWED

    for r in rows_mw:
        uid = r["position_uid"]
        has_error = bool(r["has_error"])
        raw_states = r["states_tf"]

        # условия пригодности
        if has_error or not raw_states:
            continue

        # парсинг JSON → словарь баз
        if isinstance(raw_states, dict):
            states_tf = raw_states
        else:
            try:
                states_tf = json.loads(raw_states)
            except Exception:
                continue

        if not isinstance(states_tf, dict) or not states_tf:
            continue

        # фильтруем допустимые базы
        states_tf = {k: v for k, v in states_tf.items() if k in MW_BASES_FETCH and isinstance(v, str) and v}
        if not states_tf:
            continue

        direction = uid_to_direction.get(uid, "long")

        # solo — только 'trend'
        for base in SOLO_BASES:
            state = states_tf.get(base)
            if not state:
                continue
            dict_entries.add((direction, base, state))
            link_entries.append((uid, direction, base, state))

        # combos
        def _touch_combo(combo: Tuple[str, ...]):
            # проверка наличия всех баз
            for b in combo:
                if b not in states_tf:
                    return
            agg_base = "_".join(combo)
            agg_state = "|".join(f"{b}:{states_tf[b]}" for b in combo)
            dict_entries.add((direction, agg_base, agg_state))
            link_entries.append((uid, direction, agg_base, agg_state))

        for c in combos_2:
            _touch_combo(c)
        for c in combos_3:
            _touch_combo(c)
        for c in combos_4:
            _touch_combo(c)

    return dict_entries, link_entries


# 🔸 Сбор PACK-состояний для батча UID (и для словаря, и для связей)
async def _collect_pack_states_for_batch(
    conn,
    uid_list: List[str],
    timeframe: str,
    uid_to_direction: Dict[str, str],
) -> Tuple[Set[Tuple[str, str, str, str]], List[Tuple[str, str, str, str, str]]]:
    # словарь: (direction, pack_base, agg_key, agg_value)
    dict_entries: Set[Tuple[str, str, str, str]] = set()
    # связи: (position_uid, direction, pack_base, agg_key, agg_value)
    link_entries: List[Tuple[str, str, str, str, str]] = []

    if not uid_list:
        return dict_entries, link_entries

    # обходим семейства PACK в том же разрезе, что в oracle_pack_snapshot.py
    for family, field_list in PACK_FIELDS.items():
        combos = PACK_COMBOS.get(family, [])
        if not combos:
            continue

        rows_pack = await conn.fetch(
            """
            SELECT position_uid, param_base, param_name, value_num, value_text, status
              FROM indicator_position_stat
             WHERE position_uid = ANY($1::text[])
               AND param_type = 'pack'
               AND timeframe = $2
               AND param_base LIKE $3
               AND param_name = ANY($4::text[])
            """,
            uid_list, timeframe, f"{family}%", field_list,
        )

        # группировка значений по uid → base → {field: value}
        by_uid: Dict[str, Dict[str, Dict[str, str]]] = {}
        bad: Dict[str, set] = {}

        for r in rows_pack:
            uid = r["position_uid"]
            base = r["param_base"]
            status = r["status"]

            if status != "ok":
                bad.setdefault(uid, set()).add(base)
                continue

            name = r["param_name"]
            # нормализация числового/текстового значения (строго как в snapshot)
            val = str(r["value_text"]) if r["value_text"] is not None else (
                f"{float(r['value_num'] or 0.0):.8f}".rstrip('0').rstrip('.')
            )
            by_uid.setdefault(uid, {}).setdefault(base, {})[name] = val

        # генерация комбо
        for uid, base_map in by_uid.items():
            direction = uid_to_direction.get(uid, "long")
            for base, fields in base_map.items():
                if base in bad.get(uid, set()):
                    continue
                for combo_str in combos:
                    parts = combo_str.split("|")
                    # наличие всех полей в этом base
                    if not all(f in fields for f in parts):
                        continue
                    agg_key = combo_str
                    agg_value = "|".join(f"{f}:{fields[f]}" for f in parts)

                    dict_entries.add((direction, base, agg_key, agg_value))
                    link_entries.append((uid, direction, base, agg_key, agg_value))

    return dict_entries, link_entries


# 🔸 Идемпотентная пачечная вставка в oracle_mw_dict, возврат числа новых строк
async def _insert_mw_dict_batch(
    conn,
    strategy_id: int,
    timeframe: str,
    entries: Set[Tuple[str, str, str]],  # (direction, agg_base, agg_state)
) -> int:
    if not entries:
        return 0

    directions, agg_bases, agg_states = zip(*entries)
    rows = await conn.fetch(
        """
        WITH data AS (
          SELECT
            unnest($1::text[]) AS direction,
            unnest($2::text[]) AS agg_base,
            unnest($3::text[]) AS agg_state
        )
        INSERT INTO oracle_mw_dict (strategy_id, timeframe, direction, agg_base, agg_state)
        SELECT $4::int, $5::text, d.direction, d.agg_base, d.agg_state
          FROM data d
        ON CONFLICT (strategy_id, timeframe, direction, agg_base, agg_state)
        DO NOTHING
        RETURNING id
        """,
        list(directions), list(agg_bases), list(agg_states),
        int(strategy_id), str(timeframe),
    )
    return len(rows or [])


# 🔸 Идемпотентная пачечная вставка в oracle_pack_dict, возврат числа новых строк
async def _insert_pack_dict_batch(
    conn,
    strategy_id: int,
    timeframe: str,
    entries: Set[Tuple[str, str, str, str]],  # (direction, pack_base, agg_key, agg_value)
) -> int:
    if not entries:
        return 0

    directions, pack_bases, agg_keys, agg_values = zip(*entries)
    rows = await conn.fetch(
        """
        WITH data AS (
          SELECT
            unnest($1::text[]) AS direction,
            unnest($2::text[]) AS pack_base,
            unnest($3::text[]) AS agg_key,
            unnest($4::text[]) AS agg_value
        )
        INSERT INTO oracle_pack_dict (strategy_id, timeframe, direction, pack_base, agg_key, agg_value)
        SELECT $5::int, $6::text, d.direction, d.pack_base, d.agg_key, d.agg_value
          FROM data d
        ON CONFLICT (strategy_id, timeframe, direction, pack_base, agg_key, agg_value)
        DO NOTHING
        RETURNING id
        """,
        list(directions), list(pack_bases), list(agg_keys), list(agg_values),
        int(strategy_id), str(timeframe),
    )
    return len(rows or [])


# 🔸 Связка позиция ↔ MW dict (batch), возврат числа новых связей
async def _link_mw_positions_batch(
    conn,
    strategy_id: int,
    timeframe: str,
    link_entries: List[Tuple[str, str, str, str]],  # (position_uid, direction, agg_base, agg_state)
) -> int:
    if not link_entries:
        return 0

    uids, directions, agg_bases, agg_states = zip(*link_entries)
    rows = await conn.fetch(
        """
        WITH data AS (
          SELECT
            unnest($1::text[]) AS position_uid,
            unnest($2::text[]) AS direction,
            unnest($3::text[]) AS agg_base,
            unnest($4::text[]) AS agg_state
        )
        INSERT INTO oracle_mw_positions (position_uid, mw_dict_id)
        SELECT
          d.position_uid,
          dict.id
        FROM data d
        JOIN oracle_mw_dict AS dict
          ON dict.strategy_id = $5
         AND dict.timeframe   = $6
         AND dict.direction   = d.direction
         AND dict.agg_base    = d.agg_base
         AND dict.agg_state   = d.agg_state
        ON CONFLICT (position_uid, mw_dict_id) DO NOTHING
        RETURNING position_uid
        """,
        list(uids), list(directions), list(agg_bases), list(agg_states),
        int(strategy_id), str(timeframe),
    )
    return len(rows or [])


# 🔸 Связка позиция ↔ PACK dict (batch), возврат числа новых связей
async def _link_pack_positions_batch(
    conn,
    strategy_id: int,
    timeframe: str,
    link_entries: List[Tuple[str, str, str, str, str]],  # (position_uid, direction, pack_base, agg_key, agg_value)
) -> int:
    if not link_entries:
        return 0

    uids, directions, pack_bases, agg_keys, agg_values = zip(*link_entries)
    rows = await conn.fetch(
        """
        WITH data AS (
          SELECT
            unnest($1::text[]) AS position_uid,
            unnest($2::text[]) AS direction,
            unnest($3::text[]) AS pack_base,
            unnest($4::text[]) AS agg_key,
            unnest($5::text[]) AS agg_value
        )
        INSERT INTO oracle_pack_positions (position_uid, pack_dict_id)
        SELECT
          d.position_uid,
          dict.id
        FROM data d
        JOIN oracle_pack_dict AS dict
          ON dict.strategy_id = $6
         AND dict.timeframe   = $7
         AND dict.direction   = d.direction
         AND dict.pack_base   = d.pack_base
         AND dict.agg_key     = d.agg_key
         AND dict.agg_value   = d.agg_value
        ON CONFLICT (position_uid, pack_dict_id) DO NOTHING
        RETURNING position_uid
        """,
        list(uids), list(directions), list(pack_bases), list(agg_keys), list(agg_values),
        int(strategy_id), str(timeframe),
    )
    return len(rows or [])


# 🔸 Публикация события «reports_start» в Redis Stream
async def _emit_reports_start(redis, *, stream: str, payload: dict):
    # подготовка полей (единое поле data с JSON)
    fields = {"data": json.dumps(payload, separators=(",", ":"))}
    # отправка в стрим
    await redis.xadd(
        name=stream,
        fields=fields,
        maxlen=STREAM_MAXLEN,
        approximate=True,
    )