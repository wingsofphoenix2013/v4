# 🔸 laboratory_auditor_config.py — кеш витрины auditor_current_best и thresholds + воркер READY

# 🔸 Импорты
import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Dict, Tuple, Optional

import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_AUDITOR_CFG")

# 🔸 Константы стрима READY аудитора
AUDITOR_READY_STREAM = "auditor:best:ready"
AUDITOR_READY_GROUP = "LAB_AUDITOR_READY_GROUP"
AUDITOR_READY_CONSUMER = "LAB_AUDITOR_READY_WORKER"

# 🔸 Типы и глобальные структуры

@dataclass
class BestIdeaRecord:
    """
    Текущая лучшая идея/маска для (strategy_id, direction).
    """
    strategy_id: int
    direction: str              # 'long' | 'short'
    idea_key: str               # emacross_cs | ema200_side | emacross_2150_spread | atr_pct_regime
    variant_key: str            # m5_only | m5_m15 | m5_m15_h1
    primary_window: str         # '7d' | '14d' | '28d'
    source_run_id: int          # run_id набора thresholds (auditor_*_thresholds)
    config_json: dict           # распарсенный config_json из витрины


@dataclass
class ThresholdsRecord:
    """
    Пороговые значения квантилей для конкретной идеи/символа/TF/окна.
    """
    idea_key: str
    run_id: int
    strategy_id: int
    direction: str
    symbol: str
    timeframe: str              # 'm5' | 'm15' | 'h1'
    window_tag: str             # '7d' | '14d' | '28d' | 'total'
    q20: float
    q40: float
    q60: float
    q80: float
    n_samples: int


# 🔸 Кеш витрины: (strategy_id, direction) -> BestIdeaRecord
best_by_sid_dir: Dict[Tuple[int, str], BestIdeaRecord] = {}

# 🔸 Кеш thresholds:
# ключ: (idea_key, run_id, strategy_id, direction, symbol, timeframe, window_tag)
thresholds_cache: Dict[Tuple[str, int, int, str, str, str, str], ThresholdsRecord] = {}

# 🔸 Маппинг идея -> таблица thresholds
THRESHOLDS_TABLE_BY_IDEA = {
    "emacross_cs": "auditor_emacross_thresholds",
    "emacross_2150_spread": "auditor_ema2150_spread_thresholds",
    "ema200_side": "auditor_ema200_side_thresholds",
    "atr_pct_regime": "auditor_atrreg_thresholds",
}


# 🔸 Предзагрузка витрины auditor_current_best

async def load_initial_auditor_best():
    """
    Единожды на старте загружает текущее состояние витрины auditor_current_best в память.
    """
    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ LAB_AUDITOR_CFG: пропуск load_initial_auditor_best — PG не инициализирован")
        return

    global best_by_sid_dir
    best_by_sid_dir = {}

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
              strategy_id,
              direction,
              idea_key,
              variant_key,
              primary_window,
              config_json,
              source_run_id
            FROM auditor_current_best
            """
        )

    total = 0
    strategies = set()
    for r in rows:
        sid = int(r["strategy_id"])
        direction = str(r["direction"]).lower().strip()
        key = (sid, direction)

        # парсинг config_json
        cfg = r["config_json"]
        if isinstance(cfg, str):
            s = cfg.strip()
            if s:
                try:
                    cfg = json.loads(s)
                except Exception:
                    cfg = {}
            else:
                cfg = {}
        elif cfg is None:
            cfg = {}
        elif not isinstance(cfg, dict):
            # на всякий случай попытаемся декодировать
            try:
                cfg = json.loads(cfg)
            except Exception:
                cfg = {}

        rec = BestIdeaRecord(
            strategy_id=sid,
            direction=direction,
            idea_key=str(r["idea_key"]),
            variant_key=str(r["variant_key"]),
            primary_window=str(r["primary_window"]),
            source_run_id=int(r["source_run_id"]),
            config_json=cfg,
        )
        best_by_sid_dir[key] = rec
        strategies.add(sid)
        total += 1

    log.info(
        "✅ LAB_AUDITOR_CFG: предзагрузка витрины завершена — записей=%d, стратегий=%d",
        total, len(strategies)
    )


# 🔸 Публичный доступ к витрине

def get_best_for(strategy_id: int, direction: str) -> Optional[BestIdeaRecord]:
    """
    Быстрый доступ к текущей лучшей идее для (strategy_id, direction).
    Возвращает None, если витрина для пары отсутствует.
    """
    key = (int(strategy_id), str(direction).lower().strip())
    return best_by_sid_dir.get(key)


# 🔸 Lazy-load thresholds

async def get_thresholds(
    idea_key: str,
    run_id: int,
    strategy_id: int,
    direction: str,
    symbol: str,
    timeframe: str,
    window_tag: str,
) -> Optional[ThresholdsRecord]:
    """
    Lazy-load thresholds для конкретной идеи/символа/TF/окна.
    Если в кеше нет — тянет из соответствующей auditor_*_thresholds,
    кладёт в thresholds_cache и возвращает.
    При отсутствии строки в БД возвращает None.
    """
    idea = str(idea_key)
    sid = int(strategy_id)
    dir_norm = str(direction).lower().strip()
    sym = str(symbol).upper().strip()
    tf = str(timeframe).lower().strip()
    win = str(window_tag).lower().strip()
    run = int(run_id)

    # ключ кеша
    cache_key = (idea, run, sid, dir_norm, sym, tf, win)

    if cache_key in thresholds_cache:
        return thresholds_cache[cache_key]

    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ LAB_AUDITOR_CFG: get_thresholds — PG не инициализирован")
        return None

    table = THRESHOLDS_TABLE_BY_IDEA.get(idea)
    if not table:
        log.info("ℹ️ LAB_AUDITOR_CFG: неизвестная идея для thresholds (idea_key=%s)", idea_key)
        return None

    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT
              run_id,
              strategy_id,
              direction,
              symbol,
              timeframe,
              window_tag,
              q20_value,
              q40_value,
              q60_value,
              q80_value,
              n_samples
            FROM {table}
            WHERE run_id      = $1
              AND strategy_id = $2
              AND direction   = $3
              AND symbol      = $4
              AND timeframe   = $5
              AND window_tag  = $6
            """,
            run,
            sid,
            dir_norm,
            sym,
            tf,
            win,
        )

    if not row:
        log.info(
            "ℹ️ LAB_AUDITOR_CFG: thresholds не найдены (idea=%s, run_id=%s, sid=%s, dir=%s, symbol=%s, tf=%s, window=%s)",
            idea, run, sid, dir_norm, sym, tf, win
        )
        return None

    rec = ThresholdsRecord(
        idea_key=idea,
        run_id=int(row["run_id"]),
        strategy_id=int(row["strategy_id"]),
        direction=str(row["direction"]).lower().strip(),
        symbol=str(row["symbol"]).upper().strip(),
        timeframe=str(row["timeframe"]).lower().strip(),
        window_tag=str(row["window_tag"]).lower().strip(),
        q20=float(row["q20_value"]),
        q40=float(row["q40_value"]),
        q60=float(row["q60_value"]),
        q80=float(row["q80_value"]),
        n_samples=int(row["n_samples"]),
    )
    thresholds_cache[cache_key] = rec

    log.debug(
        "LAB_AUDITOR_CFG: thresholds загружены в кеш (idea=%s, sid=%d, dir=%s, symbol=%s, tf=%s, window=%s, n=%d)",
        idea, sid, dir_norm, sym, tf, win, rec.n_samples
    )
    return rec


# 🔸 Инвалидация thresholds при смене run_id

def invalidate_thresholds_for(strategy_id: int, direction: str, old_run_id: int):
    """
    Удаляет из кеша все thresholds для старого run_id по данной паре (strategy_id, direction).
    """
    if not thresholds_cache:
        return

    sid = int(strategy_id)
    dir_norm = str(direction).lower().strip()
    old_run = int(old_run_id)

    # собираем ключи на удаление
    to_delete = [
        key for key in list(thresholds_cache.keys())
        if key[1] == old_run and key[2] == sid and key[3] == dir_norm
    ]
    for key in to_delete:
        thresholds_cache.pop(key, None)

    if to_delete:
        log.info(
            "ℹ️ LAB_AUDITOR_CFG: thresholds очищены из кеша (sid=%d, dir=%s, old_run_id=%d, keys=%d)",
            sid, dir_norm, old_run, len(to_delete)
        )


# 🔸 Воркер-слушатель READY стрима аудитора

async def run_laboratory_auditor_ready_listener():
    """
    Вечный воркер: слушает auditor:best:ready, при READY по (run_id, strategy_id, direction)
    перезагружает витрину для пары (sid, dir) и очищает thresholds старого run_id.
    """
    # условия достаточности
    if infra.redis_client is None or infra.pg_pool is None:
        log.info("❌ LAB_AUDITOR_CFG: пропуск READY-воркера — PG/Redis не инициализированы")
        return

    # создаём consumer group идемпотентно
    try:
        await infra.redis_client.xgroup_create(
            name=AUDITOR_READY_STREAM,
            groupname=AUDITOR_READY_GROUP,
            id="$",
            mkstream=True,
        )
        log.info("📡 LAB_AUDITOR_CFG: создана consumer group для %s", AUDITOR_READY_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ LAB_AUDITOR_CFG: ошибка создания consumer group для %s", AUDITOR_READY_STREAM)
            return

    log.info("🚀 LAB_AUDITOR_CFG: старт воркера READY (stream=%s)", AUDITOR_READY_STREAM)

    # основной цикл
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=AUDITOR_READY_GROUP,
                consumername=AUDITOR_READY_CONSUMER,
                streams={AUDITOR_READY_STREAM: ">"},
                count=128,
                block=30_000,
            )
            if not resp:
                continue

            acks = []

            for stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        # ожидаем поля run_id, strategy_id, direction в READY-сообщении
                        run_id_raw = fields.get("run_id")
                        sid_raw = fields.get("strategy_id")
                        direction_raw = fields.get("direction")

                        if not run_id_raw or not sid_raw or not direction_raw:
                            log.info(
                                "ℹ️ LAB_AUDITOR_CFG: пропуск READY-сообщения (нехватает полей) id=%s payload=%s",
                                msg_id, fields
                            )
                            acks.append(msg_id)
                            continue

                        run_id = int(run_id_raw)
                        sid = int(sid_raw)
                        direction = str(direction_raw).lower().strip()
                        key = (sid, direction)

                        old_run_id = None
                        old = best_by_sid_dir.get(key)
                        if old is not None:
                            old_run_id = old.source_run_id

                        # обновляем витрину по паре
                        await _reload_best_for_pair(sid, direction, old_run_id)

                        # очищаем thresholds по старому run_id (если отличался)
                        if old_run_id is not None and old_run_id != run_id:
                            invalidate_thresholds_for(sid, direction, old_run_id)

                        acks.append(msg_id)
                    except Exception:
                        log.exception("❌ LAB_AUDITOR_CFG: ошибка обработки READY-сообщения (id=%s)", msg_id)
                        acks.append(msg_id)

            # ACK
            if acks:
                try:
                    await infra.redis_client.xack(AUDITOR_READY_STREAM, AUDITOR_READY_GROUP, *acks)
                except Exception:
                    log.exception("⚠️ LAB_AUDITOR_CFG: ошибка ACK READY (ids=%s)", acks)

        except asyncio.CancelledError:
            log.info("⏹️ LAB_AUDITOR_CFG: READY-воркер остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_AUDITOR_CFG: ошибка цикла READY — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Вспомогательная функция перезагрузки пары (strategy_id, direction)

async def _reload_best_for_pair(strategy_id: int, direction: str, old_run_id: Optional[int]):
    """
    Перезагружает запись из auditor_current_best для пары (strategy_id, direction)
    и обновляет in-memory кеш.
    """
    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ LAB_AUDITOR_CFG: _reload_best_for_pair — PG не инициализирован")
        return

    sid = int(strategy_id)
    dir_norm = str(direction).lower().strip()

    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
              strategy_id,
              direction,
              idea_key,
              variant_key,
              primary_window,
              config_json,
              source_run_id
            FROM auditor_current_best
            WHERE strategy_id = $1
              AND direction   = $2
            """,
            sid,
            dir_norm,
        )

    if not row:
        log.info(
            "ℹ️ LAB_AUDITOR_CFG: запись в витрине не найдена (sid=%d, dir=%s)",
            sid, dir_norm
        )
        return

    cfg = row["config_json"]
    if isinstance(cfg, str):
        s = cfg.strip()
        if s:
            try:
                cfg = json.loads(s)
            except Exception:
                cfg = {}
        else:
            cfg = {}
    elif cfg is None:
        cfg = {}
    elif not isinstance(cfg, dict):
        try:
            cfg = json.loads(cfg)
        except Exception:
            cfg = {}

    rec = BestIdeaRecord(
        strategy_id=int(row["strategy_id"]),
        direction=str(row["direction"]).lower().strip(),
        idea_key=str(row["idea_key"]),
        variant_key=str(row["variant_key"]),
        primary_window=str(row["primary_window"]),
        source_run_id=int(row["source_run_id"]),
        config_json=cfg,
    )
    best_by_sid_dir[(sid, dir_norm)] = rec

    log.info(
        "✅ LAB_AUDITOR_CFG: витрина обновлена по READY (sid=%d, dir=%s, source_run_id=%d, idea=%s, variant=%s)",
        sid, dir_norm, rec.source_run_id, rec.idea_key, rec.variant_key
    )