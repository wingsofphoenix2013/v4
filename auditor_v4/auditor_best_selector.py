# 🔸 auditor_best_selector.py — оркестратор витрины «лучшая идея по стратегии/направлению»
#     Режим «только свежие прогоны» + READY-уведомления:
#     — каждое сообщение обрабатывается сразу.
#     — витрина всегда относится к ПОСЛЕДНЕМУ run_id: первая запись нового run очищает старую строку.
#     — далее идеи текущего run соревнуются между собой по ключу: ΔROI → ROI_selected → confidence → coverage.
#     — по завершении набора всех ACTIVE_IDEAS для (run_id, strategy_id, direction) — отправляем READY в stream.

# 🔸 Импорты
import asyncio
import json
import logging
from typing import Any, Dict, Optional, List

import auditor_infra as infra

# 🔸 Логгер
log = logging.getLogger("AUD_BEST")

# 🔸 Константы / параметры
STREAM_NAME = "auditor:best:candidates"
GROUP_NAME = "AUD_BEST_GROUP"
CONSUMER_NAME = "AUD_BEST_SELECTOR"

# активные идеи (добавляйте новые idea_key по мере подключения)
ACTIVE_IDEAS = {"emacross_cs", "ema200_side", "atr_pct_regime", "emacross_2150_spread", "rsimfi_energy", "bb_squeeze",}

# XREAD params / TTL
XREAD_BLOCK_MS = 30_000
XREAD_COUNT = 128
SEEN_TTL_SEC = 24 * 60 * 60

# dedupe set
SEEN_SET = "aud:best:seen"

# 🔸 Ready-уведомления
READY_STREAM = "auditor:best:ready"
RUN_PERIOD_SEC = 3 * 60 * 60          # период прогонов: 3 часа
READY_TTL_BUFFER_SEC = 10 * 60        # запас на лаг: 10 минут
IDEAS_SEEN_TTL_SEC = RUN_PERIOD_SEC + READY_TTL_BUFFER_SEC   # ~3ч10м
READY_SENT_TTL_SEC = RUN_PERIOD_SEC + READY_TTL_BUFFER_SEC   # ~3ч10м
IDEAS_SEEN_PREFIX = "aud:best:ideas"        # aud:best:ideas:{run}:{sid}:{dir}
READY_SENT_PREFIX = "aud:best:ready:sent"   # aud:best:ready:sent:{run}:{sid}:{dir}


# 🔸 Утилиты
def _as_bool(v: Any) -> bool:
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "t")

def _sf(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default

def _si(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default

def _norm_dir(v: Any) -> str:
    s = str(v or "").strip().lower()
    return "long" if s == "long" else "short" if s == "short" else s


# 🔸 Сравнение кандидата с текущей витриной (True → кандидат лучше)
def _is_better(cand: Dict[str, Any], cur: Optional[Dict[str, Any]]) -> bool:
    if cur is None:
        return True
    # ключ ранжирования: ΔROI ↓ → ROI_selected ↓ → confidence ↓ → coverage ↓
    c_key = (
        _sf(cand.get("delta_roi_pp"), float("-inf")),
        _sf(cand.get("roi_selected_pct"), float("-inf")),
        _sf(cand.get("decision_confidence"), 0.0),
        _sf(cand.get("coverage_pct"), 0.0),
    )
    cur_key = (
        _sf(cur.get("delta_roi_pp"), float("-inf")),
        _sf(cur.get("roi_selected_pct"), float("-inf")),
        _sf(cur.get("decision_confidence"), 0.0),
        _sf(cur.get("coverage_pct"), 0.0),
    )
    return c_key > cur_key


# 🔸 Возможно отправить READY (когда получены все ACTIVE_IDEAS для (run_id, sid, dir))
async def _maybe_emit_ready(sid: int, direction: str, run_id: int, idea_key: str):
    if infra.redis_client is None:
        return
    dir_tag = "long" if direction == "long" else "short"
    ideas_key = f"{IDEAS_SEEN_PREFIX}:{run_id}:{sid}:{dir_tag}"
    ready_key = f"{READY_SENT_PREFIX}:{run_id}:{sid}:{dir_tag}"

    # учесть идею и TTL
    await infra.redis_client.sadd(ideas_key, idea_key)
    await infra.redis_client.expire(ideas_key, IDEAS_SEEN_TTL_SEC)

    # все идеи получены?
    try:
        seen = await infra.redis_client.scard(ideas_key)
    except Exception:
        seen = 0
    if seen < len(ACTIVE_IDEAS):
        return

    # дедуп отправки READY
    try:
        if await infra.redis_client.setnx(ready_key, "1"):
            await infra.redis_client.expire(ready_key, READY_SENT_TTL_SEC)
            await infra.redis_client.xadd(READY_STREAM, {
                "strategy_id": str(sid),
                "direction": str(direction),
            })
            log.info("📣 AUD_BEST: READY emitted (sid=%s dir=%s run=%s)", sid, direction, run_id)
    except Exception:
        log.exception("⚠️ AUD_BEST: ошибка при отправке READY (sid=%s dir=%s run=%s)", sid, direction, run_id)


# 🔸 Запуск воркера
async def run_auditor_best_selector():
    # условия достаточности
    if infra.redis_client is None or infra.pg_pool is None:
        log.info("❌ Пропуск auditor_best_selector: Redis/PG не инициализированы")
        return

    # создать consumer group идемпотентно
    try:
        await infra.redis_client.xgroup_create(name=STREAM_NAME, groupname=GROUP_NAME, id="$", mkstream=True)
        log.info("📡 AUD_BEST: создана consumer group для стрима %s", STREAM_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ AUD_BEST: ошибка создания consumer group")
            return

    log.info("🚀 AUD_BEST: старт воркера (stream=%s, group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)

    # основной цикл
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=XREAD_COUNT,
                block=XREAD_BLOCK_MS,
            )
            if not resp:
                continue

            acks: List[str] = []
            for stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        await _handle_message(fields)
                        acks.append(msg_id)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        log.exception("❌ AUD_BEST: ошибка обработки сообщения %s", msg_id)

            if acks:
                try:
                    await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *acks)
                except Exception:
                    log.exception("⚠️ AUD_BEST: ошибка ACK (ids=%s)", acks)

        except asyncio.CancelledError:
            log.info("⏹️ AUD_BEST: остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ AUD_BEST: ошибка цикла — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Обработка одного сообщения type="result" (только свежие прогоны попадают в витрину)
async def _handle_message(fields: Dict[str, str]):
    # дедуп по event_uid (если прислан)
    event_uid = fields.get("event_uid", "")
    if event_uid:
        added = await infra.redis_client.sadd(SEEN_SET, event_uid)
        if added == 0:
            return
        await infra.redis_client.expire(SEEN_SET, SEEN_TTL_SEC)

    msg_type = str(fields.get("type", "result")).lower()
    if msg_type != "result":
        return

    # входные поля
    sid = _si(fields.get("strategy_id"))
    direction = _norm_dir(fields.get("direction"))
    idea_key = str(fields.get("idea_key", "")).strip()
    variant_key = str(fields.get("variant_key", "")).strip()
    primary_window = str(fields.get("primary_window", "")).strip()
    eligible = _as_bool(fields.get("eligible", "false"))

    # валидации сообщения
    if not sid or direction not in ("long", "short") or not idea_key:
        log.info("ℹ️ AUD_BEST: пропуск некорректного сообщения (sid=%s dir=%s idea=%s)", sid, direction, idea_key)
        return
    if idea_key not in ACTIVE_IDEAS:
        return

    # run текущего сообщения (для свежести используем ВСЕГДА run_id)
    msg_run = _si(fields.get("run_id"))

    # читаем текущую витрину
    cur = await _read_current_best(sid, direction)
    cur_run = _si(cur.get("source_run_id", 0)) if cur else 0

    # если пришёл БОЛЕЕ СВЕЖИЙ прогон — удаляем строку витрины (без сравнения)
    if cur and msg_run > cur_run:
        await _delete_current_best(sid, direction)
        log.info("🧹 AUD_BEST: sid=%s dir=%s — очищена витрина (старый run=%s < новый=%s)",
                 sid, direction, cur_run, msg_run)
        cur = None
        cur_run = 0

    # если кандидат ineligible — фиксируем и проверяем готовность по идеям этого run
    if not eligible:
        log.info("ℹ️ AUD_BEST: sid=%s dir=%s — ineligible %s (run=%s); витрина %s",
                 sid, direction, idea_key, msg_run, "пустая" if cur is None else "без изменений")
        await _maybe_emit_ready(sid, direction, msg_run, idea_key)
        return

    # распаковка метрик кандидата
    roi_sel = _sf(fields.get("roi_selected_pct"))
    roi_all = _sf(fields.get("roi_all_pct"))
    delta_roi = roi_sel - roi_all
    wr_sel = _sf(fields.get("wr_selected_pct"))
    wr_all = _sf(fields.get("wr_all_pct"))
    coverage = _sf(fields.get("coverage_pct"))
    conf = _sf(fields.get("decision_confidence"))
    source_table = (fields.get("source_table") or "unknown").strip()

    # нормализация config_json
    cfg_raw = fields.get("config_json") or "{}"
    try:
        cfg_json = json.dumps(json.loads(cfg_raw))
    except Exception:
        cfg_json = "{}"

    # source_run_id в витрине: используем source_run_id ИЛИ run_id
    cand_run = _si(fields.get("source_run_id")) or msg_run

    cand = {
        "strategy_id": sid,
        "direction": direction,
        "idea_key": idea_key,
        "variant_key": variant_key,
        "primary_window": primary_window or (cur.get("primary_window") if cur else "14d"),
        "coverage_pct": coverage,
        "roi_selected_pct": roi_sel,
        "roi_all_pct": roi_all,
        "delta_roi_pp": delta_roi,
        "wr_selected_pct": wr_sel,
        "wr_all_pct": wr_all,
        "delta_wr_pp": (wr_sel - wr_all),
        "decision_class": "green" if (delta_roi >= 5.0 and (wr_sel - wr_all) >= 3.0) else ("yellow" if delta_roi > 0.0 else "red"),
        "decision_confidence": conf,
        "config_json": cfg_json,
        "source_table": source_table,
        "source_run_id": cand_run,
    }

    # если витрина пуста — сразу ставим кандидата
    if cur is None:
        await _upsert_current_best(cand)
        log.info("🏁 AUD_BEST: sid=%s dir=%s → BEST [%s/%s] (fresh run=%s) ΔROI=%.2fpp (ROI_sel=%.2f%%) cov=%.1f%%",
                 sid, direction, idea_key, variant_key, cand_run, delta_roi, roi_sel, coverage)
        await _maybe_emit_ready(sid, direction, msg_run, idea_key)
        return

    # защита от опоздавших сообщений старых прогонов
    if msg_run < cur_run:
        log.info("⏭️ AUD_BEST: sid=%s dir=%s — кандидат [%s/%s] старее текущего (run=%s < %s), пропуск",
                 sid, direction, idea_key, variant_key, msg_run, cur_run)
        await _maybe_emit_ready(sid, direction, msg_run, idea_key)
        return

    # msg_run == cur_run → сравнение только внутри свежего прогона
    if _is_better(cand, cur):
        await _upsert_current_best(cand)
        log.info("🏁 AUD_BEST: sid=%s dir=%s → BEST [%s/%s] ΔROI=%.2fpp (ROI_sel=%.2f%%) cov=%.1f%%",
                 sid, direction, idea_key, variant_key, delta_roi, roi_sel, coverage)
    else:
        log.info("ℹ️ AUD_BEST: sid=%s dir=%s — кандидат [%s/%s] хуже текущего в том же run, пропуск",
                 sid, direction, idea_key, variant_key)

    # после обработки текущей идеи — проверяем, все ли идеи уже пришли для этого run
    await _maybe_emit_ready(sid, direction, msg_run, idea_key)


# 🔸 Чтение текущей витрины по (sid,dir)
async def _read_current_best(sid: int, direction: str) -> Optional[Dict[str, Any]]:
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT strategy_id, direction, idea_key, variant_key, primary_window,
                   coverage_pct, roi_selected_pct, roi_all_pct, delta_roi_pp,
                   wr_selected_pct, wr_all_pct, delta_wr_pp,
                   decision_class, decision_confidence, config_json, source_table, source_run_id, updated_at
            FROM auditor_current_best
            WHERE strategy_id=$1 AND direction=$2
            """,
            sid, direction
        )
        return dict(row) if row else None


# 🔸 UPSERT витрины
async def _upsert_current_best(cand: Dict[str, Any]):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO auditor_current_best
            (strategy_id, direction, idea_key, variant_key, primary_window,
             coverage_pct, roi_selected_pct, roi_all_pct, delta_roi_pp,
             wr_selected_pct, wr_all_pct, delta_wr_pp,
             decision_class, decision_confidence, config_json, source_table, source_run_id)
            VALUES
            ($1,$2,$3,$4,$5, $6,$7,$8,$9, $10,$11,$12, $13,$14,$15,$16,$17)
            ON CONFLICT (strategy_id, direction) DO UPDATE SET
              idea_key            = EXCLUDED.idea_key,
              variant_key         = EXCLUDED.variant_key,
              primary_window      = EXCLUDED.primary_window,
              coverage_pct        = EXCLUDED.coverage_pct,
              roi_selected_pct    = EXCLUDED.roi_selected_pct,
              roi_all_pct         = EXCLUDED.roi_all_pct,
              delta_roi_pp        = EXCLUDED.delta_roi_pp,
              wr_selected_pct     = EXCLUDED.wr_selected_pct,
              wr_all_pct          = EXCLUDED.wr_all_pct,
              delta_wr_pp         = EXCLUDED.delta_wr_pp,
              decision_class      = EXCLUDED.decision_class,
              decision_confidence = EXCLUDED.decision_confidence,
              config_json         = EXCLUDED.config_json,
              source_table        = EXCLUDED.source_table,
              source_run_id       = EXCLUDED.source_run_id,
              updated_at          = now()
            """,
            cand["strategy_id"], cand["direction"], cand["idea_key"], cand["variant_key"], cand["primary_window"],
            float(cand["coverage_pct"]), float(cand["roi_selected_pct"]), float(cand["roi_all_pct"]), float(cand["delta_roi_pp"]),
            float(cand["wr_selected_pct"]), float(cand["wr_all_pct"]), float(cand["delta_wr_pp"]),
            cand["decision_class"], float(cand["decision_confidence"]), cand["config_json"], cand["source_table"], int(cand["source_run_id"])
        )


# 🔸 Удаление строки витрины (пауза стратегии / смена run)
async def _delete_current_best(sid: int, direction: str):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM auditor_current_best WHERE strategy_id=$1 AND direction=$2",
            int(sid), str(direction)
        )