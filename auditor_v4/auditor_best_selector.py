# 🔸 auditor_best_selector.py — оркестратор витрины «лучшая идея по стратегии/направлению»
#     Простой событийный режим (НЕ ждёт других идей и НЕ ждёт «финала раунда»):
#     — каждое сообщение обрабатывается сразу: сравнили с текущей витриной → обновили / удалили / пропустили.
#     — если eligible=false и текущая витрина принадлежит той же идее → удаляем строку (стратегия на паузе).
#     — если eligible=true → сравниваем с текущей по ΔROI (далее ROI_selected, confidence, coverage) → при лучшем апсертим.
#
#     Формат сообщения (Redis Stream: auditor:best:candidates, поле "data" НЕ используется — поля плоско):
#       type="result"
#       strategy_id, direction ('long'|'short'), idea_key, variant_key,
#       primary_window ('7d'|'14d'|'28d'),
#       eligible ('true'|'false'),
#       roi_selected_pct?, roi_all_pct?, wr_selected_pct?, wr_all_pct?, coverage_pct?, decision_confidence?,
#       config_json?, source_table?, source_run_id?, event_uid?
#
#     Требование к идеям: всегда присылать result по каждой (strategy_id, direction).
#       eligible=true  → обязательно передать ROI/WR/coverage/conf/config_json.
#       eligible=false → метрики можно не передавать.
#
#     Витрина БД: auditor_current_best (PRIMARY KEY(strategy_id, direction))

# 🔸 Импорты
import asyncio
import json
import logging
from typing import Any, Dict, Optional, List

import auditor_infra as infra

# 🔸 Логгер
log = logging.getLogger("AUD_BEST")

# 🔸 Константы / параметры (правим здесь)
STREAM_NAME = "auditor:best:candidates"
GROUP_NAME = "AUD_BEST_GROUP"
CONSUMER_NAME = "AUD_BEST_SELECTOR"

# активные идеи (добавляем сюда новые idea_key по мере подключения)
ACTIVE_IDEAS = {"emacross_cs", "ema200_side", "atr_pct_regime", "emacross_2150_spread"}

# XREAD params / TTL
XREAD_BLOCK_MS = 30_000
XREAD_COUNT = 128
SEEN_TTL_SEC = 24 * 60 * 60

# dedupe set
SEEN_SET = "aud:best:seen"


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


# 🔸 Обработка одного сообщения type="result" (без ожиданий)
async def _handle_message(fields: Dict[str, str]):
    # дедупликатор по event_uid (если прислан)
    event_uid = fields.get("event_uid", "")
    if event_uid:
        added = await infra.redis_client.sadd(SEEN_SET, event_uid)
        if added == 0:
            return
        await infra.redis_client.expire(SEEN_SET, SEEN_TTL_SEC)

    msg_type = str(fields.get("type", "result")).lower()
    if msg_type != "result":
        return

    sid = _si(fields.get("strategy_id"))
    direction = _norm_dir(fields.get("direction"))
    idea_key = str(fields.get("idea_key", "")).strip()
    variant_key = str(fields.get("variant_key", "")).strip()
    primary_window = str(fields.get("primary_window", "")).strip()
    eligible = _as_bool(fields.get("eligible", "false"))

    if not sid or direction not in ("long", "short") or not idea_key:
        log.info("ℹ️ AUD_BEST: пропуск некорректного сообщения (sid=%s dir=%s idea=%s)", sid, direction, idea_key)
        return
    if idea_key not in ACTIVE_IDEAS:
        # игнорируем неизвестные идеи
        return

    # читаем текущую строку витрины по (sid,dir)
    cur = await _read_current_best(sid, direction)

    if not eligible:
        # идея говорит «не годится»: если текущая витрина принадлежит этой идее — удаляем; иначе ничего не делаем
        if cur and str(cur.get("idea_key")) == idea_key:
            await _delete_current_best(sid, direction)
            log.info("🗑️ AUD_BEST: sid=%s dir=%s — удалена витрина (ineligible от %s)", sid, direction, idea_key)
        else:
            log.info("ℹ️ AUD_BEST: sid=%s dir=%s — ineligible %s, витрина не тронута", sid, direction, idea_key)
        return

    # eligible=true → готовим кандидат
    roi_sel = _sf(fields.get("roi_selected_pct"))
    roi_all = _sf(fields.get("roi_all_pct"))
    delta_roi = roi_sel - roi_all
    wr_sel = _sf(fields.get("wr_selected_pct"))
    wr_all = _sf(fields.get("wr_all_pct"))
    coverage = _sf(fields.get("coverage_pct"))
    conf = _sf(fields.get("decision_confidence"))
    cfg_raw = fields.get("config_json") or "{}"
    try:
        cfg_json = json.dumps(json.loads(cfg_raw))
    except Exception:
        cfg_json = "{}"
    source_table = str(fields.get("source_table", "")).strip()
    source_run_id = _si(fields.get("source_run_id"))

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
        "source_table": source_table or "unknown",
        "source_run_id": source_run_id or 0,
    }

    # сравнить и записать при улучшении
    if _is_better(cand, cur):
        await _upsert_current_best(cand)
        log.info("🏁 AUD_BEST: sid=%s dir=%s → BEST [%s/%s] ΔROI=%.2fpp (ROI_sel=%.2f%%) cov=%.1f%%",
                 sid, direction, idea_key, variant_key, delta_roi, roi_sel, coverage)
    else:
        log.info("ℹ️ AUD_BEST: sid=%s dir=%s — кандидат [%s/%s] хуже текущего, пропуск",
                 sid, direction, idea_key, variant_key)


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


# 🔸 Удаление строки витрины (пауза стратегии)
async def _delete_current_best(sid: int, direction: str):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM auditor_current_best WHERE strategy_id=$1 AND direction=$2",
            int(sid), str(direction)
        )