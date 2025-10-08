# trader_rating.py — почасовой расчёт рейтинга стратегий по группам market_mirrow (trader_watcher)

# 🔸 Импорты
import logging
from decimal import Decimal
from typing import Dict, List, Tuple, Any

from trader_infra import infra

# 🔸 Логгер воркера рейтинга
log = logging.getLogger("TRADER_RATING")

# 🔸 Глобальное состояние победителей (в памяти)
current_group_winners: Dict[int, int] = {}


# 🔸 Публичный геттер победителей (копия словаря)
def get_current_group_winners() -> Dict[int, int]:
    # возвращаем копию, чтобы не менять оригинал снаружи
    return dict(current_group_winners)


# 🔸 Основной одноразовый проход расчёта рейтинга (вызывается раз в час из main)
async def run_trader_rating_job():
    # собираем кандидатов
    candidates = await _fetch_candidates()
    if not candidates:
        log.info("ℹ️ TRADER_RATING: кандидатов нет (enabled & trader_watcher & market_mirrow) — пропуск")
        return

    # строим список strategy_id для метрик
    strategy_ids = [c["id"] for c in candidates]

    # вытягиваем агрегаты за 24ч и 1ч
    metrics = await _fetch_metrics_24h_1h(strategy_ids)

    # группируем, считаем нормализации и скор, выбираем победителей
    group_winners, raw_results, groups_watchers = _compute_group_winners(candidates, metrics)

    # применяем флаги в БД (trader_winner) и апсерты в trader_rating_active
    await _apply_results_to_db(group_winners, groups_watchers, raw_results)

    # обновляем global в памяти
    _update_inmemory_winners(group_winners)

    # сводка в лог
    winners_pretty = ", ".join(f"{gm}->{sid}" for gm, sid in group_winners.items() if sid is not None)
    log.info(
        "✅ TRADER_RATING: обработано групп=%d, победителей=%d%s",
        len(groups_watchers),
        sum(1 for v in group_winners.values() if v is not None),
        f" | {winners_pretty}" if winners_pretty else ""
    )


# 🔸 Загрузка списка кандидатов (enabled, trader_watcher, есть market_mirrow)
async def _fetch_candidates() -> List[Dict[str, Any]]:
    rows = await infra.pg_pool.fetch(
        """
        SELECT id, deposit, market_mirrow
        FROM public.strategies_v4
        WHERE enabled = TRUE
          AND trader_watcher = TRUE
          AND market_mirrow IS NOT NULL
        """
    )
    # приводим deposit к Decimal и фильтруем на всякий случай мусор
    out = []
    for r in rows:
        try:
            dep = Decimal(str(r["deposit"])) if r["deposit"] is not None else Decimal("0")
        except Exception:
            dep = Decimal("0")
        out.append({"id": int(r["id"]), "deposit": dep, "market_mirrow": int(r["market_mirrow"])})
    return out


# 🔸 Агрегаты по позициям за 24ч и 1ч для набора стратегий
async def _fetch_metrics_24h_1h(strategy_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not strategy_ids:
        return {}

    # Postgres: все даты в UTC; столбцы в БД без таймзоны → используем (now() at time zone 'UTC')
    rows_24 = await infra.pg_pool.fetch(
        """
        SELECT
          strategy_id,
          COUNT(*)                      AS closed_24,
          SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins_24,
          COALESCE(SUM(pnl), 0)         AS pnl_24
        FROM public.positions_v4
        WHERE status = 'closed'
          AND closed_at >= ((now() at time zone 'UTC') - interval '24 hours')
          AND strategy_id = ANY($1::int[])
        GROUP BY strategy_id
        """,
        strategy_ids,
    )

    rows_1h = await infra.pg_pool.fetch(
        """
        SELECT
          strategy_id,
          COUNT(*)              AS closed_1h,
          COALESCE(SUM(pnl), 0) AS pnl_1h
        FROM public.positions_v4
        WHERE status = 'closed'
          AND closed_at >= ((now() at time zone 'UTC') - interval '1 hour')
          AND strategy_id = ANY($1::int[])
        GROUP BY strategy_id
        """,
        strategy_ids,
    )

    # собираем в единый словарь
    m: Dict[int, Dict[str, Any]] = {}
    for r in rows_24:
        sid = int(r["strategy_id"])
        m[sid] = {
            "closed_24": int(r["closed_24"]),
            "wins_24": int(r["wins_24"]),
            "pnl_24": Decimal(str(r["pnl_24"])),
            "closed_1h": 0,
            "pnl_1h": Decimal("0"),
        }
    for r in rows_1h:
        sid = int(r["strategy_id"])
        if sid not in m:
            m[sid] = {"closed_24": 0, "wins_24": 0, "pnl_24": Decimal("0"), "closed_1h": 0, "pnl_1h": Decimal("0")}
        m[sid]["closed_1h"] = int(r["closed_1h"])
        m[sid]["pnl_1h"] = Decimal(str(r["pnl_1h"]))
    return m


# 🔸 Расчёт победителей по группам
def _compute_group_winners(
    candidates: List[Dict[str, Any]],
    metrics: Dict[int, Dict[str, Any]],
) -> Tuple[Dict[int, int | None], Dict[int, Dict[str, Any]], Dict[int, List[int]]]:
    # группируем кандидатов по market_mirrow и собираем watcher-список
    groups_watchers: Dict[int, List[int]] = {}
    deposits: Dict[int, Decimal] = {}
    for c in candidates:
        sid = c["id"]
        gm = c["market_mirrow"]
        dep = c["deposit"]
        groups_watchers.setdefault(gm, []).append(sid)
        deposits[sid] = dep

    group_winners: Dict[int, int | None] = {}
    raw_results: Dict[int, Dict[str, Any]] = {}

    # проходим по группам
    for gm, sids in groups_watchers.items():
        # формируем список активных (N24>0 и депозит>0)
        active = []
        per_strategy_results = {}

        for sid in sids:
            dep = deposits.get(sid, Decimal("0"))
            met = metrics.get(sid, {"closed_24": 0, "wins_24": 0, "pnl_24": Decimal("0"), "closed_1h": 0, "pnl_1h": Decimal("0")})
            closed_24 = met["closed_24"]
            wins_24 = met["wins_24"]
            pnl_24 = met["pnl_24"]
            closed_1h = met["closed_1h"]
            pnl_1h = met["pnl_1h"]

            # если нет закрытий за 24ч — стратегия «спит» в смысле рейтинга
            if closed_24 <= 0 or dep <= 0:
                # сырые метрики всё равно фиксируем
                per_strategy_results[str(sid)] = {
                    "roi24": 0.0,
                    "wr24": 0.0,
                    "roi1h": 0.0,
                    "n24": int(closed_24),
                    "n1": int(closed_1h),
                    "score": 0.0,
                }
                continue

            # расчёт метрик
            roi24 = float((pnl_24 / dep))
            wr24 = float(wins_24) / float(closed_24) if closed_24 > 0 else 0.0
            roi1h = float((pnl_1h / dep)) if dep > 0 else 0.0

            active.append((sid, roi24, wr24, roi1h, int(closed_24)))
            # временно кладём без score (посчитаем после нормализации)
            per_strategy_results[str(sid)] = {
                "roi24": roi24,
                "wr24": wr24,
                "roi1h": roi1h,
                "n24": int(closed_24),
                "n1": int(closed_1h),
            }

        # если активных нет — победителя нет
        if not active:
            group_winners[gm] = None
            raw_results[gm] = {"strategies": per_strategy_results}
            continue

        # нормализация по группе
        roi24_vals = [a[1] for a in active]
        roi1h_vals = [a[3] for a in active]

        roi24_min, roi24_max = min(roi24_vals), max(roi24_vals)
        roi1h_min, roi1h_max = min(roi1h_vals), max(roi1h_vals)

        def minmax(x: float, lo: float, hi: float) -> float:
            # если у всех одинаковые значения — возвращаем 0.5
            if hi == lo:
                return 0.5
            return (x - lo) / (hi - lo)

        # считаем score
        scored = []
        for sid, roi24, wr24, roi1h, n24 in active:
            roi24_norm = minmax(roi24, roi24_min, roi24_max)
            roi1h_norm = minmax(roi1h, roi1h_min, roi1h_max)
            score = 0.60 * roi24_norm + 0.25 * wr24 + 0.15 * roi1h_norm
            scored.append((sid, score, roi24, roi1h, n24))

            # дополняем сырые результаты
            per_strategy_results[str(sid)]["score"] = score

        # выбираем победителя (тай-брейки: по ROI_24h, затем ROI_1h, затем N24)
        scored.sort(key=lambda t: (-t[1], -t[2], -t[3], -t[4], t[0]))
        winner_sid = scored[0][0]

        group_winners[gm] = winner_sid
        raw_results[gm] = {"strategies": per_strategy_results}

    return group_winners, raw_results, groups_watchers


# 🔸 Применение результатов в БД: trader_winner-флаги и апсерты в trader_rating_active
async def _apply_results_to_db(
    group_winners: Dict[int, int | None],
    groups_watchers: Dict[int, List[int]],
    raw_results: Dict[int, Dict[str, Any]],
):
    if not groups_watchers:
        return

    group_ids = list(groups_watchers.keys())
    winner_ids = [sid for sid in group_winners.values() if sid is not None]

    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            # сброс флагов в обработанных группах
            await conn.execute(
                """
                UPDATE public.strategies_v4
                SET trader_winner = FALSE
                WHERE trader_watcher = TRUE
                  AND market_mirrow = ANY($1::int[])
                """,
                group_ids,
            )

            # установка флага победителям (если они есть)
            if winner_ids:
                await conn.execute(
                    """
                    UPDATE public.strategies_v4
                    SET trader_winner = TRUE
                    WHERE id = ANY($1::int[])
                    """,
                    winner_ids,
                )

            # апсерты в trader_rating_active по каждой группе
            for gm in group_ids:
                winner = group_winners.get(gm)
                raw_json = raw_results.get(gm, {"strategies": {}})
                await conn.execute(
                    """
                    INSERT INTO public.trader_rating_active AS tra (
                      group_master_id, current_winner_id, consecutive_wins, raw_results, last_run_at
                    ) VALUES
                      ($1, $2, CASE WHEN $2 IS NULL THEN 0 ELSE 1 END, $3::jsonb, (now() at time zone 'UTC'))
                    ON CONFLICT (group_master_id) DO UPDATE
                    SET
                      current_winner_id = EXCLUDED.current_winner_id,
                      consecutive_wins  = CASE
                                            WHEN EXCLUDED.current_winner_id IS NULL
                                              THEN 0
                                            WHEN tra.current_winner_id = EXCLUDED.current_winner_id
                                              THEN tra.consecutive_wins + 1
                                            ELSE 1
                                          END,
                      raw_results       = EXCLUDED.raw_results,
                      last_run_at       = (now() at time zone 'UTC')
                    """,
                    gm,
                    winner,
                    raw_json,  # asyncpg сам сконвертирует dict → jsonb
                )


# 🔸 Обновление in-memory списка победителей (глобальная переменная)
def _update_inmemory_winners(group_winners: Dict[int, int | None]) -> None:
    # очищаем и записываем только группы с валидными победителями
    current_group_winners.clear()
    for gm, sid in group_winners.items():
        if sid is not None:
            current_group_winners[gm] = sid