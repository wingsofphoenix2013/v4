# bt_analysis_utils.py — вспомогательные утилиты для анализаторов фич backtester_v1

import logging
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional, Tuple


# 🔸 Настройки Decimal
getcontext().prec = 28


# 🔸 Квантование чисел до 4 знаков (Decimal)
def q4(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


# 🔸 Безопасное деление для Decimal (0 при делении на 0)
def safe_div(n: Decimal, d: Decimal) -> Decimal:
    if d == 0:
        return Decimal("0")
    return n / d


# 🔸 Разруливание feature_name по семейству/ключу/TF/source_key
def resolve_feature_name(
    family_key: str,
    key: str,
    timeframe: str,
    source_key: str,
) -> str:
    """
    Общая точка формирования имени фичи. Сейчас реализована ветка для семейства RSI,
    остальные семьи могут расширяться позже.
    """
    family = (family_key or "").lower()
    tf = str(timeframe).strip()
    src = str(source_key).strip()
    k = str(key).strip()

    if family == "rsi":
        if k == "rsi_value":
            return f"rsi_value_{tf}_{src}"
        if k == "rsi_dist_from_50":
            return f"rsi_dist_from_50_{tf}_{src}"
        if k == "rsi_zone":
            return f"rsi_zone_{tf}_{src}"
        return f"{k}_{tf}_{src}"

    # базовый fallback для будущих семейств
    return f"{family}_{k}_{tf}_{src}"


# 🔸 Универсальная запись агрегатов фич в bt_scenario_feature_bins (v1 по умолчанию)
async def write_feature_bins(
    pg,
    *,
    scenario_id: int,
    signal_id: int,
    timeframe: str,
    feature_name: str,
    agg: Dict[Tuple[str, str], Dict[str, Any]],
    deposit: Optional[Decimal],
    inst_id: Optional[int] = None,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Запись агрегированных бинов фичи в bt_scenario_feature_bins.

    Ожидается, что agg имеет структуру:
        key: (direction, bin_label)
        value: {
            "bin_from": float,
            "bin_to": float,
            "trades": int,
            "wins": int,
            "losses": int,
            "pnl_abs_total": Decimal,
        }

    version/analysis_window пока не трогаем — полагаемся на default 'v1' и NULL в схеме.
    """

    log = logger or logging.getLogger("BT_ANALYSIS_UTILS")

    # если агрегаций нет — просто чистим старые бины по этой фиче/TF
    if not agg:
        log.debug(
            "BT_ANALYSIS_UTILS: inst_id=%s, feature_name=%s — нет данных для записи (agg пустой), "
            "очищаем старые бины",
            inst_id,
            feature_name,
        )
        async with pg.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM bt_scenario_feature_bins
                WHERE scenario_id  = $1
                  AND signal_id    = $2
                  AND timeframe    = $3
                  AND feature_name = $4
                """,
                scenario_id,
                signal_id,
                timeframe,
                feature_name,
            )
        return

    rows_to_insert: List[Tuple[Any, ...]] = []

    for (direction, bin_label), stat in agg.items():
        trades = int(stat["trades"])
        wins = int(stat["wins"])
        losses = int(stat["losses"])
        pnl_abs_total = Decimal(str(stat["pnl_abs_total"]))

        if trades <= 0:
            continue

        winrate = safe_div(Decimal(wins), Decimal(trades))
        if deposit is not None and deposit != 0:
            roi = safe_div(pnl_abs_total, deposit)
        else:
            roi = Decimal("0")

        rows_to_insert.append(
            (
                scenario_id,                 # scenario_id
                signal_id,                   # signal_id
                direction,                   # direction
                timeframe,                   # timeframe
                feature_name,                # feature_name
                bin_label,                   # bin_label
                stat["bin_from"],            # bin_from
                stat["bin_to"],              # bin_to
                trades,                      # trades
                wins,                        # wins
                losses,                      # losses
                q4(pnl_abs_total),           # pnl_abs_total
                q4(winrate),                 # winrate
                q4(roi),                     # roi
            )
        )

    async with pg.acquire() as conn:
        # сначала удаляем старые бины по этой фиче/TF для данного сценария/сигнала
        await conn.execute(
            """
            DELETE FROM bt_scenario_feature_bins
            WHERE scenario_id  = $1
              AND signal_id    = $2
              AND timeframe    = $3
              AND feature_name = $4
            """,
            scenario_id,
            signal_id,
            timeframe,
            feature_name,
        )

        if rows_to_insert:
            await conn.executemany(
                """
                INSERT INTO bt_scenario_feature_bins (
                    scenario_id,
                    signal_id,
                    direction,
                    timeframe,
                    feature_name,
                    bin_label,
                    bin_from,
                    bin_to,
                    trades,
                    wins,
                    losses,
                    pnl_abs_total,
                    winrate,
                    roi
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9, $10,
                    $11, $12, $13, $14
                )
                """,
                rows_to_insert,
            )

    log.debug(
        "BT_ANALYSIS_UTILS: inst_id=%s, feature_name=%s, timeframe=%s — бинов записано=%s",
        inst_id,
        feature_name,
        timeframe,
        len(rows_to_insert),
    )