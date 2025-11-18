# indicators/supertrend.py
import pandas as pd

def compute(df: pd.DataFrame, params: dict) -> dict[str, float]:
    """
    Supertrend-индикатор в стиле TradingView ta.supertrend(factor, atrPeriod).

    Ожидаемые параметры:
        length (int) — период ATR (ATR Length)
        mult   (float) — множитель ATR (Factor)

    Требуемые колонки в df: 'h', 'l', 'c'
    На выходе:
        supertrend{length}_{mult}_   — значение линии Supertrend на последнем баре
        supertrend{length}_{mult}_trend — направление тренда (+1 / -1)
    """

    length = int(params.get("length", 10))
    mult = float(params.get("mult", 3.0))

    required_cols = {"h", "l", "c"}
    if not required_cols.issubset(df.columns):
        raise ValueError("Для расчёта Supertrend нужны колонки 'h', 'l', 'c'")

    df = df.copy()

    high = df["h"].astype(float)
    low = df["l"].astype(float)
    close = df["c"].astype(float)

    # True Range
    prev_close = close.shift(1)
    tr_components = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1)
    tr = tr_components.max(axis=1)

    # ATR (Wilder / RMA)
    atr = tr.ewm(alpha=1 / float(length), adjust=False).mean()

    # Базовые уровни
    hl2 = (high + low) / 2.0
    basic_upper = hl2 + mult * atr
    basic_lower = hl2 - mult * atr

    # Финальные уровни (подтягиваем к предыдущим)
    final_upper = basic_upper.copy()
    final_lower = basic_lower.copy()

    for i in range(1, len(df)):
        # если ATR ещё не прогрет — пропускаем
        if pd.isna(basic_upper.iat[i]) or pd.isna(basic_lower.iat[i]):
            continue

        # upper band
        if (basic_upper.iat[i] < final_upper.iat[i - 1]) or (close.iat[i - 1] > final_upper.iat[i - 1]):
            final_upper.iat[i] = basic_upper.iat[i]
        else:
            final_upper.iat[i] = final_upper.iat[i - 1]

        # lower band
        if (basic_lower.iat[i] > final_lower.iat[i - 1]) or (close.iat[i - 1] < final_lower.iat[i - 1]):
            final_lower.iat[i] = basic_lower.iat[i]
        else:
            final_lower.iat[i] = final_lower.iat[i - 1]

    # Серии Supertrend и направления
    supertrend = pd.Series(index=df.index, dtype=float)
    direction = pd.Series(index=df.index, dtype=float)

    # Находим первый бар, где уже есть валидные уровни
    start_idx = None
    for i in range(len(df)):
        if not (
            pd.isna(final_upper.iat[i])
            or pd.isna(final_lower.iat[i])
            or pd.isna(close.iat[i])
        ):
            start_idx = i
            break

    if start_idx is None:
        # недостаточно данных для расчёта
        return {}

    # Инициализация направления
    if close.iat[start_idx] <= final_upper.iat[start_idx]:
        supertrend.iat[start_idx] = final_upper.iat[start_idx]
        direction.iat[start_idx] = -1.0  # downtrend
    else:
        supertrend.iat[start_idx] = final_lower.iat[start_idx]
        direction.iat[start_idx] = 1.0   # uptrend

    # Основной цикл
    for i in range(start_idx + 1, len(df)):
        if pd.isna(final_upper.iat[i]) or pd.isna(final_lower.iat[i]):
            continue

        # предыдущий ST был на верхней границе
        if supertrend.iat[i - 1] == final_upper.iat[i - 1]:
            if close.iat[i] <= final_upper.iat[i]:
                supertrend.iat[i] = final_upper.iat[i]
                direction.iat[i] = -1.0
            else:
                supertrend.iat[i] = final_lower.iat[i]
                direction.iat[i] = 1.0
        # предыдущий ST был на нижней границе
        else:
            if close.iat[i] >= final_lower.iat[i]:
                supertrend.iat[i] = final_lower.iat[i]
                direction.iat[i] = 1.0
            else:
                supertrend.iat[i] = final_upper.iat[i]
                direction.iat[i] = -1.0

    st_valid = supertrend.dropna()
    dir_valid = direction.dropna()

    if st_valid.empty or dir_valid.empty:
        return {}

    last_st = float(st_valid.iloc[-1])
    last_dir = float(dir_valid.iloc[-1])

    # Имя в стиле bb: supertrend{length}_{mult}
    mult_str = str(round(mult, 2)).replace(".", "_")
    base = f"supertrend{length}_{mult_str}"

    return {
        base: last_st,
        f"{base}_trend": last_dir,
    }