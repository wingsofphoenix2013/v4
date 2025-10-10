# indicators/kama.py
import pandas as pd
import numpy as np

def compute(df: pd.DataFrame, params: dict) -> dict[str, float]:
    """
    Расчёт адаптивной скользящей средней Kaufman (KAMA).
    Формула совместима с TradingView (на базе close).

    Параметры:
        length: период окна (обычно 10)

    Возвращает:
        value — последнее значение KAMA
    """
    length = int(params.get("length", 10))
    close = df["c"].astype(float)

    if len(close) < length + 1:
        raise ValueError("Недостаточно данных для расчёта KAMA")

    change = close.diff(length).abs()
    volatility = close.diff().abs().rolling(length).sum()
    er = change / volatility.replace(0, 1e-9)  # efficiency ratio

    sc = (er * (2/(2+1) - 2/(30+1)) + 2/(30+1)) ** 2  # smoothing constant

    kama = close.copy()
    for i in range(length, len(close)):
        kama.iloc[i] = kama.iloc[i-1] + sc.iloc[i] * (close.iloc[i] - kama.iloc[i-1])

    return {"value": float(kama.dropna().iloc[-1])}