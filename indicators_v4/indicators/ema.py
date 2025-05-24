# indicators/ema.py
import pandas as pd

def compute(df: pd.DataFrame, params: dict) -> dict[str, float]:
    """
    Рассчитывает EMA по цене закрытия.

    params:
        length: int — длина окна EMA

    Возвращает:
        {'value': float} — последнее значение EMA
    """
    length = int(params.get("length", 14))

    if "c" not in df or df["c"].isna().all():
        raise ValueError("Нет данных в колонке 'c' для расчёта EMA")

    df = df.copy()
    ema_series = df["c"].astype(float).ewm(span=length, adjust=False).mean()

    last_valid = ema_series.dropna()
    value = float(last_valid.iloc[-1]) if not last_valid.empty else float("nan")

    return {"value": value}