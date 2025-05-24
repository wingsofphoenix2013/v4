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

    if "c" not in df:
        raise ValueError("DataFrame должен содержать колонку 'c' (close)")

    ema_series = df["c"].ewm(span=length, adjust=False).mean()

    return {"value": float(ema_series.iloc[-1])}
