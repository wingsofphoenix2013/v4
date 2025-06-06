# indicators/adx_dmi.py
import pandas as pd

def compute(df: pd.DataFrame, params: dict) -> dict[str, float]:
    """
    Расчёт ADX / DMI по стандартной формуле:
    +DI, -DI, ADX

    Параметры:
        length: период сглаживания (обычно 14)

    Требуемые колонки в df: 'h', 'l', 'c'
    """
    length = int(params.get("length", 14))

    high = df["h"].astype(float)
    low = df["l"].astype(float)
    close = df["c"].astype(float)

    plus_dm = high.diff()
    minus_dm = -low.diff()

    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm < 0] = 0

    plus_dm[plus_dm <= minus_dm] = 0
    minus_dm[minus_dm < plus_dm] = 0

    tr1 = (high - low).abs()
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.rolling(length).mean()
    plus_di = 100 * (plus_dm.rolling(length).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(length).mean() / atr)

    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    adx = dx.rolling(length).mean()

    result = {
        "adx": round(float(adx.dropna().iloc[-1]), 2),
        "plus_di": round(float(plus_di.dropna().iloc[-1]), 2),
        "minus_di": round(float(minus_di.dropna().iloc[-1]), 2)
    }

    return result