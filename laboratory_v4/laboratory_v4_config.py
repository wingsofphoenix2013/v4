# laboratory_v4_config.py — конфигурация Laboratory v4 из ENV + справочник тестовых источников

import os

# 🔸 Тайминги
LAB_START_DELAY_SEC = int(os.getenv("LAB_START_DELAY_SEC", "0"))     # задержка перед стартом, с
LAB_REFRESH_SEC     = int(os.getenv("LAB_REFRESH_SEC", "300"))       # период рефреша кешей, с
LAB_BATCH_SIZE      = int(os.getenv("LAB_BATCH_SIZE", "5000"))       # размер пачки позиций по умолчанию

# ──────────────────────────────────────────────────────────────────────────────
# БАЗОВЫЕ ПРАВИЛА ЛАБОРАТОРИИ (интерпретация сущностей)
# ──────────────────────────────────────────────────────────────────────────────
#
# 1) laboratory_instances_v4:
#    - combine_logic: 'AND' | 'OR' — как объединять ВСЕ параметры (laboratory_parameters_v4) одного инстанса
#    - min_trade_type: 'absolute' | 'percent'
#         'absolute' -> min_trade_value = целое N (минимум закрытых сделок в строке агрегата)
#         'percent'  -> min_trade_value = доля (0..1) от total closed по стратегии+направлению
#    - min_winrate: порог winrate (0..1)
#
# 2) laboratory_parameters_v4:
#    - test_name: 'mw'|'emastat'|'rsi'|'bb'|'adx'|'dmigap'|'dmigap_trend'
#    - test_type: 'solo'|'comp'
#         'solo' -> работаем с таблицей per-TF (ключ включает timeframe)
#         'comp' -> работаем с таблицей композитов (триплет, без timeframe)
#    - test_tf: 'm5'|'m15'|'h1' или NULL (NULL при test_type='comp')
#    - param_spec: JSON со значимыми ключами для конкретного источника.
#
#    Примеры param_spec:
#    - mw + comp:
#         {"marker3_code": "1-0-3"}
#    - emastat + solo (по TF):
#         {"ema_len": 9, "state_code": [1,3]}
#    - emastat + comp (триплет):
#         {"ema_len": 9, "status_triplet": "1-0-3"}
#    - rsi + solo:
#         {"rsi_len": 14, "rsi_bin": [60,65,70]}
#    - bb + solo:
#         {"bb_len": 20, "bb_std": 2.0, "bin_code": [3,4,5,6,7,8]}
#    - adx + solo:
#         {"adx_len": 14, "bin_code": [20,25,30]}
#    - dmigap + solo:
#         {"dmi_len": 14, "gap_bin": [-10,-5,0,5,10]}
#    - dmigap_trend + solo:
#         {"dmi_len": 14, "trend_code": [1]}  # +1=rising, 0=stable, -1=falling
#
# 3) Пороги:
#    - ЧТО проверяем в строке агрегатора: closed_trades и winrate
#    - КАК проверяем минимум закрытых:
#         if min_trade_type == 'absolute':
#             требуем: closed_trades >= min_trade_value
#         if min_trade_type == 'percent':
#             требуем: closed_trades >= total_closed(strategy_id,direction) * min_trade_value
#
# 4) Объединение параметров (в одном инстансе):
#    - combine_logic='AND' => все параметры должны выполнить пороги
#    - combine_logic='OR'  => достаточно одного параметра
#
# ──────────────────────────────────────────────────────────────────────────────
# СПРАВОЧНИК ИСТОЧНИКОВ: КАКУЮ ТАБЛИЦУ И КАКИЕ КЛЮЧИ ИСПОЛЬЗОВАТЬ
# ──────────────────────────────────────────────────────────────────────────────
#
# Структура: AGG_SOURCE_MAP[test_name][test_type] -> словарь:
#   {
#     "table": <имя таблицы>,
#     "tf_field": <имя поля TF или None>,
#     "key_fields": [...],   # список обязательных ключей param_spec
#     "multi_keys": {...},   # ключи, где допускается список значений в param_spec
#   }
#
# Раннер будет использовать:
#   - table: строить SELECT из этой таблицы
#   - tf_field: если не None, добавлять фильтр timeframe = test_tf
#   - key_fields: проверять наличие в param_spec
#   - multi_keys: если param_spec[val] — список -> gen WHERE key IN (...)
#
AGG_SOURCE_MAP = {
    # Market Watcher (режимы): только композиты
    "mw": {
        "comp": {
            "table": "positions_marketwatcher_stat",
            "tf_field": None,
            "key_fields": ["marker3_code"],
            "multi_keys": {"marker3_code": True}
        }
    },

    # EMA Status
    "emastat": {
        "solo": {
            "table": "positions_emastatus_stat_tf",
            "tf_field": "timeframe",
            "key_fields": ["ema_len"],              # обязательный
            "multi_keys": {"state_code": True}     # допускается список state_code
        },
        "comp": {
            "table": "positions_emastatus_stat_comp",
            "tf_field": None,
            "key_fields": ["ema_len", "status_triplet"],
            "multi_keys": {"status_triplet": True}
        }
    },

    # RSI
    "rsi": {
        "solo": {
            "table": "positions_rsibins_stat_tf",
            "tf_field": "timeframe",
            "key_fields": ["rsi_len"],
            "multi_keys": {"rsi_bin": True}
        },
        "comp": {
            "table": "positions_rsibins_stat_comp",
            "tf_field": None,
            "key_fields": ["rsi_len", "status_triplet"],
            "multi_keys": {"status_triplet": True}
        }
    },

    # Bollinger Bands
    "bb": {
        "solo": {
            "table": "positions_bbbins_stat_tf",
            "tf_field": "timeframe",
            "key_fields": ["bb_len", "bb_std"],
            "multi_keys": {"bin_code": True}
        },
        "comp": {
            "table": "positions_bbbins_stat_comp",
            "tf_field": None,
            "key_fields": ["bb_len", "bb_std", "status_triplet"],
            "multi_keys": {"status_triplet": True}
        }
    },

    # ADX
    "adx": {
        "solo": {
            "table": "positions_adxbins_stat_tf",
            "tf_field": "timeframe",
            "key_fields": ["adx_len"],
            "multi_keys": {"bin_code": True}
        },
        "comp": {
            "table": "positions_adxbins_stat_comp",
            "tf_field": None,
            "key_fields": ["status_triplet"],
            "multi_keys": {"status_triplet": True}
        }
    },

    # DMI GAP (величина)
    "dmigap": {
        "solo": {
            "table": "positions_dmigap_stat_tf",
            "tf_field": "timeframe",
            "key_fields": ["dmi_len"],
            "multi_keys": {"gap_bin": True}
        },
        "comp": {
            "table": "positions_dmigap_stat_comp",
            "tf_field": None,
            "key_fields": ["status_triplet"],
            "multi_keys": {"status_triplet": True}
        }
    },

    # DMI GAP (тренд: -1/0/+1)
    "dmigap_trend": {
        "solo": {
            "table": "positions_dmigaptrend_stat_tf",
            "tf_field": "timeframe",
            "key_fields": ["dmi_len"],
            "multi_keys": {"trend_code": True}
        },
        "comp": {
            "table": "positions_dmigaptrend_stat_comp",
            "tf_field": None,
            "key_fields": ["status_triplet"],
            "multi_keys": {"status_triplet": True}
        }
    },
}

# ──────────────────────────────────────────────────────────────────────────────
# ДОПОЛНИТЕЛЬНЫЕ КОНСТАНТЫ ДЛЯ РАННЕРА
# ──────────────────────────────────────────────────────────────────────────────

# допустимые значения test_name и test_type
ALLOWED_TEST_NAMES = set(AGG_SOURCE_MAP.keys())
ALLOWED_TEST_TYPES = {"solo", "comp"}

# поля метрик в агрегатах (на них проверяются пороги)
AGG_METRICS = {
    "closed_trades": "closed_trades",
    "winrate": "winrate",
    "pnl_sum": "pnl_sum",
    "avg_pnl": "avg_pnl",
}

# значения по умолчанию, если инстанс не задаёт
DEFAULT_MIN_TRADE_TYPE  = "absolute"   # или "percent"
DEFAULT_MIN_TRADE_VALUE = 2            # 2 сделки (для absolute)
DEFAULT_MIN_WINRATE     = 0.50         # 50%