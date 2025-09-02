# laboratory_v4_config.py — конфигурация Laboratory v4 из ENV

import os

# 🔸 Тайминги
LAB_START_DELAY_SEC = int(os.getenv("LAB_START_DELAY_SEC", "0"))     # задержка перед стартом, с
LAB_REFRESH_SEC     = int(os.getenv("LAB_REFRESH_SEC", "300"))       # период рефреша кешей, с

# 🔸 Параметры лаборатории (на будущее)
LAB_BATCH_SIZE      = int(os.getenv("LAB_BATCH_SIZE", "5000"))       # размер пачки позиций по умолчанию