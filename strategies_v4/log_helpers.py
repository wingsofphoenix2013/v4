# log_helpers.py

import json
import logging
from datetime import datetime

from infra import infra

log = logging.getLogger("LOG_HELPER")

# 🔸 Логирование действия маршрута SL-protect
async def route_protect(strategy_id, symbol, log_uid, note, position_uid, sl_targets=None):
    record = {
        "log_uid": log_uid,
        "strategy_id": str(strategy_id),
        "status": "protect",
        "note": note,
        "position_uid": str(position_uid),
        "logged_at": datetime.utcnow().isoformat()
    }

    if sl_targets:
        record["sl_targets"] = json.dumps(sl_targets, default=str)

    try:
        await infra.redis_client.xadd("signal_log_queue", record)
    except Exception:
        log.exception("❌ Ошибка при логировании protect в signal_log_queue")