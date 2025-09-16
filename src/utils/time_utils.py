from __future__ import annotations

from datetime import datetime, timezone
from dateutil import parser


def parse_time_to_ms(value: str | int) -> int:
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text.isdigit():
        # 纯数字当作毫秒
        return int(text)
    # 解析日期字符串为 UTC 毫秒
    dt = parser.parse(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000) 