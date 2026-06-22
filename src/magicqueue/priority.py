"""消息优先级：高 / 普通 / 低三档。

``Payload.priority`` 按符号映射：

- ``> 0``  -> 高 (HIGH)
- ``== 0`` -> 普通 (NORMAL，默认)
- ``< 0``  -> 低 (LOW)

同一档内保持 FIFO。底层每个逻辑队列拆成三个子队列（key 加后缀 ``:p2/:p1/:p0``），
消费时按 高 -> 普通 -> 低 的顺序取消息。
"""

from __future__ import annotations

NUM_LEVELS = 3

LEVEL_LOW = 0
LEVEL_NORMAL = 1
LEVEL_HIGH = 2

# 子队列被消费的优先顺序（高 -> 普通 -> 低）。
POP_ORDER = (LEVEL_HIGH, LEVEL_NORMAL, LEVEL_LOW)


def priority_level(priority: int) -> int:
    """将 priority 映射到子队列档位。"""
    if priority > 0:
        return LEVEL_HIGH
    if priority < 0:
        return LEVEL_LOW
    return LEVEL_NORMAL


def level_sub_key(base: str, level: int) -> str:
    """返回某个优先级档位对应的子队列 key。"""
    return f"{base}:p{level}"


def ordered_keys(base: str) -> list[str]:
    """返回按消费优先级（高 -> 低）排列的子队列 key 列表。"""
    return [level_sub_key(base, level) for level in POP_ORDER]
