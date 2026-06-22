"""可插拔日志接口。"""

from __future__ import annotations

import sys
from typing import Protocol, runtime_checkable


@runtime_checkable
class Logger(Protocol):
    """日志接口。实现 ``log(msg)`` 即可接入任意日志框架。"""

    def log(self, msg: str) -> None: ...


class DefaultLogger:
    """默认日志实现：带 ``[MagicQueue]`` 前缀输出到 stderr。"""

    def log(self, msg: str) -> None:
        print(f"[MagicQueue] {msg}", file=sys.stderr, flush=True)
