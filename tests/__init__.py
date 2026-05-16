"""Unit tests for IBKR bots. Run from repo root:

    python -m unittest discover -s tests -t . -v
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_SRC = _ROOT / "src"
for _path in (_SRC, _ROOT):
    _s = str(_path)
    if _s not in sys.path:
        sys.path.insert(0, _s)
