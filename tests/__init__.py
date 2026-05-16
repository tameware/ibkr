"""Unit tests for IBKR bots. Run from repo root:

    python -m unittest discover -s tests -t . -v
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
