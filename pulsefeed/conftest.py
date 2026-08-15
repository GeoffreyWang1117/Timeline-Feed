"""Make the test suite runnable from the repository root as well as from here.

Without this, ``pytest pulsefeed/tests`` from the repo root resolves ``import
pulsefeed`` to the *project directory* (which has no ``__init__.py``) instead of
the package inside it, and every import fails.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
