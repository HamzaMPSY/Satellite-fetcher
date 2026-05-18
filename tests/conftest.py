from __future__ import annotations

import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
TEST_RUNTIME_ROOT = ROOT / ".pytest_runtime"
TEST_HOME = TEST_RUNTIME_ROOT / "home"
TEST_CACHE = TEST_RUNTIME_ROOT / "cache"
TEST_CONFIG = TEST_RUNTIME_ROOT / "config"
TEST_DATA = TEST_RUNTIME_ROOT / "data"
TEST_MPL = TEST_RUNTIME_ROOT / "mpl"

for directory in (
    TEST_HOME,
    TEST_CACHE,
    TEST_CONFIG,
    TEST_DATA,
    TEST_MPL,
):
    directory.mkdir(parents=True, exist_ok=True)

# Scientific dependencies may try to write into user-scoped cache/config paths
# on import. Point them to a repo-local test runtime so the suite remains
# hermetic and works in sandboxed environments.
os.environ["HOME"] = str(TEST_HOME)
os.environ["XDG_CACHE_HOME"] = str(TEST_CACHE)
os.environ["XDG_CONFIG_HOME"] = str(TEST_CONFIG)
os.environ["XDG_DATA_HOME"] = str(TEST_DATA)
os.environ["MPLCONFIGDIR"] = str(TEST_MPL)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
