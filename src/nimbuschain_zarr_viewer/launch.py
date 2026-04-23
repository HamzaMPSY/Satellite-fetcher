from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    repo_root = Path(__file__).resolve().parents[2]
    browser_path = Path(__file__).resolve().with_name("browser.py")
    command = [sys.executable, "-m", "streamlit", "run", str(browser_path)]
    if argv:
        command.extend(argv)
    return subprocess.call(command, cwd=str(repo_root))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
