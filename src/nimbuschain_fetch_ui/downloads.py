"""Download management helpers (state, logs, CLI building)."""
from __future__ import annotations

import math
import os
import re
import signal
import subprocess
import time
import datetime as dt
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st
from loguru import logger

from nimbuschain_fetch_ui.constants import (
    DOWNLOADS_DIR,
    NOHUP_PATH,
    PID_PATH,
    PROVIDER_CLI_MAP,
)


# ───────────────────────────── Helpers ─────────────────────────────
def _ss(key, default=None):
    return st.session_state.get(key, default)


def _close_log_fh():
    fh = st.session_state.pop("_dl_log_fh", None)
    if fh is not None:
        try:
            if not fh.closed:
                fh.close()
                logger.debug("[DL] Closed log file handle")
        except Exception as e:
            logger.warning(f"[DL] Error closing log fh: {e}")


def reset_downloads(dl_dir: Optional[str] = None, clear_files: bool = True):
    logger.info(f"[DL] reset_downloads() called (clear_files={clear_files})")
    dl_path = Path(dl_dir) if dl_dir else DOWNLOADS_DIR
    if clear_files:
        if dl_path.exists():
            import shutil
            shutil.rmtree(dl_path, ignore_errors=True)
        dl_path.mkdir(parents=True, exist_ok=True)
    else:
        dl_path.mkdir(parents=True, exist_ok=True)

    _close_log_fh()
    try:
        NOHUP_PATH.write_text("")
    except Exception:
        pass
    try:
        PID_PATH.write_text("")
    except Exception:
        pass
    for key in list(st.session_state.keys()):
        if key.startswith("dl_") or key == "_dl_log_fh":
            del st.session_state[key]
    st.session_state.update({
        "dl_start_time": None, "dl_total_products": 0,
        "dl_completed": 0, "dl_running": False,
    })
    logger.info("[DL] Reset complete")


@st.cache_data(ttl=2, show_spinner=False)
def count_downloaded_products(dl_dir: Optional[str] = None):
    dl_path = Path(dl_dir) if dl_dir else DOWNLOADS_DIR
    if not dl_path.exists():
        return 0, 0.0
    total_files = 0
    total_size_bytes = 0
    for file_path in iter_download_files(dl_path):
        try:
            stat = file_path.stat()
        except OSError:
            continue
        total_files += 1
        total_size_bytes += stat.st_size
    return total_files, total_size_bytes / (1024 * 1024)


def iter_download_files(dl_dir: str | Path | None = None):
    dl_path = Path(dl_dir) if dl_dir else DOWNLOADS_DIR
    if not dl_path.exists():
        return

    root = dl_path.resolve()
    for parent, dirnames, filenames in os.walk(root, topdown=True, onerror=_ignore_scan_error):
        parent_path = Path(parent)
        dirnames[:] = [
            dirname
            for dirname in dirnames
            if not _skip_download_scan_dir(parent_path / dirname, root)
        ]
        for filename in filenames:
            file_path = parent_path / filename
            try:
                if file_path.is_file():
                    yield file_path
            except OSError:
                continue


def _skip_download_scan_dir(path: Path, root: Path) -> bool:
    try:
        relative_parts = tuple(part.lower() for part in path.resolve().relative_to(root).parts)
    except (OSError, ValueError):
        relative_parts = tuple(part.lower() for part in path.parts)
    if len(relative_parts) >= 2 and relative_parts[-2:] == ("sen2like", "spark"):
        return True
    if len(relative_parts) >= 3 and relative_parts[-3:-1] == ("sen2like", "spark"):
        return True
    return False


def _ignore_scan_error(_error: OSError) -> None:
    return None


def parse_download_logs(path: Optional[str] = None):
    lp = Path(path) if path else NOHUP_PATH
    if not lp.exists():
        logger.debug(f"[DL] Log file does not exist: {lp}")
        return {"batch": None, "files": {}, "logs": [], "products_found": 0,
                "errors": [], "phase": "starting"}

    brx = re.compile(r"Concurrent Downloads:\s*(?P<pct>\d+)%\|.*?\|\s*(?P<d>\d+)/(?P<tot>\d+)")
    drx = re.compile(r"Downloading\s+(?P<fn>.+?):\s*(?P<pct>\d+)%\|.*?\|\s*(?P<d>[\d.]+\S*)/(?P<t>[\d.]+\S*)\s*\[(?:.+?)<(?P<eta>[0-9:?\-]+)\]")

    prx = re.compile(r"Found\s+(?P<n>\d+)\s+products?", re.IGNORECASE)
    search_rx = re.compile(r"Searching for products", re.IGNORECASE)
    config_rx = re.compile(r"Configuration loaded", re.IGNORECASE)
    geom_rx = re.compile(r"Geometry loaded", re.IGNORECASE)
    provider_rx = re.compile(r"Initialized provider", re.IGNORECASE)
    done_rx = re.compile(r"completed successfully", re.IGNORECASE)
    downloading_rx = re.compile(r"Downloading all products", re.IGNORECASE)

    erx = re.compile(r"(ERROR\s*\||Traceback \(most recent|raise \w+Error|Exception:)", re.IGNORECASE)

    result = {"batch": None, "files": {}, "logs": [], "products_found": 0,
              "errors": [], "phase": "starting"}

    try:
        text = lp.read_text(errors="replace")
        text = text.replace("\r", "\n")
    except Exception as e:
        logger.warning(f"[DL] Failed to read log file: {e}")
        return result

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        if (m := brx.search(line)):
            result["batch"] = {
                "done": int(m.group("d")),
                "pct": int(m.group("pct")),
                "total": int(m.group("tot")),
            }
            result["phase"] = "downloading"
            continue

        if (m := drx.search(line)):
            result["files"][m.group("fn")] = {
                "pct": int(m.group("pct")),
                "done": m.group("d"),
                "total": m.group("t"),
                "eta": m.group("eta"),
            }
            result["phase"] = "downloading"
            continue

        if (m := prx.search(line)):
            result["products_found"] = max(result["products_found"], int(m.group("n")))
            if result["phase"] != "downloading":
                result["phase"] = "found"
            continue

        if done_rx.search(line):
            result["phase"] = "done"
            continue
        if downloading_rx.search(line):
            result["phase"] = "downloading"
            continue
        if search_rx.search(line):
            if result["phase"] in ("starting", "ready"):
                result["phase"] = "searching"
            continue
        if provider_rx.search(line):
            if result["phase"] == "starting":
                result["phase"] = "ready"
            continue
        if config_rx.search(line) or geom_rx.search(line):
            if result["phase"] == "starting":
                result["phase"] = "initializing"
            continue

        if erx.search(line):
            result["errors"].append(line)
        else:
            result["logs"].append(line)
            if len(result["logs"]) > 30:
                result["logs"] = result["logs"][-30:]
    return result


def _read_pid() -> Optional[int]:
    try:
        txt = PID_PATH.read_text().strip()
        return int(txt) if txt else None
    except Exception:
        return None


def _write_pid(pid: int) -> None:
    try:
        PID_PATH.write_text(str(pid))
        logger.debug(f"[DL] Wrote PID {pid} to {PID_PATH}")
    except Exception as e:
        logger.warning(f"[DL] Failed to write PID: {e}")


def _pid_is_running(pid: Optional[int]) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _terminate_pid(pid: Optional[int], grace_seconds: float = 1.5) -> bool:
    if not pid:
        return True
    if not _pid_is_running(pid):
        return True
    try:
        os.kill(pid, signal.SIGTERM)
    except Exception as e:
        logger.warning(f"[DL] Failed to SIGTERM PID {pid}: {e}")
    deadline = time.time() + max(0.0, grace_seconds)
    while time.time() < deadline:
        if not _pid_is_running(pid):
            return True
        time.sleep(0.1)
    if _pid_is_running(pid):
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception as e:
            logger.warning(f"[DL] Failed to SIGKILL PID {pid}: {e}")
    return not _pid_is_running(pid)


def _find_cli_pids() -> List[int]:
    try:
        cp = subprocess.run(
            ["pgrep", "-f", "cli.py --provider"],
            capture_output=True,
            text=True,
            check=False,
        )
        pids: List[int] = []
        for line in (cp.stdout or "").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                pid = int(line)
            except Exception:
                continue
            if pid != os.getpid():
                pids.append(pid)
        return sorted(set(pids))
    except Exception as e:
        logger.debug(f"[DL] Unable to list CLI processes with pgrep: {e}")
        return []


def _unlock_download_runtime(kill_orphans: bool = False) -> bool:
    ok = True
    pid = st.session_state.get("dl_pid") or _read_pid()
    if pid and _pid_is_running(pid):
        logger.info(f"[DL] Unlock: terminating active PID {pid}")
        ok = _terminate_pid(pid) and ok

    if kill_orphans:
        for opid in _find_cli_pids():
            if pid and opid == pid:
                continue
            if _pid_is_running(opid):
                logger.info(f"[DL] Unlock: terminating orphan PID {opid}")
                ok = _terminate_pid(opid) and ok

    _close_log_fh()
    st.session_state["dl_running"] = False
    st.session_state.pop("dl_pid", None)
    try:
        PID_PATH.write_text("")
    except Exception:
        pass
    return ok


def _bootstrap_download_runtime() -> None:
    if st.session_state.get("_dl_bootstrapped", False):
        return

    pid = st.session_state.get("dl_pid") or _read_pid()
    alive = _pid_is_running(pid)

    if alive:
        st.session_state["dl_running"] = True
        st.session_state["dl_pid"] = pid
    else:
        st.session_state["dl_running"] = False
        st.session_state.pop("dl_pid", None)
        try:
            PID_PATH.write_text("")
        except Exception:
            pass
        try:
            if NOHUP_PATH.exists() and NOHUP_PATH.stat().st_size > 0:
                NOHUP_PATH.write_text("")
        except Exception:
            pass

    st.session_state["_dl_bootstrapped"] = True


def _recent_rate_limit_hits(path: Optional[Path] = None, tail_chars: int = 25000) -> int:
    lp = path or NOHUP_PATH
    if not lp.exists():
        return 0
    try:
        raw = lp.read_text(errors="replace")
    except Exception:
        return 0
    tail = raw[-tail_chars:] if len(raw) > tail_chars else raw
    return len(re.findall(r"(?:\b429\b|rate limit)", tail, flags=re.IGNORECASE))


def _auto_parallel_strategy(
    provider: str,
    start_date: dt.date,
    end_date: dt.date,
    preview_total: int,
    selected_tile_count: int,
) -> Dict[str, int]:
    try:
        n_days = max(1, (end_date - start_date).days + 1)
    except Exception:
        n_days = 1

    est_products = max(1, int(preview_total or 0), int(selected_tile_count or 0))
    recent_429 = _recent_rate_limit_hits()

    if provider == "Copernicus":
        parallel_days = 1
        concurrent_per_day = 2
        if n_days >= 10 and est_products >= 30 and recent_429 == 0:
            parallel_days = 3
            concurrent_per_day = 2
        elif n_days >= 3 and est_products >= 10:
            parallel_days = 2
            concurrent_per_day = 2
        elif est_products >= 4:
            parallel_days = 1
            concurrent_per_day = 3

        if recent_429 >= 10:
            parallel_days = 1
            concurrent_per_day = 1
        elif recent_429 >= 5:
            parallel_days = min(parallel_days, 2)
            concurrent_per_day = max(1, concurrent_per_day - 1)
        elif recent_429 >= 2 and parallel_days >= 3:
            parallel_days = 2

        concurrent_per_day = max(1, min(3, concurrent_per_day))
        total = max(1, parallel_days * concurrent_per_day)
        if total > 6:
            parallel_days = max(1, 6 // concurrent_per_day)
        return {
            "max_concurrent": max(1, min(4, concurrent_per_day if parallel_days > 1 else total)),
            "parallel_days": max(1, parallel_days),
            "concurrent_per_day": max(1, concurrent_per_day),
        }

    if provider == "USGS":
        if est_products <= 2:
            mc = 3
        elif est_products <= 8:
            mc = 6
        elif est_products <= 20:
            mc = 8
        else:
            mc = 10
        if n_days >= 7:
            mc = min(12, mc + 2)
        if recent_429 >= 5:
            mc = max(2, mc - 2)
        return {"max_concurrent": mc, "parallel_days": 1, "concurrent_per_day": 1}

    return {"max_concurrent": 4, "parallel_days": 1, "concurrent_per_day": 1}


def _build_download_command(
    provider, satellite, product, start_date, end_date, aoi_file,
    selected_tiles=None,
    max_concurrent: int = 4,
    parallel_days: int = 1,
    concurrent_per_day: int = 2,
):
    from nimbuschain_fetch_ui.constants import PROJECT_ROOT

    cli_path = None
    candidates = [
        PROJECT_ROOT / "cli.py",
        PROJECT_ROOT / "src" / "cli.py",
    ]
    for candidate in candidates:
        if candidate.exists():
            cli_path = str(candidate)
            break
    if not cli_path:
        return None, "cli.py not found — check your project structure"

    import shlex

    cli_provider = PROVIDER_CLI_MAP.get(provider)
    if not cli_provider:
        cli_provider = provider.lower().replace(" ", "_")
        logger.warning(
            f"[DL] Provider '{provider}' not in PROVIDER_CLI_MAP, falling back to '{cli_provider}'"
        )

    collection = str(satellite).split(" ")[0]

    aoi_path = Path(aoi_file)
    if not aoi_path.is_absolute():
        aoi_path = Path(PROJECT_ROOT) / aoi_path

    cmd_parts = [
        sys.executable,
        "-u",
        cli_path,
        "--provider",
        cli_provider,
        "--collection",
        collection,
    ]

    try:
        mc = max(1, int(max_concurrent))
        cmd_parts.extend(["--max-concurrent", str(mc)])
    except Exception:
        pass

    if cli_provider == "copernicus":
        try:
            pd = max(1, int(parallel_days))
            cmd_parts.extend(["--parallel-days", str(pd)])
        except Exception:
            pass
        try:
            cpd = max(1, int(concurrent_per_day))
            cmd_parts.extend(["--concurrent-per-day", str(cpd)])
        except Exception:
            pass

    if product and str(product).strip():
        cmd_parts.extend(["--product-type", str(product)])

    if selected_tiles and cli_provider == "copernicus":
        if len(selected_tiles) == 1:
            cmd_parts.extend(["--tile-id", selected_tiles[0]])

    cmd_parts.extend(
        [
            "--start-date",
            str(start_date),
            "--end-date",
            str(end_date),
            "--aoi-file",
            str(aoi_path),
            "--log-type",
            "all",
        ]
    )
    cmd = " ".join(shlex.quote(str(part)) for part in cmd_parts)
    logger.info(f"[DL] Built command: {cmd}")
    return cmd, None


__all__ = [
    "reset_downloads",
    "count_downloaded_products",
    "parse_download_logs",
    "_auto_parallel_strategy",
    "_build_download_command",
    "_bootstrap_download_runtime",
    "_unlock_download_runtime",
    "_read_pid",
    "_pid_is_running",
]
