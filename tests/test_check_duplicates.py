"""Host tests for scripts/check_duplicates.py and scripts/failure_demo.ps1 (no Spark, no Docker)."""
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "check_duplicates.py"
DEMO = ROOT / "scripts" / "failure_demo.ps1"
WATCH = ROOT / "scripts" / "watch_demo.ps1"
# The pipeline start command. failure_demo.ps1 restart must use exactly what watch_demo.ps1 uses.
PIPELINE_CMD = ("docker compose run --rm --name fs-pipeline -e 'FS_SPARK_MASTER=local[4]' spark python -m streaming.jobs.pipeline "
                "--lake data/lake --checkpoints data/checkpoints --show $Sample --live-store")


def _run(*args):
    return subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True, timeout=60)


def test_help():
    r = _run("--help")
    assert r.returncode == 0 and "--lake" in r.stdout


def test_missing_lake_exits_2_without_traceback(tmp_path):
    r = _run("--lake", str(tmp_path / "nope"))
    assert r.returncode == 2
    assert "Lake path not found" in r.stdout
    assert "Traceback" not in r.stderr and "Traceback" not in r.stdout


def test_lake_without_silver_folder_exits_2(tmp_path):
    r = _run("--lake", str(tmp_path))
    assert r.returncode == 2 and "Lake path not found" in r.stdout


def test_output_format():
    sys.path.insert(0, str(ROOT))
    from scripts.check_duplicates import format_line
    line = format_line("silver_events", ["event_id"], 120, 0)
    assert line.startswith("silver_events") and "rows=120" in line and "duplicate_keys=0" in line


def test_tables_use_the_keys_defined_in_the_writer():
    text = SCRIPT.read_text()
    assert "SILVER_KEYS[table]" in text
    for t in ("silver_events", "silver_orders", "silver_order_status", "silver_order_items"):
        assert t in text


def test_inventory_and_express_tables_are_checked_with_their_writer_keys():
    sys.path.insert(0, str(ROOT))
    from scripts.check_duplicates import TABLES
    from streaming.silver.writer import SILVER_KEYS
    assert len(TABLES) == len(set(TABLES))
    for t, key in (("silver_stock_movements", ["event_id"]), ("silver_stock_snapshots", ["event_id"]),
                   ("silver_express_orders", ["order_id"])):
        assert t in TABLES
        assert SILVER_KEYS[t] == key


def test_failure_demo_kills_hard_and_never_redirects_stderr():
    text = DEMO.read_text()
    assert "docker kill fs-pipeline" in text
    assert "2>&1" not in text
    assert "docker stop" not in text


def test_failure_demo_restart_command_matches_watch_demo():
    assert PIPELINE_CMD in WATCH.read_text()
    assert PIPELINE_CMD in DEMO.read_text()


@pytest.mark.skipif(shutil.which("powershell") is None, reason="PowerShell not available")
def test_failure_demo_parses():
    cmd = ("$e = $null; [void][System.Management.Automation.Language.Parser]::ParseFile('%s', [ref]$null, [ref]$e); "
           "if ($e -and $e.Count) { $e | ForEach-Object { $_.Message }; exit 1 }" % DEMO)
    r = subprocess.run(["powershell", "-NoProfile", "-Command", cmd], capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stdout + r.stderr
