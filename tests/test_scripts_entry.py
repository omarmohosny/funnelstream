"""Scripts must run as plain files from any working directory (repo root not on sys.path by default)."""
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ["run_simulator.py", "seed_silver_reference.py", "show_live.py", "gen_ddl.py", "apply_ddl.py",
           "publish_silver.py", "check_databricks.py"]


@pytest.mark.parametrize("name", SCRIPTS)
def test_script_help_as_plain_file_from_other_cwd(name, tmp_path):
    r = subprocess.run([sys.executable, str(ROOT / "scripts" / name), "--help"], cwd=tmp_path,
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr
    assert "usage" in r.stdout.lower()


@pytest.mark.parametrize("name", [n[:-3] for n in SCRIPTS])
def test_script_help_as_module(name, tmp_path):
    r = subprocess.run([sys.executable, "-m", "scripts." + name, "--help"], cwd=ROOT,
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr
