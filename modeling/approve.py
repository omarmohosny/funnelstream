"""Human approval gate: turns a reviewed model_spec.proposed.json into the model_spec.json the ETL reads.

    python -m modeling.approve --style galaxy --by "Your Name"

The proposal is validated again (so hand edits are checked), stamped with who approved it and when, and written
with its source_to_target.csv. The proposal files stay next to it as the audit trail.
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from modeling.generate_spec import ROOT, write_outputs
from modeling.source_catalog import load_catalog
from modeling.validate_spec import validate_spec


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--style", choices=["star", "snowflake", "galaxy"], default="galaxy")
    p.add_argument("--by", required=True, help="reviewer name, recorded in the spec")
    p.add_argument("--metadata", help="Silver metadata JSON used for validation")
    p.add_argument("--dir", help="default spec/<style>")
    a = p.parse_args(argv)

    out_dir = Path(a.dir) if a.dir else ROOT / "spec" / a.style
    proposed = out_dir / "model_spec.proposed.json"
    if not proposed.exists():
        print("no proposal at %s; run modeling.generate_spec --use-llm first" % proposed, file=sys.stderr)
        return 1
    spec = json.loads(proposed.read_text(encoding="utf-8"))
    errors = validate_spec(spec, load_catalog(a.metadata), a.style)
    if errors:
        print("NOT approved, fix the proposal first:\n" + "\n".join("  " + e for e in errors), file=sys.stderr)
        return 1
    spec["status"] = "approved"
    spec["approved_by"] = a.by
    spec["approved_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    spec_path, csv_path = write_outputs(spec, out_dir)
    print("approved by %s -> %s, %s" % (a.by, spec_path, csv_path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
