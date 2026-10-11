"""CLI: metadata JSON -> model_spec.json + source_to_target.csv (baseline model, or an LLM proposal).

    python -m modeling.generate_spec --style galaxy                    # baseline spec
    python -m modeling.generate_spec --style snowflake --use-llm       # LLM proposal -> *.proposed.json, for review
    python -m modeling.approve --style snowflake --by "Your Name"      # after review: becomes model_spec.json
    python -m modeling.generate_spec --metadata my_catalog.json --out-dir spec/custom
"""
import argparse
import csv
import json
import os
import sys
from pathlib import Path

from modeling import model_definition
from modeling.llm_modeler import propose_spec
from modeling.source_catalog import load_catalog
from modeling.validate_spec import validate_spec

ROOT = Path(__file__).resolve().parents[1]


def load_dotenv(path=ROOT / ".env"):
    try:
        for line in open(path, encoding="utf-8"):
            k, sep, v = line.strip().partition("=")
            if sep and not k.startswith("#"):
                os.environ.setdefault(k.strip(), v.strip().strip("'\""))
    except FileNotFoundError:
        pass


def write_outputs(spec: dict, out_dir: Path, stem: str = "") -> tuple:
    """stem '' -> model_spec.json (approved); stem '.proposed' -> model_spec.proposed.json (needs review)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    spec_path = out_dir / ("model_spec%s.json" % stem)
    csv_path = out_dir / ("source_to_target%s.csv" % stem)
    spec_path.write_text(json.dumps(spec, indent=2) + "\n", encoding="utf-8")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["table", "column", "role", "data_type", "source", "transformation", "references"])
        for t in spec["tables"]:
            for c in t["columns"]:
                w.writerow([t["name"], c["column"], c["role"], c["data_type"], c.get("source_column", ""),
                            c.get("expression") or "direct copy", c.get("references", "")])
    return spec_path, csv_path


def review_summary(proposed: dict, baseline: dict) -> str:
    """Markdown checklist for the human reviewer: what the LLM changed compared with the committed baseline."""
    pt = {t["name"]: t for t in proposed["tables"]}
    bt = {t["name"]: t for t in baseline["tables"]}
    lines = ["# Review: %s model proposed by %s" % (proposed["style"], proposed.get("model", "the LLM")), "",
             "Status: PROPOSED, not approved. Nothing reads this file until you run "
             "`python -m modeling.approve --style %s --by \"<your name>\"`." % proposed["style"], "",
             "Edit `model_spec.proposed.json` first if anything below is wrong; approval validates it again.", "",
             "## Tables (%d proposed, %d in baseline)" % (len(pt), len(bt))]
    for n in sorted(set(pt) | set(bt)):
        if n not in bt:
            lines.append("- NEW %s (%s): %s" % (n, pt[n]["type"], pt[n].get("grain", "")))
        elif n not in pt:
            lines.append("- MISSING vs baseline: %s" % n)
        else:
            pc, bc = {c["column"] for c in pt[n]["columns"]}, {c["column"] for c in bt[n]["columns"]}
            diff = ["+" + c for c in sorted(pc - bc)] + ["-" + c for c in sorted(bc - pc)]
            lines.append("- %s: %d columns%s" % (n, len(pc), " (vs baseline: %s)" % " ".join(diff) if diff else ""))
    lines += ["", "## Check by hand",
              "- Grain and primary key of every table are right for the business questions.",
              "- Every expression means what its column name says (rates, delivered vs placed revenue, status rank).",
              "- No personal data (names, phones); the validator already blocks invented columns.",
              "- Foreign keys point at the dimension you expect; SCD2 tables keep history."]
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--style", choices=["star", "snowflake", "galaxy"], default="galaxy")
    p.add_argument("--use-llm", action="store_true", help="ask the LLM to propose the model (needs ANTHROPIC_API_KEY)")
    p.add_argument("--metadata", help="Silver metadata JSON (default modeling/metadata/silver_catalog.json)")
    p.add_argument("--out-dir", help="default spec/<style>")
    p.add_argument("--model", help="LLM model id (default FUNNELSTREAM_LLM_MODEL or claude-sonnet-5-5)")
    a = p.parse_args(argv)

    load_dotenv()
    catalog = load_catalog(a.metadata)
    if a.use_llm:
        spec, info = propose_spec(catalog, a.style, model=a.model)
        print("source: %s after %d round(s)%s" % (info["source"], info["rounds"],
                                                  " (fallback: %s)" % info["reason"] if info["reason"] else ""))
        for e in info["errors"][:10]:
            print("  last error:", e)
    else:
        spec = model_definition.build(a.style)
    errors = validate_spec(spec, catalog, a.style)
    if errors:
        print("INVALID spec:\n" + "\n".join("  " + e for e in errors), file=sys.stderr)
        return 1
    out_dir = Path(a.out_dir) if a.out_dir else ROOT / "spec" / a.style
    if a.use_llm and spec.get("generated_by") == "llm":
        spec["status"] = "proposed"
        spec_path, csv_path = write_outputs(spec, out_dir, ".proposed")
        (out_dir / "review.md").write_text(review_summary(spec, model_definition.build(a.style)), encoding="utf-8")
        print("PROPOSED (human review required): %s\n  1. read %s\n  2. edit %s if needed\n  3. python -m "
              "modeling.approve --style %s --by \"<your name>\"" % (spec_path, out_dir / "review.md", spec_path.name,
                                                                   a.style))
        return 0
    spec_path, csv_path = write_outputs(spec, out_dir)
    print("%s spec: %d tables (%s) -> %s, %s" % (a.style, len(spec["tables"]),
                                                  ", ".join(t["name"] for t in spec["tables"]), spec_path, csv_path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
