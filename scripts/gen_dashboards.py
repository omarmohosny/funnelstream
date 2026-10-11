"""Write the provisioned dashboard files from monitoring/dashboards.py. Run: python scripts/gen_dashboards.py"""
import json
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from monitoring import dashboards

OUT = os.path.join(_ROOT, "monitoring", "grafana", "dashboards")
FILES = {"inventory-ops.json": dashboards.build_inventory_ops,
         "inventory-daily.json": dashboards.build_inventory_daily,
         "marketing-d1.json": dashboards.build_marketing_d1}


def main():
    for name, build in FILES.items():
        path = os.path.join(OUT, name)
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(build(), fh, indent=2, ensure_ascii=False)
            fh.write("\n")
        print("wrote", path)


if __name__ == "__main__":
    main()
