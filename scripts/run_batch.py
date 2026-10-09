"""One command: publish Silver, build Gold, run the checks, print a summary.

  docker compose run --rm spark python scripts/run_batch.py            (publish needs Spark: run it in the container)
  python scripts/run_batch.py --skip-publish --dry-run                 (host: nothing executed)

Phases, in order, each timed: publish (scripts/publish_silver.py code path), gold (warehouse.gold_build.run_build, every
step except checks) and checks (run_build with steps=["checks"]). A failed phase stops the run.
Exit codes: 0 success, 1 a phase failed, 2 bad arguments or missing store settings.

Checks contract (T16, warehouse/checks.py): the checks step returns {"status": "ok" | "failed", "failed": [names],
"results": [...]} and does NOT raise for a failed check, so every check runs and the whole list is printed. The batch
fails at the checks phase when run_build raises GoldBuildError (a query that cannot run) or when the step result has
status "failed" or a non-empty "failed" entry; then Power BI must not be refreshed. --manifest PATH adds the noise
manifest check (quarantine count within 2%); without it that check is reported as skipped.
Heartbeats: publish_silver.report() writes "publish" and gold_build.record_heartbeat() writes "gold_build", each once,
after its phase succeeded (never in a dry run); both swallow every error.
"""
import argparse
import os
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from scripts import publish_silver  # no pyspark import at module level (publish_silver imports Spark lazily)
from warehouse import gold_build
from warehouse.store import StoreConfigError, get_store

CHECKS_STEP = "checks"


class PhaseFailed(Exception):
    def __init__(self, phase, text, code=1):
        super().__init__(text)
        self.phase, self.text, self.code = phase, text, code


def _clean(text):
    """One line, no secret: the Databricks token (if set in the environment) is masked."""
    text = " ".join(str(text).split())[:300]
    for var in ("DATABRICKS_TOKEN", "PG_DSN"):
        value = os.environ.get(var)
        if value:
            text = text.replace(value, "***")
    return text


def _safely(fn, *a):
    """Reports and heartbeats are best effort: they never fail the batch."""
    try:
        fn(*a)
    except Exception as e:
        print("warning: %s failed: %s" % (getattr(fn, "__name__", "report"), type(e).__name__), file=sys.stderr)


def _publish_argv(args):
    argv = ["--lake", args.lake, "--export-dir", args.export_dir]
    return argv + (["--dry-run"] if args.dry_run else [])


def _checks_failure(step):
    """Text if a checks step result reports failed checks, else None."""
    failed = step.get("failed")
    if step.get("status") == "failed" or failed:
        return "failed checks: %s" % (", ".join(str(f) for f in failed) if isinstance(failed, (list, tuple))
                                      else (failed or "see the checks output"))
    return None


def _print_checks(step):
    """Every check with its value (stdout), so a failed batch shows what failed and by how much."""
    if step.get("results") is None:
        return
    from warehouse.checks import format_results
    for line in format_results(step):
        print(line)


def run(args, store, phases, results):
    """Run the phases, appending to phases [(name, seconds)] and results as each finishes; raises PhaseFailed."""
    window = dict(from_date=args.from_date, to_date=args.to_date, full=args.full, days=args.days,
                  dry_run=args.dry_run)
    manifest = getattr(args, "manifest", None)
    gold_steps = args.steps if args.steps else [s for s in gold_build.STEPS if s != CHECKS_STEP]

    # publish
    if args.skip_publish:
        print("publish: skipped (--skip-publish)")
    else:
        t0 = time.monotonic()
        try:
            rc, result, pargs = publish_silver.run(_publish_argv(args))
        except Exception as e:  # run() handles its own errors; this is a safety net
            raise PhaseFailed("publish", "%s: %s" % (type(e).__name__, _clean(e))) from e
        if rc != 0:
            raise PhaseFailed("publish", "publish_silver exited with code %d (see the error above)" % rc, rc)
        _safely(publish_silver.report, result, pargs)
        phases.append(("publish", time.monotonic() - t0))
        results["publish"] = result

    # gold
    t0 = time.monotonic()
    try:
        gold = gold_build.run_build(store, steps=gold_steps, **window)
    except ValueError as e:
        raise PhaseFailed("gold", _clean(e), 2) from e
    except gold_build.GoldBuildError as e:
        raise PhaseFailed("gold", _clean(e)) from e
    except Exception as e:
        raise PhaseFailed("gold", "%s: %s" % (type(e).__name__, _clean(e))) from e
    if not args.dry_run:
        _safely(gold_build.record_heartbeat, gold)
    phases.append(("gold", time.monotonic() - t0))
    results["gold"] = gold

    # checks
    t0 = time.monotonic()
    try:
        checks = gold_build.run_build(store, steps=[CHECKS_STEP], manifest_path=manifest, **window)
    except ValueError as e:
        raise PhaseFailed("checks", _clean(e), 2) from e
    except gold_build.GoldBuildError as e:
        raise PhaseFailed("checks", _clean(e)) from e
    except Exception as e:
        raise PhaseFailed("checks", "%s: %s" % (type(e).__name__, _clean(e))) from e
    step = checks["steps"].get(CHECKS_STEP, {})
    _print_checks(step)
    problem = _checks_failure(step)
    if problem:
        raise PhaseFailed("checks", _clean(problem))
    phases.append(("checks", time.monotonic() - t0))
    results["checks"] = checks


def _summary(phases, results, total, dry_run):
    print("--- summary ---")
    for name, secs in phases:
        print("phase %-8s %.1fs" % (name, secs))
    for table, r in (results.get("publish") or {}).items():
        print("publish %-28s rows=%d files=%d merged=%s" % (table, r["rows"], r["files"], r["merged"]))
    gold = results["gold"]
    w = gold["window"]
    print("gold window: %s .. %s%s" % (w["from"], w["to"], " (full)" if w["full"] else ""))
    for name, r in gold["steps"].items():
        extra = "" if r["rows"] is None else " rows=%d" % r["rows"]
        print("gold %-24s %-8s statements=%d%s" % (name, r["status"], r["statements"], extra))
    for k, v in gold["reported"].items():
        print("reported %s: %d" % (k, v))
    cs = results["checks"]["steps"].get(CHECKS_STEP, {})
    print("checks status: %s" % cs.get("status", "unknown"))
    if cs.get("status") == "dry-run":
        print("checks planned: %d query(ies), %s" % (cs.get("statements", 0), cs.get("manifest", "")))
    print("BATCH OK%s in %.1fs" % (" (dry run, nothing executed)" if dry_run else "", total))


def main(argv=None):
    ap = argparse.ArgumentParser(description="Publish Silver, build Gold, run the checks, print a summary.")
    ap.add_argument("--skip-publish", action="store_true", help="skip the publish phase")
    ap.add_argument("--from-date", help="first Cairo date of the Gold window (with --to-date), yyyy-mm-dd")
    ap.add_argument("--to-date", help="last Cairo date of the Gold window, yyyy-mm-dd")
    ap.add_argument("--days", type=int, help="Gold window = the last N days up to today (Cairo). Default: 7")
    ap.add_argument("--full", action="store_true", help="rebuild all of Gold")
    ap.add_argument("--steps", help="comma separated Gold steps (not checks), always run in fixed order")
    ap.add_argument("--dry-run", action="store_true", help="publish dry-run and Gold dry-run, execute nothing")
    ap.add_argument("--manifest", help="noise manifest JSON for the manifest check (quarantine count within 2%%); "
                    "only meaningful for a fresh single run that covers the whole Silver")
    ap.add_argument("--lake", default="data/lake", help="passed to publish")
    ap.add_argument("--export-dir", default="data/export", help="passed to publish")
    args = ap.parse_args(argv)
    args.steps = [s.strip() for s in args.steps.split(",") if s.strip()] if args.steps else None
    started = time.monotonic()
    store = None
    phases, results = [], {}
    try:
        if args.steps and CHECKS_STEP in args.steps:
            raise ValueError("--steps takes Gold steps only; the checks phase always runs after them")
        gold_build.resolve_window(args.from_date, args.to_date, days=args.days, full=args.full)  # same rules
        if args.manifest:  # an unusable manifest is a usage error: stop before publishing anything
            from warehouse.checks import load_manifest
            load_manifest(args.manifest)
        if args.steps:
            unknown = [s for s in args.steps if s not in gold_build.STEPS]
            if unknown:
                raise ValueError("unknown step(s): %s" % ", ".join(unknown))
        try:
            store = get_store("databricks")
        except StoreConfigError:
            if not args.dry_run:
                raise
            from warehouse.store import env_catalog
            store = gold_build._OfflineStore(
                env_catalog() if os.environ.get("DATABRICKS_CATALOG") else gold_build.CATALOG)
        run(args, store, phases, results)
    except (StoreConfigError, ValueError) as e:
        print("error: %s" % _clean(e), file=sys.stderr)
        return 2
    except PhaseFailed as e:
        print("BATCH FAILED at %s: %s" % (e.phase, e.text), file=sys.stderr)
        print("phases finished: %s" % (", ".join(n for n, _ in phases) or "none"),
              file=sys.stderr)
        return e.code
    finally:
        if store is not None:
            store.close()
    _summary(phases, results, time.monotonic() - started, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
