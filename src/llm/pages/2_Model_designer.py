"""Streamlit page: generate or propose a Gold model (star, snowflake, galaxy), review it, edit it, approve it.

Metadata JSON in (Silver catalog + business questions) -> model spec JSON out. An LLM proposal is never used
until a person approves it here (or with `python -m modeling.approve`).
"""
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import agent  # noqa: E402  (also puts the repo root on sys.path)
from modeling import llm_provider, model_definition  # noqa: E402
from modeling.generate_spec import review_summary, write_outputs  # noqa: E402
from modeling.llm_modeler import propose_spec  # noqa: E402
from modeling.source_catalog import DEFAULT_CATALOG, load_catalog  # noqa: E402
from modeling.validate_spec import validate_spec  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]

st.set_page_config(page_title="FunnelStream Model designer", page_icon="🧩", layout="wide")
st.title("Model designer")
st.caption("Metadata JSON in, model spec JSON out. An LLM proposal needs your review and approval before it is used.")
agent.load_dotenv()

# ---- inputs -------------------------------------------------------------------------------------------------
st.sidebar.caption("LLM: %s, model %s" % (llm_provider.provider_label(), llm_provider.default_model()))
style = st.sidebar.radio("Model style", ["galaxy", "star", "snowflake"],
                         help="star: 1 fact; galaxy: facts sharing dimensions; snowflake: galaxy + outriggers")
mode = st.sidebar.radio("Generate with", ["LLM proposal", "Baseline (no LLM)"])
upload = st.sidebar.file_uploader("Metadata JSON (optional)", type="json",
                                  help="Default: modeling/metadata/silver_catalog.json")
catalog = load_catalog(DEFAULT_CATALOG)
if upload is not None:
    tmp = ROOT / "spec" / "_uploaded_catalog.json"
    tmp.parent.mkdir(exist_ok=True)
    tmp.write_bytes(upload.getvalue())
    try:
        catalog = load_catalog(tmp)
    finally:
        tmp.unlink(missing_ok=True)

with st.expander("Metadata the model sees (%d Silver tables, %d business questions)" % (
        len(catalog["tables"]), len(catalog.get("business_questions", [])))):
    st.json(catalog, expanded=False)

if st.sidebar.button("Generate", type="primary"):
    with st.spinner("Asking %s ..." % llm_provider.provider_label() if mode == "LLM proposal" else "Building ..."):
        if mode == "LLM proposal":
            spec, info = propose_spec(catalog, style, log=lambda *a: None)
        else:
            spec, info = model_definition.build(style), {"source": "baseline", "rounds": 0, "errors": [], "reason": ""}
    spec["status"] = "proposed" if spec.get("generated_by") == "llm" else "baseline"
    st.session_state.update(spec_text=json.dumps(spec, indent=2), info=info, style_made=style)

if "spec_text" not in st.session_state:
    st.info("Pick a style and press **Generate** in the sidebar.")
    st.stop()

info = st.session_state["info"]
style_made = st.session_state["style_made"]
if info["source"] == "llm":
    st.success("Proposed by the LLM after %d round(s). Review it below before approving." % info["rounds"])
elif info["reason"]:
    st.warning("LLM not used, showing the baseline model instead: %s" % info["reason"])
else:
    st.info("Baseline model (written by the team, checked by tests).")

# ---- review -------------------------------------------------------------------------------------------------
st.subheader("1. Review")
text = st.text_area("Model spec JSON (edit if needed)", st.session_state["spec_text"], height=300)
try:
    spec = json.loads(text)
    errors = validate_spec(spec, catalog, style_made)
except ValueError as e:
    spec, errors = None, ["not valid JSON: %s" % e]

if errors:
    st.error("Validation failed (%d). This cannot be approved yet:" % len(errors))
    st.code("\n".join(errors[:30]))
else:
    st.success("Validation passed: columns trace to Silver, keys and foreign keys are consistent, style rules hold.")

if spec and not errors:
    st.markdown("**Tables**")
    st.dataframe([{"table": t["name"], "type": t["type"], "scd": t.get("scd"), "grain": t.get("grain", ""),
                   "pk": ", ".join(t["pk"]), "columns": len(t["columns"])} for t in spec["tables"]],
                 width="stretch", hide_index=True)
    for t in spec["tables"]:
        with st.expander("%s (%s): %s" % (t["name"], t["type"], t.get("grain", ""))):
            st.dataframe([{"column": c["column"], "type": c["data_type"], "role": c["role"],
                           "source": c.get("source_column", ""), "transformation": c.get("expression") or "direct copy",
                           "references": c.get("references", "")} for c in t["columns"]],
                         width="stretch", hide_index=True)
    with st.expander("What changed compared with the baseline", expanded=info["source"] == "llm"):
        st.markdown(review_summary(dict(spec, model=spec.get("model", "the model")), model_definition.build(style_made)))

# ---- approve ------------------------------------------------------------------------------------------------
st.subheader("2. Approve")
st.caption("Writes spec/%s/model_spec.json and source_to_target.csv, the files the ETL reads. "
           "The proposal is kept next to them as the audit trail." % style_made)
who = st.text_input("Reviewer name")
if st.button("Approve and save", disabled=bool(errors) or not who.strip()):
    out_dir = ROOT / "spec" / style_made
    write_outputs(spec, out_dir, ".proposed")
    spec.update(status="approved", approved_by=who.strip(),
                approved_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    spec_path, csv_path = write_outputs(spec, out_dir)
    st.success("Approved by %s. Saved %s and %s" % (who.strip(), spec_path.relative_to(ROOT), csv_path.relative_to(ROOT)))

if spec and not errors:
    st.download_button("Download spec JSON", json.dumps(spec, indent=2), "model_spec.json", "application/json")
