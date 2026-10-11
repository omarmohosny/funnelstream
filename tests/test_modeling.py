"""Modeling package: baseline specs, validator guardrails, LLM modeler (fake client), CLI outputs."""
import copy
import csv
import json

import pytest

from modeling import generate_spec, llm_modeler, model_definition
from modeling.source_catalog import load_catalog
from modeling.validate_spec import validate_spec

CATALOG = load_catalog()


@pytest.mark.parametrize("style,count", [("star", 4), ("galaxy", 7), ("snowflake", 9)])
def test_baseline_valid_for_every_style(style, count):
    spec = model_definition.build(style)
    assert len(spec["tables"]) == count
    assert validate_spec(spec, CATALOG, style) == []


def test_unknown_source_column_rejected():
    spec = model_definition.build("galaxy")
    spec["tables"][0]["columns"][1]["source_column"] = "silver_customers.shoe_size"
    assert any("not in the Silver catalog" in e for e in validate_spec(spec, CATALOG))


def test_pii_never_a_source():
    cat = copy.deepcopy(CATALOG)
    cat["tables"][0]["columns"].append({"name": "phone", "type": "string"})
    path = generate_spec.ROOT / "spec" / "_pii_catalog.json"
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(cat), encoding="utf-8")
    try:
        loaded = load_catalog(path)
    finally:
        path.unlink()
    assert not any(c["name"] == "phone" for t in loaded["tables"] for c in t["columns"])


def test_missing_pk_and_bad_fk_rejected():
    spec = model_definition.build("galaxy")
    fact = next(t for t in spec["tables"] if t["name"] == "fact_orders")
    fact["pk"] = ["nope"]
    fact["columns"][1]["references"] = "dim_customer.customer_id"  # not the pk
    errs = validate_spec(spec, CATALOG)
    assert any("pk" in e and "own columns" in e for e in errs)
    assert any("single-column pk" in e for e in errs)


def test_scd2_columns_required():
    spec = model_definition.build("galaxy")
    dim = next(t for t in spec["tables"] if t["name"] == "dim_product")
    dim["columns"] = [c for c in dim["columns"] if c["column"] != "is_current"]
    assert any("SCD2 table needs is_current" in e for e in validate_spec(spec, CATALOG))


def test_style_rules():
    star = model_definition.build("star")
    assert validate_spec(star, CATALOG, "galaxy")  # one fact cannot share a dimension across facts
    assert validate_spec(model_definition.build("galaxy"), CATALOG, "snowflake")  # no outrigger
    assert validate_spec(model_definition.build("galaxy"), CATALOG, "star")  # three facts


class FakeClient:
    """Stands in for anthropic.Anthropic: replies come from a list, one per call."""

    def __init__(self, replies):
        self.replies, self.calls = list(replies), []
        outer = self

        class _Stream:
            def __init__(self, text):
                self.text = text

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def get_final_message(self):
                block = type("B", (), {"type": "text", "text": self.text})()
                return type("M", (), {"content": [block]})()

        class _Messages:
            def stream(self, **kw):
                outer.calls.append(kw)
                return _Stream(outer.replies.pop(0))

        self.messages = _Messages()


def test_llm_repair_round_then_accept():
    good = model_definition.build("galaxy")
    bad = copy.deepcopy(good)
    bad["tables"][0]["columns"][0]["source_column"] = "silver_customers.invented"
    client = FakeClient(["```json\n%s\n```" % json.dumps(bad), json.dumps(good)])
    spec, info = llm_modeler.propose_spec(CATALOG, "galaxy", client=client, log=lambda *a: None)
    assert info["source"] == "llm" and info["rounds"] == 2
    assert spec["generated_by"] == "llm"
    assert "invented" in client.calls[1]["messages"][-1]["content"]  # errors were sent back


def test_llm_prompt_has_metadata_only():
    client = FakeClient([json.dumps(model_definition.build("galaxy"))])
    llm_modeler.propose_spec(CATALOG, "galaxy", client=client, log=lambda *a: None)
    prompt = client.calls[0]["messages"][0]["content"]
    assert "silver_orders" in prompt and "order_total" in prompt
    assert '"name":"phone"' not in prompt and '"name":"raw_payload"' not in prompt


def test_llm_falls_back_to_baseline_when_invalid():
    client = FakeClient(["not json"] * (llm_modeler.MAX_REPAIRS + 1))
    spec, info = llm_modeler.propose_spec(CATALOG, "galaxy", client=client, log=lambda *a: None)
    assert info["source"] == "baseline" and "repair rounds" in info["reason"]
    assert validate_spec(spec, CATALOG) == []


@pytest.mark.parametrize("provider,reason", [("anthropic", "no ANTHROPIC_API_KEY"), ("huggingface", "no Hugging Face token")])
def test_llm_falls_back_without_credentials(monkeypatch, provider, reason):
    for k in ("ANTHROPIC_API_KEY", "HF_TOKEN", "HUGGINGFACE_TOKEN", "HUGGINGFACE_API_KEY", "FUNNEL_STREAM", "OLLAMA_API_KEY"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("FUNNELSTREAM_LLM_PROVIDER", provider)
    spec, info = llm_modeler.propose_spec(CATALOG, "star")
    assert info["source"] == "baseline" and info["reason"].startswith(reason)
    assert len(spec["tables"]) == 4


def test_provider_selection(monkeypatch):
    from modeling import llm_provider as lp
    for k in ("ANTHROPIC_API_KEY", "HF_TOKEN", "HUGGINGFACE_TOKEN", "OLLAMA_API_KEY", "FUNNELSTREAM_LLM_PROVIDER"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("FUNNEL_STREAM", "hf_abc")
    assert lp.provider() == "huggingface" and lp.hf_token() == "hf_abc"
    monkeypatch.setenv("ANTHROPIC_API_KEY", "x")
    assert lp.provider() == "anthropic"
    monkeypatch.setenv("FUNNELSTREAM_LLM_PROVIDER", "huggingface")
    assert lp.provider() == "huggingface"
    monkeypatch.setenv("FUNNELSTREAM_LLM_PROVIDER", "ollama")
    monkeypatch.setenv("OLLAMA_API_KEY", "k")
    assert lp.provider() == "ollama" and lp.chat_url() == "https://ollama.com/v1/chat/completions"
    assert lp.default_model() == "gpt-oss:120b" and lp.active_token() == "k"


def test_hf_client_path(monkeypatch):
    from modeling import llm_provider as lp
    reply = json.dumps(model_definition.build("star"))
    monkeypatch.setattr(lp.HFClient, "chat", lambda self, messages, system=None, tools=None, max_tokens=4000, model=None:
                        {"content": reply})
    spec, info = llm_modeler.propose_spec(CATALOG, "star", client=lp.HFClient(token="hf_x"), log=lambda *a: None)
    assert info["source"] == "llm" and len(spec["tables"]) == 4


def test_llm_proposal_waits_for_review_then_approval(tmp_path, monkeypatch):
    from modeling import approve
    good = model_definition.build("galaxy")
    good["generated_by"] = "llm"
    monkeypatch.setattr(generate_spec, "propose_spec",
                        lambda catalog, style, model=None: (good, {"source": "llm", "rounds": 1, "errors": [], "reason": ""}))
    assert generate_spec.main(["--style", "galaxy", "--use-llm", "--out-dir", str(tmp_path)]) == 0
    assert (tmp_path / "model_spec.proposed.json").exists() and (tmp_path / "review.md").exists()
    assert not (tmp_path / "model_spec.json").exists()  # not usable before approval
    assert approve.main(["--style", "galaxy", "--by", "Reviewer", "--dir", str(tmp_path)]) == 0
    spec = json.loads((tmp_path / "model_spec.json").read_text(encoding="utf-8"))
    assert spec["status"] == "approved" and spec["approved_by"] == "Reviewer" and spec["approved_at"]


def test_approve_rejects_a_bad_hand_edit(tmp_path):
    from modeling import approve
    spec = model_definition.build("galaxy")
    spec["tables"][0]["columns"][0]["source_column"] = "silver_customers.invented"
    (tmp_path / "model_spec.proposed.json").write_text(json.dumps(spec), encoding="utf-8")
    assert approve.main(["--style", "galaxy", "--by", "R", "--dir", str(tmp_path)]) == 1
    assert not (tmp_path / "model_spec.json").exists()


def test_cli_writes_json_and_csv(tmp_path):
    assert generate_spec.main(["--style", "snowflake", "--out-dir", str(tmp_path)]) == 0
    spec = json.loads((tmp_path / "model_spec.json").read_text(encoding="utf-8"))
    rows = list(csv.DictReader(open(tmp_path / "source_to_target.csv", encoding="utf-8")))
    assert {t["name"] for t in spec["tables"]} >= {"dim_city", "dim_category", "fact_orders"}
    assert len(rows) == sum(len(t["columns"]) for t in spec["tables"])
    assert {"table", "column", "source", "transformation", "references"} <= set(rows[0])
