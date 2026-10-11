"""LLM modeling assistant: metadata JSON in, validated model spec JSON out.

The model receives only table and column names, types, keys, the business questions and one example table.
It never sees data rows and never runs SQL. Its reply is parsed and validated; validation errors go back
to it for up to MAX_REPAIRS repair rounds. If it still fails (or there is no API key) the committed baseline
is used, so the pipeline never depends on the LLM.
"""
import json
import os
import re

from modeling import llm_provider, model_definition
from modeling.validate_spec import validate_spec

MAX_REPAIRS = 2

SYSTEM = """You are a data modeling assistant for a Kimball dimensional model in Databricks.
You receive a JSON catalog of Silver tables and a list of business questions, and you reply with ONE JSON
object: the Gold model spec. Reply with JSON only, no prose, no code fences.

Spec format:
{"style": "<star|snowflake|galaxy>", "catalog": "funnelstream", "schema": "gold", "generated_by": "llm",
 "tables": [{"name": str, "type": "dimension|fact|outrigger", "scd": 0|1|2, "grain": str, "pk": [column names],
   "ctes": [Spark SQL CTE definitions "name AS (SELECT ...)"], "from_sql": str, "group_by": [str],
   "columns": [{"column": str, "data_type": str, "role": "pk|bk|fk|attribute|measure|scd_meta",
     "source_column": "silver_table.column or empty", "expression": "Spark SQL or empty", "references":
     "dim_table.pk_column for foreign keys, else empty"}]}]}

Rules:
- Every source_column must exist in the catalog. Never invent Silver columns. Never use personal data.
- Every column needs a source_column or an expression. Column names are unique per table, snake_case.
- data_type is one of STRING, INT, BIGINT, BOOLEAN, DATE, TIMESTAMP, DECIMAL(p,s). Money is DECIMAL, never float.
- Every table has a pk made of its own columns. A foreign key references the single-column pk of a table in the spec.
- SCD2 dimensions (scd = 2) have valid_from, valid_to, is_current; surrogate keys are xxhash64(business_key, updated_at).
- Dimension joins in from_sql are written as @gold.<table>. Current order status = highest status_rank, tie latest status_time.
- Style rules: star = exactly one fact; galaxy = several facts sharing at least one conformed dimension;
  snowflake = dimensions normalised into at least one outrigger table.
- Delivered revenue is the headline metric; rates divide by orders placed and are NULL when the denominator is 0."""


def make_client(api_key=None, workspace_id=None):
    import anthropic
    ws = workspace_id or os.environ.get("ANTHROPIC_WORKSPACE_ID")
    return anthropic.Anthropic(api_key=api_key or os.environ.get("ANTHROPIC_API_KEY"),
                               default_headers={"anthropic-workspace-id": ws} if ws else None)


def parse_json_reply(text: str) -> dict:
    text = text.strip()
    m = re.search(r"```(?:json)?\s*(.*?)```", text, re.S)
    if m:
        text = m.group(1).strip()
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end < start:
        raise ValueError("reply contains no JSON object")
    return json.loads(text[start:end + 1])


def _call(client, messages, model) -> str:
    if isinstance(client, llm_provider.HFClient):
        return client.chat(messages, system=SYSTEM, max_tokens=16000, model=model).get("content") or ""
    with client.messages.stream(model=model, max_tokens=24000, system=SYSTEM, messages=messages) as s:
        msg = s.get_final_message()
    return "".join(b.text for b in msg.content if b.type == "text")


def propose_spec(catalog: dict, style: str, client=None, model: str = None, log=print):
    """Returns (spec, info). info = {"source": "llm"|"baseline", "rounds": n, "errors": [...], "reason": str}."""
    info = {"source": "baseline", "rounds": 0, "errors": [], "reason": ""}
    if client is None:
        if not llm_provider.has_credentials():
            info["reason"] = llm_provider.missing_credentials_reason()
            return model_definition.build(style), info
        client = llm_provider.HFClient() if llm_provider.openai_style() else make_client()
    model = model or llm_provider.default_model()
    prompt = ("Style: %s\n\nSilver catalog (metadata JSON):\n%s\n\nBusiness questions:\n%s\n\n"
              "Example table in the expected format:\n%s\n\nReturn the full spec JSON for style %s." % (
                  style, json.dumps(catalog, separators=(",", ":")),
                  "\n".join("- " + q for q in catalog.get("business_questions", [])),
                  json.dumps(model_definition.dim_product(), separators=(",", ":")), style))
    messages = [{"role": "user", "content": prompt}]
    errors = []
    for attempt in range(MAX_REPAIRS + 1):
        info["rounds"] = attempt + 1
        try:
            reply = _call(client, messages, model)
        except Exception as e:  # network, auth, quota: fall back instead of failing the run
            info["reason"] = "LLM call failed: %s: %s" % (type(e).__name__, e)
            return model_definition.build(style), info
        try:
            spec = parse_json_reply(reply)
            spec.update({"style": style, "generated_by": "llm", "model": model})
            errors = validate_spec(spec, catalog, style)
        except ValueError as e:  # includes json.JSONDecodeError
            spec, errors = None, ["reply is not valid JSON: %s" % e]
        if not errors:
            info.update(source="llm", errors=[])
            return spec, info
        log("  LLM round %d rejected: %d error(s)" % (attempt + 1, len(errors)))
        messages += [{"role": "assistant", "content": reply},
                     {"role": "user", "content": "The spec failed validation. Fix every error and return the full "
                      "corrected JSON only:\n" + "\n".join("- " + e for e in errors[:40])}]
    info.update(errors=errors, reason="LLM spec still invalid after %d repair rounds" % MAX_REPAIRS)
    return model_definition.build(style), info
