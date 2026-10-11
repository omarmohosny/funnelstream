"""Which LLM backs the modeler and the assistant: Ollama cloud, Hugging Face (Inference Providers) or Anthropic.

Select with FUNNELSTREAM_LLM_PROVIDER=ollama|huggingface|anthropic. If unset, the first provider with credentials
wins in the order Anthropic, Ollama, Hugging Face.
  ollama       OLLAMA_API_KEY; model FUNNELSTREAM_OLLAMA_MODEL (default gpt-oss:120b); https://ollama.com/v1
  huggingface  HF_TOKEN / HUGGINGFACE_TOKEN / HUGGINGFACE_API_KEY / FUNNEL_STREAM (hf_...); model FUNNELSTREAM_HF_MODEL
  anthropic    ANTHROPIC_API_KEY; model FUNNELSTREAM_LLM_MODEL
FUNNELSTREAM_LLM_BASE_URL (+ FUNNELSTREAM_LLM_API_KEY) points ollama/huggingface at any OpenAI-compatible server,
for example a local Ollama at http://localhost:11434/v1.
"""
import os

ROUTER_URL = "https://router.huggingface.co/v1/chat/completions"
OLLAMA_CLOUD_BASE = "https://ollama.com/v1"
DEFAULT_HF_MODEL = "Qwen/Qwen2.5-72B-Instruct"
DEFAULT_OLLAMA_MODEL = "gpt-oss:120b"
DEFAULT_ANTHROPIC_MODEL = "claude-sonnet-5-5"
_HF_ENV = ("HF_TOKEN", "HUGGINGFACE_TOKEN", "HUGGINGFACE_API_KEY", "HUGGING_FACE_HUB_TOKEN", "FUNNEL_STREAM")


def hf_token() -> str:
    for k in _HF_ENV:
        v = os.environ.get(k, "").strip()
        if v.startswith("hf_"):
            return v
    return ""


def ollama_key() -> str:
    return os.environ.get("OLLAMA_API_KEY", "").strip()


def provider() -> str:
    p = os.environ.get("FUNNELSTREAM_LLM_PROVIDER", "").strip().lower()
    if p in ("anthropic", "huggingface", "ollama"):
        return p
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "anthropic"
    if ollama_key():
        return "ollama"
    return "huggingface" if hf_token() else "anthropic"


def openai_style() -> bool:
    """True for the providers served by HFClient (OpenAI-style chat API)."""
    return provider() != "anthropic"


def provider_label() -> str:
    return {"ollama": "Ollama cloud", "huggingface": "Hugging Face", "anthropic": "Anthropic"}[provider()]


def default_model() -> str:
    p = provider()
    if p == "ollama":
        return os.environ.get("FUNNELSTREAM_OLLAMA_MODEL", DEFAULT_OLLAMA_MODEL)
    if p == "huggingface":
        return os.environ.get("FUNNELSTREAM_HF_MODEL", DEFAULT_HF_MODEL)
    return os.environ.get("FUNNELSTREAM_LLM_MODEL", DEFAULT_ANTHROPIC_MODEL)


def active_token() -> str:
    return (os.environ.get("FUNNELSTREAM_LLM_API_KEY", "").strip()
            or (ollama_key() if provider() == "ollama" else hf_token()))


def chat_url() -> str:
    """FUNNELSTREAM_LLM_BASE_URL overrides the endpoint of any OpenAI-style provider."""
    base = os.environ.get("FUNNELSTREAM_LLM_BASE_URL", "").strip().rstrip("/")
    if not base and provider() == "ollama":
        base = OLLAMA_CLOUD_BASE
    return base + "/chat/completions" if base else ROUTER_URL


def has_credentials() -> bool:
    if provider() == "anthropic":
        return bool(os.environ.get("ANTHROPIC_API_KEY"))
    return bool(active_token() or os.environ.get("FUNNELSTREAM_LLM_BASE_URL"))


def missing_credentials_reason() -> str:
    return "no %s" % {"ollama": "OLLAMA_API_KEY", "huggingface": "Hugging Face token (HUGGINGFACE_TOKEN=hf_...)",
                      "anthropic": "ANTHROPIC_API_KEY"}[provider()]


class HFClient:
    """Minimal OpenAI-style chat client (Ollama cloud, Hugging Face router, any compatible server); tools supported."""

    def __init__(self, token: str = None, model: str = None, timeout: int = 300):
        self.token = token or active_token()
        self.model = model or default_model()
        self.timeout = timeout

    def chat(self, messages, system=None, tools=None, max_tokens=4000, model=None) -> dict:
        """Returns the assistant message dict (content, optional tool_calls). Raises RuntimeError on API errors."""
        import requests
        body = {"model": model or self.model, "max_tokens": max_tokens,
                "messages": ([{"role": "system", "content": system}] if system else []) + list(messages)}
        if tools:
            body["tools"] = tools
        r = requests.post(chat_url(), json=body, timeout=self.timeout,
                          headers={"Authorization": "Bearer " + self.token} if self.token else {})
        if r.status_code != 200:
            raise RuntimeError("%s API %d: %s" % (provider_label(), r.status_code, r.text[:300]))
        return r.json()["choices"][0]["message"]
