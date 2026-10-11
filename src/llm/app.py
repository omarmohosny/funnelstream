"""Streamlit chat UI for the FunnelStream analytics assistant.

    streamlit run src/llm/app.py
"""
import os
import sys

import streamlit as st

sys.path.insert(0, os.path.dirname(__file__))
import agent  # noqa: E402
import data_layer as dl  # noqa: E402

st.set_page_config(page_title="FunnelStream Assistant", page_icon="📊", layout="wide")
st.title("FunnelStream analytics assistant")

SOURCES = ["Simulated stream (no Docker)", "Kafka topics", "Delta tables", "Batch CSV"]
source = st.sidebar.radio("Data source", SOURCES)
key, extra = "small", ""
if source == SOURCES[2]:
    key = st.sidebar.text_input("Lake folder", "data/lake")
else:
    key = st.sidebar.selectbox("Dataset scale", ["small", "medium", "large"])
if source == SOURCES[0]:
    extra = st.sidebar.slider("Sessions to simulate", 500, 20000, 2000, 500)
elif source == SOURCES[1]:
    extra = st.sidebar.text_input("Kafka bootstrap", "localhost:9092")
if st.sidebar.button("Reload data"):
    st.cache_resource.clear()
agent.load_dotenv()
from modeling import llm_provider  # noqa: E402

st.sidebar.caption("LLM: %s, model %s (set FUNNELSTREAM_LLM_PROVIDER in .env to switch)" % (
    llm_provider.provider_label(), llm_provider.default_model()))
api_key = st.sidebar.text_input("%s key" % llm_provider.provider_label(),
                                llm_provider.active_token() if llm_provider.openai_style()
                                else os.environ.get("ANTHROPIC_API_KEY", ""), type="password")
workspace = st.sidebar.text_input("Workspace ID (only for sk-ant-usr keys)",
                                  os.environ.get("ANTHROPIC_WORKSPACE_ID", ""))


@st.cache_resource(show_spinner="Loading data...")
def get_con(src, k, x):
    if src == SOURCES[2]:
        return dl.connect_delta(k)
    if src == SOURCES[3]:
        return dl.connect(k)
    import event_source as es
    return es.connect_events("sim" if src == SOURCES[0] else "kafka", k,
                             bootstrap=x if src == SOURCES[1] else "localhost:9092",
                             sessions=x if src == SOURCES[0] else 2000)[0]


try:
    con = get_con(source, key, extra)
except Exception as e:  # missing folder, no broker, missing package
    st.error(f"{type(e).__name__}: {e}")
    st.stop()

with st.sidebar.expander("Schema the model sees"):
    st.code(dl.describe(con), language="text")

with st.expander("Run SQL directly (no LLM)"):
    sql = st.text_area("SELECT ...", "SELECT current_status, count(*) FROM order_current GROUP BY 1")
    if st.button("Run"):
        try:
            cols, rows, _ = dl.run_readonly(con, sql)
            st.dataframe([dict(zip(cols, r)) for r in rows])
        except Exception as e:
            st.error(e)

if "history" not in st.session_state:
    st.session_state.history, st.session_state.shown = [], []

for role, text in st.session_state.shown:
    st.chat_message(role).write(text)

if q := st.chat_input("e.g. Which platform has the best CTR?"):
    st.chat_message("user").write(q)
    st.session_state.shown.append(("user", q))
    if not api_key:
        st.error("Paste your Anthropic API key in the sidebar (or put it in a .env file).")
        st.stop()
    with st.chat_message("assistant"), st.spinner("Querying..."):
        a = agent.ask(agent.make_client(api_key, workspace), con, q, st.session_state.history, verbose=False)
        st.write(a)
    st.session_state.shown.append(("assistant", a))
