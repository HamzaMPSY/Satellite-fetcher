"""UI styling assets (CSS) for the Streamlit app."""

CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
:root {
    --bg-0: #040814;
    --bg-1: #08101f;
    --bg-2: rgba(15, 23, 42, 0.78);
    --panel: rgba(12, 18, 34, 0.82);
    --panel-strong: rgba(17, 24, 39, 0.92);
    --line: rgba(56, 120, 200, 0.14);
    --line-strong: rgba(56, 189, 248, 0.28);
    --text: #e2e8f0;
    --muted: #8aa0bc;
    --accent: #38bdf8;
    --accent-2: #2dd4bf;
    --shadow: 0 18px 48px rgba(2, 8, 23, 0.42);
}
html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"] {
    background:
        radial-gradient(circle at top left, rgba(56, 189, 248, 0.12), transparent 28%),
        radial-gradient(circle at top right, rgba(45, 212, 191, 0.08), transparent 24%),
        linear-gradient(180deg, var(--bg-1) 0%, var(--bg-0) 56%, #02050d 100%) !important;
    color: var(--text) !important;
    font-family: 'DM Sans', system-ui, sans-serif !important;
}
[data-testid="stHeader"] {
    background: rgba(4, 8, 20, 0.72) !important;
    backdrop-filter: blur(14px);
}
[data-testid="stMainBlockContainer"],
.main .block-container {
    padding-top: 4.5rem !important;
    padding-bottom: 2rem !important;
}
[data-testid="stSidebar"] {
    background:
        linear-gradient(180deg, rgba(8, 16, 31, 0.97), rgba(5, 10, 21, 0.98)) !important;
    border-right: 1px solid var(--line) !important;
    box-shadow: inset -1px 0 0 rgba(255,255,255,0.02) !important;
}
[data-testid="stSidebar"] p, [data-testid="stSidebar"] label, [data-testid="stSidebar"] span {
    color: var(--text) !important;
}
[data-testid="stSidebar"] .stTextInput > div > div,
[data-testid="stSidebar"] .stSelectbox > div > div,
[data-testid="stSidebar"] .stDateInput > div > div,
[data-testid="stSidebar"] .stTextArea textarea {
    background: rgba(10, 16, 30, 0.9) !important;
    border: 1px solid rgba(56, 120, 200, 0.12) !important;
    border-radius: 12px !important;
}
.stButton > button {
    background: rgba(56,189,248,0.08) !important;
    color: var(--accent) !important;
    border: 1px solid rgba(56,189,248,0.2) !important;
    border-radius: 12px !important;
    font-weight: 600 !important;
    transition: all 0.2s ease !important;
    box-shadow: 0 10px 30px rgba(2, 8, 23, 0.22) !important;
}
.stButton > button:hover {
    background: rgba(56,189,248,0.16) !important;
    border-color: var(--accent) !important;
    box-shadow: 0 14px 36px rgba(56,189,248,0.12) !important;
    transform: translateY(-1px);
}
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, var(--accent), var(--accent-2)) !important;
    border: none !important;
    color: #060a14 !important;
    font-weight: 700 !important;
}
.stTextInput > div > div > input,
.stTextArea textarea,
.stSelectbox > div > div,
.stDateInput > div > div {
    border-radius: 12px !important;
}
.stTabs [data-baseweb="tab-list"] {
    background: linear-gradient(180deg, rgba(8,16,31,0.92), rgba(7,12,24,0.95)) !important;
    border-radius: 16px !important;
    padding: 6px !important;
    border: 1px solid var(--line) !important;
    gap: 4px !important;
    width: 100% !important;
    box-shadow: var(--shadow) !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    border-radius: 12px !important;
    color: var(--muted) !important;
    font-weight: 600 !important;
    flex: 1 1 0% !important;
    justify-content: center !important;
    padding: 10px 14px !important;
    font-size: 0.9rem !important;
}
.stTabs [aria-selected="true"] {
    background: linear-gradient(180deg, rgba(56,189,248,0.18), rgba(45,212,191,0.10)) !important;
    color: var(--accent) !important;
    border: 1px solid rgba(56,189,248,0.14) !important;
}
[data-testid="stExpander"] {
    background: var(--panel-strong) !important;
    border: 1px solid var(--line) !important;
    border-radius: 12px !important;
    box-shadow: var(--shadow) !important;
}
[data-testid="stMetric"] {
    background: linear-gradient(180deg, rgba(12,18,34,0.92), rgba(9,14,28,0.92)) !important;
    border: 1px solid var(--line) !important;
    border-radius: 14px !important;
    padding: 0.7rem 0.9rem !important;
    box-shadow: var(--shadow) !important;
}
[data-testid="stMetricLabel"] {
    color: var(--muted) !important;
}
[data-testid="stMetricValue"] {
    color: var(--text) !important;
}
pre, code {
    background: #0b1120 !important;
    color: var(--text) !important;
    border-radius: 8px !important;
}
[data-testid="stDataFrame"],
[data-testid="stMarkdownContainer"] table {
    border: 1px solid var(--line) !important;
    border-radius: 14px !important;
    overflow: hidden !important;
    box-shadow: var(--shadow) !important;
}
::-webkit-scrollbar { width: 5px; }
::-webkit-scrollbar-track { background: #060a14; }
::-webkit-scrollbar-thumb { background: rgba(56,189,248,0.18); border-radius: 3px; }
/* Hide the leaflet component iframe border */
iframe[title="leaflet_map"] {
    border: none !important;
    border-radius: 14px !important;
    box-shadow: var(--shadow) !important;
}
.nimbus-stage-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 12px;
    margin: 12px 0 10px 0;
}
.nimbus-stage-card {
    background: linear-gradient(180deg, rgba(12,18,34,0.96), rgba(8,13,26,0.96));
    border: 1px solid var(--line);
    border-radius: 18px;
    padding: 12px 14px;
    min-height: 102px;
    box-shadow: var(--shadow);
    transition: transform 0.18s ease, border-color 0.18s ease, box-shadow 0.18s ease;
}
.nimbus-stage-card.is-current {
    transform: translateY(-2px);
}
.nimbus-stage-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 10px;
}
.nimbus-stage-badge {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 48px;
    padding: 4px 8px;
    border-radius: 999px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.68rem;
    font-weight: 700;
    letter-spacing: 0.06em;
    background: rgba(148, 163, 184, 0.12);
    color: #dbeafe;
}
.nimbus-stage-status {
    font-size: 0.68rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--muted);
}
.nimbus-stage-title {
    margin-top: 14px;
    font-size: 1rem;
    font-weight: 700;
    color: var(--text);
}
.nimbus-stage-meta {
    margin-top: 6px;
    font-size: 0.76rem;
    line-height: 1.45;
    color: var(--muted);
}
.nimbus-stage-done {
    border-color: rgba(74, 222, 128, 0.28);
    background: linear-gradient(180deg, rgba(12,30,22,0.98), rgba(9,19,18,0.98));
}
.nimbus-stage-done .nimbus-stage-badge {
    background: rgba(74, 222, 128, 0.14);
    color: #86efac;
}
.nimbus-stage-done .nimbus-stage-status {
    color: #86efac;
}
.nimbus-stage-running {
    border-color: rgba(56, 189, 248, 0.34);
    background:
        radial-gradient(circle at top right, rgba(56, 189, 248, 0.12), transparent 38%),
        linear-gradient(180deg, rgba(10,20,36,0.98), rgba(8,13,26,0.98));
    box-shadow: 0 20px 50px rgba(8, 47, 73, 0.24);
}
.nimbus-stage-running .nimbus-stage-badge {
    background: rgba(56, 189, 248, 0.14);
    color: #7dd3fc;
}
.nimbus-stage-running .nimbus-stage-status {
    color: #7dd3fc;
}
.nimbus-stage-queued {
    border-color: rgba(251, 191, 36, 0.28);
    background: linear-gradient(180deg, rgba(33,24,9,0.98), rgba(20,15,8,0.98));
}
.nimbus-stage-queued .nimbus-stage-badge {
    background: rgba(251, 191, 36, 0.16);
    color: #fde68a;
}
.nimbus-stage-queued .nimbus-stage-status {
    color: #fde68a;
}
.nimbus-stage-failed {
    border-color: rgba(248, 113, 113, 0.30);
    background: linear-gradient(180deg, rgba(40,12,17,0.98), rgba(27,10,16,0.98));
}
.nimbus-stage-failed .nimbus-stage-badge {
    background: rgba(248, 113, 113, 0.14);
    color: #fca5a5;
}
.nimbus-stage-failed .nimbus-stage-status {
    color: #fca5a5;
}
.nimbus-stage-cancelled {
    border-color: rgba(192, 132, 252, 0.30);
    background: linear-gradient(180deg, rgba(28,13,40,0.98), rgba(20,10,30,0.98));
}
.nimbus-stage-cancelled .nimbus-stage-badge {
    background: rgba(192, 132, 252, 0.16);
    color: #d8b4fe;
}
.nimbus-stage-cancelled .nimbus-stage-status {
    color: #d8b4fe;
}
.nimbus-stage-pending {
    opacity: 0.84;
}
</style>
"""

__all__ = ["CUSTOM_CSS"]
