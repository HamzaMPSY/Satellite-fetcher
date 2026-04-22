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
.nimbus-pipeline-shell {
    position: relative;
    margin: 12px 0 10px 0;
    padding: 16px;
    border-radius: 24px;
    border: 1px solid rgba(56, 189, 248, 0.14);
    background:
        radial-gradient(circle at top right, rgba(45, 212, 191, 0.10), transparent 24%),
        radial-gradient(circle at left center, rgba(56, 189, 248, 0.08), transparent 22%),
        linear-gradient(180deg, rgba(8, 14, 28, 0.98), rgba(5, 9, 18, 0.98));
    box-shadow: 0 24px 60px rgba(2, 8, 23, 0.40);
    overflow: hidden;
}
.nimbus-pipeline-overview {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 16px;
    flex-wrap: wrap;
}
.nimbus-pipeline-title-block {
    display: flex;
    flex-direction: column;
    gap: 6px;
    min-width: 220px;
    flex: 1 1 280px;
}
.nimbus-pipeline-eyebrow {
    font-size: 0.68rem;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #7dd3fc;
}
.nimbus-pipeline-headline {
    font-size: 1.04rem;
    line-height: 1.35;
    font-weight: 700;
    color: #f8fafc;
}
.nimbus-pipeline-subtitle {
    font-size: 0.78rem;
    color: #94a3b8;
}
.nimbus-pipeline-metrics {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    justify-content: flex-end;
    flex: 0 1 auto;
}
.nimbus-pipeline-metric {
    min-width: 110px;
    padding: 10px 12px;
    border-radius: 16px;
    border: 1px solid rgba(148, 163, 184, 0.12);
    background: rgba(9, 15, 29, 0.72);
    display: flex;
    flex-direction: column;
    gap: 4px;
}
.nimbus-pipeline-metric span {
    font-size: 0.68rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: #7b8da8;
}
.nimbus-pipeline-metric strong {
    font-size: 0.92rem;
    line-height: 1.25;
    color: #e2e8f0;
}
.nimbus-pipeline-progress {
    margin: 14px 0 18px 0;
    height: 10px;
    border-radius: 999px;
    background: rgba(15, 23, 42, 0.92);
    border: 1px solid rgba(148, 163, 184, 0.10);
    overflow: hidden;
}
.nimbus-pipeline-progress span {
    display: block;
    height: 100%;
    border-radius: 999px;
    background: linear-gradient(90deg, #4ade80 0%, #22d3ee 48%, #38bdf8 100%);
    box-shadow: 0 0 24px rgba(34, 211, 238, 0.34);
}
.nimbus-stage-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(165px, 1fr));
    gap: 12px;
}
.nimbus-stage-card {
    position: relative;
    min-height: 148px;
    padding: 14px 14px 13px 14px;
    border-radius: 20px;
    border: 1px solid rgba(148, 163, 184, 0.14);
    background: linear-gradient(180deg, rgba(12, 18, 34, 0.96), rgba(8, 13, 26, 0.96));
    box-shadow: 0 18px 42px rgba(2, 8, 23, 0.28);
    transition: transform 0.18s ease, border-color 0.18s ease, box-shadow 0.18s ease;
    overflow: hidden;
}
.nimbus-stage-card::before {
    content: "";
    position: absolute;
    inset: 0 0 auto 0;
    height: 3px;
    background: rgba(148, 163, 184, 0.12);
}
.nimbus-stage-card.is-current {
    transform: translateY(-3px);
}
.nimbus-stage-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 10px;
}
.nimbus-stage-chip-row {
    display: inline-flex;
    align-items: center;
    gap: 8px;
}
.nimbus-stage-index {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 28px;
    height: 28px;
    border-radius: 999px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.67rem;
    font-weight: 700;
    color: #dbeafe;
    background: rgba(148, 163, 184, 0.14);
    border: 1px solid rgba(148, 163, 184, 0.10);
}
.nimbus-stage-badge {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 52px;
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
    letter-spacing: 0.10em;
    text-transform: uppercase;
    color: var(--muted);
}
.nimbus-stage-title {
    margin-top: 16px;
    font-size: 1.02rem;
    font-weight: 700;
    color: #f8fafc;
}
.nimbus-stage-detail {
    margin-top: 8px;
    min-height: 40px;
    font-size: 0.80rem;
    line-height: 1.5;
    color: #cbd5e1;
}
.nimbus-stage-pills {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 12px;
}
.nimbus-stage-pill {
    display: inline-flex;
    align-items: center;
    padding: 4px 8px;
    border-radius: 999px;
    font-size: 0.68rem;
    font-weight: 700;
    color: #cbd5e1;
    background: rgba(148, 163, 184, 0.12);
}
.nimbus-stage-done {
    border-color: rgba(74, 222, 128, 0.26);
    background:
        radial-gradient(circle at top right, rgba(74, 222, 128, 0.12), transparent 36%),
        linear-gradient(180deg, rgba(11, 29, 24, 0.98), rgba(8, 19, 18, 0.98));
}
.nimbus-stage-done::before {
    background: linear-gradient(90deg, rgba(74, 222, 128, 0.96), rgba(45, 212, 191, 0.82));
}
.nimbus-stage-done .nimbus-stage-index,
.nimbus-stage-done .nimbus-stage-badge,
.nimbus-stage-done .nimbus-stage-pill {
    background: rgba(74, 222, 128, 0.14);
    color: #86efac;
}
.nimbus-stage-done .nimbus-stage-status {
    color: #86efac;
}
.nimbus-stage-running {
    border-color: rgba(56, 189, 248, 0.34);
    background:
        radial-gradient(circle at top right, rgba(56, 189, 248, 0.14), transparent 38%),
        linear-gradient(180deg, rgba(10, 20, 36, 0.98), rgba(8, 13, 26, 0.98));
    box-shadow: 0 22px 54px rgba(8, 47, 73, 0.24);
}
.nimbus-stage-running::before {
    background: linear-gradient(90deg, rgba(56, 189, 248, 0.96), rgba(34, 211, 238, 0.82));
}
.nimbus-stage-running .nimbus-stage-index,
.nimbus-stage-running .nimbus-stage-badge,
.nimbus-stage-running .nimbus-stage-pill {
    background: rgba(56, 189, 248, 0.16);
    color: #7dd3fc;
}
.nimbus-stage-running .nimbus-stage-status {
    color: #7dd3fc;
}
.nimbus-stage-queued {
    border-color: rgba(251, 191, 36, 0.28);
    background:
        radial-gradient(circle at top right, rgba(251, 191, 36, 0.12), transparent 36%),
        linear-gradient(180deg, rgba(34, 24, 9, 0.98), rgba(20, 15, 8, 0.98));
}
.nimbus-stage-queued::before {
    background: linear-gradient(90deg, rgba(251, 191, 36, 0.94), rgba(249, 168, 37, 0.82));
}
.nimbus-stage-queued .nimbus-stage-index,
.nimbus-stage-queued .nimbus-stage-badge,
.nimbus-stage-queued .nimbus-stage-pill {
    background: rgba(251, 191, 36, 0.16);
    color: #fde68a;
}
.nimbus-stage-queued .nimbus-stage-status {
    color: #fde68a;
}
.nimbus-stage-failed {
    border-color: rgba(248, 113, 113, 0.30);
    background:
        radial-gradient(circle at top right, rgba(248, 113, 113, 0.12), transparent 36%),
        linear-gradient(180deg, rgba(40, 12, 17, 0.98), rgba(27, 10, 16, 0.98));
}
.nimbus-stage-failed::before {
    background: linear-gradient(90deg, rgba(248, 113, 113, 0.96), rgba(244, 63, 94, 0.82));
}
.nimbus-stage-failed .nimbus-stage-index,
.nimbus-stage-failed .nimbus-stage-badge,
.nimbus-stage-failed .nimbus-stage-pill {
    background: rgba(248, 113, 113, 0.14);
    color: #fca5a5;
}
.nimbus-stage-failed .nimbus-stage-status {
    color: #fca5a5;
}
.nimbus-stage-cancelled {
    border-color: rgba(192, 132, 252, 0.30);
    background:
        radial-gradient(circle at top right, rgba(192, 132, 252, 0.12), transparent 36%),
        linear-gradient(180deg, rgba(28, 13, 40, 0.98), rgba(20, 10, 30, 0.98));
}
.nimbus-stage-cancelled::before {
    background: linear-gradient(90deg, rgba(192, 132, 252, 0.96), rgba(168, 85, 247, 0.82));
}
.nimbus-stage-cancelled .nimbus-stage-index,
.nimbus-stage-cancelled .nimbus-stage-badge,
.nimbus-stage-cancelled .nimbus-stage-pill {
    background: rgba(192, 132, 252, 0.16);
    color: #d8b4fe;
}
.nimbus-stage-cancelled .nimbus-stage-status {
    color: #d8b4fe;
}
.nimbus-stage-pending {
    opacity: 0.96;
    border-color: rgba(100, 116, 139, 0.22);
    background:
        radial-gradient(circle at top right, rgba(148, 163, 184, 0.08), transparent 38%),
        linear-gradient(180deg, rgba(13, 18, 32, 0.96), rgba(9, 13, 24, 0.96));
}
.nimbus-stage-pending::before {
    background: rgba(100, 116, 139, 0.32);
}
.nimbus-stage-pending .nimbus-stage-index,
.nimbus-stage-pending .nimbus-stage-badge,
.nimbus-stage-pending .nimbus-stage-pill {
    background: rgba(100, 116, 139, 0.18);
    color: #cbd5e1;
}
.nimbus-stage-pending .nimbus-stage-status {
    color: #94a3b8;
}
@media (max-width: 900px) {
    .nimbus-pipeline-shell {
        padding: 14px;
        border-radius: 20px;
    }
    .nimbus-pipeline-metrics {
        width: 100%;
        justify-content: flex-start;
    }
    .nimbus-pipeline-metric {
        flex: 1 1 120px;
    }
    .nimbus-stage-grid {
        grid-template-columns: repeat(auto-fit, minmax(145px, 1fr));
    }
}
</style>
"""

__all__ = ["CUSTOM_CSS"]
