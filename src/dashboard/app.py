"""
app.py  (v5 – Professional HR Dashboard)
- yaxis 중복 에러 수정
- 다크/라이트 테마 전환
- 배색 개선
- 미래 항목 placeholder 포함
"""
from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yaml

# ─────────────────────────────────────────────
# 테마
# ─────────────────────────────────────────────
THEMES = {
    "dark": {
        "bg":       "#0F172A",
        "sidebar":  "#1E293B",
        "card":     "#1E293B",
        "card2":    "#0F172A",
        "border":   "#334155",
        "text":     "#F1F5F9",
        "muted":    "#94A3B8",
        "grid":     "#1E293B",
        "hire":     "#3B82F6",
        "exit":     "#EF4444",
        "active":   "#10B981",
        "warn":     "#F59E0B",
        "plotbg":   "rgba(0,0,0,0)",
        "paperbg":  "rgba(0,0,0,0)",
    },
    "light": {
        "bg":       "#F8FAFC",
        "sidebar":  "#FFFFFF",
        "card":     "#FFFFFF",
        "card2":    "#F1F5F9",
        "border":   "#E2E8F0",
        "text":     "#0F172A",
        "muted":    "#64748B",
        "grid":     "#E2E8F0",
        "hire":     "#2563EB",
        "exit":     "#DC2626",
        "active":   "#059669",
        "warn":     "#D97706",
        "plotbg":   "rgba(248,250,252,0)",
        "paperbg":  "rgba(248,250,252,0)",
    },
}

def _css(t: dict) -> str:
    return f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&display=swap');
html,body,[class*="css"]{{ font-family:'Noto Sans KR',sans-serif; }}
.stApp{{ background:{t['bg']}; color:{t['text']}; }}
section[data-testid="stSidebar"]{{
    background:{t['sidebar']};
    border-right:1px solid {t['border']};
}}
/* KPI 카드 */
.kpi-card{{
    background:linear-gradient(135deg,{t['card']},{t['card2']});
    border:1px solid {t['border']};
    border-radius:14px;
    padding:22px 16px;
    text-align:center;
    transition:border-color .2s, box-shadow .2s;
}}
.kpi-card:hover{{
    border-color:{t['hire']};
    box-shadow:0 0 16px {t['hire']}33;
}}
.kpi-label{{
    font-size:11px; color:{t['muted']};
    letter-spacing:1.2px; text-transform:uppercase; margin-bottom:8px;
}}
.kpi-value{{ font-size:34px; font-weight:700; line-height:1; }}
.kpi-delta{{ font-size:11px; margin-top:6px; color:{t['muted']}; }}

/* 섹션 헤더 */
.sh{{
    font-size:13px; font-weight:600; color:{t['muted']};
    letter-spacing:1.5px; text-transform:uppercase;
    border-left:3px solid {t['hire']};
    padding-left:10px; margin:24px 0 12px 0;
}}

/* 준비중 카드 */
.soon-card{{
    background:{t['card']};
    border:1px dashed {t['border']};
    border-radius:10px;
    padding:32px;
    text-align:center;
    color:{t['muted']};
}}

/* 탭 */
button[data-baseweb="tab"]{{ color:{t['muted']} !important; }}
button[data-baseweb="tab"][aria-selected="true"]{{
    color:{t['text']} !important;
    border-bottom-color:{t['hire']} !important;
}}

/* metric */
[data-testid="metric-container"]{{
    background:{t['card']};
    border:1px solid {t['border']};
    border-radius:8px; padding:12px;
}}
[data-testid="metric-container"] label{{color:{t['muted']} !important;}}
[data-testid="metric-container"] [data-testid="stMetricValue"]{{color:{t['text']} !important;}}

/* 사이드바 텍스트 */
section[data-testid="stSidebar"] *{{ color:{t['text']}; }}
section[data-testid="stSidebar"] .stCaption{{ color:{t['muted']} !important; }}

/* 연도별 숫자 행 */
.yr-row{{
    display:flex; justify-content:space-between;
    padding:5px 0; border-bottom:1px solid {t['border']}; font-size:12px;
}}
</style>
"""

def _plotly_base(t: dict) -> dict:
    """yaxis/xaxis 없는 순수 base layout — update_layout 에서 yaxis 추가 시 충돌 없음"""
    return dict(
        paper_bgcolor=t["paperbg"],
        plot_bgcolor=t["plotbg"],
        font=dict(color=t["text"], family="Noto Sans KR, sans-serif"),
        margin=dict(l=20, r=20, t=60, b=20),
    )

def _axis(t: dict) -> dict:
    return dict(gridcolor=t["grid"], linecolor=t["border"], zerolinecolor=t["border"])


# ─────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────
def _load_config():
    p = Path("config.yaml")
    if not p.exists():
        return {"paths":{"raw_dir":"data/raw","processed_dir":"data/processed"}}
    return yaml.safe_load(p.read_text(encoding="utf-8"))

def _csv(path):
    if not path.exists(): return pd.DataFrame()
    try: return pd.read_csv(path, encoding="utf-8-sig")
    except:
        try: return pd.read_csv(path)
        except: return pd.DataFrame()

def _json(path):
    if not path.exists(): return {}
    try: return json.loads(path.read_text(encoding="utf-8"))
    except: return {}

def _standardize_hc(df: pd.DataFrame, col_map: dict) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    d = df.copy()
    already = set()
    for std, kr in col_map.items():
        if not kr or not std or kr in already:
            continue
        if kr in d.columns and std not in d.columns:
            if std in ("join_date", "exit_date"):
                d[std] = pd.to_datetime(d[kr], errors="coerce")
            else:
                d[std] = d[kr].astype(str).str.strip()
            already.add(kr)
    d = d.drop(columns=[kr for kr in already if kr in d.columns], errors="ignore")
    if "name" in d.columns:
        d = d[d["name"].notna() & ~d["name"].isin(["nan","None",""])]
    return d.reset_index(drop=True)


@st.cache_data(show_spinner=False)
def _load_data(processed_dir):
    d = Path(processed_dir)

    # processed root 우선, 없으면 gold 서브폴더
    def _pick(name):
        return _csv(d/name) if (d/name).exists() else _csv(d/"gold"/name)

    base = {
        "master":    _pick("master_auto.csv"),
        "yearly":    _pick("turnover_yearly.csv"),
        "monthly":   _pick("monthly.csv"),
        "dept":      _pick("dept_monthly.csv"),
        "early":     _pick("early_exit_30d.csv"),
        "attrition": _csv(Path("outputs/reports/attrition_scored.csv")),
        "report":    _json(d/"ingest_report.json"),
        "hc_active": pd.DataFrame(),
        "hc_exited": pd.DataFrame(),
        "_hc_error": "",
        "_hc_sheets": [],
    }

    # 1순위: processed/headcount_active.csv (preprocess.py가 이미 만들어 놓은 것)
    if (d/"headcount_active.csv").exists():
        base["hc_active"] = _csv(d/"headcount_active.csv")
        if (d/"headcount_exited.csv").exists():
            base["hc_exited"] = _csv(d/"headcount_exited.csv")
        return base

    # 2순위: raw headcount 폴더에서 직접 로드
    try:
        cfg     = _load_config()
        col_map = (cfg.get("columns") or {}).get("headcount_2024", {})
        raw_dir = Path(cfg["paths"]["raw_dir"])
        hc_file = (cfg.get("files") or {}).get("headcount_2024_xlsx", "")

        # 경로 후보 탐색
        candidates = [
            raw_dir / hc_file,
            raw_dir / "headcount" / hc_file,
        ]
        # headcount 폴더 내 xlsx 전체도 후보
        hc_dir = raw_dir / "headcount"
        if hc_dir.exists():
            candidates += sorted(hc_dir.glob("*.xlsx"))
            candidates += sorted(hc_dir.glob("*.xls"))

        hc_path = None
        for c in candidates:
            if Path(c).exists():
                hc_path = Path(c)
                break

        if hc_path is None:
            base["_hc_error"] = f"파일 없음. 탐색: {[str(c) for c in candidates[:3]]}"
        else:
            from src.processor.headcount_2024_loader import load_headcount_2024
            hc = load_headcount_2024(hc_path)
            try:
                import openpyxl
                wb = openpyxl.load_workbook(hc_path, read_only=True)
                base["_hc_sheets"] = wb.sheetnames; wb.close()
            except Exception: pass

            if isinstance(hc.get("active"), pd.DataFrame) and not hc["active"].empty:
                base["hc_active"] = _standardize_hc(hc["active"], col_map)
            else:
                base["_hc_error"] = f"active 시트 인식 실패. 시트: {base['_hc_sheets']}"

            if isinstance(hc.get("exited"), pd.DataFrame) and not hc["exited"].empty:
                base["hc_exited"] = _standardize_hc(hc["exited"], col_map)

    except Exception:
        import traceback
        base["_hc_error"] = traceback.format_exc()

    return base

@st.cache_data(ttl=30, show_spinner=False)
def _new_files(raw_dir, processed_dir):
    try:
        from src.processor.ingest_manifest import file_fingerprint, is_new_or_changed, load_manifest
        manifest = load_manifest(Path(processed_dir)/"ingest_manifest.json")
        out = []
        for f in sorted(Path(raw_dir).glob("*.xls*")):
            if f.name.startswith("~$"): continue
            try:
                if is_new_or_changed(file_fingerprint(f), manifest): out.append(f.name)
            except: out.append(f.name)
        return out
    except: return []

def _apply(df, sy, ey, orgs, depts):
    if df is None or df.empty: return df
    out = df.copy()
    if sy and ey:
        for col in ["year","snapshot_year"]:
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce")
                out = out[(out[col]>=sy)&(out[col]<=ey)]; break
        if "month" in out.columns:
            out["month"] = pd.to_datetime(out["month"], errors="coerce")
            out = out[(out["month"].dt.year>=sy)&(out["month"].dt.year<=ey)]
    if orgs and "org" in out.columns:
        out = out[out["org"].astype(str).isin(orgs)]
    if depts and "dept" in out.columns:
        out = out[out["dept"].astype(str).isin(depts)]
    return out

def _kpis(hc_active, hc_exited, yearly, early_df):
    """
    총재직인원  → headcount 최신파일 active 행수 (가장 신뢰)
    당해 입퇴사 → turnover_yearly 기반
    조기퇴사    → early_exit_30d
    """
    import datetime
    ty = datetime.date.today().year

    # 총재직인원: headcount active 우선
    total = len(hc_active) if not hc_active.empty else 0

    # 당해 입퇴사
    hires = exits = 0
    yoy = None
    if not yearly.empty and "year" in yearly.columns:
        yr = yearly.copy()
        yr["year"] = pd.to_numeric(yr["year"], errors="coerce")
        if "hires" in yr.columns:
            hires = int(yr[yr["year"]==ty]["hires"].sum())
            prv   = yr[yr["year"]==ty-1]["hires"].sum()
            if prv > 0: yoy = round((hires-prv)/prv*100, 1)
        if "exits" in yr.columns:
            exits = int(yr[yr["year"]==ty]["exits"].sum())

    rate  = round(exits/max(total,1)*100, 1) if total > 0 else 0.0
    early = len(early_df) if not early_df.empty else 0

    return {"total":total,"hires":hires,"exits":exits,"rate":rate,
            "early":early,"yoy":yoy,"ty":ty}

def _dl(fig, label, key):
    try:
        img = fig.to_image(format="png", scale=2)
        st.download_button(f"⬇ {label}", img, f"{key}.png", "image/png", key=key)
    except: pass

def _soon(t, msg="데이터 연동 후 활성화됩니다"):
    st.markdown(f'<div class="soon-card">🔒 준비중<br><small>{msg}</small></div>',
                unsafe_allow_html=True)


# ─────────────────────────────────────────────
# 차트
# ─────────────────────────────────────────────
def _chart_yearly(yearly, t):
    if yearly.empty: return None
    df = yearly.copy()
    df["year"] = pd.to_numeric(df["year"],errors="coerce").astype("Int64")
    df = df.dropna(subset=["year"]).sort_values("year")
    if "hires" in df.columns and "exits" in df.columns:
        df["총원"] = (df["hires"].cumsum()-df["exits"].cumsum()).clip(lower=0)

    fig = go.Figure()
    if "hires" in df.columns:
        fig.add_trace(go.Bar(x=df["year"],y=df["hires"],name="입사",
            marker_color=t["hire"],opacity=0.85))
    if "exits" in df.columns:
        fig.add_trace(go.Bar(x=df["year"],y=df["exits"],name="퇴사",
            marker_color=t["exit"],opacity=0.85))
    if "총원" in df.columns:
        fig.add_trace(go.Scatter(x=df["year"],y=df["총원"],name="총원",
            mode="lines+markers",
            line=dict(color=t["active"],width=2.5),
            marker=dict(size=7), yaxis="y2"))

    base = _plotly_base(t)
    ax   = _axis(t)
    fig.update_layout(
        **base,
        title=dict(text="연도별 입사 · 퇴사 · 총원", font=dict(size=15)),
        barmode="group", height=390,
        xaxis=dict(**ax, title="연도", tickmode="linear", dtick=1, tickformat="d"),
        yaxis=dict(**ax, title="인원(명)"),
        yaxis2=dict(title="총원(명)", overlaying="y", side="right",
                    showgrid=False, color=t["active"],
                    tickfont=dict(color=t["active"])),
        legend=dict(orientation="h", y=1.12, x=0,
                    bgcolor="rgba(0,0,0,0)"),
    )
    return fig

def _chart_monthly(monthly, t):
    if monthly.empty: return None
    df = monthly.copy()
    df["month"] = pd.to_datetime(df["month"],errors="coerce")
    df = df.dropna(subset=["month"]).sort_values("month")
    # 누적 순증감 (총원 추이)
    if "hires" in df.columns and "exits" in df.columns:
        df["누적총원"] = (df["hires"].cumsum() - df["exits"].cumsum()).clip(lower=0)

    fig = go.Figure()
    if "hires" in df.columns:
        fig.add_trace(go.Bar(
            x=df["month"], y=df["hires"], name="입사",
            marker_color=t["hire"], opacity=0.8, yaxis="y1"))
    if "exits" in df.columns:
        fig.add_trace(go.Bar(
            x=df["month"], y=df["exits"], name="퇴사",
            marker_color=t["exit"], opacity=0.8, yaxis="y1"))
    if "누적총원" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["month"], y=df["누적총원"], name="누적총원",
            mode="lines", line=dict(color=t["active"], width=2),
            yaxis="y2"))

    ax = _axis(t)
    fig.update_layout(**_plotly_base(t),
        title=dict(text="월별 입사 · 퇴사 · 누적총원", font=dict(size=15)),
        hovermode="x unified", height=380,
        barmode="overlay",
        xaxis=dict(**ax, tickformat="%y.%m"),
        yaxis=dict(**ax, title="인원(명)"),
        yaxis2=dict(title="누적총원(명)", overlaying="y", side="right",
                    showgrid=False, color=t["active"],
                    tickfont=dict(color=t["active"])),
        legend=dict(orientation="h", y=1.12, x=0, bgcolor="rgba(0,0,0,0)"))
    return fig

def _chart_treemap(master, t):
    if master.empty or "org" not in master.columns: return None
    m = master.copy()
    if "exit_date" in m.columns: m=m[m["exit_date"].isna()]
    cnt = m.groupby("org").size().reset_index(name="인원")
    cnt = cnt[cnt["인원"]>0]
    if cnt.empty: return None
    fig = px.treemap(cnt, path=["org"], values="인원",
        color="인원",
        color_continuous_scale=[[0,"#1E3A5F"],[0.5,"#2563EB"],[1,"#60A5FA"]])
    fig.update_layout(**_plotly_base(t),
        title=dict(text="본부별 재직 인원 분포", font=dict(size=15)), height=370)
    fig.update_traces(textinfo="label+value", textfont_size=13,
        marker=dict(line=dict(width=1.5, color=t["bg"])))
    return fig

def _chart_dept(master, org_sel, t):
    if master.empty or "dept" not in master.columns: return None
    m = master.copy()
    # dept가 datetime으로 잘못 파싱된 경우 문자열 복원
    if pd.api.types.is_datetime64_any_dtype(m["dept"]):
        return None  # 데이터 오류 - 표시 안 함
    m["dept"] = m["dept"].astype(str).str.strip()
    m = m[m["dept"].ne("") & m["dept"].ne("nan") & m["dept"].ne("None")]
    if "exit_date" in m.columns: m=m[m["exit_date"].isna()]
    if org_sel and "org" in m.columns:
        m=m[m["org"].astype(str).isin(org_sel)]
    cnt = m.groupby("dept").size().reset_index(name="인원").sort_values("인원",ascending=True)
    if cnt.empty: return None
    ax = _axis(t)
    fig = go.Figure(go.Bar(
        x=cnt["인원"], y=cnt["dept"], orientation="h",
        marker=dict(
            color=cnt["인원"],
            colorscale=[[0,"#1E3A5F"],[1,t["hire"]]],
            showscale=False,
        ),
        text=cnt["인원"], textposition="outside",
        textfont=dict(color=t["text"]),
    ))
    fig.update_layout(**_plotly_base(t),
        title=dict(text="팀/부서별 재직 인원", font=dict(size=15)),
        xaxis=dict(**ax, title="인원(명)"),
        yaxis=dict(**ax, title=""),
        height=max(320, len(cnt)*28),
        showlegend=False)
    return fig

def _chart_early(early, t):
    if early.empty or "exit_date" not in early.columns: return None
    df = early.copy()
    df["exit_date"]=pd.to_datetime(df["exit_date"],errors="coerce")
    df["month"]=df["exit_date"].dt.to_period("M").dt.to_timestamp()
    cnt=df.groupby("month").size().reset_index(name="건수").sort_values("month")
    ax = _axis(t)
    fig=go.Figure(go.Bar(x=cnt["month"],y=cnt["건수"],
        marker_color=t["warn"], opacity=0.85,
        text=cnt["건수"], textposition="outside",
        textfont=dict(color=t["text"])))
    fig.update_layout(**_plotly_base(t),
        title=dict(text="30일 이내 조기퇴사 월별 건수", font=dict(size=15)),
        xaxis=dict(**ax), yaxis=dict(**ax, title="건수"),
        height=300)
    return fig

def _chart_attrition(attr, t):
    if attr.empty or "attrition_risk_score" not in attr.columns: return None
    top = attr.sort_values("attrition_risk_score",ascending=False).head(20).copy()
    nc = "name" if "name" in top.columns else top.columns[0]
    top = top.sort_values("attrition_risk_score",ascending=True)
    colors = top["attrition_risk_score"].apply(
        lambda x: t["exit"] if x>=70 else (t["warn"] if x>=40 else t["hire"]))
    ax = _axis(t)
    fig=go.Figure(go.Bar(
        x=top["attrition_risk_score"], y=top[nc], orientation="h",
        marker_color=colors,
        text=top["attrition_risk_score"].round(1), textposition="outside",
        textfont=dict(color=t["text"])))
    fig.update_layout(**_plotly_base(t),
        title=dict(text="Attrition Risk Top 20", font=dict(size=15)),
        xaxis=dict(**ax, range=[0,108], title="Risk Score"),
        yaxis=dict(**ax, title=""),
        height=520)
    return fig


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────
def run():
    st.set_page_config(page_title="HR Intelligence", page_icon="📊", layout="wide")

    cfg = _load_config()
    raw_dir       = cfg["paths"]["raw_dir"]
    processed_dir = cfg["paths"]["processed_dir"]
    data   = _load_data(processed_dir)
    master = data["master"]; yearly  = data["yearly"]
    monthly= data["monthly"]; early  = data["early"]
    attr   = data["attrition"]; rep  = data["report"]
    kpi    = {}  # _f 정의 후 아래에서 계산

    # ── 테마 선택 (session_state)
    if "theme" not in st.session_state:
        st.session_state.theme = "dark"

    t = THEMES[st.session_state.theme]
    st.markdown(_css(t), unsafe_allow_html=True)

    # ─── SIDEBAR ──────────────────────────────
    with st.sidebar:
        st.markdown(f"### 📊 HR Intelligence")
        st.caption("v5.0")

        # 테마 토글
        col_a, col_b = st.columns(2)
        if col_a.button("🌙 다크", use_container_width=True,
                        type="primary" if st.session_state.theme=="dark" else "secondary"):
            st.session_state.theme="dark"; st.rerun()
        if col_b.button("☀️ 라이트", use_container_width=True,
                        type="primary" if st.session_state.theme=="light" else "secondary"):
            st.session_state.theme="light"; st.rerun()

        st.divider()

        # 신규 파일 감지
        nf = _new_files(raw_dir, processed_dir)
        if nf:
            st.error(f"🆕 신규 파일 {len(nf)}개 감지")
            with st.expander("목록"):
                for f in nf: st.caption(f"• {f}")
            st.caption("`python preprocess.py` 실행 후 새로고침")
            st.divider()

        # 데이터 현황
        with st.expander("📦 데이터 현황", expanded=True):
            c1,c2=st.columns(2)
            c1.metric("처리파일", rep.get("read_ok","-") if rep else "-")
            c2.metric("실패", rep.get("read_fail","-") if rep else "-")
            if rep: st.caption(f"⏱ {rep.get('elapsed_sec','-')}초")
            # headcount 로드 상태
            hc_ok = not data["hc_active"].empty
            hc_n  = len(data["hc_active"]) if hc_ok else 0
            if hc_ok:
                st.success(f"✅ 인원현황: {hc_n}명")
                with st.expander("컬럼 확인"):
                    st.caption(", ".join(data["hc_active"].columns.tolist()))
            else:
                err = data.get("_hc_error", "")
                sheets = data.get("_hc_sheets", [])
                st.warning("⚠ 인원현황 미로드")
                with st.expander("🔍 진단 정보"):
                    if sheets:
                        st.caption("시트 목록: " + ", ".join(sheets))
                    if err:
                        st.code(err[:600])
                    if not sheets and not err:
                        st.caption("파일 로드 자체가 실패 — 경로/파일명 확인 필요")

        st.divider()
        st.markdown("**🔍 필터**")

        # 기간 슬라이더: yearly 기준
        all_years=[]
        if not yearly.empty and "year" in yearly.columns:
            all_years = sorted(pd.to_numeric(yearly["year"],errors="coerce").dropna().astype(int).unique())
        elif not master.empty:
            for col in ["join_date","exit_date"]:
                if col in master.columns:
                    yrs=pd.to_datetime(master[col],errors="coerce").dt.year.dropna().astype(int).tolist()
                    all_years.extend(yrs)
            all_years=sorted(set(all_years))

        if len(all_years)>=2:
            latest=max(all_years)
            sy,ey=st.select_slider("기간",options=all_years,
                value=(max(min(all_years),latest-4),latest))
        elif len(all_years)==1:
            sy=ey=all_years[0]
        else:
            sy=ey=None

        # 본부/팀 필터: hc_active 기준 (실제 재직자 기준이 정확)
        org_src = data["hc_active"] if hc_ok else master
        orgs = sorted(org_src["org"].dropna().astype(str).unique()) if "org" in org_src.columns else []
        org_sel=st.multiselect("본부",orgs)
        depts=[]
        if "dept" in org_src.columns:
            tmp = org_src[org_src["org"].astype(str).isin(org_sel)] if org_sel else org_src
            depts=sorted(tmp["dept"].dropna().astype(str).unique())
        dept_sel=st.multiselect("팀/부서",depts)

        st.divider()
        if st.button("🔄 새로고침",use_container_width=True):
            st.cache_data.clear(); st.rerun()

    def _f(df): return _apply(df,sy,ey,org_sel,dept_sel)
    # KPI는 필터 무관 — headcount 최신파일 기준 전체
    kpi = _kpis(data['hc_active'], data['hc_exited'], yearly, early)

    # ─── HEADER ───────────────────────────────
    st.markdown(f"## 📊 HR Intelligence Dashboard")
    hc_src = '2024년 인원현황 파일 기준 (기간필터 미적용)' if not data['hc_active'].empty else '스냅샷 추정 (기간필터 미적용)'
    if kpi:
        st.caption(f"총인원 기준: {hc_src}  |  연도별/월별: 필터 적용  |  테마: {'다크' if st.session_state.theme=='dark' else '라이트'}")
    st.divider()

    # ─── KPI 카드 ─────────────────────────────
    if kpi:
        c1,c2,c3,c4,c5=st.columns(5)
        def _card(col, label, val, color, delta=""):
            with col:
                st.markdown(f"""<div class="kpi-card">
                    <div class="kpi-label">{label}</div>
                    <div class="kpi-value" style="color:{color}">{val}</div>
                    <div class="kpi-delta">{delta}</div>
                </div>""", unsafe_allow_html=True)

        _card(c1,"기준일 재직인원",f"{kpi['total']:,}명",t["active"],"2024년 인원현황 기준")
        yoy_txt = (f"{'▲' if (kpi['yoy'] or 0)>=0 else '▼'} YoY {abs(kpi['yoy'] or 0)}%") if kpi["yoy"] else "당해 입사"
        _card(c2,f"{kpi['ty']}년 입사",f"{kpi['hires']:,}명",t["hire"],yoy_txt)
        _card(c3,f"{kpi['ty']}년 퇴사",f"{kpi['exits']:,}명",t["exit"])
        rate_color = t["exit"] if kpi["rate"]>=15 else (t["warn"] if kpi["rate"]>=10 else t["active"])
        _card(c4,"연간 퇴사율",f"{kpi['rate']}%",rate_color,"⚠ 주의" if kpi["rate"]>=15 else "양호")
        _card(c5,"조기퇴사(30일↓)",f"{kpi['early']}건",t["warn"],"누적")

    st.markdown("<br>", unsafe_allow_html=True)

    if master.empty:
        st.warning("데이터 없음. `python preprocess.py` 를 먼저 실행하세요.")
        return

    # ─── TABS ─────────────────────────────────
    tabs = st.tabs([
        "📈 인력 변동", "📅 월별 추세", "🏢 조직 현황",
        "⚠️ 리스크", "🎯 Attrition",
        "💰 보상 분석", "🔬 스킬 현황", "📋 채용 현황"
    ])

    # ── TAB 1: 인력 변동
    with tabs[0]:
        st.markdown('<div class="sh">연도별 인력 변동</div>', unsafe_allow_html=True)
        yr_f = _f(yearly)
        fig  = _chart_yearly(yr_f, t)
        if fig:
            col1,col2=st.columns([3,1])
            with col1:
                st.plotly_chart(fig, use_container_width=True, key="yr_bar")
            with col2:
                st.markdown("<br>",unsafe_allow_html=True)
                for _,row in yr_f.iterrows():
                    net=int(row.get("hires",0))-int(row.get("exits",0))
                    nc = t["active"] if net>=0 else t["exit"]
                    st.markdown(f"""<div class="yr-row">
                        <span style="color:{t['muted']}">{int(row['year'])}</span>
                        <span>입<b style="color:{t['hire']}"> {int(row.get('hires',0))}</b>
                        퇴<b style="color:{t['exit']}"> {int(row.get('exits',0))}</b>
                        <span style="color:{nc}"> ({'+' if net>=0 else ''}{net})</span></span>
                        </div>""", unsafe_allow_html=True)
                st.markdown("<br>",unsafe_allow_html=True)
                _dl(fig,"연도별현황","dl_yr")
        else:
            st.info("연도별 데이터가 없습니다.")
        with st.expander("📋 상세 데이터"):
            if not yr_f.empty:
                show=yr_f.copy()
                show.columns=[c.replace("hires","입사").replace("exits","퇴사")
                               .replace("net","순증감").replace("year","연도") for c in show.columns]
                st.dataframe(show, use_container_width=True, hide_index=True)

    # ── TAB 2: 월별 추세
    with tabs[1]:
        st.markdown('<div class="sh">월별 입사 · 퇴사 추세</div>', unsafe_allow_html=True)
        mon_f=_f(monthly); fig=_chart_monthly(mon_f,t)
        if fig:
            st.plotly_chart(fig, use_container_width=True, key="mon_line")
            _dl(fig,"월별추세","dl_mon")
            if not mon_f.empty:
                st.markdown('<div class="sh">최근 12개월 (입사▲ / 퇴사▼)</div>', unsafe_allow_html=True)
                st.caption("※ 퇴사자 명단 기반 집계. 스냅샷 파일과 차이 있을 수 있음.")
                df_m=mon_f.copy()
                df_m["month"]=pd.to_datetime(df_m["month"],errors="coerce")
                recent=df_m.sort_values("month").tail(12)
                cols=st.columns(len(recent))
                for i,(_,row) in enumerate(recent.iterrows()):
                    with cols[i]:
                        lbl=row["month"].strftime("%y.%m") if pd.notna(row.get("month")) else "-"
                        h=int(row["hires"]) if "hires" in row and pd.notna(row["hires"]) else 0
                        e=int(row["exits"]) if "exits" in row and pd.notna(row["exits"]) else 0
                        st.markdown(f"""<div style="text-align:center;padding:6px 2px;
                            background:{t['card']};border-radius:6px;
                            border:1px solid {t['border']};font-size:10px;">
                            <div style="color:{t['muted']};margin-bottom:2px">{lbl}</div>
                            <div style="color:{t['hire']}">▲{h}</div>
                            <div style="color:{t['exit']}">▼{e}</div>
                            </div>""", unsafe_allow_html=True)
        else:
            st.info("월별 데이터가 없습니다.")

    # ── TAB 3: 조직 현황
    with tabs[2]:
        st.markdown('<div class="sh">조직별 인원 현황</div>', unsafe_allow_html=True)
        # 조직현황: headcount 최신파일 우선. 없으면 master 최신 snapshot_year 재직자만
        if not data["hc_active"].empty:
            mf = data["hc_active"].copy()
        else:
            mf = master.copy()
            if "snapshot_year" in mf.columns:
                mf["snapshot_year"] = pd.to_numeric(mf["snapshot_year"], errors="coerce")
                latest_snap = int(mf["snapshot_year"].dropna().max()) if mf["snapshot_year"].notna().any() else 9999
                mf = mf[mf["snapshot_year"] == latest_snap]  # 최신 연도만
            # 퇴사자 제외
            if "exit_date" in mf.columns:
                mf["exit_date"] = pd.to_datetime(mf["exit_date"], errors="coerce")
                cut = pd.Timestamp(year=latest_snap if "snapshot_year" in master.columns else 2099, month=12, day=31)
                mf = mf[(mf["exit_date"].isna()) | (mf["exit_date"] > cut)]
            # 중복 제거 (같은 연도라도 중복 행 있을 수 있음)
            key = "emp_id" if "emp_id" in mf.columns else ("name" if "name" in mf.columns else None)
            if key:
                mf = mf.drop_duplicates(subset=[key], keep="first")
        # org/dept 필터 적용
        if org_sel and "org" in mf.columns:
            mf = mf[mf["org"].astype(str).isin(org_sel)]
        c1,c2=st.columns(2)
        with c1:
            fig=_chart_treemap(mf,t)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="treemap")
                _dl(fig,"조직분포","dl_tree")
        with c2:
            fig=_chart_dept(mf,org_sel,t)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="dept_bar")
                _dl(fig,"부서별인원","dl_dept")
        dept_f=_f(data["dept"])
        if not dept_f.empty:
            with st.expander("📋 부서별 월간 상세"):
                st.dataframe(dept_f.head(300), use_container_width=True, hide_index=True)

    # ── TAB 4: 리스크
    with tabs[3]:
        st.markdown('<div class="sh">인력 리스크</div>', unsafe_allow_html=True)
        ef=_f(early)
        r1,r2,r3=st.columns(3)
        with r1:
            st.metric("30일 조기퇴사",f"{len(ef)}건",
                delta="⚠ 주의" if len(ef)>5 else "양호",
                delta_color="inverse" if len(ef)>5 else "normal")
        with r2:
            if not yearly.empty and "exits" in yearly.columns:
                yr2=yearly.copy(); yr2["year"]=pd.to_numeric(yr2["year"],errors="coerce")
                idx=yr2["exits"].idxmax()
                st.metric("최대 퇴사 연도",f"{int(yr2.loc[idx,'year'])}년",f"{int(yr2.loc[idx,'exits'])}명")
        with r3:
            if kpi:
                st.metric("연간 퇴사율",f"{kpi['rate']}%",
                    delta="위험" if kpi["rate"]>=15 else "정상",
                    delta_color="inverse" if kpi["rate"]>=15 else "normal")
        st.markdown("<br>",unsafe_allow_html=True)
        fig=_chart_early(ef,t)
        if fig:
            st.plotly_chart(fig, use_container_width=True, key="early_bar")
            _dl(fig,"조기퇴사","dl_early")
        if not ef.empty:
            st.markdown('<div class="sh">조기퇴사자 목록</div>', unsafe_allow_html=True)
            sc=[c for c in ["name","org","dept","join_date","exit_date"] if c in ef.columns]
            st.dataframe(ef[sc].rename(columns={"name":"이름","org":"본부","dept":"팀",
                "join_date":"입사일","exit_date":"퇴사일"}),
                use_container_width=True, hide_index=True)

    # ── TAB 5: Attrition
    with tabs[4]:
        st.markdown('<div class="sh">Attrition Risk 분석</div>', unsafe_allow_html=True)
        if attr.empty:
            st.info("Attrition 데이터 없음")
            st.code("python src/models/attrition_prediction.py")
        else:
            af=_f(attr)
            hi=len(af[af["attrition_risk_score"]>=70]) if "attrition_risk_score" in af.columns else 0
            mi=len(af[(af["attrition_risk_score"]>=40)&(af["attrition_risk_score"]<70)]) if "attrition_risk_score" in af.columns else 0
            a1,a2,a3=st.columns(3)
            a1.metric("고위험(70↑)",f"{hi}명",delta="즉시조치" if hi>0 else None,delta_color="inverse")
            a2.metric("중위험(40~70)",f"{mi}명")
            a3.metric("분석대상",f"{len(af)}명")
            fig=_chart_attrition(af,t)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="attr_bar")
                _dl(fig,"Attrition","dl_attr")
            with st.expander("📋 전체 Risk 데이터"):
                st.dataframe(af.sort_values("attrition_risk_score",ascending=False),
                    use_container_width=True, hide_index=True)

    # ── TAB 6: 보상 분석 (준비중)
    with tabs[5]:
        st.markdown('<div class="sh">보상 분석</div>', unsafe_allow_html=True)
        p1,p2=st.columns(2)
        with p1:
            _soon(t,"연봉/단가 데이터 연동 후 활성화")
            st.caption("예정 항목: Compa-ratio · 직급별 연봉 분포 · 시장 대비 경쟁력")
        with p2:
            _soon(t,"성과등급 데이터 연동 후 활성화")
            st.caption("예정 항목: 등급별 인원 분포 · 보상 공정성 지수")

    # ── TAB 7: 스킬 현황 (준비중)
    with tabs[6]:
        st.markdown('<div class="sh">스킬 현황</div>', unsafe_allow_html=True)
        p1,p2=st.columns(2)
        with p1:
            _soon(t,"기술스택 데이터 연동 후 활성화")
            st.caption("예정 항목: 스킬 보유 현황 · 기술스택 분포 · 스킬 갭 분석")
        with p2:
            _soon(t,"역량평가 데이터 연동 후 활성화")
            st.caption("예정 항목: 역량 레이더 차트 · 등급별 스킬 매트릭스")

    # ── TAB 8: 채용 현황 (준비중)
    with tabs[7]:
        st.markdown('<div class="sh">채용 현황</div>', unsafe_allow_html=True)
        p1,p2=st.columns(2)
        with p1:
            _soon(t,"채용 파이프라인 데이터 연동 후 활성화")
            st.caption("예정 항목: Time to Hire · 채널별 효율 · 합격률 Funnel")
        with p2:
            _soon(t,"채용 요청 데이터 연동 후 활성화")
            st.caption("예정 항목: 채용 목표 대비 달성률 · 부서별 채용 현황")

if __name__ == "__main__":
    run()
