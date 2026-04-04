import pandas as pd


def build_employment_sessions(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    d = df.copy()

    # 날짜 컬럼 보정
    if "hire_date" not in d.columns:
        if "join_date" in d.columns:
            d["hire_date"] = pd.to_datetime(d["join_date"], errors="coerce")

    if "exit_date" not in d.columns:
        if "exit_date" in d.columns:
            d["exit_date"] = pd.to_datetime(d["exit_date"], errors="coerce")

    d = d.sort_values(["person_key", "hire_date"])

    sessions = []

    for person, g in d.groupby("person_key"):
        g = g.sort_values("hire_date")

        current_start = None
        current_end = None

        for _, r in g.iterrows():
            h = r.get("hire_date")
            e = r.get("exit_date")

            if pd.isna(h):
                continue

            if current_start is None:
                current_start = h
                current_end = e
                continue

            # 🔥 핵심: 이어진 재직이면 merge
            if current_end is None or (h <= current_end):
                if pd.notna(e):
                    current_end = max(current_end, e) if current_end else e
            else:
                sessions.append({
                    "person_key": person,
                    "hire_date": current_start,
                    "exit_date": current_end
                })
                current_start = h
                current_end = e

        if current_start is not None:
            sessions.append({
                "person_key": person,
                "hire_date": current_start,
                "exit_date": current_end
            })

    return pd.DataFrame(sessions)