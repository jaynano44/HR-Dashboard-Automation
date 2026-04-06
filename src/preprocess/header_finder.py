import pandas as pd


KEYWORDS = ["이름", "성명", "사번", "입사", "퇴사"]


def find_header_row(df: pd.DataFrame, max_scan=20):
    for i in range(min(len(df), max_scan)):
        row = df.iloc[i].astype(str).tolist()

        score = sum(any(k in cell for k in KEYWORDS) for cell in row)

        if score >= 2:
            return i

    return 0


def read_with_auto_header(file, sheet):
    raw = pd.read_excel(file, sheet_name=sheet, header=None)

    header_idx = find_header_row(raw)

    df = pd.read_excel(file, sheet_name=sheet, header=header_idx)

    return df