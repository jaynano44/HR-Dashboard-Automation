from pathlib import Path
import pandas as pd


def read_all_excels(raw_dir: str):
    raw_path = Path(raw_dir)

    files = list(raw_path.glob("*.xlsx"))
    files = [f for f in raw_path.glob("*.xlsx") if not f.name.startswith("~$")]
    if not files:
        print("❌ raw 폴더에 파일 없음")
        return pd.DataFrame()

    all_data = []

    for file in files:
        print(f"📂 읽는 중: {file.name}")

        try:
            xls = pd.ExcelFile(file)

            for sheet in xls.sheet_names:
                from src.preprocess.header_finder import read_with_auto_header
                df = read_with_auto_header(file, sheet)

                if df is None or df.empty:
                    continue

                df["source_file"] = file.name
                df["source_sheet"] = sheet

                all_data.append(df)

        except Exception as e:
            print(f"❌ 실패: {file.name} / {e}")

    if not all_data:
        return pd.DataFrame()

    return pd.concat(all_data, ignore_index=True)