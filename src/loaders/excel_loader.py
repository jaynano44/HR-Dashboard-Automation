\
from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Optional, Union

import pandas as pd


@dataclass
class ExcelLoadResult:
    df: pd.DataFrame
    meta: dict


def decrypt_office_file(path: Union[str, Path], password: str) -> BytesIO:
    """
    Decrypt password-protected Office file (xls/xlsx) into an in-memory BytesIO.
    Uses msoffcrypto-tool.
    """
    import msoffcrypto  # type: ignore

    path = Path(path)
    with path.open("rb") as f:
        office = msoffcrypto.OfficeFile(f)
        office.load_key(password=password)
        decrypted = BytesIO()
        office.decrypt(decrypted)
    decrypted.seek(0)
    return decrypted


def read_excel_any(path: Union[str, Path], password: Optional[str] = None, sheet_name=0, header=0) -> ExcelLoadResult:
    """
    Read excel file (xls/xlsx). If password is provided, decrypt first.
    """
    path = Path(path)
    meta = {"path": str(path), "sheet_name": sheet_name, "password_used": bool(password)}
    if password:
        bio = decrypt_office_file(path, password=password)
        # xls needs xlrd engine; xlsx will use openpyxl by default
        df = pd.read_excel(bio, sheet_name=sheet_name, header=header, engine="xlrd" if path.suffix.lower() == ".xls" else None)
        return ExcelLoadResult(df=df, meta=meta)

    df = pd.read_excel(path, sheet_name=sheet_name, header=header)
    return ExcelLoadResult(df=df, meta=meta)
