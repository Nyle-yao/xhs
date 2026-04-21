from __future__ import annotations

import argparse
import re
from datetime import datetime
from pathlib import Path

import pandas as pd


def _extract_note_id(url: str) -> str:
    u = str(url or "")
    for pat in [r"/explore/([a-zA-Z0-9]+)", r"/user/profile/[a-zA-Z0-9]+/([a-zA-Z0-9]+)"]:
        m = re.search(pat, u)
        if m:
            return m.group(1)
    return ""


def _repair_df(df: pd.DataFrame, note_id_col: str = "笔记ID", note_url_col: str = "笔记链接") -> tuple[pd.DataFrame, int]:
    if df.empty or note_id_col not in df.columns or note_url_col not in df.columns:
        return df, 0
    x = df.copy()
    nid = x[note_id_col].fillna("").astype(str).str.strip()
    nurl = x[note_url_col].fillna("").astype(str).str.strip()
    mask = nid == ""
    fill_vals = nurl.map(_extract_note_id)
    changed = int((mask & (fill_vals != "")).sum())
    x.loc[mask, note_id_col] = fill_vals[mask]
    return x, changed


def main() -> None:
    p = argparse.ArgumentParser(description="修复工作簿中评论表的笔记ID（由笔记链接回填）")
    p.add_argument("--input-xlsx", required=True)
    p.add_argument("--output-dir", default="./outputs")
    p.add_argument("--inplace", action="store_true", help="原地覆盖（默认输出新文件）")
    args = p.parse_args()

    in_xlsx = Path(args.input_xlsx).expanduser().resolve()
    out_dir = Path(args.output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    xl = pd.ExcelFile(in_xlsx)
    sheets = {s: pd.read_excel(in_xlsx, sheet_name=s) for s in xl.sheet_names}

    total_changed = 0
    if "comment_export" in sheets:
        sheets["comment_export"], c = _repair_df(sheets["comment_export"])
        total_changed += c
    if "comment_export_enhanced" in sheets:
        sheets["comment_export_enhanced"], c = _repair_df(sheets["comment_export_enhanced"])
        total_changed += c
    if "comment_self_only" in sheets:
        sheets["comment_self_only"], c = _repair_df(sheets["comment_self_only"])
        total_changed += c

    if args.inplace:
        out_xlsx = in_xlsx
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_xlsx = out_dir / f"{in_xlsx.stem}_noteid_repaired_{ts}.xlsx"

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        for s, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=s[:31])

    print({
        "input": str(in_xlsx),
        "output": str(out_xlsx),
        "changed_rows": total_changed,
    })


if __name__ == "__main__":
    main()
