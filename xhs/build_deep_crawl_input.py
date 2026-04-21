from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd


def profile_id_from_link(link: str) -> str:
    s = str(link or "").strip()
    if "/user/profile/" not in s:
        return ""
    t = s.split("/user/profile/")[-1]
    t = t.split("?")[0].split("/")[0].strip()
    return t


def main() -> None:
    p = argparse.ArgumentParser(description="从failed_retry结果生成二次深抓输入表")
    p.add_argument("--retry-xlsx", required=True, help="failed_retry_*.xlsx")
    p.add_argument("--source-xlsx", required=True, help="原始博主输入xlsx（用于补充原始昵称）")
    p.add_argument("--source-sheet", default="Sheet1")
    p.add_argument("--output-dir", default="./outputs")
    args = p.parse_args()

    retry_xlsx = Path(args.retry_xlsx).expanduser().resolve()
    source_xlsx = Path(args.source_xlsx).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    retry_df = pd.read_excel(retry_xlsx, sheet_name="retry_success")
    src_df = pd.read_excel(source_xlsx, sheet_name=args.source_sheet)

    # source map
    src_rows = src_df.to_dict(orient="records")
    src_map = {}
    for r in src_rows:
        link = str(r.get("博主链接", "")).strip()
        pid = profile_id_from_link(link)
        if pid and pid not in src_map:
            src_map[pid] = r

    out_rows = []
    for r in retry_df.to_dict(orient="records"):
        link = str(r.get("profile_link", "")).strip()
        pid = profile_id_from_link(link)
        src = src_map.get(pid, {})
        out_rows.append(
            {
                "博主ID": pid,
                "博主链接": link,
                "博主蒲公英链接": str(src.get("博主蒲公英链接", "")),
                "博主昵称": str(r.get("nickname", "") or src.get("博主昵称", "")),
                "芬姐提供的博主原名": str(src.get("芬姐提供的博主原名", "")),
                "博主头像链接": str(src.get("博主头像链接", "")),
                "回补分类": str(r.get("retry_category", "")),
                "回补时间": str(r.get("retried_at", "")),
            }
        )

    out_df = pd.DataFrame(out_rows).drop_duplicates(subset=["博主ID", "博主链接"], keep="first")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"deep_crawl_input_from_retry_{ts}.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        out_df.to_excel(writer, index=False, sheet_name="Sheet1")

    print(
        {
            "retry_xlsx": str(retry_xlsx),
            "source_xlsx": str(source_xlsx),
            "row_count": len(out_df),
            "output_xlsx": str(out_path),
        }
    )


if __name__ == "__main__":
    main()

