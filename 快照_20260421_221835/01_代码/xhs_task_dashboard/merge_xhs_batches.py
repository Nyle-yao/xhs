from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd


def _safe_read(xlsx: Path, sheet: str) -> pd.DataFrame:
    try:
        xl = pd.ExcelFile(xlsx)
        if sheet not in xl.sheet_names:
            return pd.DataFrame()
        return pd.read_excel(xlsx, sheet_name=sheet)
    except Exception:
        return pd.DataFrame()


def _concat_dedup(dfs: Iterable[pd.DataFrame], subset: list[str] | None = None) -> pd.DataFrame:
    arr = [d for d in dfs if d is not None and not d.empty]
    if not arr:
        return pd.DataFrame()
    all_cols: list[str] = []
    for d in arr:
        for c in d.columns:
            if c not in all_cols:
                all_cols.append(c)
    norm = []
    for d in arr:
        x = d.copy()
        for c in all_cols:
            if c not in x.columns:
                x[c] = ""
        norm.append(x[all_cols])
    out = pd.concat(norm, ignore_index=True)
    if subset:
        valid_subset = [c for c in subset if c in out.columns]
        if valid_subset:
            out = out.drop_duplicates(subset=valid_subset, keep="first")
        else:
            out = out.drop_duplicates(keep="first")
    else:
        out = out.drop_duplicates(keep="first")
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="合并多轮小红书主抓结果（去重）")
    p.add_argument("--inputs", nargs="+", required=True, help="多个 blogger_batch_*_result.xlsx")
    p.add_argument("--output-dir", default="./outputs")
    args = p.parse_args()

    inputs = [Path(x).expanduser().resolve() for x in args.inputs]
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    src_list = []
    blogger_list = []
    note_list = []
    comment_list = []
    mention_list = []
    failed_list = []

    for x in inputs:
        src_list.append(_safe_read(x, "source_blogger_list"))
        blogger_list.append(_safe_read(x, "blogger_export"))
        note_list.append(_safe_read(x, "note_export"))
        comment_list.append(_safe_read(x, "comment_export"))
        mention_list.append(_safe_read(x, "fund_mentions"))
        failed_list.append(_safe_read(x, "failed"))

    source_merged = _concat_dedup(src_list, subset=["profile_id", "profile_link"])
    blogger_merged = _concat_dedup(blogger_list, subset=["博主ID", "博主链接"])
    note_merged = _concat_dedup(note_list, subset=["笔记ID"])
    comment_merged = _concat_dedup(comment_list, subset=["笔记ID", "评论内容", "用户名称"])
    mention_merged = _concat_dedup(mention_list, subset=["note_id", "source_field", "alias_hit"])
    failed_merged = _concat_dedup(failed_list, subset=["profile_link", "note_link", "message"])

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = output_dir / f"xhs_batch_merged_{ts}.xlsx"

    meta_df = pd.DataFrame(
        [
            {
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "input_count": len(inputs),
                "input_files": "\n".join([str(x) for x in inputs]),
                "source_rows": len(source_merged),
                "blogger_rows": len(blogger_merged),
                "note_rows": len(note_merged),
                "comment_rows": len(comment_merged),
                "fund_mention_rows": len(mention_merged),
                "failed_rows": len(failed_merged),
            }
        ]
    )

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        source_merged.to_excel(writer, index=False, sheet_name="source_blogger_list")
        blogger_merged.to_excel(writer, index=False, sheet_name="blogger_export")
        note_merged.to_excel(writer, index=False, sheet_name="note_export")
        comment_merged.to_excel(writer, index=False, sheet_name="comment_export")
        mention_merged.to_excel(writer, index=False, sheet_name="fund_mentions")
        failed_merged.to_excel(writer, index=False, sheet_name="failed")
        meta_df.to_excel(writer, index=False, sheet_name="meta")

    summary = {
        "output_excel": str(out_xlsx),
        "input_count": len(inputs),
        "source_rows": len(source_merged),
        "blogger_rows": len(blogger_merged),
        "note_rows": len(note_merged),
        "comment_rows": len(comment_merged),
        "fund_mention_rows": len(mention_merged),
        "failed_rows": len(failed_merged),
    }
    out_json = output_dir / f"xhs_batch_merged_{ts}_summary.json"
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

