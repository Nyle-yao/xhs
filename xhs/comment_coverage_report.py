from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

SELF_COL = "是否博主本人评论(猜测)"


def _safe_read(xlsx: Path, sheet: str) -> pd.DataFrame:
    try:
        xl = pd.ExcelFile(xlsx)
        if sheet not in xl.sheet_names:
            return pd.DataFrame()
        return pd.read_excel(xlsx, sheet_name=sheet)
    except Exception:
        return pd.DataFrame()


def _norm_str(v: Any) -> str:
    if v is None:
        return ""
    s = str(v)
    if s.lower() == "nan":
        return ""
    return s.strip()


def _resolve_source_blogger_list(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["博主ID", "博主昵称", "博主链接"])
    col_map = {
        "博主ID": ["博主ID", "profile_id", "source_blogger_id"],
        "博主昵称": ["博主昵称", "source_blogger_nickname", "nickname"],
        "博主链接": ["博主链接", "profile_link", "source_blogger_link"],
    }
    out = pd.DataFrame()
    for target, cands in col_map.items():
        chosen = None
        for c in cands:
            if c in df.columns:
                chosen = c
                break
        if chosen is None:
            out[target] = ""
        else:
            out[target] = df[chosen]
    out["博主ID"] = out["博主ID"].map(_norm_str)
    out["博主昵称"] = out["博主昵称"].map(_norm_str)
    out["博主链接"] = out["博主链接"].map(_norm_str)
    out = out[(out["博主ID"] != "") | (out["博主链接"] != "")]
    return out.drop_duplicates(subset=["博主ID", "博主链接"], keep="first").reset_index(drop=True)


def _extract_note_id_from_url(note_url: str) -> str:
    u = _norm_str(note_url)
    patterns = [
        r"/explore/([a-zA-Z0-9]+)",
        r"/user/profile/[a-zA-Z0-9]+/([a-zA-Z0-9]+)",
    ]
    for pat in patterns:
        m = re.search(pat, u)
        if m:
            return m.group(1)
    return ""


def _to_count(v: Any) -> float | None:
    s = _norm_str(v).replace(",", "")
    if not s:
        return None
    if s.lower() in {"nan", "none", "null"}:
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    x = float(m.group(0))
    sl = s.lower()
    if "万" in s or "w" in sl:
        x *= 10000
    elif "k" in sl or "千" in s:
        x *= 1000
    return x


def _failure_bucket(msg: str) -> str:
    m = _norm_str(msg).lower()
    if not m:
        return "未知"
    if "captcha" in m or "风控" in m or "验证" in m or "访问频繁" in m:
        return "风控拦截"
    if "risk_skip" in m:
        return "风控跳过"
    if "note_failed" in m or "笔记" in m:
        return "笔记抓取失败"
    if "登录" in m:
        return "登录态问题"
    return "其他失败"


def build_report(input_xlsx: Path, output_dir: Path) -> tuple[Path, Path, dict[str, Any]]:
    source_df = _resolve_source_blogger_list(_safe_read(input_xlsx, "source_blogger_list"))
    blogger_df = _safe_read(input_xlsx, "blogger_export")
    note_df = _safe_read(input_xlsx, "note_export")
    comment_df = _safe_read(input_xlsx, "comment_export")
    failed_df = _safe_read(input_xlsx, "failed")

    for c in ["博主ID", "博主昵称", "博主链接"]:
        if c not in source_df.columns:
            source_df[c] = ""
    for c in ["博主ID", "博主昵称", "笔记ID", "笔记链接", "笔记标题", "发布时间", "评论量"]:
        if c not in note_df.columns:
            note_df[c] = ""
    for c in ["博主ID", "博主昵称", "笔记ID", "笔记链接", SELF_COL, "评论内容", "评论时间"]:
        if c not in comment_df.columns:
            comment_df[c] = ""
    if "message" not in failed_df.columns:
        failed_df["message"] = ""

    note_df["博主ID"] = note_df["博主ID"].map(_norm_str)
    note_df["笔记ID"] = note_df["笔记ID"].map(_norm_str)
    comment_df["博主ID"] = comment_df["博主ID"].map(_norm_str)
    comment_df["笔记ID"] = comment_df["笔记ID"].map(_norm_str)
    comment_df["笔记链接"] = comment_df["笔记链接"].map(_norm_str)
    # 回填评论笔记ID（兼容历史数据里签名链接未解析的情况）
    missing_note_id = comment_df["笔记ID"] == ""
    if missing_note_id.any():
        comment_df.loc[missing_note_id, "笔记ID"] = comment_df.loc[missing_note_id, "笔记链接"].map(_extract_note_id_from_url)
    source_df["博主ID"] = source_df["博主ID"].map(_norm_str)
    blogger_df["博主ID"] = blogger_df.get("博主ID", "").map(_norm_str) if not blogger_df.empty else ""

    # note-level coverage
    c_note = comment_df.groupby("笔记ID", dropna=False).size().rename("实抓评论条数").reset_index()
    self_note = (
        comment_df[comment_df[SELF_COL].astype(str) == "是"].groupby("笔记ID", dropna=False).size().rename("博主本人评论条数").reset_index()
        if not comment_df.empty
        else pd.DataFrame(columns=["笔记ID", "博主本人评论条数"])
    )

    note_cov = note_df[["博主ID", "博主昵称", "笔记ID", "笔记链接", "笔记标题", "发布时间", "评论量"]].copy()
    note_cov = note_cov.rename(columns={"评论量": "声明评论量"})
    note_cov["声明评论量_数值"] = note_cov["声明评论量"].map(_to_count)
    note_cov = note_cov.merge(c_note, on="笔记ID", how="left")
    note_cov = note_cov.merge(self_note, on="笔记ID", how="left")
    note_cov["实抓评论条数"] = note_cov["实抓评论条数"].fillna(0).astype(int)
    note_cov["博主本人评论条数"] = note_cov["博主本人评论条数"].fillna(0).astype(int)
    note_cov["是否抓到评论"] = note_cov["实抓评论条数"].apply(lambda x: "是" if x > 0 else "否")

    def _ratio_row(r: pd.Series) -> float | None:
        d = r.get("声明评论量_数值")
        a = float(r.get("实抓评论条数", 0) or 0)
        if d is None or pd.isna(d) or float(d) <= 0:
            return None
        return round(a / float(d), 4)

    note_cov["评论抓取覆盖率(实抓/声明)"] = note_cov.apply(_ratio_row, axis=1)
    note_cov["缺口标签"] = ""
    note_cov.loc[(note_cov["声明评论量_数值"].fillna(0) > 0) & (note_cov["实抓评论条数"] == 0), "缺口标签"] = "声明有评论但实抓为0"
    note_cov.loc[(note_cov["声明评论量_数值"].fillna(0) > 0) & (note_cov["实抓评论条数"] > 0), "缺口标签"] = "有评论抓取"
    note_cov.loc[(note_cov["声明评论量_数值"].fillna(0) == 0) & (note_cov["实抓评论条数"] == 0), "缺口标签"] = "笔记未展示评论或评论量未知"

    # blogger-level coverage
    c_blogger = comment_df.groupby("博主ID", dropna=False).size().rename("评论总条数_实抓").reset_index()
    self_blogger = (
        comment_df[comment_df[SELF_COL].astype(str) == "是"].groupby("博主ID", dropna=False).size().rename("博主本人评论条数").reset_index()
        if not comment_df.empty
        else pd.DataFrame(columns=["博主ID", "博主本人评论条数"])
    )
    n_blogger = note_df.groupby("博主ID", dropna=False).size().rename("笔记数_已抓取").reset_index()
    n_with_comment = (
        note_cov[note_cov["实抓评论条数"] > 0].groupby("博主ID", dropna=False).size().rename("有评论笔记数").reset_index()
        if not note_cov.empty
        else pd.DataFrame(columns=["博主ID", "有评论笔记数"])
    )

    blogger_cov = source_df[["博主ID", "博主昵称", "博主链接"]].drop_duplicates().copy()
    blogger_cov["是否博主抓取成功"] = blogger_cov["博主ID"].isin(set(blogger_df["博主ID"]))
    blogger_cov["是否博主抓取成功"] = blogger_cov["是否博主抓取成功"].map(lambda x: "是" if x else "否")
    blogger_cov = blogger_cov.merge(n_blogger, on="博主ID", how="left")
    blogger_cov = blogger_cov.merge(n_with_comment, on="博主ID", how="left")
    blogger_cov = blogger_cov.merge(c_blogger, on="博主ID", how="left")
    blogger_cov = blogger_cov.merge(self_blogger, on="博主ID", how="left")
    for c in ["笔记数_已抓取", "有评论笔记数", "评论总条数_实抓", "博主本人评论条数"]:
        blogger_cov[c] = blogger_cov[c].fillna(0).astype(int)
    blogger_cov["评论覆盖率(有评论笔记/已抓笔记)"] = blogger_cov.apply(
        lambda r: round(r["有评论笔记数"] / r["笔记数_已抓取"], 4) if r["笔记数_已抓取"] > 0 else None,
        axis=1,
    )

    def _status_row(r: pd.Series) -> str:
        if r["是否博主抓取成功"] != "是":
            return "博主未抓取成功"
        if r["笔记数_已抓取"] <= 0:
            return "博主成功但无笔记"
        if r["评论总条数_实抓"] <= 0:
            return "有笔记但无评论"
        return "评论抓取有数据"

    blogger_cov["状态标签"] = blogger_cov.apply(_status_row, axis=1)

    # failed summary
    failed_sum = failed_df.copy()
    if failed_sum.empty:
        failed_bucket = pd.DataFrame(columns=["失败分类", "条数"])
    else:
        failed_sum["失败分类"] = failed_sum["message"].map(_failure_bucket)
        failed_bucket = (
            failed_sum.groupby("失败分类", dropna=False).size().rename("条数").reset_index().sort_values("条数", ascending=False)
        )

    # gap focus table
    note_gap = note_cov[note_cov["缺口标签"] == "声明有评论但实抓为0"].copy()
    note_gap = note_gap.sort_values(["博主ID", "发布时间"], ascending=[True, False])

    # summary
    # 若 source_blogger_list 缺失，则退化为已抓博主列表
    if source_df.empty and not blogger_df.empty:
        source_total = int(len(blogger_df["博主ID"].dropna().astype(str).str.strip().replace("", pd.NA).dropna().unique()))
    else:
        source_total = int(len(blogger_cov))
    blogger_success = int((blogger_cov["是否博主抓取成功"] == "是").sum())
    note_total = int(len(note_cov))
    note_with_comments = int((note_cov["实抓评论条数"] > 0).sum())
    comment_total = int(len(comment_df))
    self_comment_total = int((comment_df[SELF_COL].astype(str) == "是").sum()) if not comment_df.empty else 0

    summary = {
        "input_result": str(input_xlsx),
        "source_blogger_total": source_total,
        "blogger_success_total": blogger_success,
        "blogger_success_rate": round(blogger_success / max(1, source_total), 4),
        "note_total": note_total,
        "note_with_comments": note_with_comments,
        "note_comment_hit_rate": round(note_with_comments / max(1, note_total), 4),
        "comment_total": comment_total,
        "self_comment_total": self_comment_total,
        "note_gap_total": int(len(note_gap)),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = output_dir / f"comment_coverage_{ts}.xlsx"
    out_json = output_dir / f"comment_coverage_{ts}_summary.json"

    meta_df = pd.DataFrame([
        {"指标": "源博主数", "数值": source_total},
        {"指标": "博主成功数", "数值": blogger_success},
        {"指标": "博主成功率", "数值": summary["blogger_success_rate"]},
        {"指标": "笔记总数", "数值": note_total},
        {"指标": "有评论笔记数", "数值": note_with_comments},
        {"指标": "笔记评论命中率", "数值": summary["note_comment_hit_rate"]},
        {"指标": "实抓评论总数", "数值": comment_total},
        {"指标": "博主本人评论数", "数值": self_comment_total},
        {"指标": "评论缺口笔记数", "数值": int(len(note_gap))},
        {"指标": "生成时间", "数值": summary["generated_at"]},
    ])

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        meta_df.to_excel(writer, index=False, sheet_name="meta")
        blogger_cov.sort_values(["状态标签", "评论总条数_实抓"], ascending=[True, False]).to_excel(
            writer, index=False, sheet_name="blogger_comment_coverage"
        )
        note_cov.sort_values(["是否抓到评论", "实抓评论条数"], ascending=[True, False]).to_excel(
            writer, index=False, sheet_name="note_comment_coverage"
        )
        note_gap.to_excel(writer, index=False, sheet_name="note_gap_focus")
        failed_bucket.to_excel(writer, index=False, sheet_name="failed_reason_summary")
        if not failed_df.empty:
            failed_df.to_excel(writer, index=False, sheet_name="failed_raw")

    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_xlsx, out_json, summary


def main() -> None:
    p = argparse.ArgumentParser(description="生成评论抓取覆盖率与缺口报告")
    p.add_argument("--input-result", required=True, help="输入结果xlsx（merged或batch result）")
    p.add_argument("--output-dir", default="./outputs", help="输出目录")
    args = p.parse_args()

    input_xlsx = Path(args.input_result).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    out_xlsx, out_json, summary = build_report(input_xlsx, output_dir)
    print(json.dumps({
        "output_excel": str(out_xlsx),
        "output_summary_json": str(out_json),
        **summary,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
