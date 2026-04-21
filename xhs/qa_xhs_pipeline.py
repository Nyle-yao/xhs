from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

import pandas as pd

FUND_INTENT_TOKENS = [
    "基金",
    "债券",
    "混合",
    "指数",
    "etf",
    "qdii",
    "联接",
    "加仓",
    "减仓",
    "定投",
    "持有",
    "申购",
    "赎回",
]


@dataclass
class CheckItem:
    level: str  # PASS/WARN/FAIL
    name: str
    detail: str


def _add(items: list[CheckItem], level: str, name: str, detail: str) -> None:
    items.append(CheckItem(level=level, name=name, detail=detail))


def _check_required_sheets(xls: pd.ExcelFile, required: list[str], items: list[CheckItem]) -> None:
    existing = set(xls.sheet_names)
    for s in required:
        if s in existing:
            _add(items, "PASS", f"sheet:{s}", "存在")
        else:
            _add(items, "FAIL", f"sheet:{s}", "缺失")


def _check_columns(df: pd.DataFrame, sheet: str, required_cols: list[str], items: list[CheckItem]) -> None:
    cols = set(df.columns)
    for c in required_cols:
        if c in cols:
            _add(items, "PASS", f"{sheet}.{c}", "存在")
        else:
            _add(items, "FAIL", f"{sheet}.{c}", "缺失")


def _extract_note_id_from_url(url: str) -> str:
    u = str(url or "")
    for pat in [r"/explore/([a-zA-Z0-9]+)", r"/user/profile/[a-zA-Z0-9]+/([a-zA-Z0-9]+)"]:
        m = re.search(pat, u)
        if m:
            return m.group(1)
    return ""


def _fund_intent_note_count(note_df: pd.DataFrame) -> int:
    if note_df.empty:
        return 0
    text_cols = [c for c in ["笔记标题", "笔记内容", "笔记话题", "笔记标签"] if c in note_df.columns]
    if not text_cols:
        return 0
    text_blob = (
        note_df[text_cols]
        .fillna("")
        .astype(str)
        .agg(" ".join, axis=1)
        .str.lower()
        .str.replace(r"\s+", "", regex=True)
    )
    return int(text_blob.map(lambda x: any(tok in x for tok in FUND_INTENT_TOKENS)).sum())


def _mention_gate_level(
    mention_rows: int,
    note_rows: int,
    intent_note_rows: int,
    min_mentions_for_pass: int,
    min_notes_for_fail: int,
    min_intent_notes_for_fail: int,
) -> tuple[str, str]:
    min_mentions_for_pass = max(1, int(min_mentions_for_pass))
    min_notes_for_fail = max(1, int(min_notes_for_fail))
    min_intent_notes_for_fail = max(1, int(min_intent_notes_for_fail))

    if mention_rows >= min_mentions_for_pass:
        return "PASS", (
            f"rows={mention_rows}; note_rows={note_rows}; intent_note_rows={intent_note_rows}; "
            f"gate=pass_if_mentions>={min_mentions_for_pass}"
        )
    if note_rows <= 0:
        return "WARN", (
            f"rows={mention_rows}; note_rows=0; 未采到笔记，按WARN处理"
        )
    if intent_note_rows < min_intent_notes_for_fail:
        return "PASS", (
            f"rows={mention_rows}; note_rows={note_rows}; intent_note_rows={intent_note_rows}; "
            f"低基金语义样本，零提及可接受"
        )
    if note_rows >= min_notes_for_fail:
        return "FAIL", (
            f"rows={mention_rows}; note_rows={note_rows}; intent_note_rows={intent_note_rows}; "
            f"高语义样本仍零提及，触发FAIL"
        )
    return "WARN", (
        f"rows={mention_rows}; note_rows={note_rows}; intent_note_rows={intent_note_rows}; "
        f"样本未达FAIL阈值，按WARN处理"
    )


def run_batch_qa(
    path: Path,
    min_mentions_for_pass: int = 1,
    min_notes_for_fail: int = 20,
    min_intent_notes_for_fail: int = 3,
) -> dict:
    items: list[CheckItem] = []
    if not path.exists():
        return {"status": "FAIL", "checks": [asdict(CheckItem("FAIL", "file", f"不存在: {path}"))]}

    xls = pd.ExcelFile(path)
    _check_required_sheets(
        xls,
        required=["source_blogger_list", "blogger_export", "note_export", "comment_export", "fund_mentions", "failed"],
        items=items,
    )

    note_df = pd.DataFrame()
    if "note_export" in xls.sheet_names:
        note_df = pd.read_excel(path, sheet_name="note_export")
        _check_columns(note_df, "note_export", ["笔记ID", "笔记链接", "笔记标题", "笔记内容", "博主ID"], items)
        rows = len(note_df)
        _add(items, "PASS" if rows > 0 else "WARN", "note_export.rows", f"rows={rows}")
        if rows > 0 and "笔记ID" in note_df.columns:
            uniq = note_df["笔记ID"].astype(str).nunique()
            ratio = round(uniq / max(rows, 1), 4)
            _add(items, "PASS" if ratio >= 0.7 else "WARN", "note_export.id_uniqueness", f"ratio={ratio}")
        if rows > 0:
            def _nonempty_ratio(series: pd.Series) -> float:
                s = series.fillna("").astype(str).str.strip()
                return float((s != "").mean())

            if "笔记内容" in note_df.columns:
                content = note_df["笔记内容"].fillna("").astype(str)
                compact = content.str.replace(r"\s+", "", regex=True)
                generic_ratio = float((compact == "发现直播发布通知").mean())
                _add(
                    items,
                    "PASS" if generic_ratio <= 0.03 else ("WARN" if generic_ratio <= 0.10 else "FAIL"),
                    "note_export.generic_placeholder_ratio",
                    f"value={generic_ratio:.4f}",
                )

            inter_cols = [c for c in ["点赞量", "收藏量", "评论量", "分享量"] if c in note_df.columns]
            if inter_cols:
                inter_any = note_df[inter_cols].fillna("").astype(str).apply(lambda r: any(str(x).strip() != "" for x in r), axis=1)
                inter_any_ratio = float(inter_any.mean())
                _add(
                    items,
                    "PASS" if inter_any_ratio >= 0.50 else ("WARN" if inter_any_ratio >= 0.25 else "FAIL"),
                    "note_export.interaction_any_fill_ratio",
                    f"value={inter_any_ratio:.4f}",
                )

            if "发布时间" in note_df.columns:
                pub_ratio = _nonempty_ratio(note_df["发布时间"])
                _add(
                    items,
                    "PASS" if pub_ratio >= 0.50 else ("WARN" if pub_ratio >= 0.25 else "FAIL"),
                    "note_export.publish_time_fill_ratio",
                    f"value={pub_ratio:.4f}",
                )

    if "blogger_export" in xls.sheet_names:
        bdf = pd.read_excel(path, sheet_name="blogger_export")
        _check_columns(bdf, "blogger_export", ["博主ID", "博主链接"], items)
        rows = len(bdf)
        _add(items, "PASS" if rows > 0 else "WARN", "blogger_export.rows", f"rows={rows}")
        if rows > 0:
            profile_fill = float((bdf["博主链接"].fillna("").astype(str).str.strip() != "").mean()) if "博主链接" in bdf.columns else 0.0
            _add(
                items,
                "PASS" if profile_fill >= 0.95 else ("WARN" if profile_fill >= 0.8 else "FAIL"),
                "blogger_export.profile_link_fill_ratio",
                f"value={profile_fill:.4f}",
            )

    if "failed" in xls.sheet_names:
        fdf = pd.read_excel(path, sheet_name="failed")
        rows = len(fdf)
        _add(items, "PASS", "failed.rows", f"rows={rows}")

    if "comment_export" in xls.sheet_names:
        cdf = pd.read_excel(path, sheet_name="comment_export")
        _check_columns(cdf, "comment_export", ["评论ID", "评论内容", "笔记ID", "笔记链接", "博主ID"], items)
        c_rows = len(cdf)
        _add(items, "PASS", "comment_export.rows", f"rows={c_rows}")
        if c_rows > 0:
            if "笔记ID" in cdf.columns:
                nid = cdf["笔记ID"].fillna("").astype(str).str.strip()
            else:
                nid = pd.Series([""] * c_rows)
            if "笔记链接" in cdf.columns:
                nurl = cdf["笔记链接"].fillna("").astype(str).str.strip()
            else:
                nurl = pd.Series([""] * c_rows)
            recovered = nid.mask(nid == "", nurl.map(_extract_note_id_from_url))
            nonempty_ratio = float((recovered != "").mean()) if len(recovered) > 0 else 0.0
            _add(items, "PASS" if nonempty_ratio >= 0.9 else "WARN", "comment_export.note_id_fill_ratio", f"value={nonempty_ratio:.4f}")

            if not note_df.empty and "笔记ID" in note_df.columns:
                note_ids = set(note_df["笔记ID"].fillna("").astype(str).str.strip())
                note_ids.discard("")
                covered_ids = set(recovered[recovered != ""])
                hit_ratio = float(len(note_ids & covered_ids) / max(1, len(note_ids)))
                _add(items, "PASS" if hit_ratio >= 0.1 else "WARN", "comment_export.note_hit_ratio", f"value={hit_ratio:.4f}")

            if "评论时间" in cdf.columns:
                time_fill = float((cdf["评论时间"].fillna("").astype(str).str.strip() != "").mean())
                _add(
                    items,
                    "PASS" if time_fill >= 0.30 else ("WARN" if time_fill >= 0.10 else "FAIL"),
                    "comment_export.comment_time_fill_ratio",
                    f"value={time_fill:.4f}",
                )
            if "用户名称" in cdf.columns:
                user_fill = float((cdf["用户名称"].fillna("").astype(str).str.strip() != "").mean())
                _add(
                    items,
                    "PASS" if user_fill >= 0.30 else ("WARN" if user_fill >= 0.10 else "FAIL"),
                    "comment_export.user_name_fill_ratio",
                    f"value={user_fill:.4f}",
                )

    if "fund_mentions" in xls.sheet_names:
        mdf = pd.read_excel(path, sheet_name="fund_mentions")
        rows = len(mdf)
        note_rows = int(len(note_df))
        intent_note_rows = _fund_intent_note_count(note_df)
        level, detail = _mention_gate_level(
            mention_rows=rows,
            note_rows=note_rows,
            intent_note_rows=intent_note_rows,
            min_mentions_for_pass=min_mentions_for_pass,
            min_notes_for_fail=min_notes_for_fail,
            min_intent_notes_for_fail=min_intent_notes_for_fail,
        )
        _add(items, level, "fund_mentions.rows", detail)
        if rows > 0 and "fund_code" in mdf.columns:
            s = mdf["fund_code"].astype(str).str.strip()
            ok = s.str.fullmatch(r"\d{6}|").fillna(False).mean()
            _add(items, "PASS" if ok >= 0.95 else "WARN", "fund_mentions.fund_code_format", f"valid_ratio={ok:.4f}")
        if rows > 0 and "note_id" in mdf.columns and (not note_df.empty) and ("笔记ID" in note_df.columns):
            mention_note_ids = set(mdf["note_id"].fillna("").astype(str).str.strip())
            mention_note_ids.discard("")
            note_ids = set(note_df["笔记ID"].fillna("").astype(str).str.strip())
            note_ids.discard("")
            cov = float(len(mention_note_ids & note_ids) / max(1, len(note_ids)))
            _add(items, "PASS" if cov >= 0.15 else "WARN", "fund_mentions.note_coverage_ratio", f"value={cov:.4f}")

    status = "PASS"
    if any(i.level == "FAIL" for i in items):
        status = "FAIL"
    elif any(i.level == "WARN" for i in items):
        status = "WARN"
    return {"status": status, "checks": [asdict(i) for i in items]}


def run_enriched_qa(
    path: Path,
    min_mentions_for_pass: int = 1,
    min_notes_for_fail: int = 20,
    min_intent_notes_for_fail: int = 3,
) -> dict:
    items: list[CheckItem] = []
    if not path.exists():
        return {"status": "FAIL", "checks": [asdict(CheckItem("FAIL", "file", f"不存在: {path}"))]}

    xls = pd.ExcelFile(path)
    _check_required_sheets(
        xls,
        required=[
            "fund_mentions_enhanced",
            "ops_summary_fund",
            "ops_summary_blogger",
            "ops_action_fund",
            "ops_sponsor_note",
            "ops_sponsor_blogger",
            "ops_sponsor_fund",
            "fund_alias_suggest",
            "ops_digest",
            "signal_tag_summary",
            "meta",
            "comment_export_enhanced",
            "image_audit_all",
            "image_invalid_archive",
            "image_valid_kept",
        ],
        items=items,
    )

    note_df = pd.DataFrame()
    if "note_export" in xls.sheet_names:
        note_df = pd.read_excel(path, sheet_name="note_export")

    mention_gate_level = "WARN"
    mention_rows = 0
    low_intent_no_mention = False
    if "fund_mentions_enhanced" in xls.sheet_names:
        mdf = pd.read_excel(path, sheet_name="fund_mentions_enhanced")
        _check_columns(mdf, "fund_mentions_enhanced", ["fund_name", "mention_role", "sentiment", "match_type"], items)
        rows = len(mdf)
        mention_rows = rows
        note_rows = int(len(note_df))
        intent_note_rows = _fund_intent_note_count(note_df)
        level, detail = _mention_gate_level(
            mention_rows=rows,
            note_rows=note_rows,
            intent_note_rows=intent_note_rows,
            min_mentions_for_pass=min_mentions_for_pass,
            min_notes_for_fail=min_notes_for_fail,
            min_intent_notes_for_fail=min_intent_notes_for_fail,
        )
        mention_gate_level = level
        low_intent_no_mention = rows == 0 and level == "PASS"
        _add(items, level, "fund_mentions_enhanced.rows", detail)

    if "ops_summary_fund" in xls.sheet_names:
        df = pd.read_excel(path, sheet_name="ops_summary_fund")
        _check_columns(df, "ops_summary_fund", ["fund_name", "提及次数", "主推占比", "正向占比", "运营建议"], items)
        if len(df) > 0:
            _add(items, "PASS", "ops_summary_fund.rows", f"rows={len(df)}")
        elif low_intent_no_mention:
            _add(items, "PASS", "ops_summary_fund.rows", "rows=0; 低基金语义样本且无提及，按PASS处理")
        else:
            _add(items, "WARN", "ops_summary_fund.rows", f"rows={len(df)}")

    if "comment_export_enhanced" in xls.sheet_names:
        cdf = pd.read_excel(path, sheet_name="comment_export_enhanced")
        _add(items, "PASS", "comment_export_enhanced.rows", f"rows={len(cdf)}")
        if "是否博主本人评论(猜测)" in cdf.columns:
            self_cnt = int((cdf["是否博主本人评论(猜测)"] == "是").sum())
            _add(items, "PASS", "comment_export_enhanced.self_comment_count", f"rows={self_cnt}")
        else:
            _add(items, "WARN", "comment_export_enhanced.self_comment_flag", "缺失列: 是否博主本人评论(猜测)")

    if "ocr_note_images" in xls.sheet_names:
        odf = pd.read_excel(path, sheet_name="ocr_note_images")
        _add(items, "PASS", "ocr_note_images.rows", f"rows={len(odf)}")
        if "ocr_char_count" in odf.columns:
            hit = int((odf["ocr_char_count"].fillna(0) > 0).sum())
            _add(items, "PASS" if hit > 0 else "WARN", "ocr_note_images.hit_rows", f"rows={hit}")
            avg_char = float(odf["ocr_char_count"].fillna(0).mean()) if len(odf) > 0 else 0.0
            _add(items, "PASS" if avg_char >= 8 else "WARN", "ocr_note_images.avg_char", f"value={avg_char:.2f}")

    if "note_export" in xls.sheet_names:
        ndf = pd.read_excel(path, sheet_name="note_export")
        if "图片数量(有效)" in ndf.columns:
            kept_notes = int((ndf["图片数量(有效)"].fillna(0).astype(float) > 0).sum())
            _add(items, "PASS" if kept_notes > 0 else "WARN", "note_export.valid_image_notes", f"rows={kept_notes}")
        else:
            _add(items, "WARN", "note_export.valid_image_columns", "缺失列: 图片数量(有效)")

    if "image_audit_all" in xls.sheet_names:
        adf = pd.read_excel(path, sheet_name="image_audit_all")
        _add(items, "PASS" if len(adf) > 0 else "WARN", "image_audit_all.rows", f"rows={len(adf)}")
        if len(adf) > 0 and "is_valid" in adf.columns:
            invalid_ratio = float((adf["is_valid"].astype(str) != "是").mean())
            # 无效占比过高说明抓图质量或风控异常
            _add(items, "PASS" if invalid_ratio <= 0.75 else "WARN", "image_audit_all.invalid_ratio", f"value={invalid_ratio:.4f}")

    if "ops_digest" in xls.sheet_names:
        ddf = pd.read_excel(path, sheet_name="ops_digest")
        _check_columns(ddf, "ops_digest", ["模块", "指标", "数值", "说明"], items)
        _add(items, "PASS" if len(ddf) >= 4 else "WARN", "ops_digest.rows", f"rows={len(ddf)}")

    if "signal_tag_summary" in xls.sheet_names:
        sdf = pd.read_excel(path, sheet_name="signal_tag_summary")
        _check_columns(sdf, "signal_tag_summary", ["信号标签", "命中次数", "命中笔记数", "命中博主数", "说明"], items)
        _add(items, "PASS" if len(sdf) > 0 else "WARN", "signal_tag_summary.rows", f"rows={len(sdf)}")

    if "ops_sponsor_note" in xls.sheet_names:
        ndf = pd.read_excel(path, sheet_name="ops_sponsor_note")
        _check_columns(
            ndf,
            "ops_sponsor_note",
            ["笔记ID", "博主ID", "广告可能性分", "风险等级", "运营建议"],
            items,
        )
        _add(items, "PASS" if len(ndf) > 0 else "WARN", "ops_sponsor_note.rows", f"rows={len(ndf)}")

    if "ops_sponsor_blogger" in xls.sheet_names:
        bdf = pd.read_excel(path, sheet_name="ops_sponsor_blogger")
        _check_columns(
            bdf,
            "ops_sponsor_blogger",
            ["博主ID", "样本笔记数", "高风险笔记数", "平均广告分", "运营建议"],
            items,
        )
        _add(items, "PASS" if len(bdf) > 0 else "WARN", "ops_sponsor_blogger.rows", f"rows={len(bdf)}")

    if "ops_sponsor_fund" in xls.sheet_names:
        fdf = pd.read_excel(path, sheet_name="ops_sponsor_fund")
        _check_columns(
            fdf,
            "ops_sponsor_fund",
            ["fund_code", "fund_name", "涉及笔记数", "高风险关联笔记数", "平均关联广告分", "运营建议"],
            items,
        )
        if len(fdf) > 0:
            _add(items, "PASS", "ops_sponsor_fund.rows", f"rows={len(fdf)}")
        elif low_intent_no_mention or mention_gate_level == "PASS":
            _add(items, "PASS", "ops_sponsor_fund.rows", "rows=0; 当前样本未形成基金级投放关联，按PASS处理")
        else:
            _add(items, "WARN", "ops_sponsor_fund.rows", f"rows={len(fdf)}")

    if "fund_alias_suggest" in xls.sheet_names:
        adf = pd.read_excel(path, sheet_name="fund_alias_suggest")
        _check_columns(
            adf,
            "fund_alias_suggest",
            ["候选基金名", "建议匹配fund_code", "建议匹配fund_name", "相似度", "匹配依据"],
            items,
        )
        if len(adf) > 0:
            _add(items, "PASS", "fund_alias_suggest.rows", f"rows={len(adf)}")
        elif mention_rows > 0:
            _add(items, "WARN", "fund_alias_suggest.rows", "rows=0; 有提及但无可建议别名，建议人工抽检")
        else:
            _add(items, "PASS", "fund_alias_suggest.rows", "rows=0; 当前样本无基金提及，按PASS处理")

    status = "PASS"
    if any(i.level == "FAIL" for i in items):
        status = "FAIL"
    elif any(i.level == "WARN" for i in items):
        status = "WARN"
    return {"status": status, "checks": [asdict(i) for i in items]}


def main() -> None:
    p = argparse.ArgumentParser(description="小红书爬虫全链路QA")
    p.add_argument("--batch-result", required=True, help="主抓结果 xlsx")
    p.add_argument("--enriched-result", default="", help="增强结果 xlsx（可选）")
    p.add_argument("--output-json", default="", help="输出json路径（可选）")
    p.add_argument("--min-batch-mentions", type=int, default=1, help="batch提及最小PASS阈值")
    p.add_argument("--min-enriched-mentions", type=int, default=1, help="enriched提及最小PASS阈值")
    p.add_argument("--min-notes-for-mention-fail", type=int, default=20, help="达到该笔记样本量时，零提及可触发FAIL")
    p.add_argument(
        "--min-intent-notes-for-mention-fail",
        type=int,
        default=3,
        help="达到该基金语义笔记量时，零提及可触发FAIL",
    )
    p.add_argument("--strict", action="store_true", help="严格模式：WARN也返回非0")
    args = p.parse_args()

    batch_path = Path(args.batch_result).expanduser().resolve()
    enriched_path = Path(args.enriched_result).expanduser().resolve() if args.enriched_result else None

    batch_qa = run_batch_qa(
        batch_path,
        min_mentions_for_pass=args.min_batch_mentions,
        min_notes_for_fail=args.min_notes_for_mention_fail,
        min_intent_notes_for_fail=args.min_intent_notes_for_mention_fail,
    )
    enriched_qa = (
        run_enriched_qa(
            enriched_path,
            min_mentions_for_pass=args.min_enriched_mentions,
            min_notes_for_fail=args.min_notes_for_mention_fail,
            min_intent_notes_for_fail=args.min_intent_notes_for_mention_fail,
        )
        if enriched_path
        else None
    )

    overall = "PASS"
    statuses = [batch_qa["status"]]
    if enriched_qa:
        statuses.append(enriched_qa["status"])
    if "FAIL" in statuses:
        overall = "FAIL"
    elif "WARN" in statuses:
        overall = "WARN"

    payload = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "overall_status": overall,
        "batch_result": str(batch_path),
        "enriched_result": str(enriched_path) if enriched_path else "",
        "qa_gate": {
            "min_batch_mentions": int(args.min_batch_mentions),
            "min_enriched_mentions": int(args.min_enriched_mentions),
            "min_notes_for_mention_fail": int(args.min_notes_for_mention_fail),
            "min_intent_notes_for_mention_fail": int(args.min_intent_notes_for_mention_fail),
            "strict": bool(args.strict),
        },
        "batch_qa": batch_qa,
        "enriched_qa": enriched_qa,
    }

    if args.output_json:
        out = Path(args.output_json).expanduser().resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(payload, ensure_ascii=False, indent=2))

    if overall == "FAIL":
        raise SystemExit(2)
    if overall == "WARN" and args.strict:
        raise SystemExit(3)


if __name__ == "__main__":
    main()
