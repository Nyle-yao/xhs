from __future__ import annotations

import argparse
import json
import random
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


def _run(cmd: list[str], cwd: Path, timeout_sec: int = 0) -> subprocess.CompletedProcess:
    if timeout_sec > 0:
        return subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=True, check=False, timeout=timeout_sec)
    return subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=True, check=False)


def _latest(pattern: str, out_dir: Path, since_ts: float) -> Path | None:
    cands = [p for p in out_dir.glob(pattern) if p.stat().st_mtime >= since_ts]
    if not cands:
        return None
    return sorted(cands, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def _norm_profile_link(v: str) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    s = s.replace("&amp;", "&")
    s = re.sub(r"#.*$", "", s)
    if "?" in s:
        s = s.split("?", 1)[0]
    s = s.rstrip("/")
    return s


def _make_record_key(blogger_id: str, blogger_link: str) -> str:
    bid = str(blogger_id or "").strip()
    lnk = _norm_profile_link(blogger_link)
    if bid:
        return f"id:{bid}"
    if lnk:
        return f"link:{lnk}"
    return ""


def _norm_source(df: pd.DataFrame, id_col: str, nick_col: str, link_col: str) -> pd.DataFrame:
    x = df[[id_col, nick_col, link_col]].copy()
    x.columns = ["博主ID", "博主昵称", "博主链接"]
    x["博主ID"] = x["博主ID"].fillna("").astype(str).str.strip()
    x["博主昵称"] = x["博主昵称"].fillna("").astype(str).str.strip()
    x["博主链接"] = x["博主链接"].fillna("").astype(str).str.strip()
    x["source_link_norm"] = x["博主链接"].map(_norm_profile_link)
    x["source_record_key"] = x.apply(lambda r: _make_record_key(r.get("博主ID", ""), r.get("博主链接", "")), axis=1)
    x = x[(x["博主链接"] != "") | (x["博主ID"] != "")].copy()
    x = x.drop_duplicates(subset=["source_record_key", "博主ID", "source_link_norm"], keep="first")
    return x.reset_index(drop=True)


def _extract_success_signature(result_xlsx: Path) -> dict[str, set[str]]:
    if not result_xlsx.exists():
        return {"ids": set(), "links": set(), "keys": set()}
    try:
        xl = pd.ExcelFile(result_xlsx)
        if "blogger_export" not in xl.sheet_names:
            return {"ids": set(), "links": set(), "keys": set()}
        df = pd.read_excel(result_xlsx, sheet_name="blogger_export")
        ids: set[str] = set()
        links: set[str] = set()
        keys: set[str] = set()
        for _, r in df.iterrows():
            bid = str(r.get("博主ID", "")).strip()
            blnk = _norm_profile_link(str(r.get("博主链接", "")).strip())
            if bid:
                ids.add(bid)
            if blnk:
                links.add(blnk)
            key = _make_record_key(bid, blnk)
            if key:
                keys.add(key)
        return {"ids": ids, "links": links, "keys": keys}
    except Exception:
        return {"ids": set(), "links": set(), "keys": set()}


def _resolved_mask(df: pd.DataFrame, signature: dict[str, set[str]]) -> pd.Series:
    ids = signature.get("ids", set())
    links = signature.get("links", set())
    keys = signature.get("keys", set())
    id_col = df["博主ID"].fillna("").astype(str).str.strip()
    link_col = df["source_link_norm"].fillna("").astype(str).str.strip()
    key_col = df["source_record_key"].fillna("").astype(str).str.strip()
    return ((id_col != "") & id_col.isin(ids)) | ((link_col != "") & link_col.isin(links)) | ((key_col != "") & key_col.isin(keys))


def _count_resolved(df: pd.DataFrame, signature: dict[str, set[str]]) -> int:
    if df.empty:
        return 0
    mask = _resolved_mask(df, signature)
    return int(mask.sum())


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="小红书抗风控续跑器：自动按未完成名单分批续跑并合并")
    ap.add_argument("--input", required=True, help="源博主名单xlsx")
    ap.add_argument("--sheet", default="Sheet1")
    ap.add_argument("--link-column", default="博主链接")
    ap.add_argument("--id-column", default="博主ID")
    ap.add_argument("--nickname-column", default="博主昵称")
    ap.add_argument("--profile-dir", default="./.xhs_profile")
    ap.add_argument("--fund-aliases", default="./fund_aliases_expanded.json")
    ap.add_argument("--existing-merged", default="", help="已有合并结果xlsx，作为续跑基线")
    ap.add_argument("--output-dir", default="./outputs")

    ap.add_argument("--headless", action="store_true")
    ap.add_argument(
        "--include-comments",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="是否抓评论（默认开启，可用 --no-include-comments 关闭）",
    )
    ap.add_argument(
        "--allow-degraded-run",
        action="store_true",
        help="允许降级运行（预检全失败仍继续）。默认关闭，质量优先。",
    )
    ap.add_argument("--max-notes-per-blogger", type=int, default=8)
    ap.add_argument("--max-comments-per-note", type=int, default=40)
    ap.add_argument("--comment-scroll-rounds", type=int, default=8)
    ap.add_argument("--max-note-age-days", type=int, default=0, help="只分析近N天笔记；0=不过滤")
    ap.add_argument("--stop-after-old-notes", type=int, default=3, help="同一博主连续过期笔记达到N篇后停止该博主后续笔记")
    ap.add_argument("--retry-times", type=int, default=2)
    ap.add_argument("--chunk-size", type=int, default=10)
    ap.add_argument("--chunk-select", choices=["random", "head"], default="random", help="每轮待抓切片选择策略")
    ap.add_argument("--max-rounds", type=int, default=30)
    ap.add_argument("--batch-timeout-sec", type=int, default=900)
    ap.add_argument("--per-batch-max-runtime-sec", type=int, default=120, help="传递给批抓脚本的最大运行秒数，避免单批长时间卡住")
    ap.add_argument("--cooldown-sec", type=int, default=180)
    ap.add_argument("--stop-if-no-progress-rounds", type=int, default=6)
    ap.add_argument("--run-post-checks", action="store_true", help="结束后自动生成评论覆盖报告与QA")
    ap.add_argument(
        "--strict-qa",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="后置QA是否严格阻断（默认开启，WARN/FAIL都标记阻断）",
    )
    ap.add_argument("--qa-min-batch-mentions", type=int, default=1, help="batch提及最小PASS阈值")
    ap.add_argument("--qa-min-notes-for-mention-fail", type=int, default=20, help="达到该笔记量时零提及触发FAIL")
    ap.add_argument(
        "--qa-min-intent-notes-for-mention-fail",
        type=int,
        default=3,
        help="达到该基金语义笔记量时零提及触发FAIL",
    )
    ap.add_argument("--auto-captcha-recover", action="store_true", help="触发验证码拦截时，自动拉起人工验证窗口后重试")
    ap.add_argument("--captcha-wait-sec", type=int, default=180, help="验证码恢复窗口最长等待秒数")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    out_dir = Path(args.output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    src = pd.read_excel(Path(args.input).expanduser().resolve(), sheet_name=args.sheet)
    src_df = _norm_source(src, args.id_column, args.nickname_column, args.link_column)
    all_ids = set(str(x).strip() for x in src_df["博主ID"].fillna("").astype(str) if str(x).strip())
    all_links = set(str(x).strip() for x in src_df["source_link_norm"].fillna("").astype(str) if str(x).strip())
    all_record_keys = set(str(x).strip() for x in src_df["source_record_key"].fillna("").astype(str) if str(x).strip())

    merged_path = Path(args.existing_merged).expanduser().resolve() if args.existing_merged else None
    if not (merged_path and merged_path.exists()):
        latest = sorted(out_dir.glob("xhs_batch_merged_*.xlsx"))
        merged_path = latest[-1] if latest else None

    success_signature = _extract_success_signature(merged_path) if merged_path else {"ids": set(), "links": set(), "keys": set()}
    pending_df = src_df[~_resolved_mask(src_df, success_signature)].copy().reset_index(drop=True)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    report = {
        "run_id": run_id,
        "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_total": int(len(src_df)),
        "source_unique_records": int(len(all_record_keys)),
        "source_unique_ids": int(len(all_ids)),
        "source_unique_links": int(len(all_links)),
        "baseline_merged": str(merged_path) if merged_path else "",
        "baseline_success_records": int(_count_resolved(src_df, success_signature)),
        "baseline_success_ids": int(len(success_signature.get("ids", set()))),
        "baseline_success_links": int(len(success_signature.get("links", set()))),
        "rounds": [],
        "status": "RUNNING",
    }
    report_path = out_dir / f"resilient_crawl_report_{run_id}.json"
    _save_json(report_path, report)

    batch_inputs: list[Path] = []
    if merged_path and merged_path.exists():
        # 仅用于最终合并输入列表，先放现有基线
        pass

    no_progress_rounds = 0
    for r in range(1, max(1, int(args.max_rounds)) + 1):
        pending_count = len(pending_df)
        if pending_count <= 0:
            break

        take_n = max(1, int(args.chunk_size))
        if args.chunk_select == "random" and len(pending_df) > take_n:
            rs = int(time.time()) ^ r ^ random.randint(0, 99999)
            part = pending_df.sample(n=take_n, random_state=rs).copy()
        else:
            part = pending_df.head(take_n).copy()
        part_xlsx = out_dir / f"resilient_round_{run_id}_{r:03d}_input.xlsx"
        part.to_excel(part_xlsx, index=False, sheet_name="Sheet1")

        t0 = time.time()
        cmd = [
            "python3",
            "batch_crawl_from_blogger_excel.py",
            "--input",
            str(part_xlsx),
            "--sheet",
            "Sheet1",
            "--link-column",
            "博主链接",
            "--id-column",
            "博主ID",
            "--nickname-column",
            "博主昵称",
            "--profile-dir",
            str(Path(args.profile_dir).expanduser().resolve()),
            "--max-notes-per-blogger",
            str(args.max_notes_per_blogger),
            "--retry-times",
            str(args.retry_times),
            "--max-comments-per-note",
            str(args.max_comments_per_note),
            "--comment-scroll-rounds",
            str(args.comment_scroll_rounds),
            "--max-note-age-days",
            str(args.max_note_age_days),
            "--stop-after-old-notes",
            str(args.stop_after_old_notes),
            "--fund-aliases",
            str(Path(args.fund_aliases).expanduser().resolve()),
            "--max-runtime-sec",
            str(max(0, int(args.per_batch_max_runtime_sec))),
            "--output-dir",
            str(out_dir),
        ]
        if args.headless:
            cmd.append("--headless")
        if args.include_comments:
            cmd.append("--include-comments")
        if args.allow_degraded_run:
            cmd.append("--allow-degraded-run")

        blocked = False
        got_result = False
        res_xlsx = None
        res_sum = None
        stdout_tail = ""
        stderr_tail = ""
        rc = -1
        try:
            cp = _run(cmd, root, timeout_sec=max(60, int(args.batch_timeout_sec)))
            rc = cp.returncode
            stdout_tail = "\n".join((cp.stdout or "").splitlines()[-20:])
            stderr_tail = "\n".join((cp.stderr or "").splitlines()[-20:])
            out_txt = ((cp.stdout or "") + "\n" + (cp.stderr or "")).lower()
            blocked = ("preflight_failed" in out_txt) or ("验证码" in out_txt) or ("captcha" in out_txt)
            res_sum = _latest("blogger_batch_*_summary.json", out_dir, t0)
            res_xlsx = _latest("blogger_batch_*_result.xlsx", out_dir, t0)
            got_result = bool(res_xlsx and res_xlsx.exists())
        except subprocess.TimeoutExpired:
            rc = -9
            stderr_tail = f"timeout>{args.batch_timeout_sec}s"

        # 若整批被验证码拦截，可尝试拉起人工恢复窗口后同轮重试一次
        recover_returncode = None
        recover_stdout_tail = ""
        recover_stderr_tail = ""
        if blocked and args.auto_captcha_recover:
            recover_cmd = [
                "python3",
                "captcha_recover.py",
                "--profile-dir",
                str(Path(args.profile_dir).expanduser().resolve()),
                "--wait-sec",
                str(max(60, int(args.captcha_wait_sec))),
            ]
            rec = _run(recover_cmd, root, timeout_sec=max(90, int(args.captcha_wait_sec) + 60))
            recover_returncode = int(rec.returncode)
            recover_stdout_tail = "\n".join((rec.stdout or "").splitlines()[-20:])
            recover_stderr_tail = "\n".join((rec.stderr or "").splitlines()[-20:])

            if rec.returncode == 0:
                t1 = time.time()
                cp2 = _run(cmd, root, timeout_sec=max(60, int(args.batch_timeout_sec)))
                rc = cp2.returncode
                stdout_tail = "\n".join((cp2.stdout or "").splitlines()[-20:])
                stderr_tail = "\n".join((cp2.stderr or "").splitlines()[-20:])
                out_txt2 = ((cp2.stdout or "") + "\n" + (cp2.stderr or "")).lower()
                blocked = ("preflight_failed" in out_txt2) or ("验证码" in out_txt2) or ("captcha" in out_txt2)
                res_sum = _latest("blogger_batch_*_summary.json", out_dir, t1) or res_sum
                res_xlsx = _latest("blogger_batch_*_result.xlsx", out_dir, t1) or res_xlsx
                got_result = bool(res_xlsx and res_xlsx.exists())

        newly_success = 0
        if got_result and res_xlsx:
            round_signature = _extract_success_signature(res_xlsx)
            newly_success = _count_resolved(part, round_signature)
            batch_inputs.append(res_xlsx)

        round_row = {
            "round": r,
            "pending_before": int(pending_count),
            "chunk_size": int(len(part)),
            "returncode": int(rc),
            "blocked": bool(blocked),
            "result_xlsx": str(res_xlsx) if res_xlsx else "",
            "summary_json": str(res_sum) if res_sum else "",
            "newly_success": int(newly_success),
            "stdout_tail": stdout_tail,
            "stderr_tail": stderr_tail,
            "recover_returncode": recover_returncode,
            "recover_stdout_tail": recover_stdout_tail,
            "recover_stderr_tail": recover_stderr_tail,
        }
        report["rounds"].append(round_row)

        # 发生有效抓取就和基线合并
        if got_result and res_xlsx:
            merge_inputs = []
            if merged_path and merged_path.exists():
                merge_inputs.append(str(merged_path))
            merge_inputs.extend(str(x) for x in batch_inputs if x and x.exists())
            if merge_inputs:
                tm = time.time()
                mg = _run(["python3", "merge_xhs_batches.py", "--inputs", *merge_inputs, "--output-dir", str(out_dir)], root)
                latest_merged = _latest("xhs_batch_merged_*.xlsx", out_dir, tm)
                if latest_merged and latest_merged.exists():
                    merged_path = latest_merged
                    success_signature = _extract_success_signature(merged_path)

        current_success_records = _count_resolved(src_df, success_signature)
        pending_df = src_df[~_resolved_mask(src_df, success_signature)].copy().reset_index(drop=True)
        report["current_success_records"] = int(current_success_records)
        report["current_success_ids"] = int(len(set(x for x in success_signature.get("ids", set()) if x in all_ids)))
        report["current_success_links"] = int(len(set(x for x in success_signature.get("links", set()) if x in all_links)))
        report["current_pending"] = int(len(pending_df))
        report["current_coverage"] = round((current_success_records / max(1, len(all_record_keys))) * 100, 2)
        _save_json(report_path, report)

        if newly_success <= 0:
            no_progress_rounds += 1
        else:
            no_progress_rounds = 0

        if len(pending_df) <= 0:
            break

        if no_progress_rounds >= max(1, int(args.stop_if_no_progress_rounds)):
            report["status"] = "PAUSED_NO_PROGRESS"
            report["hint"] = "多轮无新增，通常为验证码风控；建议人工完成一次验证后继续。"
            _save_json(report_path, report)
            break

        time.sleep(max(5, int(args.cooldown_sec)))

    report["ended_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if report.get("status") == "RUNNING":
        report["status"] = "DONE" if int(report.get("current_pending", len(pending_df))) <= 0 else "PARTIAL"
    report["final_merged"] = str(merged_path) if merged_path else ""
    final_success_records = _count_resolved(src_df, success_signature)
    report["final_success_records"] = int(final_success_records)
    report["final_success_ids"] = int(len(set(x for x in success_signature.get("ids", set()) if x in all_ids)))
    report["final_success_links"] = int(len(set(x for x in success_signature.get("links", set()) if x in all_links)))
    report["final_pending"] = int(len(pending_df))
    report["final_coverage"] = round((final_success_records / max(1, len(all_record_keys))) * 100, 2)
    pending_out = out_dir / f"resilient_pending_{run_id}.xlsx"
    pending_df.to_excel(pending_out, index=False, sheet_name="Sheet1")
    report["pending_list_xlsx"] = str(pending_out)

    # 后置检查：评论覆盖报告 + QA
    if args.run_post_checks and merged_path and Path(merged_path).exists():
        post = {}
        t_post = time.time()
        cc = _run(
            [
                "python3",
                "comment_coverage_report.py",
                "--input-result",
                str(merged_path),
                "--output-dir",
                str(out_dir),
            ],
            root,
        )
        cc_xlsx = _latest("comment_coverage_*.xlsx", out_dir, t_post)
        cc_json = _latest("comment_coverage_*_summary.json", out_dir, t_post)
        post["comment_coverage_report_xlsx"] = str(cc_xlsx) if cc_xlsx else ""
        post["comment_coverage_summary_json"] = str(cc_json) if cc_json else ""
        post["comment_coverage_returncode"] = int(cc.returncode)

        t_qa = time.time()
        qa_json = out_dir / f"qa_pipeline_resilient_{run_id}.json"
        qa = _run(
            [
                "python3",
                "qa_xhs_pipeline.py",
                "--batch-result",
                str(merged_path),
                "--output-json",
                str(qa_json),
                "--min-batch-mentions",
                str(args.qa_min_batch_mentions),
                "--min-notes-for-mention-fail",
                str(args.qa_min_notes_for_mention_fail),
                "--min-intent-notes-for-mention-fail",
                str(args.qa_min_intent_notes_for_mention_fail),
                *(["--strict"] if args.strict_qa else []),
            ],
            root,
        )
        post["qa_json"] = str(qa_json) if qa_json.exists() else ""
        post["qa_returncode"] = int(qa.returncode)
        qa_latest = _latest("qa_pipeline_resilient_*.json", out_dir, t_qa)
        if qa_latest and qa_latest.exists():
            post["qa_json"] = str(qa_latest)
        report["post_checks"] = post
        if args.strict_qa and int(post.get("qa_returncode", 0)) != 0:
            report["status"] = "QA_BLOCKED"

    _save_json(report_path, report)
    print(json.dumps({
        "report": str(report_path),
        "status": report["status"],
        "final_coverage": report["final_coverage"],
        "final_success_records": report["final_success_records"],
        "final_success_ids": report["final_success_ids"],
        "final_pending": report["final_pending"],
        "pending_list_xlsx": report.get("pending_list_xlsx", ""),
        "comment_coverage_report_xlsx": (report.get("post_checks", {}) or {}).get("comment_coverage_report_xlsx", ""),
        "qa_json": (report.get("post_checks", {}) or {}).get("qa_json", ""),
        "final_merged": report.get("final_merged", ""),
    }, ensure_ascii=False, indent=2))
    if report["status"] == "QA_BLOCKED":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
