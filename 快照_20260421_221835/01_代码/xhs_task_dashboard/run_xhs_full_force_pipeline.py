from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


def _run(cmd: list[str], cwd: Path, timeout_sec: int = 0) -> subprocess.CompletedProcess:
    print(f"[run] {' '.join(cmd)}")
    if timeout_sec and timeout_sec > 0:
        return subprocess.run(cmd, cwd=str(cwd), check=False, text=True, capture_output=True, timeout=timeout_sec)
    return subprocess.run(cmd, cwd=str(cwd), check=False, text=True, capture_output=True)


def _latest(output_dir: Path, pattern: str, since_ts: float) -> Path | None:
    cands = [p for p in output_dir.glob(pattern) if p.stat().st_mtime >= since_ts]
    if not cands:
        return None
    return sorted(cands, key=lambda x: x.stat().st_mtime, reverse=True)[0]


def _load_json(p: Path | None) -> dict[str, Any]:
    if not p or not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_source(input_xlsx: Path, sheet: str, link_col: str, id_col: str, nick_col: str) -> pd.DataFrame:
    df = pd.read_excel(input_xlsx, sheet_name=sheet)
    cols = set(df.columns)
    for c in [link_col, id_col, nick_col]:
        if c not in cols:
            raise SystemExit(f"输入缺少列: {c}")
    x = df[[id_col, nick_col, link_col]].copy()
    x.columns = ["博主ID", "博主昵称", "博主链接"]
    x["博主ID"] = x["博主ID"].fillna("").astype(str).str.strip()
    x["博主昵称"] = x["博主昵称"].fillna("").astype(str).str.strip()
    x["博主链接"] = x["博主链接"].fillna("").astype(str).str.strip()
    x = x[(x["博主链接"] != "") | (x["博主ID"] != "")]
    x = x.drop_duplicates(subset=["博主ID", "博主链接"], keep="first").reset_index(drop=True)
    return x


def main() -> None:
    ap = argparse.ArgumentParser(description="小红书全力攻坚流水线（分批主抓+回补+刷新+增强+QA）")
    ap.add_argument("--input", required=True, help="博主名单xlsx")
    ap.add_argument("--sheet", default="Sheet1")
    ap.add_argument("--link-column", default="博主链接")
    ap.add_argument("--id-column", default="博主ID")
    ap.add_argument("--nickname-column", default="博主昵称")
    ap.add_argument("--profile-dir", default="./.xhs_profile")
    ap.add_argument("--fund-aliases", default="./fund_aliases_expanded.json")
    ap.add_argument("--leshu-tag-file", default="", help="外部标签表（可选，输出双标签桥接）")
    ap.add_argument("--output-dir", default="./outputs")
    ap.add_argument("--headless", action="store_true")

    ap.add_argument("--chunk-size", type=int, default=8)
    ap.add_argument("--chunk-timeout-sec", type=int, default=480)
    ap.add_argument("--max-notes-per-blogger", type=int, default=8)
    ap.add_argument("--include-comments", action="store_true")
    ap.add_argument("--max-comments-per-note", type=int, default=60)
    ap.add_argument("--comment-scroll-rounds", type=int, default=8)
    ap.add_argument("--max-note-age-days", type=int, default=0, help="只分析近N天笔记；0=不过滤")
    ap.add_argument("--stop-after-old-notes", type=int, default=3, help="同一博主连续过期笔记达到N篇后停止该博主后续笔记")
    ap.add_argument("--retry-times", type=int, default=1)
    ap.add_argument("--blogger-sleep-ms", type=int, default=750)
    ap.add_argument("--note-sleep-ms", type=int, default=420)
    ap.add_argument("--max-bloggers", type=int, default=0, help="仅调试用，0=全部")

    ap.add_argument("--retry-rounds", type=int, default=2)
    ap.add_argument("--retry-max-targets", type=int, default=300)
    ap.add_argument("--retry-max-notes", type=int, default=3)
    ap.add_argument("--retry-sleep-ms", type=int, default=450)

    ap.add_argument("--refresh-max-total", type=int, default=0, help="媒体刷新总上限，0=全部")
    ap.add_argument("--refresh-chunk-size", type=int, default=8)
    ap.add_argument("--refresh-batch-timeout-sec", type=int, default=420)
    ap.add_argument("--refresh-sleep-ms", type=int, default=650)

    ap.add_argument("--enrich-crawl-comments", action="store_true")
    ap.add_argument("--enrich-max-notes-for-comments", type=int, default=100)
    ap.add_argument("--enrich-max-comments-per-note", type=int, default=80)
    ap.add_argument("--enrich-ocr-images", action="store_true")
    ap.add_argument("--enrich-ocr-max-notes", type=int, default=160)
    ap.add_argument("--enrich-ocr-max-images-per-note", type=int, default=2)
    ap.add_argument("--enrich-image-audit-timeout-sec", type=int, default=15)
    ap.add_argument(
        "--strict-qa",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="是否启用严格QA闸门（默认开启，WARN/FAIL都会阻断）",
    )
    ap.add_argument("--qa-min-batch-mentions", type=int, default=1, help="batch提及最小PASS阈值")
    ap.add_argument("--qa-min-enriched-mentions", type=int, default=1, help="enriched提及最小PASS阈值")
    ap.add_argument("--qa-min-notes-for-mention-fail", type=int, default=20, help="达到该笔记量时零提及触发FAIL")
    ap.add_argument(
        "--qa-min-intent-notes-for-mention-fail",
        type=int,
        default=3,
        help="达到该基金语义笔记量时零提及触发FAIL",
    )
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    input_path = Path(args.input).expanduser().resolve()

    started = datetime.now()
    started_ts = started.timestamp()
    run_id = started.strftime("%Y%m%d_%H%M%S")
    manifest: dict[str, Any] = {
        "run_id": run_id,
        "started_at": started.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "RUNNING",
        "steps": [],
    }
    out_manifest = output_dir / f"full_force_manifest_{run_id}.json"
    _save_manifest(out_manifest, manifest)

    src_df = _normalize_source(input_path, args.sheet, args.link_column, args.id_column, args.nickname_column)
    if args.max_bloggers > 0:
        src_df = src_df.head(args.max_bloggers).copy()
    total = len(src_df)
    if total <= 0:
        raise SystemExit("输入博主名单为空")

    batch_results: list[Path] = []
    with tempfile.TemporaryDirectory(prefix="xhs_chunks_") as td:
        tdir = Path(td)
        chunk_size = max(1, int(args.chunk_size))
        for chunk_idx, start in enumerate(range(0, total, chunk_size), start=1):
            end = min(total, start + chunk_size)
            part = src_df.iloc[start:end].copy()
            chunk_xlsx = tdir / f"chunk_{chunk_idx:03d}_{start}_{end}.xlsx"
            part.to_excel(chunk_xlsx, index=False, sheet_name="Sheet1")

            t0 = datetime.now().timestamp()
            cmd = [
                "python3",
                "batch_crawl_from_blogger_excel.py",
                "--input",
                str(chunk_xlsx),
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
                "--blogger-sleep-ms",
                str(args.blogger_sleep_ms),
                "--note-sleep-ms",
                str(args.note_sleep_ms),
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
                "--output-dir",
                str(output_dir),
            ]
            if args.headless:
                cmd.append("--headless")
            if args.include_comments:
                cmd.append("--include-comments")

            try:
                cp = _run(cmd, cwd=root, timeout_sec=max(60, int(args.chunk_timeout_sec)))
                bsum = _latest(output_dir, "blogger_batch_*_summary.json", t0)
                bxlsx = _latest(output_dir, "blogger_batch_*_result.xlsx", t0)
                if bxlsx and bxlsx.exists():
                    batch_results.append(bxlsx)
                manifest["steps"].append(
                    {
                        "name": "chunk_batch_crawl",
                        "chunk_index": chunk_idx,
                        "range": [start, end],
                        "returncode": cp.returncode,
                        "summary_json": str(bsum) if bsum else "",
                        "result_xlsx": str(bxlsx) if bxlsx else "",
                        "stdout_tail": "\n".join((cp.stdout or "").splitlines()[-15:]),
                        "stderr_tail": "\n".join((cp.stderr or "").splitlines()[-15:]),
                    }
                )
                s = _load_json(bsum)
                if s:
                    print(
                        "[chunk_progress] "
                        f"#{chunk_idx} bloggers={s.get('source_unique_bloggers', 0)} "
                        f"success={s.get('blogger_success_count', 0)} "
                        f"failed={s.get('blogger_failed_count', 0)} "
                        f"notes={s.get('note_export_count', 0)} "
                        f"risk_events={s.get('risk_event_count', 0)}"
                    )
                _save_manifest(out_manifest, manifest)
            except subprocess.TimeoutExpired:
                manifest["steps"].append(
                    {
                        "name": "chunk_batch_crawl",
                        "chunk_index": chunk_idx,
                        "range": [start, end],
                        "returncode": -9,
                        "summary_json": "",
                        "result_xlsx": "",
                        "stdout_tail": "",
                        "stderr_tail": f"timeout>{args.chunk_timeout_sec}s",
                    }
                )
                _save_manifest(out_manifest, manifest)
                continue

    if not batch_results:
        manifest["status"] = "FAILED"
        manifest["error"] = "no_batch_results"
        _save_manifest(out_manifest, manifest)
        raise SystemExit(f"未产出任何批次结果: {out_manifest}")

    # 合并初始批次
    t_merge = datetime.now().timestamp()
    mg = _run(["python3", "merge_xhs_batches.py", "--inputs", *[str(x) for x in batch_results], "--output-dir", str(output_dir)], cwd=root)
    merged_xlsx = _latest(output_dir, "xhs_batch_merged_*.xlsx", t_merge)
    merged_sum = _latest(output_dir, "xhs_batch_merged_*_summary.json", t_merge)
    manifest["steps"].append(
        {
            "name": "merge_initial_batches",
            "returncode": mg.returncode,
            "result_xlsx": str(merged_xlsx) if merged_xlsx else "",
            "summary_json": str(merged_sum) if merged_sum else "",
        }
    )
    _save_manifest(out_manifest, manifest)
    if not merged_xlsx or not merged_xlsx.exists():
        manifest["status"] = "FAILED"
        manifest["error"] = "merge_failed"
        _save_manifest(out_manifest, manifest)
        raise SystemExit(f"合并失败: {out_manifest}")

    # 多轮回补 + 深抓
    current_merged = merged_xlsx
    for r in range(1, max(0, int(args.retry_rounds)) + 1):
        t0 = datetime.now().timestamp()
        rr = _run(
            [
                "python3",
                "retry_failed_bloggers.py",
                "--input-result",
                str(current_merged),
                "--profile-dir",
                str(Path(args.profile_dir).expanduser().resolve()),
                "--max-targets",
                str(args.retry_max_targets),
                "--max-notes",
                str(args.retry_max_notes),
                "--sleep-ms",
                str(args.retry_sleep_ms),
                "--output-dir",
                str(output_dir),
            ],
            cwd=root,
        )
        rsum = _latest(output_dir, "failed_retry_*_summary.json", t0)
        rxlsx = _latest(output_dir, "failed_retry_*.xlsx", t0)
        rmeta = _load_json(rsum)
        rsuccess = int(rmeta.get("retry_success_count", 0))
        step_retry = {
            "name": "retry_failed",
            "round": r,
            "returncode": rr.returncode,
            "summary_json": str(rsum) if rsum else "",
            "result_xlsx": str(rxlsx) if rxlsx else "",
            "retry_success_count": rsuccess,
        }
        manifest["steps"].append(step_retry)
        _save_manifest(out_manifest, manifest)
        if not rxlsx or not rxlsx.exists() or rsuccess <= 0:
            break

        tb = datetime.now().timestamp()
        bi = _run(
            [
                "python3",
                "build_deep_crawl_input.py",
                "--retry-xlsx",
                str(rxlsx),
                "--source-xlsx",
                str(input_path),
                "--source-sheet",
                args.sheet,
                "--output-dir",
                str(output_dir),
            ],
            cwd=root,
        )
        deep_input = _latest(output_dir, "deep_crawl_input_from_retry_*.xlsx", tb)
        manifest["steps"].append(
            {
                "name": "build_deep_input",
                "round": r,
                "returncode": bi.returncode,
                "result_xlsx": str(deep_input) if deep_input else "",
            }
        )
        _save_manifest(out_manifest, manifest)
        if not deep_input or not deep_input.exists():
            continue

        tc = datetime.now().timestamp()
        cb = _run(
            [
                "python3",
                "batch_crawl_from_blogger_excel.py",
                "--input",
                str(deep_input),
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
                str(max(2, args.max_notes_per_blogger)),
                "--retry-times",
                str(args.retry_times),
                "--blogger-sleep-ms",
                str(max(300, args.blogger_sleep_ms)),
                "--note-sleep-ms",
                str(max(200, args.note_sleep_ms)),
                "--fund-aliases",
                str(Path(args.fund_aliases).expanduser().resolve()),
                "--output-dir",
                str(output_dir),
            ]
            + (["--headless"] if args.headless else [])
            + [
                "--max-note-age-days",
                str(args.max_note_age_days),
                "--stop-after-old-notes",
                str(args.stop_after_old_notes),
            ]
            + (["--include-comments", "--max-comments-per-note", str(args.max_comments_per_note), "--comment-scroll-rounds", str(args.comment_scroll_rounds)] if args.include_comments else []),
            cwd=root,
        )
        deep_batch = _latest(output_dir, "blogger_batch_*_result.xlsx", tc)
        deep_sum = _latest(output_dir, "blogger_batch_*_summary.json", tc)
        manifest["steps"].append(
            {
                "name": "deep_batch_crawl",
                "round": r,
                "returncode": cb.returncode,
                "summary_json": str(deep_sum) if deep_sum else "",
                "result_xlsx": str(deep_batch) if deep_batch else "",
            }
        )
        _save_manifest(out_manifest, manifest)
        if deep_batch and deep_batch.exists():
            batch_results.append(deep_batch)
            tm = datetime.now().timestamp()
            mg2 = _run(
                ["python3", "merge_xhs_batches.py", "--inputs", *[str(x) for x in batch_results], "--output-dir", str(output_dir)],
                cwd=root,
            )
            current_merged = _latest(output_dir, "xhs_batch_merged_*.xlsx", tm) or current_merged
            manifest["steps"].append(
                {
                    "name": "merge_after_deep_round",
                    "round": r,
                    "returncode": mg2.returncode,
                    "result_xlsx": str(current_merged),
                }
            )
            _save_manifest(out_manifest, manifest)

    # 媒体刷新（分批可恢复）
    t_ref = datetime.now().timestamp()
    rf = _run(
        [
            "python3",
            "refresh_note_media_chunked.py",
            "--input-result",
            str(current_merged),
            "--profile-dir",
            str(Path(args.profile_dir).expanduser().resolve()),
            "--start-index",
            "0",
            "--max-total",
            str(args.refresh_max_total),
            "--chunk-size",
            str(args.refresh_chunk_size),
            "--retry-times",
            "1",
            "--sleep-ms",
            str(args.refresh_sleep_ms),
            "--batch-timeout-sec",
            str(args.refresh_batch_timeout_sec),
            "--output-dir",
            str(output_dir),
        ]
        + (["--headless"] if args.headless else []),
        cwd=root,
    )
    rchunk = _latest(output_dir, "note_media_refresh_chunked_*.json", t_ref)
    rchunk_meta = _load_json(rchunk)
    refreshed_xlsx = Path(rchunk_meta.get("final_output_xlsx", "")) if rchunk_meta.get("final_output_xlsx") else None
    if not refreshed_xlsx or not refreshed_xlsx.exists():
        refreshed_xlsx = current_merged
    manifest["steps"].append(
        {
            "name": "refresh_note_media_chunked",
            "returncode": rf.returncode,
            "chunk_report_json": str(rchunk) if rchunk else "",
            "result_xlsx": str(refreshed_xlsx),
        }
    )
    _save_manifest(out_manifest, manifest)

    # 增强
    t_en = datetime.now().timestamp()
    ecmd = [
        "python3",
        "ops_enrich_pipeline.py",
        "--input-result",
        str(refreshed_xlsx),
        "--fund-aliases",
        str(Path(args.fund_aliases).expanduser().resolve()),
        "--output-dir",
        str(output_dir),
        "--image-audit-timeout-sec",
        str(args.enrich_image_audit_timeout_sec),
    ]
    if args.leshu_tag_file:
        ecmd += ["--leshu-tag-file", str(Path(args.leshu_tag_file).expanduser().resolve())]
    if args.enrich_crawl_comments:
        ecmd += [
            "--crawl-comments",
            "--max-notes-for-comments",
            str(args.enrich_max_notes_for_comments),
            "--max-comments-per-note",
            str(args.enrich_max_comments_per_note),
        ]
    if args.enrich_ocr_images:
        ecmd += [
            "--ocr-images",
            "--ocr-max-notes",
            str(args.enrich_ocr_max_notes),
            "--ocr-max-images-per-note",
            str(args.enrich_ocr_max_images_per_note),
        ]
    er = _run(ecmd, cwd=root)
    enrich_xlsx = _latest(output_dir, "ops_enriched_*.xlsx", t_en)
    enrich_sum = _latest(output_dir, "ops_enriched_*_summary.json", t_en)
    manifest["steps"].append(
        {
            "name": "ops_enrich",
            "returncode": er.returncode,
            "result_xlsx": str(enrich_xlsx) if enrich_xlsx else "",
            "summary_json": str(enrich_sum) if enrich_sum else "",
        }
    )
    _save_manifest(out_manifest, manifest)

    # QA
    qa_out = output_dir / f"qa_pipeline_full_force_{run_id}.json"
    qa_returncode = None
    if enrich_xlsx and enrich_xlsx.exists():
        qcmd = [
            "python3",
            "qa_xhs_pipeline.py",
            "--batch-result",
            str(refreshed_xlsx),
            "--enriched-result",
            str(enrich_xlsx),
            "--output-json",
            str(qa_out),
            "--min-batch-mentions",
            str(args.qa_min_batch_mentions),
            "--min-enriched-mentions",
            str(args.qa_min_enriched_mentions),
            "--min-notes-for-mention-fail",
            str(args.qa_min_notes_for_mention_fail),
            "--min-intent-notes-for-mention-fail",
            str(args.qa_min_intent_notes_for_mention_fail),
        ]
        if args.strict_qa:
            qcmd.append("--strict")
        qr = _run(qcmd, cwd=root)
        qa_returncode = int(qr.returncode)
        manifest["steps"].append(
            {
                "name": "qa",
                "returncode": qr.returncode,
                "qa_json": str(qa_out),
            }
        )
        manifest["qa"] = _load_json(qa_out)
        _save_manifest(out_manifest, manifest)
    else:
        manifest["qa"] = {}

    manifest["outputs"] = {
        "merged_xlsx": str(current_merged),
        "refreshed_xlsx": str(refreshed_xlsx),
        "enriched_xlsx": str(enrich_xlsx) if enrich_xlsx else "",
        "enriched_summary_json": str(enrich_sum) if enrich_sum else "",
        "qa_json": str(qa_out) if qa_out.exists() else "",
    }
    if qa_returncode is not None and qa_returncode != 0:
        manifest["status"] = "QA_BLOCKED"
    else:
        manifest["status"] = "DONE"
    manifest["ended_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    _save_manifest(out_manifest, manifest)
    print(json.dumps({"manifest": str(out_manifest), "status": manifest["status"]}, ensure_ascii=False, indent=2))
    if manifest["status"] == "QA_BLOCKED":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
