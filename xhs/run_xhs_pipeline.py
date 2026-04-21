from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


def _run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess:
    print(f"[run] {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=str(cwd), check=False, text=True, capture_output=True)


def _latest_file(glob_pat: str, output_dir: Path, since_ts: float) -> Path | None:
    cands = [p for p in output_dir.glob(glob_pat) if p.stat().st_mtime >= since_ts]
    if not cands:
        return None
    return sorted(cands, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def _load_json(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _analyze_static_image_ratio(result_xlsx: Path) -> dict[str, Any]:
    probe = {
        "ok": False,
        "note_rows": 0,
        "image_rows": 0,
        "static_rows": 0,
        "static_ratio": 0.0,
        "error": "",
    }
    try:
        xls = pd.ExcelFile(result_xlsx)
        if "note_export" not in xls.sheet_names:
            probe["error"] = "note_export_missing"
            return probe
        df = pd.read_excel(result_xlsx, sheet_name="note_export")
        probe["note_rows"] = int(len(df))
        if "笔记图片链接" not in df.columns:
            probe["error"] = "note_image_col_missing"
            return probe
        s = df["笔记图片链接"].fillna("").astype(str).str.strip()
        nonempty = s != ""
        image_rows = int(nonempty.sum())
        if image_rows <= 0:
            probe["ok"] = True
            probe["image_rows"] = 0
            return probe
        static_mask = s.str.contains(r"/fe-platform/|picasso-static\.xiaohongshu\.com/fe-platform", case=False, regex=True)
        static_rows = int((nonempty & static_mask).sum())
        probe["ok"] = True
        probe["image_rows"] = image_rows
        probe["static_rows"] = static_rows
        probe["static_ratio"] = round(static_rows / max(1, image_rows), 4)
        return probe
    except Exception as e:
        probe["error"] = str(e)
        return probe


def main() -> None:
    p = argparse.ArgumentParser(description="小红书任务：一键全流程（主抓->重试->增强->QA->报告）")
    p.add_argument("--input", required=True, help="输入博主清单xlsx")
    p.add_argument("--sheet", default="Sheet1")
    p.add_argument("--link-column", default="博主链接")
    p.add_argument("--id-column", default="博主ID")
    p.add_argument("--nickname-column", default="博主昵称")
    p.add_argument("--profile-dir", default="./.xhs_profile")
    p.add_argument("--fund-aliases", default="./fund_aliases_expanded.json")
    p.add_argument("--leshu-tag-file", default="", help="外部标签表（可选，输出双标签桥接）")
    p.add_argument("--output-dir", default="./outputs")
    p.add_argument("--max-bloggers", type=int, default=0)
    p.add_argument("--max-notes-per-blogger", type=int, default=10)
    p.add_argument(
        "--include-comments",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="是否抓评论（默认开启，可用 --no-include-comments 关闭）",
    )
    p.add_argument("--max-comments-per-note", type=int, default=80)
    p.add_argument("--headless", action="store_true")
    p.add_argument("--retry-times", type=int, default=2)
    p.add_argument("--blogger-sleep-ms", type=int, default=900)
    p.add_argument("--note-sleep-ms", type=int, default=450)
    p.add_argument("--run-retry", action="store_true", help="主抓后自动跑failed重试")
    p.add_argument("--retry-max-targets", type=int, default=80)
    p.add_argument("--retry-max-notes", type=int, default=3)
    p.add_argument("--run-refresh-media", action="store_true", help="主抓后先刷新note_export媒体字段（修复历史静态图）")
    p.add_argument("--refresh-max-notes", type=int, default=0)
    p.add_argument("--refresh-retry-times", type=int, default=1)
    p.add_argument("--refresh-sleep-ms", type=int, default=700)
    p.add_argument(
        "--auto-refresh-on-static",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="自动检测静态图占比并触发媒体刷新（默认开启）",
    )
    p.add_argument("--auto-refresh-static-threshold", type=float, default=0.35, help="静态图占比阈值，超过则自动刷新")
    p.add_argument("--auto-refresh-min-image-rows", type=int, default=30, help="触发自动刷新的最小含图笔记数")
    p.add_argument("--run-enrich", action="store_true", help="主抓后自动跑增强汇总")
    p.add_argument("--enrich-crawl-comments", action="store_true")
    p.add_argument("--enrich-headed", action="store_true")
    p.add_argument("--enrich-max-notes-for-comments", type=int, default=40)
    p.add_argument("--enrich-max-comments-per-note", type=int, default=50)
    p.add_argument("--enrich-ocr-images", action="store_true")
    p.add_argument("--enrich-ocr-max-notes", type=int, default=80)
    p.add_argument("--enrich-ocr-max-images-per-note", type=int, default=0)
    p.add_argument("--enrich-image-audit-timeout-sec", type=int, default=12)
    p.add_argument(
        "--strict-qa",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="是否启用严格QA闸门（默认开启，WARN/FAIL都会阻断）",
    )
    p.add_argument("--qa-min-batch-mentions", type=int, default=1, help="batch提及最小PASS阈值")
    p.add_argument("--qa-min-enriched-mentions", type=int, default=1, help="enriched提及最小PASS阈值")
    p.add_argument("--qa-min-notes-for-mention-fail", type=int, default=20, help="达到该笔记量时零提及触发FAIL")
    p.add_argument(
        "--qa-min-intent-notes-for-mention-fail",
        type=int,
        default=3,
        help="达到该基金语义笔记量时零提及触发FAIL",
    )
    p.add_argument(
        "--allow-degraded-run",
        action="store_true",
        help="允许降级运行（预检全失败仍继续），默认关闭（质量优先）",
    )
    args = p.parse_args()

    root = Path(__file__).resolve().parent
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    started = datetime.now()
    started_ts = started.timestamp()

    run_id = started.strftime("%Y%m%d_%H%M%S")
    report_json = output_dir / f"pipeline_run_{run_id}.json"
    report_md = output_dir / f"pipeline_run_{run_id}.md"

    steps: list[dict[str, Any]] = []

    # 1) 主抓
    batch_cmd = [
        "python3",
        "batch_crawl_from_blogger_excel.py",
        "--input",
        str(Path(args.input).expanduser().resolve()),
        "--sheet",
        args.sheet,
        "--link-column",
        args.link_column,
        "--id-column",
        args.id_column,
        "--nickname-column",
        args.nickname_column,
        "--profile-dir",
        str(Path(args.profile_dir).expanduser().resolve()),
        "--max-bloggers",
        str(args.max_bloggers),
        "--max-notes-per-blogger",
        str(args.max_notes_per_blogger),
        "--max-comments-per-note",
        str(args.max_comments_per_note),
        "--retry-times",
        str(args.retry_times),
        "--blogger-sleep-ms",
        str(args.blogger_sleep_ms),
        "--note-sleep-ms",
        str(args.note_sleep_ms),
        "--fund-aliases",
        str(Path(args.fund_aliases).expanduser().resolve()),
        "--output-dir",
        str(output_dir),
    ]
    if args.headless:
        batch_cmd.append("--headless")
    if args.include_comments:
        batch_cmd.append("--include-comments")
    if args.allow_degraded_run:
        batch_cmd.append("--allow-degraded-run")
    r1 = _run(batch_cmd, cwd=root)
    batch_summary = _latest_file("blogger_batch_*_summary.json", output_dir, started_ts)
    batch_result = _latest_file("blogger_batch_*_result.xlsx", output_dir, started_ts)
    steps.append(
        {
            "name": "batch_crawl",
            "returncode": r1.returncode,
            "stdout_tail": "\n".join((r1.stdout or "").splitlines()[-30:]),
            "stderr_tail": "\n".join((r1.stderr or "").splitlines()[-30:]),
            "summary_json": str(batch_summary) if batch_summary else "",
            "result_xlsx": str(batch_result) if batch_result else "",
        }
    )

    retry_summary = None
    retry_result = None
    if args.run_retry and batch_result and batch_result.exists():
        rr = _run(
            [
                "python3",
                "retry_failed_bloggers.py",
                "--input-result",
                str(batch_result),
                "--profile-dir",
                str(Path(args.profile_dir).expanduser().resolve()),
                "--max-targets",
                str(args.retry_max_targets),
                "--max-notes",
                str(args.retry_max_notes),
                "--output-dir",
                str(output_dir),
            ],
            cwd=root,
        )
        retry_summary = _latest_file("failed_retry_*_summary.json", output_dir, started_ts)
        retry_result = _latest_file("failed_retry_*.xlsx", output_dir, started_ts)
        steps.append(
            {
                "name": "retry_failed",
                "returncode": rr.returncode,
                "stdout_tail": "\n".join((rr.stdout or "").splitlines()[-30:]),
                "stderr_tail": "\n".join((rr.stderr or "").splitlines()[-30:]),
                "summary_json": str(retry_summary) if retry_summary else "",
                "result_xlsx": str(retry_result) if retry_result else "",
            }
        )

    refreshed_result = None
    refreshed_summary = None
    refresh_input = retry_result if (retry_result and retry_result.exists()) else batch_result
    should_refresh = bool(args.run_refresh_media)
    refresh_reason = "manual"
    static_probe: dict[str, Any] = {}
    if (not should_refresh) and args.auto_refresh_on_static and refresh_input and refresh_input.exists():
        static_probe = _analyze_static_image_ratio(refresh_input)
        image_rows = int(static_probe.get("image_rows", 0))
        static_ratio = float(static_probe.get("static_ratio", 0) or 0)
        if static_probe.get("ok") and image_rows >= max(1, int(args.auto_refresh_min_image_rows)) and static_ratio >= float(
            args.auto_refresh_static_threshold
        ):
            should_refresh = True
            refresh_reason = "auto_static_repair"
        steps.append(
            {
                "name": "media_static_probe",
                "returncode": 0,
                "input_xlsx": str(refresh_input),
                **static_probe,
                "auto_refresh_triggered": bool(should_refresh and refresh_reason == "auto_static_repair"),
            }
        )

    if should_refresh and refresh_input and refresh_input.exists():
        rm = _run(
            [
                "python3",
                "refresh_note_media.py",
                "--input-result",
                str(refresh_input),
                "--profile-dir",
                str(Path(args.profile_dir).expanduser().resolve()),
                "--max-notes",
                str(args.refresh_max_notes),
                "--retry-times",
                str(args.refresh_retry_times),
                "--sleep-ms",
                str(args.refresh_sleep_ms),
                "--output-dir",
                str(output_dir),
                *(["--headless"] if args.headless else []),
            ],
            cwd=root,
        )
        refreshed_summary = _latest_file("note_media_refreshed_*_summary.json", output_dir, started_ts)
        refreshed_result = _latest_file("note_media_refreshed_*.xlsx", output_dir, started_ts)
        steps.append(
            {
                "name": "refresh_note_media",
                "returncode": rm.returncode,
                "reason": refresh_reason,
                "input_xlsx": str(refresh_input),
                "stdout_tail": "\n".join((rm.stdout or "").splitlines()[-30:]),
                "stderr_tail": "\n".join((rm.stderr or "").splitlines()[-30:]),
                "summary_json": str(refreshed_summary) if refreshed_summary else "",
                "result_xlsx": str(refreshed_result) if refreshed_result else "",
            }
        )

    enriched_summary = None
    enriched_result = None
    enrich_input = refreshed_result if (refreshed_result and refreshed_result.exists()) else batch_result
    if args.run_enrich and enrich_input and enrich_input.exists():
        ecmd = [
            "python3",
            "ops_enrich_pipeline.py",
            "--input-result",
            str(enrich_input),
            "--fund-aliases",
            str(Path(args.fund_aliases).expanduser().resolve()),
            "--output-dir",
            str(output_dir),
        ]
        if args.leshu_tag_file:
            ecmd.extend(["--leshu-tag-file", str(Path(args.leshu_tag_file).expanduser().resolve())])
        if args.enrich_crawl_comments:
            ecmd.extend(
                [
                    "--crawl-comments",
                    "--max-notes-for-comments",
                    str(args.enrich_max_notes_for_comments),
                    "--max-comments-per-note",
                    str(args.enrich_max_comments_per_note),
                ]
            )
        if args.enrich_headed:
            ecmd.append("--headed")
        if args.enrich_ocr_images:
            ecmd.extend(
                [
                    "--ocr-images",
                    "--ocr-max-notes",
                    str(args.enrich_ocr_max_notes),
                    "--ocr-max-images-per-note",
                    str(args.enrich_ocr_max_images_per_note),
                ]
            )
        ecmd.extend(["--image-audit-timeout-sec", str(args.enrich_image_audit_timeout_sec)])

        re = _run(ecmd, cwd=root)
        enriched_summary = _latest_file("ops_enriched_*_summary.json", output_dir, started_ts)
        enriched_result = _latest_file("ops_enriched_*.xlsx", output_dir, started_ts)
        steps.append(
            {
                "name": "ops_enrich",
                "returncode": re.returncode,
                "stdout_tail": "\n".join((re.stdout or "").splitlines()[-30:]),
                "stderr_tail": "\n".join((re.stderr or "").splitlines()[-30:]),
                "summary_json": str(enriched_summary) if enriched_summary else "",
                "result_xlsx": str(enriched_result) if enriched_result else "",
            }
        )

    # QA
    qa_payload: dict[str, Any] = {}
    if batch_result and batch_result.exists():
        qa_json = output_dir / f"qa_pipeline_{run_id}.json"
        qcmd = [
            "python3",
            "qa_xhs_pipeline.py",
            "--batch-result",
            str(batch_result),
            "--output-json",
            str(qa_json),
            "--min-batch-mentions",
            str(args.qa_min_batch_mentions),
            "--min-enriched-mentions",
            str(args.qa_min_enriched_mentions),
            "--min-notes-for-mention-fail",
            str(args.qa_min_notes_for_mention_fail),
            "--min-intent-notes-for-mention-fail",
            str(args.qa_min_intent_notes_for_mention_fail),
        ]
        if enriched_result and enriched_result.exists():
            qcmd.extend(["--enriched-result", str(enriched_result)])
        if args.strict_qa:
            qcmd.append("--strict")
        rq = _run(qcmd, cwd=root)
        qa_payload = _load_json(qa_json)
        steps.append(
            {
                "name": "qa_check",
                "returncode": rq.returncode,
                "stdout_tail": "\n".join((rq.stdout or "").splitlines()[-30:]),
                "stderr_tail": "\n".join((rq.stderr or "").splitlines()[-30:]),
                "qa_json": str(qa_json),
            }
        )

    ended = datetime.now()
    batch_meta = _load_json(batch_summary)
    retry_meta = _load_json(retry_summary)
    enrich_meta = _load_json(enriched_summary)

    overall = "PASS"
    if any((s.get("returncode", 1) != 0) for s in steps if s["name"] in {"batch_crawl", "qa_check"}):
        overall = "FAIL"
    elif qa_payload.get("overall_status") == "WARN":
        overall = "WARN"

    report = {
        "run_id": run_id,
        "started_at": started.strftime("%Y-%m-%d %H:%M:%S"),
        "ended_at": ended.strftime("%Y-%m-%d %H:%M:%S"),
        "duration_sec": round((ended - started).total_seconds(), 2),
        "overall_status": overall,
        "outputs": {
            "batch_result": str(batch_result) if batch_result else "",
            "batch_summary": str(batch_summary) if batch_summary else "",
            "retry_summary": str(retry_summary) if retry_summary else "",
            "refresh_summary": str(refreshed_summary) if refreshed_summary else "",
            "enriched_result": str(enriched_result) if enriched_result else "",
            "enriched_summary": str(enriched_summary) if enriched_summary else "",
            "qa": qa_payload,
        },
        "metrics": {
            "batch": batch_meta,
            "retry": retry_meta,
            "enrich": enrich_meta,
        },
        "steps": steps,
    }

    report_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        f"# 小红书流程运行报告 {run_id}",
        "",
        f"- 状态: **{overall}**",
        f"- 开始: {report['started_at']}",
        f"- 结束: {report['ended_at']}",
        f"- 耗时: {report['duration_sec']} 秒",
        "",
        "## 关键产物",
        f"- 主抓结果: {report['outputs']['batch_result']}",
        f"- 增强结果: {report['outputs']['enriched_result']}",
        f"- QA结果: {report_json.parent / ('qa_pipeline_' + run_id + '.json')}",
        "",
        "## 核心指标",
        f"- 主抓成功博主数: {batch_meta.get('blogger_success_count', '-')}",
        f"- 主抓失败记录数: {batch_meta.get('blogger_failed_count', '-')}",
        f"- 笔记数: {batch_meta.get('note_export_count', '-')}",
        f"- 提及数(增强): {enrich_meta.get('mention_count', '-')}",
        "",
        "## 说明",
        "- 此报告用于追踪链路完整性与运行稳定性。",
        "- 若状态为FAIL，请先看 steps 里的 stderr_tail。",
    ]
    report_md.write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps(report, ensure_ascii=False, indent=2))

    if overall == "FAIL":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
