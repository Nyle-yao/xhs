from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any


def run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess:
    print(f"[run] {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=str(cwd), check=False, text=True, capture_output=True)


def latest(output_dir: Path, glob_pat: str, since_ts: float) -> Path | None:
    files = [p for p in output_dir.glob(glob_pat) if p.stat().st_mtime >= since_ts]
    if not files:
        return None
    return sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)[0]


def load_json(p: Path | None) -> dict[str, Any]:
    if not p or not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def main() -> None:
    ap = argparse.ArgumentParser(description="回补成功账号自动多轮深抓")
    ap.add_argument("--base-batch-result", required=True, help="起始 blogger_batch_*_result.xlsx")
    ap.add_argument("--source-xlsx", required=True, help="原始博主清单xlsx")
    ap.add_argument("--source-sheet", default="Sheet1")
    ap.add_argument("--rounds", type=int, default=3, help="最多迭代轮数")
    ap.add_argument("--min-retry-success", type=int, default=3, help="回补成功低于此值时提前停止")
    ap.add_argument("--max-notes-per-blogger", type=int, default=3)
    ap.add_argument("--max-comments-per-note", type=int, default=25)
    ap.add_argument("--comment-scroll-rounds", type=int, default=4)
    ap.add_argument("--output-dir", default="./outputs")
    ap.add_argument("--profile-dir", default="./.xhs_profile")
    ap.add_argument("--fund-aliases", default="./fund_aliases_expanded.json")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    current_batch = Path(args.base_batch_result).expanduser().resolve()
    if not current_batch.exists():
        raise SystemExit(f"base batch not found: {current_batch}")

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    report = {"run_id": run_id, "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "rounds": []}

    for i in range(1, max(1, args.rounds) + 1):
        t0 = datetime.now().timestamp()
        row: dict[str, Any] = {"round": i, "base_batch": str(current_batch)}

        # 1) retry failed from current batch
        rr = run(
            [
                "python3",
                "retry_failed_bloggers.py",
                "--input-result",
                str(current_batch),
                "--profile-dir",
                str(Path(args.profile_dir).expanduser().resolve()),
                "--max-targets",
                "9999",
                "--max-notes",
                "2",
                "--sleep-ms",
                "200",
                "--output-dir",
                str(output_dir),
            ],
            cwd=root,
        )
        retry_summary = latest(output_dir, "failed_retry_*_summary.json", t0)
        retry_meta = load_json(retry_summary)
        retry_success = int(retry_meta.get("retry_success_count", 0))
        row["retry_summary"] = str(retry_summary) if retry_summary else ""
        row["retry_success_count"] = retry_success
        row["retry_returncode"] = rr.returncode
        if retry_success < max(0, args.min_retry_success):
            row["stop_reason"] = f"retry_success<{args.min_retry_success}"
            report["rounds"].append(row)
            break

        # 2) build deep input from retry success
        rb = latest(output_dir, "failed_retry_*.xlsx", t0)
        bd = run(
            [
                "python3",
                "build_deep_crawl_input.py",
                "--retry-xlsx",
                str(rb),
                "--source-xlsx",
                str(Path(args.source_xlsx).expanduser().resolve()),
                "--source-sheet",
                args.source_sheet,
                "--output-dir",
                str(output_dir),
            ],
            cwd=root,
        )
        deep_input = latest(output_dir, "deep_crawl_input_from_retry_*.xlsx", t0)
        row["deep_input"] = str(deep_input) if deep_input else ""
        row["build_input_returncode"] = bd.returncode

        # 3) deep batch crawl with comments
        rb2 = run(
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
                "--max-bloggers",
                "9999",
                "--max-notes-per-blogger",
                str(args.max_notes_per_blogger),
                "--include-comments",
                "--max-comments-per-note",
                str(args.max_comments_per_note),
                "--comment-scroll-rounds",
                str(args.comment_scroll_rounds),
                "--retry-times",
                "0",
                "--blogger-sleep-ms",
                "100",
                "--note-sleep-ms",
                "80",
                "--fund-aliases",
                str(Path(args.fund_aliases).expanduser().resolve()),
                "--output-dir",
                str(output_dir),
            ],
            cwd=root,
        )
        batch_summary = latest(output_dir, "blogger_batch_*_summary.json", t0)
        batch_result = latest(output_dir, "blogger_batch_*_result.xlsx", t0)
        row["batch_summary"] = str(batch_summary) if batch_summary else ""
        row["batch_result"] = str(batch_result) if batch_result else ""
        row["batch_returncode"] = rb2.returncode
        report["rounds"].append(row)

        if not batch_result:
            row["stop_reason"] = "no_batch_result"
            break
        current_batch = batch_result

    report["ended_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out_json = output_dir / f"deep_iterative_report_{run_id}.json"
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"report_json": str(out_json), "round_count": len(report["rounds"])}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

