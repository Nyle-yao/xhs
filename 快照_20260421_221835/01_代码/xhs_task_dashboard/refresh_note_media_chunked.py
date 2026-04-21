from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any


def _latest_file(pattern: str, output_dir: Path, since_ts: float) -> Path | None:
    cands = [p for p in output_dir.glob(pattern) if p.stat().st_mtime >= since_ts]
    if not cands:
        return None
    return sorted(cands, key=lambda x: x.stat().st_mtime, reverse=True)[0]


def main() -> None:
    p = argparse.ArgumentParser(description="分批刷新note媒体字段，支持批次超时与断点推进")
    p.add_argument("--input-result", required=True, help="输入xlsx")
    p.add_argument("--profile-dir", default="./.xhs_profile")
    p.add_argument("--headless", action="store_true")
    p.add_argument("--start-index", type=int, default=0)
    p.add_argument("--max-total", type=int, default=0, help="最多刷新总条数，0=不限制")
    p.add_argument("--chunk-size", type=int, default=8, help="每批刷新条数")
    p.add_argument("--retry-times", type=int, default=1)
    p.add_argument("--sleep-ms", type=int, default=700)
    p.add_argument("--batch-timeout-sec", type=int, default=420, help="单批超时秒数")
    p.add_argument("--output-dir", default="./outputs")
    args = p.parse_args()

    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    current_input = Path(args.input_result).expanduser().resolve()
    offset = max(0, int(args.start_index))
    processed = 0
    batch_no = 0
    logs: list[dict[str, Any]] = []

    while True:
        if args.max_total > 0 and processed >= args.max_total:
            break
        this_chunk = int(args.chunk_size)
        if args.max_total > 0:
            this_chunk = min(this_chunk, args.max_total - processed)
        if this_chunk <= 0:
            break

        batch_no += 1
        started_ts = datetime.now().timestamp()
        cmd = [
            "python3",
            "refresh_note_media.py",
            "--input-result",
            str(current_input),
            "--profile-dir",
            str(Path(args.profile_dir).expanduser().resolve()),
            "--start-index",
            str(offset),
            "--max-notes",
            str(this_chunk),
            "--retry-times",
            str(args.retry_times),
            "--sleep-ms",
            str(args.sleep_ms),
            "--output-dir",
            str(output_dir),
        ]
        if args.headless:
            cmd.append("--headless")

        print(f"[chunk] #{batch_no} offset={offset} size={this_chunk}")
        try:
            cp = subprocess.run(cmd, check=False, text=True, capture_output=True, timeout=max(60, int(args.batch_timeout_sec)))
            latest_summary = _latest_file("note_media_refreshed_*_summary.json", output_dir, started_ts)
            latest_xlsx = _latest_file("note_media_refreshed_*.xlsx", output_dir, started_ts)
            row = {
                "batch_no": batch_no,
                "offset": offset,
                "size": this_chunk,
                "returncode": cp.returncode,
                "summary_json": str(latest_summary) if latest_summary else "",
                "output_xlsx": str(latest_xlsx) if latest_xlsx else "",
                "stdout_tail": "\n".join((cp.stdout or "").splitlines()[-20:]),
                "stderr_tail": "\n".join((cp.stderr or "").splitlines()[-20:]),
            }
            logs.append(row)
            if latest_xlsx and latest_xlsx.exists():
                current_input = latest_xlsx
            offset += this_chunk
            processed += this_chunk
            if cp.returncode != 0:
                break
        except subprocess.TimeoutExpired:
            logs.append(
                {
                    "batch_no": batch_no,
                    "offset": offset,
                    "size": this_chunk,
                    "returncode": -9,
                    "summary_json": "",
                    "output_xlsx": "",
                    "stdout_tail": "",
                    "stderr_tail": f"batch_timeout>{args.batch_timeout_sec}s",
                }
            )
            # 超时后跳过该批区间，避免全流程卡死
            offset += this_chunk
            processed += this_chunk
            continue

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = output_dir / f"note_media_refresh_chunked_{ts}.json"
    payload = {
        "input_result": str(Path(args.input_result).expanduser().resolve()),
        "final_output_xlsx": str(current_input),
        "batch_count": batch_no,
        "processed_slots": processed,
        "logs": logs,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

