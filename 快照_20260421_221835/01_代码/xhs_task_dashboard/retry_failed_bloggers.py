from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from scraper import XHSScraper


def classify_fail(msg: str) -> str:
    t = (msg or "").strip()
    if ("风控" in t) or ("验证" in t) or ("受限" in t):
        return "风控/验证"
    if "博主抓取失败" in t:
        return "抓取失败"
    if "note_failed" in t:
        return "笔记失败"
    return "其他"


def pick_retry_targets(failed_df: pd.DataFrame, max_targets: int) -> list[dict[str, str]]:
    if failed_df.empty:
        return []
    rows = failed_df.to_dict(orient="records")
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for r in rows:
        link = str(r.get("profile_link", "")).strip()
        msg = str(r.get("message", "")).strip()
        if not link or link in seen:
            continue
        seen.add(link)
        out.append({"profile_link": link, "message": msg, "category": classify_fail(msg)})
        if max_targets > 0 and len(out) >= max_targets:
            break
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="小红书失败重试：对 failed 里的账号做快速复检")
    p.add_argument("--input-result", required=True, help="第一轮结果 xlsx（含 failed）")
    p.add_argument("--profile-dir", default="./.xhs_profile", help="登录态目录")
    p.add_argument("--max-targets", type=int, default=100, help="最多重试多少个博主")
    p.add_argument("--max-notes", type=int, default=3, help="重试时每个博主抓多少篇笔记链接")
    p.add_argument("--sleep-ms", type=int, default=600, help="每次重试间隔（毫秒）")
    p.add_argument("--output-dir", default="./outputs", help="输出目录")
    args = p.parse_args()

    input_path = Path(args.input_result).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    xls = pd.ExcelFile(input_path)
    if "failed" not in xls.sheet_names:
        raise SystemExit("输入结果不含 failed 表，无法重试。")
    failed_df = pd.read_excel(input_path, sheet_name="failed")
    targets = pick_retry_targets(failed_df, max_targets=args.max_targets)
    if not targets:
        raise SystemExit("failed 表没有可重试目标。")

    scraper = XHSScraper(profile_dir=args.profile_dir, headless=True)
    retry_success: list[dict[str, Any]] = []
    retry_failed: list[dict[str, Any]] = []
    try:
        for i, t in enumerate(targets, start=1):
            link = t["profile_link"]
            probe = scraper.probe_profile_access(link)
            if probe.ok:
                rb = scraper.scrape_blogger(link, max_notes=args.max_notes)
                bd = rb.data or {}
                note_links = bd.get("note_links") or []
                useful = bool(
                    str(bd.get("nickname", "")).strip()
                    or note_links
                    or str(bd.get("followers", "")).strip()
                    or str(bd.get("likes_total", "")).strip()
                )
                if rb.ok and useful:
                    retry_success.append(
                        {
                            "profile_link": link,
                            "retry_category": t["category"],
                            "nickname": bd.get("nickname", ""),
                            "followers": bd.get("followers", ""),
                            "likes_total": bd.get("likes_total", ""),
                            "note_link_count": len(note_links),
                            "retry_message": rb.message,
                            "retried_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    )
                else:
                    retry_failed.append(
                        {
                            "profile_link": link,
                            "retry_category": t["category"],
                            "retry_message": rb.message if rb.message else "retry_empty",
                            "retried_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    )
            else:
                retry_failed.append(
                    {
                        "profile_link": link,
                        "retry_category": t["category"],
                        "retry_message": probe.message,
                        "retried_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
            if i % 10 == 0 or i == len(targets):
                print(f"[retry_progress] {i}/{len(targets)}")
            time.sleep(max(0, args.sleep_ms) / 1000.0)
    finally:
        scraper.close()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = output_dir / f"failed_retry_{ts}.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        pd.DataFrame(targets).to_excel(writer, index=False, sheet_name="retry_targets")
        pd.DataFrame(retry_success).to_excel(writer, index=False, sheet_name="retry_success")
        pd.DataFrame(retry_failed).to_excel(writer, index=False, sheet_name="retry_failed")

    summary = {
        "input_result": str(input_path),
        "retry_target_count": len(targets),
        "retry_success_count": len(retry_success),
        "retry_failed_count": len(retry_failed),
        "output_excel": str(out_xlsx),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    out_json = output_dir / f"failed_retry_{ts}_summary.json"
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

