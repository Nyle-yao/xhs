from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from scraper import XHSScraper


def _safe_sheet(name: str) -> str:
    return str(name or "sheet")[:31]


def main() -> None:
    p = argparse.ArgumentParser(description="按既有 note_export 的笔记链接刷新媒体字段（图片/视频/封面/话题）")
    p.add_argument("--input-result", required=True, help="输入xlsx（含 note_export）")
    p.add_argument("--profile-dir", default="./.xhs_profile", help="登录态目录")
    p.add_argument("--headless", action="store_true", help="无头模式")
    p.add_argument("--start-index", type=int, default=0, help="从第几个去重笔记链接开始刷新（0基）")
    p.add_argument("--max-notes", type=int, default=0, help="最多刷新笔记数，0=全部")
    p.add_argument("--retry-times", type=int, default=1, help="每条笔记失败重试次数")
    p.add_argument("--sleep-ms", type=int, default=800, help="每条笔记刷新后休眠毫秒")
    p.add_argument("--output-dir", default="./outputs", help="输出目录")
    args = p.parse_args()

    in_path = Path(args.input_result).expanduser().resolve()
    out_dir = Path(args.output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    xls = pd.ExcelFile(in_path)
    if "note_export" not in xls.sheet_names:
        raise SystemExit("输入文件缺少 note_export 分表")
    note_df = pd.read_excel(in_path, sheet_name="note_export")
    all_sheets: dict[str, pd.DataFrame] = {}
    for s in xls.sheet_names:
        all_sheets[s] = pd.read_excel(in_path, sheet_name=s)

    if note_df.empty:
        raise SystemExit("note_export 为空，无可刷新数据")

    note_df = note_df.copy()
    if "笔记图片链接(刷新前)" not in note_df.columns:
        note_df["笔记图片链接(刷新前)"] = note_df.get("笔记图片链接", "")
    if "图片数量(刷新前)" not in note_df.columns:
        note_df["图片数量(刷新前)"] = note_df.get("图片数量", 0)

    links_all = []
    seen: set[str] = set()
    for _, r in note_df.iterrows():
        u = str(r.get("笔记链接", "")).strip()
        if not u or u in seen:
            continue
        seen.add(u)
        links_all.append(u)
    start = max(0, int(args.start_index))
    links = links_all[start:]
    if args.max_notes > 0:
        links = links[: int(args.max_notes)]

    scraper = XHSScraper(profile_dir=args.profile_dir, headless=args.headless)
    refresh_log: list[dict[str, Any]] = []
    refreshed = 0
    failed = 0

    def _with_retry(url: str):
        last = None
        attempts = max(0, args.retry_times) + 1
        for _ in range(attempts):
            r = scraper.scrape_note(url)
            if r.ok and r.data:
                return r
            last = r
        return last

    try:
        for i, link in enumerate(links, start=1):
            r = _with_retry(link)
            if r and r.ok and r.data:
                d = r.data
                images = d.get("image_urls") or []
                videos = d.get("video_urls") or []
                cover = d.get("cover_url", "")
                topic = d.get("note_topic", "")
                note_id = str(d.get("note_id", "")).strip()

                mask = note_df["笔记链接"].astype(str).str.strip() == link
                if note_id and "笔记ID" in note_df.columns:
                    mask = mask | (note_df["笔记ID"].astype(str).str.strip() == note_id)

                note_df.loc[mask, "笔记图片链接"] = "\n".join([str(x) for x in images])
                note_df.loc[mask, "笔记视频链接"] = "\n".join([str(x) for x in videos])
                note_df.loc[mask, "笔记封面链接"] = str(cover or "")
                note_df.loc[mask, "图片数量"] = len(images)
                if topic:
                    note_df.loc[mask, "笔记话题"] = topic
                refreshed += 1
                refresh_log.append(
                    {
                        "seq": i,
                        "note_link": link,
                        "note_id": note_id,
                        "ok": "是",
                        "image_count_new": len(images),
                        "video_count_new": len(videos),
                        "message": "refresh_success",
                    }
                )
            else:
                failed += 1
                refresh_log.append(
                    {
                        "seq": i,
                        "note_link": link,
                        "note_id": "",
                        "ok": "否",
                        "image_count_new": 0,
                        "video_count_new": 0,
                        "message": "" if r is None else str(getattr(r, "message", "")),
                    }
                )
            if i % 10 == 0 or i == len(links):
                print(f"[refresh_progress] {i}/{len(links)}")
            time.sleep(max(0, int(args.sleep_ms)) / 1000.0)
    finally:
        scraper.close()

    all_sheets["note_export"] = note_df
    all_sheets["refresh_log"] = pd.DataFrame(refresh_log)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = out_dir / f"note_media_refreshed_{ts}.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        for s, df in all_sheets.items():
            df.to_excel(writer, index=False, sheet_name=_safe_sheet(s))

    summary = {
        "input_result": str(in_path),
        "note_links_total": len(links),
        "refresh_success_count": refreshed,
        "refresh_failed_count": failed,
        "output_excel": str(out_xlsx),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    out_json = out_dir / f"note_media_refreshed_{ts}_summary.json"
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
