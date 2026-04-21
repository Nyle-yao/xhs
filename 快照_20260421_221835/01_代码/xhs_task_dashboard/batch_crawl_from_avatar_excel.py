from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from scraper import XHSScraper


AVATAR_ID_RE = re.compile(r"/avatar/([^?]+)")


def read_all_cells_as_strings(xlsx_path: Path) -> list[str]:
    df = pd.read_excel(xlsx_path, header=None)
    vals = [str(x).strip() for x in df.stack().tolist() if str(x).strip() and str(x) != "nan"]
    return vals


def avatar_to_profile_link(avatar_url: str) -> str | None:
    m = AVATAR_ID_RE.search(avatar_url)
    if not m:
        return None
    uid = m.group(1).strip()
    if not uid:
        return None
    return f"https://www.xiaohongshu.com/user/profile/{uid}"


def main() -> None:
    p = argparse.ArgumentParser(description="从头像链接Excel转换候选博主主页并批量尝试抓取")
    p.add_argument("--input", required=True, help="输入Excel路径")
    p.add_argument("--profile-dir", default="./.xhs_profile", help="Playwright持久化目录")
    p.add_argument("--headless", action="store_true", help="是否无头")
    p.add_argument("--max-notes", type=int, default=10, help="每个博主抓取笔记数上限")
    p.add_argument("--output-dir", default="./outputs", help="输出目录")
    args = p.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_vals = read_all_cells_as_strings(input_path)
    avatar_links = [x for x in raw_vals if "xhscdn.com/avatar/" in x]
    avatar_links = list(dict.fromkeys(avatar_links))
    profile_candidates = [avatar_to_profile_link(x) for x in avatar_links]
    profile_candidates = [x for x in profile_candidates if x]
    profile_candidates = list(dict.fromkeys(profile_candidates))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"avatar_to_profile_batch_{ts}"

    pd.DataFrame({"avatar_url": avatar_links}).to_csv(output_dir / f"{base_name}_avatar_links.csv", index=False)
    pd.DataFrame({"profile_candidate": profile_candidates}).to_csv(output_dir / f"{base_name}_profile_candidates.csv", index=False)

    scraper = XHSScraper(profile_dir=args.profile_dir, headless=args.headless)
    success_rows: list[dict] = []
    failed_rows: list[dict] = []

    for idx, link in enumerate(profile_candidates, start=1):
        r = scraper.scrape_blogger(link, max_notes=args.max_notes)
        d = r.data or {}
        nickname = (d.get("nickname") or "").strip()
        note_links = d.get("note_links") or []
        # 兜底：页面抓空视为失败（常见于非主页ID）
        if r.ok and (nickname or note_links):
            success_rows.append(
                {
                    "seq": idx,
                    "profile_link": link,
                    "blogger_id": d.get("blogger_id"),
                    "nickname": nickname,
                    "xhs_account": d.get("xhs_account"),
                    "followers": d.get("followers"),
                    "following": d.get("following"),
                    "likes_total": d.get("likes_total"),
                    "note_link_count": len(note_links),
                    "note_links": "\n".join(note_links),
                    "fetched_at": d.get("fetched_at"),
                }
            )
        else:
            failed_rows.append(
                {
                    "seq": idx,
                    "profile_link": link,
                    "message": r.message if r.message else "空数据",
                    "is_empty_data": bool(r.ok and not nickname and not note_links),
                }
            )

    scraper.close()

    xlsx_path = output_dir / f"{base_name}_crawl_result.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        pd.DataFrame(success_rows).to_excel(writer, index=False, sheet_name="success")
        pd.DataFrame(failed_rows).to_excel(writer, index=False, sheet_name="failed")

    summary = {
        "input_file": str(input_path),
        "avatar_link_count": len(avatar_links),
        "profile_candidate_count": len(profile_candidates),
        "success_count": len(success_rows),
        "failed_count": len(failed_rows),
        "output_excel": str(xlsx_path),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    (output_dir / f"{base_name}_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

