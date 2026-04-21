#!/usr/bin/env python3
"""Materialize XHS crawl workbook into raw assets for later OCR/semantic analysis.

Input: workbook exported by simple crawler with sheets 博主表/笔记表/评论表.
Output: folder with note text, comment text, note image files, comment image files,
        JSONL records, and an Excel manifest.
"""
from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests


def safe_name(v: object, max_len: int = 80) -> str:
    s = re.sub(r"[\\/:*?\"<>|\n\r\t]+", "_", str(v or "")).strip(" ._")
    return (s or "unknown")[:max_len]


def split_urls(v: object) -> list[str]:
    out = []
    seen = set()
    for x in re.split(r"[\n,]+", str(v or "")):
        u = x.strip()
        if not u or u in seen:
            continue
        out.append(u)
        seen.add(u)
    return out


def ext_from_url(url: str, default: str = ".jpg") -> str:
    path = urlparse(url).path.lower()
    for ext in [".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"]:
        if ext in path:
            return ext
    return default


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text or "", encoding="utf-8")


def download(url: str, out: Path, timeout: int = 20) -> dict:
    out.parent.mkdir(parents=True, exist_ok=True)
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Referer": "https://www.xiaohongshu.com/",
    }
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code != 200 or not r.content:
            return {"status": "failed", "http_status": r.status_code, "bytes": 0, "error": "empty_or_bad_status"}
        out.write_bytes(r.content)
        return {"status": "ok", "http_status": r.status_code, "bytes": len(r.content), "error": ""}
    except Exception as e:
        return {"status": "failed", "http_status": "", "bytes": 0, "error": str(e)[:200]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output-dir", default="/Users/yaoruanxingchen/Desktop/小红书爬虫/04_输出数据_当前")
    ap.add_argument("--download-images", action="store_true")
    ap.add_argument("--sleep-sec", type=float, default=0.15)
    args = ap.parse_args()

    src = Path(args.input)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = Path(args.output_dir) / f"原始素材归档_{safe_name(src.stem, 50)}_{ts}"
    text_dir = base / "01_文本"
    note_img_dir = base / "02_笔记图片"
    comment_img_dir = base / "03_评论图片"
    manifest_dir = base / "00_清单"
    manifest_dir.mkdir(parents=True, exist_ok=True)

    notes = pd.read_excel(src, sheet_name="笔记表", dtype=str).fillna("")
    comments = pd.read_excel(src, sheet_name="评论表", dtype=str).fillna("")
    bloggers = pd.read_excel(src, sheet_name="博主表", dtype=str).fillna("")

    note_text_rows = []
    note_image_rows = []
    comment_text_rows = []
    comment_image_rows = []

    for _, r in notes.iterrows():
        note_id = str(r.get("笔记ID", "") or "unknown")
        title = str(r.get("笔记标题", "") or "")
        content = str(r.get("笔记内容", "") or "")
        topic = str(r.get("笔记话题", "") or "")
        note_folder = text_dir / "笔记正文" / safe_name(note_id)
        text_path = note_folder / "note_text.txt"
        full_text = f"笔记ID：{note_id}\n笔记链接：{r.get('笔记链接','')}\n发布时间：{r.get('发布时间','')}\n标题：{title}\n话题：{topic}\n正文：\n{content}\n"
        write_text(text_path, full_text)
        note_text_rows.append({
            "笔记ID": note_id,
            "笔记链接": r.get("笔记链接", ""),
            "发布时间": r.get("发布时间", ""),
            "笔记标题": title,
            "笔记话题": topic,
            "笔记正文": content,
            "本地文本路径": str(text_path),
            "是否疑似占位正文": "是" if "发现 直播 发布" in content else "否",
            "后续用途": "语义识别/基金化名匹配/广告线索识别",
        })
        for idx, url in enumerate(split_urls(r.get("笔记图片链接", "")), start=1):
            ext = ext_from_url(url, ".webp")
            local = note_img_dir / safe_name(note_id) / f"note_{safe_name(note_id)}_img_{idx:02d}{ext}"
            meta = {"status": "url_only", "http_status": "", "bytes": 0, "error": "not_downloaded"}
            if args.download_images:
                meta = download(url, local)
                time.sleep(args.sleep_sec)
            note_image_rows.append({
                "笔记ID": note_id,
                "图片序号": idx,
                "图片URL": url,
                "本地图片路径": str(local) if args.download_images and meta["status"] == "ok" else "",
                "下载状态": meta["status"],
                "HTTP状态": meta["http_status"],
                "文件字节": meta["bytes"],
                "错误信息": meta["error"],
                "后续用途": "OCR/图片语义识别/基金化名匹配/广告线索识别",
            })

    for _, r in comments.iterrows():
        note_id = str(r.get("笔记ID", "") or "unknown")
        cid = str(r.get("评论ID", "") or "unknown")
        content = str(r.get("评论内容", "") or "")
        is_self = str(r.get("是否博主本人评论(猜测)", "") or "")
        text_path = text_dir / "评论正文" / safe_name(note_id) / f"comment_{safe_name(cid)}.txt"
        full_text = f"笔记ID：{note_id}\n评论ID：{cid}\n是否博主本人评论：{is_self}\n用户名称：{r.get('用户名称','')}\n评论时间：{r.get('评论时间','')}\n评论内容：\n{content}\n"
        write_text(text_path, full_text)
        comment_text_rows.append({
            "笔记ID": note_id,
            "评论ID": cid,
            "是否博主本人评论(猜测)": is_self,
            "用户名称": r.get("用户名称", ""),
            "评论时间": r.get("评论时间", ""),
            "评论内容": content,
            "本地文本路径": str(text_path),
            "后续用途": "博主本人补充观点识别/语义识别/广告线索识别",
        })
        for idx, url in enumerate(split_urls(r.get("评论图片链接", "")), start=1):
            ext = ext_from_url(url, ".webp")
            local = comment_img_dir / safe_name(note_id) / f"comment_{safe_name(cid)}_img_{idx:02d}{ext}"
            meta = {"status": "url_only", "http_status": "", "bytes": 0, "error": "not_downloaded"}
            if args.download_images:
                meta = download(url, local)
                time.sleep(args.sleep_sec)
            comment_image_rows.append({
                "笔记ID": note_id,
                "评论ID": cid,
                "图片序号": idx,
                "评论图片URL": url,
                "本地图片路径": str(local) if args.download_images and meta["status"] == "ok" else "",
                "下载状态": meta["status"],
                "HTTP状态": meta["http_status"],
                "文件字节": meta["bytes"],
                "错误信息": meta["error"],
                "后续用途": "评论图片OCR/语义识别/广告线索识别",
            })

    summary = pd.DataFrame([
        {"项目": "来源工作簿", "值": str(src)},
        {"项目": "归档目录", "值": str(base)},
        {"项目": "博主数", "值": len(bloggers)},
        {"项目": "笔记数", "值": len(notes)},
        {"项目": "评论数", "值": len(comments)},
        {"项目": "博主本人评论数", "值": int((comments.get("是否博主本人评论(猜测)", pd.Series(dtype=str)) == "是").sum())},
        {"项目": "笔记图片URL数", "值": len(note_image_rows)},
        {"项目": "笔记图片下载成功数", "值": sum(1 for x in note_image_rows if x.get("下载状态") == "ok")},
        {"项目": "评论图片URL数", "值": len(comment_image_rows)},
        {"项目": "评论图片下载成功数", "值": sum(1 for x in comment_image_rows if x.get("下载状态") == "ok")},
        {"项目": "疑似占位正文笔记数", "值": sum(1 for x in note_text_rows if x.get("是否疑似占位正文") == "是")},
    ])

    # JSONL for downstream semantic/OCR pipeline.
    for name, rows in [
        ("note_text_records.jsonl", note_text_rows),
        ("note_image_records.jsonl", note_image_rows),
        ("comment_text_records.jsonl", comment_text_rows),
        ("comment_image_records.jsonl", comment_image_rows),
    ]:
        with (manifest_dir / name).open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    manifest_xlsx = manifest_dir / f"原始素材归档清单_{ts}.xlsx"
    with pd.ExcelWriter(manifest_xlsx, engine="openpyxl") as w:
        summary.to_excel(w, index=False, sheet_name="00_归档摘要")
        pd.DataFrame(note_text_rows).to_excel(w, index=False, sheet_name="01_笔记文字")
        pd.DataFrame(note_image_rows).to_excel(w, index=False, sheet_name="02_笔记图片")
        pd.DataFrame(comment_text_rows).to_excel(w, index=False, sheet_name="03_评论文字")
        pd.DataFrame(comment_image_rows).to_excel(w, index=False, sheet_name="04_评论图片")
    print(json.dumps({
        "archive_dir": str(base),
        "manifest_xlsx": str(manifest_xlsx),
        "note_text_rows": len(note_text_rows),
        "note_image_rows": len(note_image_rows),
        "comment_text_rows": len(comment_text_rows),
        "comment_image_rows": len(comment_image_rows),
        "note_image_downloaded": sum(1 for x in note_image_rows if x.get("下载状态") == "ok"),
        "comment_image_downloaded": sum(1 for x in comment_image_rows if x.get("下载状态") == "ok"),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
