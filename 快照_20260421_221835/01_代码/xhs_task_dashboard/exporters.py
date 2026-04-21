from __future__ import annotations

import io
import json
import re
import zipfile
from collections import OrderedDict
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests


NOTE_EXPORT_FIELD_MAP: OrderedDict[str, str] = OrderedDict(
    [
        ("note_id", "笔记ID"),
        ("note_url", "笔记链接"),
        ("note_type", "笔记类型"),
        ("title", "笔记标题"),
        ("content", "笔记内容"),
        ("note_topic", "笔记话题"),
        ("like_count", "点赞量"),
        ("collect_count", "收藏量"),
        ("comment_count", "评论量"),
        ("share_count", "分享量"),
        ("publish_time", "发布时间"),
        ("update_time", "更新时间"),
        ("ip_address", "IP地址"),
        ("blogger_id", "博主ID"),
        ("blogger_url", "博主链接"),
        ("blogger_name", "博主昵称"),
        ("image_count", "图片数量"),
        ("cover_url", "笔记封面链接"),
        ("image_urls", "笔记图片链接"),
        ("video_urls", "笔记视频链接"),
        ("source_keyword", "来源关键词"),
    ]
)
NOTE_EXPORT_DEFAULT_FIELDS: list[str] = [x for x in NOTE_EXPORT_FIELD_MAP.keys() if x != "source_keyword"]

NOTE_TOPIC_CATEGORY_MAP: dict[str, tuple[str, str]] = {
    "text": ("tags_text", "文案标签"),
    "topic": ("tags_topic", "话题标签"),
    "all": ("tags_all", "综合标签"),
}

COMMENT_EXPORT_FIELD_MAP: OrderedDict[str, str] = OrderedDict(
    [
        ("comment_id", "评论ID"),
        ("comment_content", "评论内容"),
        ("comment_body_guess", "评论正文猜测"),
        ("is_blogger_self_guess", "是否博主本人评论(猜测)"),
        ("comment_image_url", "评论图片链接"),
        ("like_count", "点赞量"),
        ("comment_time", "评论时间"),
        ("ip_address", "IP地址"),
        ("reply_count", "子评论数"),
        ("note_id", "笔记ID"),
        ("note_url", "笔记链接"),
        ("blogger_id", "博主ID"),
        ("blogger_name", "博主昵称"),
        ("user_id", "用户ID"),
        ("user_url", "用户链接"),
        ("user_name", "用户名称"),
        ("parent_comment_id", "一级评论ID"),
        ("parent_comment_content", "一级评论内容"),
        ("quoted_comment_id", "引用的评论ID"),
        ("quoted_comment_content", "引用的评论内容"),
        ("parent_user_id", "一级评论用户ID"),
        ("parent_user_name", "一级评论用户名称"),
        ("quoted_user_id", "引用的用户ID"),
        ("quoted_user_name", "引用的用户名称"),
    ]
)

BLOGGER_EXPORT_FIELD_MAP: OrderedDict[str, str] = OrderedDict(
    [
        ("blogger_id", "博主ID"),
        ("blogger_url", "博主链接"),
        ("pugongying_url", "博主蒲公英"),
        ("nickname", "博主昵称"),
        ("avatar_url", "博主头像链接"),
        ("xhs_account", "小红书号"),
        ("followers", "粉丝数"),
        ("gender", "博主性别"),
        ("bio", "博主简介"),
        ("likes_total", "获赞与收藏"),
        ("following", "关注"),
        ("ip_address", "IP地址"),
        ("job_tag", "职业标签"),
        ("region_tag", "地区标签"),
        ("birthday_tag", "生日标签"),
        ("school_tag", "学校标签"),
        ("note_count", "笔记数"),
        ("source_keyword", "来源关键词"),
    ]
)


def safe_filename(name: str, max_len: int = 120) -> str:
    n = re.sub(r"[\\/:*?\"<>|\n\r\t]+", "_", name or "")
    n = n.strip(" ._")
    if not n:
        n = "untitled"
    return n[:max_len]


def to_excel_bytes(rows: list[dict], sheet_name: str = "data", columns: list[str] | None = None) -> bytes:
    if columns:
        if rows:
            df = pd.DataFrame(rows)
            for c in columns:
                if c not in df.columns:
                    df[c] = ""
            df = df[columns]
        else:
            df = pd.DataFrame(columns=columns)
    else:
        df = pd.DataFrame(rows)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    return buf.getvalue()


def to_csv_bytes(rows: list[dict]) -> bytes:
    df = pd.DataFrame(rows)
    return df.to_csv(index=False).encode("utf-8-sig")


def to_json_bytes(data: dict | list) -> bytes:
    return json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")


def _dedup_keep_order(items: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for x in items:
        s = str(x or "").strip()
        if not s or s in seen:
            continue
        out.append(s)
        seen.add(s)
    return out


def _normalize_tags(v: object) -> list[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return _dedup_keep_order([str(x) for x in v])
    s = str(v).strip()
    if not s:
        return []
    sep = "、" if "、" in s else ","
    return _dedup_keep_order([x.strip() for x in s.split(sep) if x.strip()])


def _compose_note_topic(d: dict, categories: list[str]) -> str:
    tags: list[str] = []
    seen: set[str] = set()
    for c in categories:
        src_key = NOTE_TOPIC_CATEGORY_MAP.get(c, ("", ""))[0]
        if not src_key:
            continue
        for t in _normalize_tags(d.get(src_key)):
            if t in seen:
                continue
            tags.append(t)
            seen.add(t)
    if not tags:
        tags = _normalize_tags(d.get("note_topic"))
    return "、".join(tags)


def build_note_export_rows(
    notes: list[dict],
    export_fields: list[str] | None = None,
    tag_categories: list[str] | None = None,
) -> list[dict]:
    """
    标准化笔记导出结构：
    - export_fields 控制基础字段列（按 NOTE_EXPORT_FIELD_MAP）
    - tag_categories 控制标签列（text/topic/all）
    """
    fields = [f for f in (export_fields or NOTE_EXPORT_DEFAULT_FIELDS) if f in NOTE_EXPORT_FIELD_MAP]
    if not fields:
        fields = NOTE_EXPORT_DEFAULT_FIELDS.copy()
    categories = [c for c in (tag_categories or ["all"]) if c in NOTE_TOPIC_CATEGORY_MAP]
    out: list[dict] = []

    for d in notes:
        image_urls = d.get("image_urls") or []
        video_urls = d.get("video_urls") or []
        note_type = d.get("note_type")
        if not note_type:
            note_type = "视频" if video_urls else "图文"
        note_topic = _compose_note_topic(d, categories)
        cover_url = d.get("cover_url") or (image_urls[0] if image_urls else (video_urls[0] if video_urls else ""))

        data_map: dict[str, object] = {
            "note_id": d.get("note_id", ""),
            "note_url": d.get("note_url", ""),
            "note_type": note_type,
            "title": d.get("title", ""),
            "content": d.get("content", ""),
            "note_topic": note_topic,
            "like_count": d.get("like_count", ""),
            "collect_count": d.get("collect_count", ""),
            "comment_count": d.get("comment_count", ""),
            "share_count": d.get("share_count", ""),
            "publish_time": d.get("publish_time", ""),
            "update_time": d.get("update_time") or d.get("fetched_at", ""),
            "ip_address": d.get("ip_address", ""),
            "blogger_id": d.get("blogger_id", ""),
            "blogger_url": d.get("blogger_url", ""),
            "blogger_name": d.get("blogger_name") or d.get("author_name", ""),
            "image_count": d.get("image_count") if d.get("image_count") is not None else len(image_urls),
            "cover_url": cover_url,
            "image_urls": "\n".join([str(x) for x in image_urls]),
            "video_urls": "\n".join([str(x) for x in video_urls]),
            "source_keyword": d.get("source_keyword", ""),
        }

        row: dict[str, object] = {}
        for f in fields:
            col = NOTE_EXPORT_FIELD_MAP[f]
            val = data_map.get(f, "")
            row[col] = val
        out.append(row)
    return out


def _extract_note_id_from_url(note_url: str) -> str:
    u = str(note_url or "")
    # 兼容两种链接形态：
    # 1) /explore/<note_id>
    # 2) /user/profile/<blogger_id>/<note_id>?xsec_token=...
    patterns = [
        r"/explore/([a-zA-Z0-9]+)",
        r"/user/profile/[a-zA-Z0-9]+/([a-zA-Z0-9]+)",
    ]
    for pat in patterns:
        m = re.search(pat, u)
        if m:
            return m.group(1)
    return ""


def build_comment_export_rows(comments: list[dict]) -> list[dict]:
    out: list[dict] = []
    for d in comments:
        note_url = str(d.get("note_url") or "")
        note_id = _extract_note_id_from_url(note_url) or str(d.get("note_id") or "")
        data_map: dict[str, object] = {
            "comment_id": d.get("comment_id", ""),
            "comment_content": d.get("comment_content") or d.get("comment_text", ""),
            "comment_body_guess": d.get("comment_body_guess") or d.get("comment_content") or d.get("comment_text", ""),
            "is_blogger_self_guess": d.get("is_blogger_self_guess", ""),
            "comment_image_url": d.get("comment_image_url", ""),
            "like_count": d.get("like_count", ""),
            "comment_time": d.get("comment_time", ""),
            "ip_address": d.get("ip_address", ""),
            "reply_count": d.get("reply_count", ""),
            "note_id": note_id,
            "note_url": note_url,
            "blogger_id": d.get("blogger_id", ""),
            "blogger_name": d.get("blogger_name", ""),
            "user_id": d.get("user_id", ""),
            "user_url": d.get("user_url", ""),
            "user_name": d.get("user_name", ""),
            "parent_comment_id": d.get("parent_comment_id", ""),
            "parent_comment_content": d.get("parent_comment_content", ""),
            "quoted_comment_id": d.get("quoted_comment_id", ""),
            "quoted_comment_content": d.get("quoted_comment_content", ""),
            "parent_user_id": d.get("parent_user_id", ""),
            "parent_user_name": d.get("parent_user_name", ""),
            "quoted_user_id": d.get("quoted_user_id", ""),
            "quoted_user_name": d.get("quoted_user_name", ""),
        }
        row: dict[str, object] = {}
        for key, label in COMMENT_EXPORT_FIELD_MAP.items():
            row[label] = data_map.get(key, "")
        out.append(row)
    return out


def build_blogger_export_rows(bloggers: list[dict]) -> list[dict]:
    out: list[dict] = []
    for d in bloggers:
        data_map: dict[str, object] = {
            "blogger_id": d.get("blogger_id", ""),
            "blogger_url": d.get("blogger_url", ""),
            "pugongying_url": d.get("pugongying_url", ""),
            "nickname": d.get("nickname", ""),
            "avatar_url": d.get("avatar_url", ""),
            "xhs_account": d.get("xhs_account", ""),
            "followers": d.get("followers", ""),
            "gender": d.get("gender", ""),
            "bio": d.get("bio", ""),
            "likes_total": d.get("likes_total", ""),
            "following": d.get("following", ""),
            "ip_address": d.get("ip_address", ""),
            "job_tag": d.get("job_tag", ""),
            "region_tag": d.get("region_tag", ""),
            "birthday_tag": d.get("birthday_tag", ""),
            "school_tag": d.get("school_tag", ""),
            "note_count": d.get("note_count") if d.get("note_count") is not None else len(d.get("note_links") or []),
            "source_keyword": d.get("source_keyword", ""),
        }
        row: dict[str, object] = {}
        for key, label in BLOGGER_EXPORT_FIELD_MAP.items():
            row[label] = data_map.get(key, "")
        out.append(row)
    return out


def download_urls_to_zip(urls: Iterable[str], zip_name: str = "media.zip", timeout: int = 25) -> bytes:
    buf = io.BytesIO()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.xiaohongshu.com/",
    }
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        idx = 1
        for u in urls:
            if not u:
                continue
            try:
                r = requests.get(u, headers=headers, timeout=timeout)
                if r.status_code != 200 or not r.content:
                    continue
                # infer extension
                suffix = Path(u.split("?")[0]).suffix.lower()
                if suffix not in {".jpg", ".jpeg", ".png", ".webp", ".mp4", ".m3u8"}:
                    ctype = (r.headers.get("Content-Type") or "").lower()
                    if "image" in ctype:
                        suffix = ".jpg"
                    elif "video" in ctype:
                        suffix = ".mp4"
                    else:
                        suffix = ".bin"
                zf.writestr(f"media_{idx:03d}{suffix}", r.content)
                idx += 1
            except Exception:
                continue
    return buf.getvalue()
