"""Export saved JSON outputs to xlsx (one file per record kind)."""
from __future__ import annotations
import io
import json
import time
from pathlib import Path
from typing import Any

import pandas as pd


def _ts(ts: Any) -> str:
    try:
        ts = int(ts)
        if ts > 1e12:
            ts = ts // 1000
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
    except Exception:
        return ""


def _pick_cover_url(note: dict) -> str:
    cov = note.get("cover") or {}
    if isinstance(cov, dict):
        for it in cov.get("info_list") or []:
            if it.get("image_scene") == "WB_DFT" and it.get("url"):
                return it["url"]
        for it in cov.get("info_list") or []:
            if it.get("url"):
                return it["url"]
        if cov.get("url"):
            return cov["url"]
    return ""


def _user_post_rows(data: dict) -> pd.DataFrame:
    rows = []
    for i, n in enumerate(data.get("notes") or []):
        u = n.get("user") or {}
        ii = n.get("interact_info") or {}
        nid = n.get("note_id", "")
        tok = n.get("xsec_token", "")
        rows.append({
            "序号": i + 1,
            "笔记ID": nid,
            "标题": n.get("display_title", ""),
            "类型": n.get("type", ""),
            "点赞数": ii.get("liked_count", 0),
            "已点赞": ii.get("liked", False),
            "置顶": ii.get("sticky", False),
            "封面URL": _pick_cover_url(n),
            "笔记链接": f"https://www.xiaohongshu.com/explore/{nid}?xsec_token={tok}&xsec_source=pc_user" if nid else "",
            "用户ID": u.get("user_id") or u.get("userId", ""),
            "用户昵称": u.get("nick_name") or u.get("nickname", ""),
            "用户头像": u.get("avatar", ""),
            "xsec_token": tok,
        })
    return pd.DataFrame(rows)


def _note_sheets(data: dict) -> dict[str, pd.DataFrame]:
    user = data.get("user") or {}
    interact = data.get("interact") or {}
    nid = data.get("note_id", "")
    tok = user.get("xsec_token", "")
    base = pd.DataFrame([{
        "笔记ID": nid,
        "标题": data.get("title", ""),
        "类型": data.get("type", ""),
        "正文": data.get("desc", ""),
        "发布时间": _ts(data.get("time")),
        "更新时间": _ts(data.get("last_update_time")),
        "IP归属": data.get("ip_location", ""),
        "点赞数": interact.get("liked_count", 0),
        "收藏数": interact.get("collected_count", 0),
        "评论数": interact.get("comment_count", 0),
        "分享数": interact.get("share_count", 0),
        "用户ID": user.get("user_id", ""),
        "用户昵称": user.get("nickname", ""),
        "用户头像": user.get("avatar", ""),
        "话题标签": " | ".join(
            (t.get("name") if isinstance(t, dict) else str(t))
            for t in (data.get("tag_list") or [])
        ),
        "笔记链接": f"https://www.xiaohongshu.com/explore/{nid}?xsec_token={tok}&xsec_source=pc_user",
    }])
    img_rows = []
    for i, im in enumerate(data.get("image_list") or []):
        url = ""
        if isinstance(im, dict):
            url = (im.get("url_default") or im.get("urlDefault") or im.get("url") or "")
            if not url:
                for it in im.get("info_list") or []:
                    if it.get("url"):
                        url = it["url"]; break
        elif isinstance(im, str):
            url = im
        img_rows.append({"序号": i + 1, "图片URL": url})
    for i, v in enumerate(data.get("video_urls") or []):
        img_rows.append({"序号": f"V{i+1}", "图片URL": v})
    return {"基础信息": base, "图片视频": pd.DataFrame(img_rows)}


def _comment_rows(data: dict) -> pd.DataFrame:
    rows = []
    for c in data.get("comments") or []:
        u = c.get("user") or {}
        pics = " | ".join(
            (p.get("url_default") or p.get("url", ""))
            for p in (c.get("pictures") or [])
        )
        sub_n = c.get("sub_comment_count") or len(c.get("sub_comments") or [])
        rows.append({
            "评论ID": c.get("id", ""),
            "笔记ID": c.get("note_id", ""),
            "用户昵称": u.get("nickname", ""),
            "用户ID": u.get("user_id", ""),
            "评论内容": c.get("content", ""),
            "点赞数": c.get("like_count", 0),
            "已点赞": c.get("liked", False),
            "评论时间": _ts(c.get("create_time")),
            "IP归属": c.get("ip_location", ""),
            "子评论数": sub_n,
            "图片附件": pics,
            "层级": "根评论",
        })
        for sc in c.get("sub_comments") or []:
            su = sc.get("user") or {}
            rows.append({
                "评论ID": sc.get("id", ""),
                "笔记ID": sc.get("note_id", ""),
                "用户昵称": su.get("nickname", ""),
                "用户ID": su.get("user_id", ""),
                "评论内容": sc.get("content", ""),
                "点赞数": sc.get("like_count", 0),
                "已点赞": sc.get("liked", False),
                "评论时间": _ts(sc.get("create_time")),
                "IP归属": sc.get("ip_location", ""),
                "子评论数": 0,
                "图片附件": "",
                "层级": f"└─ 回复 {c.get('id','')[:8]}",
            })
    return pd.DataFrame(rows)


def _autosize(writer, sheet_name: str, df: pd.DataFrame) -> None:
    try:
        ws = writer.sheets[sheet_name]
        for i, col in enumerate(df.columns, start=1):
            sample = df[col].astype(str).head(50).tolist() + [str(col)]
            width = min(60, max(8, max((len(s.encode('gbk', errors='ignore')) for s in sample), default=10) + 2))
            ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = width
    except Exception:
        pass


def _bundle_sheets(data: dict) -> dict[str, pd.DataFrame]:
    user = data.get("user") or {}
    stats = data.get("stats") or {}
    params = data.get("params") or {}
    notes = data.get("notes") or []
    details = data.get("details") or {}
    comments_map = data.get("comments") or {}
    errors = data.get("errors") or {}

    # Sheet 1: 概览
    overview = pd.DataFrame([{
        "博主昵称": user.get("nickname", ""),
        "用户ID": user.get("user_id", ""),
        "采集时间": _ts(data.get("scrape_time")),
        "目标笔记数": params.get("max_notes", 0) or "全部",
        "每篇评论数": params.get("max_comments", 0),
        "实际抓取笔记": stats.get("notes", 0),
        "成功获取详情": stats.get("details_ok", 0),
        "成功获取评论": stats.get("comments_ok", 0),
        "评论总条数": stats.get("comments_total", 0),
        "失败笔记数": stats.get("errors", 0),
        "源链接": (params.get("user_url_or_id") or "")[:200],
    }])

    # Sheet 2: 笔记列表（综合：列表 + 详情）
    note_rows = []
    for i, n in enumerate(notes, 1):
        nid = n.get("note_id") or n.get("id") or ""
        u = n.get("user") or {}
        ii = n.get("interact_info") or {}
        d = details.get(nid) or {}
        d_ii = d.get("interact") or {}
        d_user = d.get("user") or {}
        tok = n.get("xsec_token") or d_user.get("xsec_token") or ""
        note_rows.append({
            "序号": i,
            "笔记ID": nid,
            "标题": d.get("title") or n.get("display_title", ""),
            "类型": d.get("type") or n.get("type", ""),
            "正文摘要": (d.get("desc") or "")[:120],
            "发布时间": _ts(d.get("time")),
            "更新时间": _ts(d.get("last_update_time")),
            "IP归属": d.get("ip_location", ""),
            "点赞数": d_ii.get("liked_count") or ii.get("liked_count") or 0,
            "收藏数": d_ii.get("collected_count") or 0,
            "评论数": d_ii.get("comment_count") or 0,
            "分享数": d_ii.get("share_count") or 0,
            "话题标签": " | ".join(
                (t.get("name") if isinstance(t, dict) else str(t))
                for t in (d.get("tag_list") or [])
            ),
            "实抓评论数": len((comments_map.get(nid) or {}).get("comments") or []),
            "封面URL": _pick_cover_url(n),
            "笔记链接": f"https://www.xiaohongshu.com/explore/{nid}?xsec_token={tok}&xsec_source=pc_user" if nid else "",
            "用户昵称": u.get("nick_name") or u.get("nickname") or d_user.get("nickname") or "",
            "用户ID": u.get("user_id") or u.get("userId") or d_user.get("user_id") or "",
            "失败原因": errors.get(nid, ""),
        })
    notes_df = pd.DataFrame(note_rows)

    # Sheet 3: 笔记正文（完整 desc）
    body_rows = []
    for i, n in enumerate(notes, 1):
        nid = n.get("note_id") or n.get("id") or ""
        d = details.get(nid) or {}
        body_rows.append({
            "序号": i,
            "笔记ID": nid,
            "标题": d.get("title") or n.get("display_title", ""),
            "完整正文": d.get("desc", ""),
            "话题标签": " | ".join(
                (t.get("name") if isinstance(t, dict) else str(t))
                for t in (d.get("tag_list") or [])
            ),
            "@用户": " | ".join(
                (a.get("nickname") if isinstance(a, dict) else str(a))
                for a in (d.get("at_user_list") or [])
            ),
        })
    body_df = pd.DataFrame(body_rows)

    # Sheet 4: 图片汇总（一图一行）
    img_rows = []
    for n in notes:
        nid = n.get("note_id") or n.get("id") or ""
        d = details.get(nid) or {}
        title = d.get("title") or n.get("display_title", "")
        for j, im in enumerate(d.get("image_list") or [], 1):
            url = ""
            if isinstance(im, dict):
                url = im.get("url_default") or im.get("urlDefault") or im.get("url") or ""
            elif isinstance(im, str):
                url = im
            img_rows.append({
                "笔记ID": nid, "笔记标题": title, "类型": "图片",
                "序号": j, "URL": url,
            })
        for j, v in enumerate(d.get("video_urls") or [], 1):
            img_rows.append({
                "笔记ID": nid, "笔记标题": title, "类型": f"视频-{v.get('quality','')}",
                "序号": j, "URL": v.get("url", ""),
            })
    img_df = pd.DataFrame(img_rows) if img_rows else pd.DataFrame(columns=["笔记ID","笔记标题","类型","序号","URL"])

    # Sheet 5: 全部评论（含子评论，跨笔记汇总）
    cmt_rows = []
    for n in notes:
        nid = n.get("note_id") or n.get("id") or ""
        title = (details.get(nid) or {}).get("title") or n.get("display_title", "")
        cms = (comments_map.get(nid) or {}).get("comments") or []
        for c in cms:
            u = c.get("user") or {}
            pics = " | ".join(
                (p.get("url_default") or p.get("url", ""))
                for p in (c.get("pictures") or [])
            )
            sub_n = c.get("sub_comment_count") or len(c.get("sub_comments") or [])
            cmt_rows.append({
                "笔记ID": nid, "笔记标题": title,
                "评论ID": c.get("id", ""), "父评论ID": "", "层级": "根评论",
                "用户昵称": u.get("nickname", ""), "用户ID": u.get("user_id", ""),
                "评论内容": c.get("content", ""),
                "点赞数": c.get("like_count", 0),
                "评论时间": _ts(c.get("create_time")),
                "IP归属": c.get("ip_location", ""),
                "子评论数": sub_n, "图片附件": pics,
            })
            for sc in c.get("sub_comments") or []:
                su = sc.get("user") or {}
                cmt_rows.append({
                    "笔记ID": nid, "笔记标题": title,
                    "评论ID": sc.get("id", ""), "父评论ID": c.get("id", ""),
                    "层级": "└─ 回复",
                    "用户昵称": su.get("nickname", ""), "用户ID": su.get("user_id", ""),
                    "评论内容": sc.get("content", ""),
                    "点赞数": sc.get("like_count", 0),
                    "评论时间": _ts(sc.get("create_time")),
                    "IP归属": sc.get("ip_location", ""),
                    "子评论数": 0, "图片附件": "",
                })
    cmt_df = pd.DataFrame(cmt_rows) if cmt_rows else pd.DataFrame(
        columns=["笔记ID","笔记标题","评论ID","父评论ID","层级","用户昵称","用户ID","评论内容","点赞数","评论时间","IP归属","子评论数","图片附件"])

    return {
        "概览": overview,
        "笔记列表": notes_df,
        "笔记正文": body_df,
        "图片视频": img_df,
        "全部评论": cmt_df,
    }


def export(name: str, path: Path) -> tuple[bytes, str]:
    """Return (xlsx bytes, suggested filename) for a saved JSON file."""
    data = json.loads(path.read_text(encoding="utf-8"))
    base = path.stem
    buf = io.BytesIO()

    if name.startswith("user_posted_"):
        df = _user_post_rows(data)
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            sheet = "全部笔记"
            df.to_excel(w, index=False, sheet_name=sheet)
            _autosize(w, sheet, df)
        return buf.getvalue(), f"{base}.xlsx"

    if name.startswith("note_"):
        sheets = _note_sheets(data)
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            for sn, df in sheets.items():
                df.to_excel(w, index=False, sheet_name=sn)
                _autosize(w, sn, df)
        return buf.getvalue(), f"{base}.xlsx"

    if name.startswith("comments_"):
        df = _comment_rows(data)
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            sheet = "评论"
            df.to_excel(w, index=False, sheet_name=sheet)
            _autosize(w, sheet, df)
        return buf.getvalue(), f"{base}.xlsx"

    if name.startswith("bundle_"):
        sheets = _bundle_sheets(data)
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            for sn, df in sheets.items():
                df.to_excel(w, index=False, sheet_name=sn[:31])
                _autosize(w, sn[:31], df)
        return buf.getvalue(), f"{base}.xlsx"

    if name.startswith("notes_top5_"):
        notes = data if isinstance(data, list) else (data.get("notes") or [])
        df = _user_post_rows({"notes": notes})
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            sheet = "前5笔记"
            df.to_excel(w, index=False, sheet_name=sheet)
            _autosize(w, sheet, df)
        return buf.getvalue(), f"{base}.xlsx"

    raise ValueError(f"unsupported file kind: {name}")
