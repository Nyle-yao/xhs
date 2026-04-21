from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from exporters import (
    BLOGGER_EXPORT_FIELD_MAP,
    COMMENT_EXPORT_FIELD_MAP,
    NOTE_EXPORT_DEFAULT_FIELDS,
    NOTE_EXPORT_FIELD_MAP,
    NOTE_TOPIC_CATEGORY_MAP,
    build_blogger_export_rows,
    build_comment_export_rows,
    build_note_export_rows,
    download_urls_to_zip,
    to_csv_bytes,
    to_excel_bytes,
    to_json_bytes,
)
from scraper import XHSScraper


NOTE_LINK_RE = re.compile(r"https://www\.xiaohongshu\.com/explore/[a-zA-Z0-9]+")
BLOGGER_LINK_RE = re.compile(r"https://www\.xiaohongshu\.com/user/profile/[a-zA-Z0-9]+")


@st.cache_resource(show_spinner=False)
def get_scraper(profile_dir: str, headless: bool) -> XHSScraper:
    return XHSScraper(profile_dir=profile_dir, headless=headless)


def parse_links(raw: str, pattern: re.Pattern[str]) -> list[str]:
    links = pattern.findall(raw or "")
    return sorted(set(links))


def copy_box(text: str, key: str, label: str = "一键复制") -> None:
    escaped = json.dumps(text)
    components.html(
        f"""
        <div style=\"margin:8px 0;\">
          <button id=\"btn_{key}\" style=\"padding:6px 10px;border-radius:8px;border:1px solid #ddd;cursor:pointer;\">{label}</button>
          <span id=\"ok_{key}\" style=\"margin-left:8px;color:#2b8a3e;\"></span>
        </div>
        <script>
          const btn = document.getElementById('btn_{key}');
          const ok = document.getElementById('ok_{key}');
          btn.onclick = async () => {{
            try {{
              await navigator.clipboard.writeText({escaped});
              ok.innerText = '已复制';
              setTimeout(() => ok.innerText = '', 1200);
            }} catch (e) {{
              ok.innerText = '复制失败，请手动复制';
            }}
          }}
        </script>
        """,
        height=42,
    )


def save_task_snapshot(base_dir: Path, name: str, payload: dict | list) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = base_dir / f"{name}_{ts}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


st.set_page_config(page_title="小红书采集看板（MVP）", layout="wide")
st.title("小红书采集看板（MVP）")
st.caption("使用你自己的小红书登录态进行采集。请遵守平台规则与账号安全策略。")

with st.expander("采集规则参考（你指定的文档）", expanded=False):
    st.markdown(
        "\n".join(
            [
                "- 采集博主数据：https://smzs.xisence.com/help/xiaohongshu/batch-collect/blogger",
                "- 采集评论数据：https://smzs.xisence.com/help/xiaohongshu/batch-collect/comment",
                "- 采集笔记数据：https://smzs.xisence.com/help/xiaohongshu/batch-collect/note",
                "- 说明：本工具按上述流程做了可用化整合（单条+批量+导出+下载）。",
            ]
        )
    )

col_cfg1, col_cfg2, col_cfg3 = st.columns([2, 1, 1])
with col_cfg1:
    profile_dir = st.text_input("浏览器登录态目录", value=str(Path("./xhs_task_dashboard/.xhs_profile").resolve()))
with col_cfg2:
    headless = st.toggle("无头模式", value=False)
with col_cfg3:
    st.write("")
    st.write("")
    login_btn = st.button("启动并检测登录")

scraper = get_scraper(profile_dir, headless)

if login_btn:
    with st.spinner("正在打开浏览器并检测登录..."):
        res = scraper.ensure_login(timeout_sec=180)
    if res.ok:
        st.success(res.message)
    else:
        st.warning(res.message)

output_dir = Path("./xhs_task_dashboard/outputs")
output_dir.mkdir(parents=True, exist_ok=True)

tab_note, tab_comment, tab_batch_note, tab_blogger, tab_search = st.tabs(
    ["笔记采集", "评论导出", "批量笔记导出", "博主信息", "搜索导出"]
)

with tab_note:
    st.subheader("笔记信息采集 / 复制 / 媒体下载")
    note_url = st.text_input("笔记链接", placeholder="https://www.xiaohongshu.com/explore/xxxx")
    c1, c2 = st.columns([1, 1])
    with c1:
        do_note = st.button("采集笔记信息", key="do_note")
    with c2:
        st.caption("可导出：ID/标题/内容/互动/图片列表/视频列表")

    if do_note and note_url:
        with st.spinner("抓取中..."):
            res = scraper.scrape_note(note_url.strip())
        if not res.ok:
            st.error(res.message)
        else:
            st.success(res.message)
            data = res.data or {}
            st.json(data)

            info_text = json.dumps(
                {
                    "note_id": data.get("note_id"),
                    "title": data.get("title"),
                    "content": data.get("content"),
                    "like_count": data.get("like_count"),
                    "collect_count": data.get("collect_count"),
                    "comment_count": data.get("comment_count"),
                    "note_url": data.get("note_url"),
                },
                ensure_ascii=False,
                indent=2,
            )
            copy_box(info_text, "copy_note", "复制笔记信息")

            st.download_button(
                "下载笔记JSON",
                data=to_json_bytes(data),
                file_name=f"note_{data.get('note_id') or 'unknown'}.json",
                mime="application/json",
            )

            media_urls = (data.get("image_urls") or []) + (data.get("video_urls") or [])
            if media_urls:
                if st.button("下载笔记原图/视频（打包ZIP）", key="dl_media"):
                    with st.spinner("打包下载中..."):
                        z = download_urls_to_zip(media_urls)
                    st.download_button(
                        "点击保存媒体ZIP",
                        data=z,
                        file_name=f"note_media_{data.get('note_id') or 'unknown'}.zip",
                        mime="application/zip",
                        key="save_media_zip",
                    )

with tab_comment:
    st.subheader("评论数据一键导出（支持多链接）")
    raw_note_links_comment = st.text_area(
        "笔记链接（可多行粘贴）",
        placeholder="https://www.xiaohongshu.com/explore/xxxx\\nhttps://www.xiaohongshu.com/explore/yyyy",
        height=120,
        key="note_links_comment",
    )
    max_comments = st.number_input("最大评论数", min_value=20, max_value=2000, value=200, step=20)
    if st.button("抓取并导出评论", key="do_comment"):
        links = parse_links(raw_note_links_comment, NOTE_LINK_RE)
        if not links:
            st.warning("未识别到有效笔记链接，请至少粘贴1条。")
        else:
            st.info(f"识别到 {len(links)} 条笔记链接，开始抓取评论…")
            raw_rows = []
            failed = []
            p = st.progress(0)
            for idx, u in enumerate(links, start=1):
                res = scraper.scrape_comments(u, max_comments=int(max_comments))
                if res.ok and res.data:
                    raw_rows.extend((res.data or {}).get("comments", []))
                else:
                    failed.append({"note_url": u, "comment_text": f"[FAILED] {res.message}"})
                p.progress(idx / len(links))

            if failed:
                raw_rows.extend(failed)
            rows = build_comment_export_rows(raw_rows)
            st.success(f"完成：成功 {len(links) - len(failed)} 条，失败 {len(failed)} 条，评论 {len(rows)} 条")
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
            st.download_button(
                "下载评论Excel",
                data=to_excel_bytes(rows, sheet_name="comments", columns=list(COMMENT_EXPORT_FIELD_MAP.values())),
                file_name="comments_export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.download_button(
                "下载评论CSV",
                data=to_csv_bytes(rows),
                file_name="comments_export.csv",
                mime="text/csv",
            )

with tab_batch_note:
    st.subheader("批量导出笔记数据（支持链接列表）")
    raw_links = st.text_area("粘贴笔记链接（可多行）", height=160)
    include_comments = st.toggle("同时抓取评论（较慢）", value=False)
    batch_comment_limit = st.number_input("每篇评论上限", min_value=20, max_value=1000, value=100, step=20)
    st.caption("导出格式（按教程口径）支持自定义字段；“笔记话题”支持来源勾选")
    selected_export_fields = st.multiselect(
        "导出字段",
        options=list(NOTE_EXPORT_FIELD_MAP.keys()),
        default=NOTE_EXPORT_DEFAULT_FIELDS,
        format_func=lambda x: NOTE_EXPORT_FIELD_MAP.get(x, x),
    )
    if not selected_export_fields:
        st.info("未勾选导出字段，已自动回退为官方默认字段顺序。")
        selected_export_fields = NOTE_EXPORT_DEFAULT_FIELDS.copy()
    selected_tag_categories = st.multiselect(
        "笔记话题来源（可多选）",
        options=list(NOTE_TOPIC_CATEGORY_MAP.keys()),
        default=["all"],
        format_func=lambda x: NOTE_TOPIC_CATEGORY_MAP.get(x, ("", x))[1],
    )
    st.caption("也支持按博主链接批量导出笔记")
    raw_blogger_links_for_notes = st.text_area("粘贴博主链接（可多行，自动抓其近期笔记）", height=120)
    max_notes_per_blogger = st.number_input("每位博主抓取笔记上限", min_value=1, max_value=100, value=20)

    if st.button("开始批量导出", key="do_batch_note"):
        links = parse_links(raw_links, NOTE_LINK_RE)
        if not links:
            st.warning("未识别到有效笔记链接")
        else:
            st.info(f"识别到 {len(links)} 篇笔记，开始处理...")
            raw_note_rows = []
            comment_rows = []
            progress = st.progress(0)
            for idx, u in enumerate(links, start=1):
                r = scraper.scrape_note(u)
                if r.ok and r.data:
                    raw_note_rows.append(r.data)
                    if include_comments:
                        rc = scraper.scrape_comments(u, max_comments=int(batch_comment_limit))
                        if rc.ok and rc.data:
                            comment_rows.extend(rc.data.get("comments", []))
                progress.progress(idx / len(links))

            note_rows = build_note_export_rows(
                raw_note_rows,
                export_fields=selected_export_fields,
                tag_categories=selected_tag_categories,
            )
            st.success(f"完成：笔记 {len(raw_note_rows)} 篇，评论 {len(comment_rows)} 条")
            st.dataframe(pd.DataFrame(note_rows), use_container_width=True)

            st.download_button(
                "下载批量笔记Excel",
                data=to_excel_bytes(
                    note_rows,
                    sheet_name="notes",
                    columns=[NOTE_EXPORT_FIELD_MAP[f] for f in selected_export_fields if f in NOTE_EXPORT_FIELD_MAP],
                ),
                file_name="batch_notes.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.download_button(
                "下载批量笔记CSV",
                data=to_csv_bytes(note_rows),
                file_name="batch_notes.csv",
                mime="text/csv",
            )

            if comment_rows:
                crows = build_comment_export_rows(comment_rows)
                st.download_button(
                    "下载批量评论Excel",
                    data=to_excel_bytes(crows, sheet_name="comments", columns=list(COMMENT_EXPORT_FIELD_MAP.values())),
                    file_name="batch_comments.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            snap = {"notes_raw": raw_note_rows, "notes_export": note_rows, "comments": comment_rows}
            p = save_task_snapshot(output_dir, "batch_note_export", snap)
            st.caption(f"本地归档：{p.resolve()}")

    if st.button("按博主链接批量导出笔记", key="do_batch_by_blogger"):
        blogger_links = parse_links(raw_blogger_links_for_notes, BLOGGER_LINK_RE)
        if not blogger_links:
            st.warning("未识别到有效博主链接")
        else:
            st.info(f"识别到 {len(blogger_links)} 位博主，正在采集其笔记链接...")
            note_links: list[str] = []
            p1 = st.progress(0)
            for idx, bl in enumerate(blogger_links, start=1):
                rb = scraper.scrape_blogger(bl, max_notes=int(max_notes_per_blogger))
                if rb.ok and rb.data:
                    note_links.extend(rb.data.get("note_links") or [])
                p1.progress(idx / len(blogger_links))
            note_links = sorted(set(note_links))
            if not note_links:
                st.warning("未从这些博主页识别到笔记链接")
            else:
                st.success(f"共识别到 {len(note_links)} 条笔记链接，开始抓取笔记数据...")
                raw_note_rows = []
                comment_rows = []
                p2 = st.progress(0)
                for idx, u in enumerate(note_links, start=1):
                    r = scraper.scrape_note(u)
                    if r.ok and r.data:
                        raw_note_rows.append(r.data)
                        if include_comments:
                            rc = scraper.scrape_comments(u, max_comments=int(batch_comment_limit))
                            if rc.ok and rc.data:
                                comment_rows.extend(rc.data.get("comments", []))
                    p2.progress(idx / len(note_links))

                note_rows = build_note_export_rows(
                    raw_note_rows,
                    export_fields=selected_export_fields,
                    tag_categories=selected_tag_categories,
                )
                st.success(f"完成：笔记 {len(raw_note_rows)} 篇，评论 {len(comment_rows)} 条")
                st.dataframe(pd.DataFrame(note_rows), use_container_width=True)
                st.download_button(
                    "下载（博主链路）笔记Excel",
                    data=to_excel_bytes(
                        note_rows,
                        sheet_name="notes",
                        columns=[NOTE_EXPORT_FIELD_MAP[f] for f in selected_export_fields if f in NOTE_EXPORT_FIELD_MAP],
                    ),
                    file_name="batch_notes_by_blogger.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
                if comment_rows:
                    crows = build_comment_export_rows(comment_rows)
                    st.download_button(
                        "下载（博主链路）评论Excel",
                        data=to_excel_bytes(crows, sheet_name="comments", columns=list(COMMENT_EXPORT_FIELD_MAP.values())),
                        file_name="batch_comments_by_blogger.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )

with tab_blogger:
    st.subheader("博主信息复制 / 导出")
    blogger_url = st.text_input("博主主页链接", placeholder="https://www.xiaohongshu.com/user/profile/xxxx")
    c1, c2 = st.columns([1, 1])
    with c1:
        do_blogger = st.button("抓取博主信息", key="do_blogger")
    with c2:
        max_notes = st.number_input("附带采集近期笔记链接数", min_value=0, max_value=100, value=30)

    if do_blogger and blogger_url:
        with st.spinner("抓取中..."):
            rb = scraper.scrape_blogger(blogger_url.strip(), max_notes=int(max_notes))
        if not rb.ok:
            st.error(rb.message)
        else:
            st.success(rb.message)
            data = rb.data or {}
            st.json(data)
            copy_box(json.dumps(data, ensure_ascii=False, indent=2), "copy_blogger", "复制博主信息")

            st.download_button(
                "下载博主JSON",
                data=to_json_bytes(data),
                file_name=f"blogger_{data.get('blogger_id') or 'unknown'}.json",
                mime="application/json",
            )

    st.markdown("---")
    st.caption("根据博主链接批量导出")
    raw_blogger_links = st.text_area("粘贴博主链接（可多行）", height=120)
    if st.button("批量导出博主信息", key="batch_blogger"):
        links = parse_links(raw_blogger_links, BLOGGER_LINK_RE)
        if not links:
            st.warning("未识别到有效博主链接")
        else:
            raw_rows = []
            progress = st.progress(0)
            for idx, link in enumerate(links, start=1):
                res = scraper.scrape_blogger(link)
                if res.ok and res.data:
                    raw_rows.append(res.data)
                progress.progress(idx / len(links))
            rows = build_blogger_export_rows(raw_rows)
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
            st.download_button(
                "下载博主批量Excel",
                data=to_excel_bytes(rows, sheet_name="bloggers", columns=list(BLOGGER_EXPORT_FIELD_MAP.values())),
                file_name="batch_bloggers.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

with tab_search:
    st.subheader("关键词搜索导出（笔记/博主）")
    keyword = st.text_input("关键词", placeholder="如：基金定投 / 宝盈基金")
    limit = st.number_input("导出数量上限", min_value=10, max_value=500, value=50, step=10)
    c1, c2 = st.columns([1, 1])
    with c1:
        do_search_notes = st.button("搜索并导出笔记", key="search_notes")
    with c2:
        do_search_bloggers = st.button("搜索并导出博主", key="search_bloggers")

    if do_search_notes and keyword:
        with st.spinner("搜索笔记中..."):
            rs = scraper.search_notes(keyword.strip(), limit=int(limit))
        if not rs.ok:
            st.error(rs.message)
        else:
            links = (rs.data or {}).get("note_links", [])
            st.success(rs.message)
            st.dataframe(pd.DataFrame({"note_url": links}), use_container_width=True)
            st.download_button(
                "下载笔记链接CSV",
                data=to_csv_bytes([{"note_url": x, "keyword": keyword} for x in links]),
                file_name=f"search_notes_{keyword}.csv",
                mime="text/csv",
            )

    if do_search_bloggers and keyword:
        with st.spinner("搜索博主中..."):
            rb = scraper.search_bloggers(keyword.strip(), limit=int(limit))
        if not rb.ok:
            st.error(rb.message)
        else:
            links = (rb.data or {}).get("blogger_links", [])
            st.success(rb.message)
            st.dataframe(pd.DataFrame({"blogger_url": links}), use_container_width=True)
            st.download_button(
                "下载博主链接CSV",
                data=to_csv_bytes([{"blogger_url": x, "keyword": keyword} for x in links]),
                file_name=f"search_bloggers_{keyword}.csv",
                mime="text/csv",
            )

st.markdown("---")
st.caption("安全提示：请控制采集频率，避免触发平台风控；建议分批、低频、错峰执行。")
