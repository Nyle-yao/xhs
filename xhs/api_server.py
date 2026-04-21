from __future__ import annotations

import io
import os
import re
import time
import threading
import concurrent.futures
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator

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
    to_excel_bytes,
)
from scraper import ScrapeResult, XHSScraper
import xhs_quality_upgrade as xq


NOTE_LINK_RE = re.compile(r"^https?://(?:www\.)?xiaohongshu\.com/explore/[a-zA-Z0-9]+(?:[/?#].*)?$")
BLOGGER_LINK_RE = re.compile(r"^https?://(?:www\.)?xiaohongshu\.com/user/profile/[a-zA-Z0-9]+(?:[/?#].*)?$")
BLOGGER_ID_IN_URL_RE = re.compile(r"^https?://(?:www\.)?xiaohongshu\.com/user/profile/([a-zA-Z0-9]+)")


def _dedup_and_filter(links: list[str], pattern: re.Pattern[str]) -> list[str]:
    out = []
    seen = set()
    for x in links:
        s = (x or "").strip()
        if not s or s in seen:
            continue
        if pattern.match(s):
            out.append(s)
            seen.add(s)
    return out


def _normalize_blogger_links(links: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for link in links:
        m = BLOGGER_ID_IN_URL_RE.match(str(link or "").strip())
        clean = f"https://www.xiaohongshu.com/user/profile/{m.group(1)}" if m else str(link or "").strip()
        if clean and clean not in seen:
            out.append(clean)
            seen.add(clean)
    return out


def _attachment_headers(filename: str) -> dict[str, str]:
    return {"Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}"}


class LoginCheckReq(BaseModel):
    timeout_sec: int = Field(default=180, ge=30, le=600)


class NoteLinksReq(BaseModel):
    note_links: list[str]
    include_comments: bool = False
    max_comments_per_note: int = Field(default=200, ge=20, le=2000)
    skip_failed: bool = True
    export_fields: list[str] | None = None
    tag_categories: list[Literal["text", "topic", "all"]] | None = None

    @field_validator("note_links")
    @classmethod
    def _v_links(cls, v: list[str]) -> list[str]:
        vv = _dedup_and_filter(v, NOTE_LINK_RE)
        if not vv:
            raise ValueError("note_links 不能为空且必须是有效小红书笔记链接")
        return vv

    @field_validator("export_fields")
    @classmethod
    def _v_export_fields(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        vv = [x for x in v if x in NOTE_EXPORT_FIELD_MAP]
        if not vv:
            raise ValueError("export_fields 无有效字段")
        return vv


class BloggerLinksReq(BaseModel):
    blogger_links: list[str]
    max_notes_per_blogger: int = Field(default=20, ge=1, le=100)
    include_notes: bool = True
    include_comments: bool = False
    max_comments_per_note: int = Field(default=100, ge=20, le=2000)
    skip_failed: bool = True

    @field_validator("blogger_links")
    @classmethod
    def _v_links(cls, v: list[str]) -> list[str]:
        vv = _dedup_and_filter(v, BLOGGER_LINK_RE)
        if not vv:
            raise ValueError("blogger_links 不能为空且必须是有效小红书博主链接")
        return _normalize_blogger_links(vv)


class KeywordReq(BaseModel):
    keyword: str = Field(min_length=1, max_length=120)
    limit: int = Field(default=50, ge=10, le=500)


class MediaByNoteReq(BaseModel):
    note_url: str

    @field_validator("note_url")
    @classmethod
    def _v_url(cls, v: str) -> str:
        s = (v or "").strip()
        if not NOTE_LINK_RE.match(s):
            raise ValueError("note_url 必须是有效小红书笔记链接")
        return s


class MediaByUrlsReq(BaseModel):
    urls: list[str]

    @field_validator("urls")
    @classmethod
    def _v_urls(cls, v: list[str]) -> list[str]:
        vv = [x.strip() for x in (v or []) if x and x.strip()]
        if not vv:
            raise ValueError("urls 不能为空")
        return vv


class APIService:
    def __init__(self) -> None:
        self.profile_dir = os.getenv("XHS_PROFILE_DIR", "./.xhs_profile")
        self.headless = os.getenv("XHS_HEADLESS", "false").lower() in {"1", "true", "yes", "on"}
        self.lock = threading.Lock()
        self.progress_lock = threading.Lock()
        self.last_simple_result: dict[str, Any] | None = None
        self.last_analysis_xlsx: str | None = None
        self.progress: dict[str, Any] = self._initial_progress()
        self.login_scraper: XHSScraper | None = None
        # Playwright sync API binds to its starting thread/greenlet; serialize
        # all calls touching the persistent browser onto a single worker thread.
        self.pw_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="xhs-pw"
        )

    def run_pw(self, fn, *args, **kwargs):
        return self.pw_executor.submit(fn, *args, **kwargs).result()

    def _initial_progress(self) -> dict[str, Any]:
        return {
            "running": False,
            "phase": "idle",
            "stage": "等待操作",
            "message": "尚未开始",
            "started_at": "",
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": "",
            "crawl_percent": 0,
            "analysis_percent": 0,
            "total_bloggers": 0,
            "current_blogger_index": 0,
            "current_blogger": "",
            "total_notes": 0,
            "current_note_index": 0,
            "current_note": "",
            "counts": {"bloggers": 0, "notes": 0, "comments": 0, "failed": 0},
            "events": [],
        }

    def reset_progress(self, total_bloggers: int = 0) -> None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.progress_lock:
            self.progress = self._initial_progress()
            self.progress.update(
                {
                    "running": True,
                    "phase": "crawl",
                    "stage": "准备开始",
                    "message": "任务已创建，等待登录检测",
                    "started_at": now,
                    "updated_at": now,
                    "total_bloggers": total_bloggers,
                    "crawl_percent": 2,
                    "analysis_percent": 0,
                }
            )
            self.progress["events"] = [f"{now} 任务创建：待处理博主 {total_bloggers} 个"]

    def update_progress(self, **kwargs: Any) -> None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.progress_lock:
            self.progress.update(kwargs)
            self.progress["updated_at"] = now
            event = kwargs.get("message") or kwargs.get("stage")
            if event:
                events = list(self.progress.get("events") or [])
                events.append(f"{now} {event}")
                self.progress["events"] = events[-12:]

    def finish_progress(self, ok: bool, message: str) -> None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.progress_lock:
            self.progress.update(
                {
                    "running": False,
                    "phase": "done" if ok else "error",
                    "stage": "完成" if ok else "失败",
                    "message": message,
                    "updated_at": now,
                    "finished_at": now,
                    "crawl_percent": 100 if ok else self.progress.get("crawl_percent", 0),
                    "analysis_percent": 100 if ok else self.progress.get("analysis_percent", 0),
                }
            )
            events = list(self.progress.get("events") or [])
            events.append(f"{now} {message}")
            self.progress["events"] = events[-12:]

    def progress_snapshot(self) -> dict[str, Any]:
        with self.progress_lock:
            return dict(self.progress)

    def get_persistent_scraper(self) -> XHSScraper:
        if self.login_scraper is None:
            self.login_scraper = XHSScraper(profile_dir=self.profile_dir, headless=self.headless)
        return self.login_scraper

    def run(self, method_name: str, *args, **kwargs):
        with self.lock:
            scraper = XHSScraper(profile_dir=self.profile_dir, headless=self.headless)
            try:
                fn = getattr(scraper, method_name)
                return fn(*args, **kwargs)
            except Exception as e:
                return ScrapeResult(ok=False, message=f"执行失败: {e}")
            finally:
                try:
                    scraper.close()
                except Exception:
                    pass

    def login_check_visible(self, timeout_sec: int) -> ScrapeResult:
        """Open one visible login browser and keep it alive for the next crawl."""
        def _do():
            with self.lock:
                scraper = self.get_persistent_scraper()
                try:
                    return scraper.ensure_login(timeout_sec=timeout_sec, keep_open=True)
                except Exception as e:
                    return ScrapeResult(ok=False, message=f"登录检测失败: {e}")
        return self.run_pw(_do)


service = APIService()

DASHBOARD_HTML = Path(__file__).parent / "dashboard.html"

app = FastAPI(
    title="XHS Task API",
    description="小红书任务接口层（登录检测/笔记/评论/博主/搜索/媒体下载）",
    version="0.1.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
def index():
    from fastapi.responses import FileResponse, PlainTextResponse
    if DASHBOARD_HTML.exists():
        return FileResponse(str(DASHBOARD_HTML), media_type="text/html")
    return PlainTextResponse("dashboard.html not found", status_code=404)


@app.get("/xhs-crawler", include_in_schema=False)
def index_alias():
    return index()


class SimpleBloggerCrawlReq(BaseModel):
    blogger_links: list[str]
    max_notes_per_blogger: int = Field(default=50, ge=1, le=80)
    include_comments: bool = True
    max_comments_per_note: int = Field(default=40, ge=5, le=300)
    skip_login_check: bool = True
    skip_failed: bool = True
    batch_size: int = Field(default=2, ge=1, le=5)
    cooldown_sec: int = Field(default=180, ge=0, le=3600)
    risk_cooldown_sec: int = Field(default=900, ge=0, le=7200)

    @field_validator("blogger_links")
    @classmethod
    def _v_links(cls, v: list[str]) -> list[str]:
        vv = _dedup_and_filter(v, BLOGGER_LINK_RE)
        if not vv:
            raise ValueError("blogger_links 不能为空且必须是有效小红书博主链接")
        if len(vv) > 200:
            raise ValueError("为保护账号安全，一次最多输入200个博主链接；更多请分文件执行")
        return _normalize_blogger_links(vv)


def _norm_name(v: str) -> str:
    return re.sub(r"\s+", "", str(v or "")).strip().lower()


def _xlsx_multi(sheets: dict[str, tuple[list[dict], list[str] | None]]) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet, (rows, columns) in sheets.items():
            df = pd.DataFrame(rows)
            if columns:
                for c in columns:
                    if c not in df.columns:
                        df[c] = ""
                df = df[columns]
            df.to_excel(writer, index=False, sheet_name=sheet[:31])
    return buf.getvalue()


def _simple_export_rows(payload: dict[str, Any]) -> dict[str, Any]:
    bloggers = build_blogger_export_rows(payload.get("bloggers") or [])
    notes = build_note_export_rows(payload.get("notes") or [])
    comments = build_comment_export_rows(payload.get("comments") or [])
    failed = payload.get("failed") or []
    return {
        "bloggers": bloggers,
        "notes": notes,
        "comments": comments,
        "failed": failed,
    }


def _looks_like_xhs_risk(message: str) -> bool:
    msg = str(message or "")
    return any(x in msg for x in ["访问频繁", "风控", "验证", "captcha", "300013", "website-login/error"])


def _build_simple_analysis(payload: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    """Build the first analysis layer from the cached crawl result."""
    rows = _simple_export_rows(payload)
    source_df = pd.DataFrame(rows.get("bloggers") or [])
    blogger_df = pd.DataFrame(rows.get("bloggers") or [])
    note_df = pd.DataFrame(rows.get("notes") or [])
    comment_df = pd.DataFrame(rows.get("comments") or [])
    failed_df = pd.DataFrame(rows.get("failed") or [])
    old_mentions = pd.DataFrame()

    aliases = xq.load_aliases(xq.DEFAULT_ALIAS_JSON, xq.DEFAULT_FUND_TAG_XLSX)
    alias_index, code_to_name = xq.build_alias_index(aliases)

    enhanced_mentions = xq.build_enhanced_mentions(note_df, comment_df, old_mentions, alias_index, code_to_name)
    note_quality = xq.build_note_quality(note_df, comment_df, enhanced_mentions.rename(columns={"笔记ID": "note_id"}))
    note_ids = set(note_df.get("笔记ID", pd.Series(dtype=str)).fillna("").astype(str)) if not note_df.empty else set()
    comment_quality = xq.build_comment_quality(comment_df, note_ids)
    blogger_summary, fund_summary, candidate_summary, gap_focus, failed_summary = xq.build_summaries(
        blogger_df, note_quality, comment_quality, enhanced_mentions, failed_df
    )
    overview = xq.build_overview(source_df, blogger_df, note_df, comment_df, enhanced_mentions, note_quality)

    out_dir = Path(os.getenv("XHS_OUTPUT_DIR", str(Path(__file__).parent / "outputs")))
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = out_dir / f"小红书爬虫_采集后识别分析_{ts}.xlsx"

    sheets = {
        "00_总览": overview,
        "01_博主运营优先级": blogger_summary,
        "02_笔记质量诊断": note_quality,
        "03_评论质量诊断": comment_quality,
        "04_基金提及识别": enhanced_mentions,
        "05_确认基金汇总": fund_summary,
        "06_候选基金词待确认": candidate_summary,
        "07_优先补抓清单": gap_focus,
        "08_失败原因汇总": failed_summary,
        "09_原始博主": blogger_df,
        "10_原始笔记": note_df,
        "11_原始评论": comment_df,
    }
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        sheet_names: list[str] = []
        for name, df in sheets.items():
            if df is None or df.empty:
                df = pd.DataFrame({"说明": ["本表暂无数据"]})
            sheet = name[:31]
            sheet_names.append(sheet)
            df.to_excel(writer, index=False, sheet_name=sheet)
        xq.autosize(writer, sheet_names)

    confirmed = (
        int((enhanced_mentions.get("确认程度", pd.Series(dtype=str)).astype(str) == "确认").sum())
        if not enhanced_mentions.empty
        else 0
    )
    candidate = len(enhanced_mentions) - confirmed
    summary = {
        "output": str(out_xlsx),
        "bloggers": len(blogger_df),
        "notes": len(note_df),
        "comments": len(comment_df),
        "mentions": len(enhanced_mentions),
        "confirmed_mentions": confirmed,
        "candidate_mentions": candidate,
        "fund_alias_count": len(aliases),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return out_xlsx, summary


@app.get("/api/v1/meta/note-export-options")
def note_export_options() -> dict[str, Any]:
    return {
        "ok": True,
        "data": {
            "fields": NOTE_EXPORT_FIELD_MAP,
            "tag_categories": {k: v[1] for k, v in NOTE_TOPIC_CATEGORY_MAP.items()},
            "default_fields": NOTE_EXPORT_DEFAULT_FIELDS,
            "default_tag_categories": ["all"],
        },
    }


@app.get("/api/v1/health")
def health() -> dict[str, Any]:
    return {
        "ok": True,
        "service": "xhs-task-api",
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "profile_dir": service.profile_dir,
        "headless": service.headless,
        "persistent_browser": service.login_scraper is not None,
    }


@app.get("/api/v1/simple/progress")
def simple_progress() -> dict[str, Any]:
    return {"ok": True, "data": service.progress_snapshot()}


@app.post("/api/v1/simple/analyze-latest")
def simple_analyze_latest() -> dict[str, Any]:
    if not service.last_simple_result:
        raise HTTPException(status_code=400, detail="当前没有可分析的爬取结果，请先完成一次爬取或当前页面评论抓取")
    try:
        service.update_progress(
            phase="analysis",
            stage="识别分析",
            message="正在对已爬取数据做基金/化名/广告信号识别",
            analysis_percent=20,
            counts=service.progress_snapshot().get("counts", {}),
        )
        out_xlsx, summary = _build_simple_analysis(service.last_simple_result)
        service.last_analysis_xlsx = str(out_xlsx)
        service.update_progress(
            phase="done",
            stage="识别完成",
            message=f"识别完成：提及 {summary['mentions']} 条，确认 {summary['confirmed_mentions']} 条，候选 {summary['candidate_mentions']} 条",
            analysis_percent=100,
            counts=service.progress_snapshot().get("counts", {}),
        )
        return {"ok": True, "message": "识别分析完成，已生成分析Excel", "data": summary}
    except Exception as e:
        service.finish_progress(False, f"识别分析失败：{e}")
        raise HTTPException(status_code=500, detail=f"识别分析失败：{e}")


@app.get("/api/v1/simple/analysis.xlsx")
def simple_analysis_xlsx():
    if not service.last_analysis_xlsx:
        raise HTTPException(status_code=404, detail="还没有生成识别分析表，请先点击“识别分析”")
    p = Path(service.last_analysis_xlsx)
    if not p.exists():
        raise HTTPException(status_code=404, detail=f"识别分析表不存在：{p}")
    return StreamingResponse(
        io.BytesIO(p.read_bytes()),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=_attachment_headers(p.name),
    )


@app.post("/api/v1/simple/blogger-crawl")
def simple_blogger_crawl(req: SimpleBloggerCrawlReq) -> dict[str, Any]:
    """Simple UI endpoint: crawl up to 2 bloggers and cache exportable tables."""
    bloggers: list[dict[str, Any]] = []
    notes: list[dict[str, Any]] = []
    comments: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []
    service.reset_progress(total_bloggers=len(req.blogger_links))

    with service.lock:
        scraper = service.get_persistent_scraper()
        try:
            service.update_progress(stage="登录准备", message="准备使用当前 Chromium 登录态", crawl_percent=5)
            if req.skip_login_check:
                service.update_progress(stage="跳过登录检测", message="已按页面勾选跳过登录检测，直接使用当前浏览器登录态", crawl_percent=10)
            else:
                login_res = scraper.ensure_login(timeout_sec=180)
                if not login_res.ok:
                    service.finish_progress(False, f"登录失败：{login_res.message}")
                    raise HTTPException(status_code=400, detail=login_res.message)
            service.update_progress(stage="登录完成", message="登录检测通过，开始读取博主主页", crawl_percent=10)
            total_bloggers = max(len(req.blogger_links), 1)
            batch_size = max(1, min(req.batch_size, 5))
            total_batches = (total_bloggers + batch_size - 1) // batch_size
            for bidx, burl in enumerate(req.blogger_links, start=1):
                batch_idx = (bidx - 1) // batch_size + 1
                item_idx = (bidx - 1) % batch_size + 1
                if bidx > 1 and item_idx == 1 and req.cooldown_sec > 0:
                    service.update_progress(
                        stage="批次冷却",
                        message=f"第 {batch_idx-1}/{total_batches} 批完成，冷却 {req.cooldown_sec} 秒后自动继续，避免账号风控",
                        current_blogger_index=bidx,
                        current_blogger=burl,
                        crawl_percent=min(12 + int((bidx - 1) / total_bloggers * 28), 40),
                        counts={"bloggers": len(bloggers), "notes": len(notes), "comments": len(comments), "failed": len(failed)},
                    )
                    time.sleep(req.cooldown_sec)
                service.update_progress(
                    stage="爬取博主",
                    message=f"正在处理第 {bidx}/{total_bloggers} 个博主（第 {batch_idx}/{total_batches} 批，第 {item_idx}/{batch_size} 个）；若 Chromium 出现登录/验证，请在窗口内完成，程序会等待后继续",
                    current_blogger_index=bidx,
                    current_blogger=burl,
                    crawl_percent=min(12 + int((bidx - 1) / total_bloggers * 28), 40),
                    counts={"bloggers": len(bloggers), "notes": len(notes), "comments": len(comments), "failed": len(failed)},
                )
                rb = scraper.scrape_blogger(burl, max_notes=req.max_notes_per_blogger, load_wait_sec=35, keep_page_open=True)
                if rb.ok and rb.data:
                    bd = rb.data
                    bloggers.append(bd)
                    bid = str(bd.get("blogger_id", "") or "")
                    bname = str(bd.get("nickname", "") or "")
                    note_links = list(bd.get("note_links") or [])
                    total_notes = len(note_links)
                    service.update_progress(
                        stage="博主完成",
                        message=f"{bname or burl}：发现笔记 {total_notes} 篇",
                        total_notes=total_notes,
                        counts={"bloggers": len(bloggers), "notes": len(notes), "comments": len(comments), "failed": len(failed)},
                    )
                    for nidx, nurl in enumerate(note_links, start=1):
                        note_base = 40 if total_notes else 70
                        note_span = 35
                        note_pct = min(note_base + int((nidx - 1) / max(total_notes, 1) * note_span), 75)
                        service.update_progress(
                            stage="爬取笔记",
                            message=f"正在处理第 {nidx}/{total_notes} 篇笔记",
                            current_note_index=nidx,
                            current_note=str(nurl),
                            crawl_percent=note_pct,
                            counts={"bloggers": len(bloggers), "notes": len(notes), "comments": len(comments), "failed": len(failed)},
                        )
                        rn = scraper.scrape_note(str(nurl))
                        if rn.ok and rn.data:
                            nd = rn.data
                            nd["blogger_id"] = nd.get("blogger_id") or bid
                            nd["blogger_name"] = nd.get("blogger_name") or bname
                            nd["blogger_url"] = nd.get("blogger_url") or burl
                            notes.append(nd)
                            if req.include_comments:
                                service.update_progress(
                                    stage="爬取评论",
                                    message=f"正在抓取第 {nidx}/{total_notes} 篇笔记评论",
                                    current_note_index=nidx,
                                    current_note=str(nurl),
                                    crawl_percent=min(note_pct + 2, 82),
                                    counts={"bloggers": len(bloggers), "notes": len(notes), "comments": len(comments), "failed": len(failed)},
                                )
                                rc = scraper.scrape_comments(str(nurl), max_comments=req.max_comments_per_note)
                                if rc.ok and rc.data:
                                    for c in (rc.data.get("comments") or []):
                                        c["note_id"] = nd.get("note_id", "")
                                        c["note_url"] = nd.get("note_url", nurl)
                                        c["blogger_id"] = bid
                                        c["blogger_name"] = bname
                                        uid = str(c.get("user_id", "") or "")
                                        uname = str(c.get("user_name", "") or "")
                                        is_self = bool((uid and bid and uid == bid) or (_norm_name(uname) and _norm_name(uname) == _norm_name(bname)))
                                        c["is_blogger_self_guess"] = "是" if is_self else "否"
                                        comments.append(c)
                                else:
                                    fail_msg = f"评论失败: {rc.message}"
                                    failed.append({"blogger_url": burl, "note_url": str(nurl), "message": fail_msg})
                                    if _looks_like_xhs_risk(fail_msg) and req.risk_cooldown_sec > 0:
                                        service.update_progress(
                                            stage="风控冷却",
                                            message=f"检测到访问频繁/验证信号，冷却 {req.risk_cooldown_sec} 秒后继续顺延",
                                            counts={"bloggers": len(bloggers), "notes": len(notes), "comments": len(comments), "failed": len(failed)},
                                        )
                                        time.sleep(req.risk_cooldown_sec)
                        else:
                            fail_msg = f"笔记失败: {rn.message}"
                            failed.append({"blogger_url": burl, "note_url": str(nurl), "message": fail_msg})
                            if _looks_like_xhs_risk(fail_msg) and req.risk_cooldown_sec > 0:
                                service.update_progress(
                                    stage="风控冷却",
                                    message=f"检测到访问频繁/验证信号，冷却 {req.risk_cooldown_sec} 秒后继续顺延",
                                    counts={"bloggers": len(bloggers), "notes": len(notes), "comments": len(comments), "failed": len(failed)},
                                )
                                time.sleep(req.risk_cooldown_sec)
                else:
                    failed.append({"blogger_url": burl, "message": rb.message})
                    service.update_progress(
                        stage="博主失败",
                        message=f"博主抓取失败：{rb.message}",
                        counts={"bloggers": len(bloggers), "notes": len(notes), "comments": len(comments), "failed": len(failed)},
                    )
                    if _looks_like_xhs_risk(rb.message) and req.risk_cooldown_sec > 0:
                        service.update_progress(
                            stage="风控冷却",
                            message=f"检测到访问频繁/验证信号，冷却 {req.risk_cooldown_sec} 秒后继续顺延",
                            counts={"bloggers": len(bloggers), "notes": len(notes), "comments": len(comments), "failed": len(failed)},
                        )
                        time.sleep(req.risk_cooldown_sec)
                    if not req.skip_failed:
                        service.finish_progress(False, rb.message)
                        raise HTTPException(status_code=400, detail=rb.message)
            service.update_progress(
                phase="analysis",
                stage="分析整理",
                message="爬取完成，正在标准化博主/笔记/评论导出表",
                crawl_percent=90,
                analysis_percent=35,
                counts={"bloggers": len(bloggers), "notes": len(notes), "comments": len(comments), "failed": len(failed)},
            )
        except HTTPException:
            raise
        except Exception as e:
            service.finish_progress(False, f"执行失败：{e}")
            raise HTTPException(status_code=500, detail=f"执行失败：{e}")

    payload = {
        "bloggers": bloggers,
        "notes": notes,
        "comments": comments,
        "failed": failed,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "params": req.model_dump(),
    }
    service.last_simple_result = payload
    service.update_progress(
        phase="analysis",
        stage="生成导出表",
        message="正在生成可导出的 Excel 表结构",
        crawl_percent=100,
        analysis_percent=75,
        counts={"bloggers": len(bloggers), "notes": len(notes), "comments": len(comments), "failed": len(failed)},
    )
    rows = _simple_export_rows(payload)
    service.finish_progress(True, f"完成：博主 {len(bloggers)} 位，笔记 {len(notes)} 篇，评论 {len(comments)} 条，失败 {len(failed)} 条")
    return {
        "ok": True,
        "message": f"完成：博主 {len(bloggers)} 位，笔记 {len(notes)} 篇，评论 {len(comments)} 条，失败 {len(failed)} 条",
        "data": {
            "generated_at": payload["generated_at"],
            "counts": {k: len(v) for k, v in rows.items()},
            "preview": {
                "bloggers": rows["bloggers"][:5],
                "notes": rows["notes"][:5],
                "comments": rows["comments"][:5],
                "failed": rows["failed"][:5],
            },
        },
    }


@app.get("/api/v1/simple/export.xlsx")
def simple_export_excel(table: str = "all"):
    payload = service.last_simple_result
    if not payload:
        raise HTTPException(status_code=404, detail="暂无可导出的爬取结果，请先点击开始爬取")
    rows = _simple_export_rows(payload)
    table = str(table or "all").lower()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if table == "bloggers":
        excel = to_excel_bytes(rows["bloggers"], sheet_name="bloggers", columns=list(BLOGGER_EXPORT_FIELD_MAP.values()))
    elif table == "notes":
        excel = to_excel_bytes(rows["notes"], sheet_name="notes", columns=[NOTE_EXPORT_FIELD_MAP[f] for f in NOTE_EXPORT_DEFAULT_FIELDS])
    elif table == "comments":
        excel = to_excel_bytes(rows["comments"], sheet_name="comments", columns=list(COMMENT_EXPORT_FIELD_MAP.values()))
    elif table == "failed":
        excel = to_excel_bytes(rows["failed"], sheet_name="failed")
    elif table == "all":
        excel = _xlsx_multi(
            {
                "博主表": (rows["bloggers"], list(BLOGGER_EXPORT_FIELD_MAP.values())),
                "笔记表": (rows["notes"], [NOTE_EXPORT_FIELD_MAP[f] for f in NOTE_EXPORT_DEFAULT_FIELDS]),
                "评论表": (rows["comments"], list(COMMENT_EXPORT_FIELD_MAP.values())),
                "失败记录": (rows["failed"], None),
            }
        )
    else:
        raise HTTPException(status_code=400, detail="table 只能是 all/bloggers/notes/comments/failed")
    return StreamingResponse(
        io.BytesIO(excel),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=_attachment_headers(f"xhs_{table}_{ts}.xlsx"),
    )


@app.post("/api/v1/auth/login/check")
def login_check(req: LoginCheckReq) -> dict[str, Any]:
    res = service.login_check_visible(req.timeout_sec)
    return {"ok": res.ok, "message": res.message, "data": res.data}


@app.post("/api/v1/auth/login/open")
def login_open() -> dict[str, Any]:
    """Start persistent browser and navigate to the QR login page (non-blocking)."""
    def _do():
        with service.lock:
            scraper = service.get_persistent_scraper()
            scraper.start()
            ctx = scraper.context
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            try:
                page.goto("https://www.xiaohongshu.com/explore", timeout=30000, wait_until="domcontentloaded")
            except Exception:
                pass
            try:
                page.goto("https://www.xiaohongshu.com/login", timeout=30000, wait_until="domcontentloaded")
            except Exception:
                pass
            try:
                page.wait_for_timeout(1500)
            except Exception:
                pass
        return {"ok": True, "message": "已打开登录页，请等待二维码加载"}
    try:
        return service.run_pw(_do)
    except Exception as e:
        return {"ok": False, "message": f"打开登录页失败: {e}"}


@app.get("/api/v1/auth/login/qr")
def login_qr():
    """Return a PNG screenshot of the current persistent browser page (the login page)."""
    from fastapi.responses import Response
    def _do():
        scraper = service.get_persistent_scraper()
        if scraper.context is None:
            scraper.start()
        ctx = scraper.context
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        return page.screenshot(type="png", full_page=False)
    try:
        png = service.run_pw(_do)
        return Response(content=png, media_type="image/png", headers={"Cache-Control": "no-store"})
    except Exception as e:
        return Response(content=str(e).encode("utf-8"), media_type="text/plain", status_code=500)


class CookieImportReq(BaseModel):
    cookies: Any
    target_url: str = "https://www.xiaohongshu.com/explore"


def _normalize_cookies(raw: Any) -> list[dict[str, Any]]:
    """Accept Playwright/EditThisCookie array, JSON string, or 'name=val; ...' header."""
    import json as _json
    items: list[dict[str, Any]] = []
    data = raw
    if isinstance(data, str):
        s = data.strip()
        if s.startswith("[") or s.startswith("{"):
            data = _json.loads(s)
        else:
            # Cookie header: name=val; name2=val2
            for part in s.split(";"):
                part = part.strip()
                if not part or "=" not in part:
                    continue
                name, _, value = part.partition("=")
                items.append({
                    "name": name.strip(),
                    "value": value.strip(),
                    "domain": ".xiaohongshu.com",
                    "path": "/",
                })
            return items
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        raise ValueError("cookies 必须是数组、对象或 'name=val; ...' 字符串")
    for c in data:
        if not isinstance(c, dict):
            continue
        name = c.get("name")
        value = c.get("value")
        if not name or value is None:
            continue
        domain = c.get("domain") or ".xiaohongshu.com"
        if not domain.startswith(".") and "xiaohongshu.com" in domain and not domain.startswith("www"):
            pass
        item: dict[str, Any] = {
            "name": str(name),
            "value": str(value),
            "domain": str(domain),
            "path": str(c.get("path") or "/"),
        }
        # Optional fields Playwright accepts
        if "expires" in c or "expirationDate" in c:
            try:
                exp = c.get("expires", c.get("expirationDate"))
                item["expires"] = float(exp)
            except Exception:
                pass
        if "httpOnly" in c:
            item["httpOnly"] = bool(c["httpOnly"])
        if "secure" in c:
            item["secure"] = bool(c["secure"])
        ss = c.get("sameSite")
        if isinstance(ss, str):
            m = ss.lower()
            if m in ("no_restriction", "none", "unspecified"):
                item["sameSite"] = "None"
            elif m == "lax":
                item["sameSite"] = "Lax"
            elif m == "strict":
                item["sameSite"] = "Strict"
        items.append(item)
    return items


@app.post("/api/v1/auth/cookies/import")
def cookies_import(req: CookieImportReq) -> dict[str, Any]:
    """Inject user-provided cookies into the persistent browser, then verify login."""
    try:
        normalized = _normalize_cookies(req.cookies)
    except Exception as e:
        return {"ok": False, "message": f"cookie 解析失败：{e}"}
    if not normalized:
        return {"ok": False, "message": "未解析到任何 cookie"}

    def _do():
        with service.lock:
            scraper = service.get_persistent_scraper()
            scraper.start()
            ctx = scraper.context
            try:
                ctx.add_cookies(normalized)
            except Exception as e:
                return {"ok": False, "message": f"注入 cookie 失败：{e}", "data": None}
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            try:
                page.goto(req.target_url, timeout=30000, wait_until="domcontentloaded")
                page.wait_for_timeout(1500)
            except Exception:
                pass
            res = scraper.login_debug_snapshot()
            return {
                "ok": bool(res.ok and (res.data or {}).get("logged_in")),
                "message": res.message,
                "data": res.data,
                "injected": len(normalized),
            }
    try:
        return service.run_pw(_do)
    except Exception as e:
        return {"ok": False, "message": f"cookie 登录失败：{e}"}


class SpyReq(BaseModel):
    url: str
    wait_ms: int = 8000
    pattern: str = "/api/sns/"


@app.post("/api/v1/raw/spy")
def raw_spy(req: SpyReq) -> dict[str, Any]:
    """Navigate the page and capture all matching outgoing API requests + their headers."""
    def _do():
        with service.lock:
            scraper = service.get_persistent_scraper()
            scraper.start()
            ctx = scraper.context
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            captured: list[dict[str, Any]] = []
            def _on_request(request):
                try:
                    if req.pattern in request.url:
                        captured.append({
                            "method": request.method,
                            "url": request.url,
                            "headers": dict(request.headers),
                        })
                except Exception:
                    pass
            page.on("request", _on_request)
            try:
                page.goto(req.url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(req.wait_ms)
                # try to scroll to trigger lazy loaders
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(2000)
                except Exception:
                    pass
            finally:
                try:
                    page.remove_listener("request", _on_request)
                except Exception:
                    pass
            return {"ok": True, "page_url": page.url, "count": len(captured), "requests": captured}
    return service.run_pw(_do)


class UserPostedAllReq(BaseModel):
    user_url_or_id: str
    xsec_token: str | None = None
    xsec_source: str = "pc_search"
    max_pages: int = 50
    save: bool = True


@app.post("/api/v1/raw/user-posted-all")
def raw_user_posted_all(req: UserPostedAllReq) -> dict[str, Any]:
    """Open a profile page, scroll to bottom repeatedly, capture every user_posted XHR,
    and aggregate notes until has_more is false (or max_pages reached)."""
    from urllib.parse import urlparse, parse_qs
    s = req.user_url_or_id.strip()
    user_id = s
    xsec_token = req.xsec_token
    xsec_source = req.xsec_source
    if s.startswith("http"):
        try:
            u = urlparse(s)
            parts = [p for p in u.path.split("/") if p]
            if "profile" in parts:
                user_id = parts[parts.index("profile") + 1]
            elif parts:
                user_id = parts[-1]
            q = parse_qs(u.query)
            if not xsec_token and "xsec_token" in q: xsec_token = q["xsec_token"][0]
            if "xsec_source" in q: xsec_source = q["xsec_source"][0]
        except Exception as e:
            return {"ok": False, "error": f"解析 URL 失败：{e}"}

    profile_url = f"https://www.xiaohongshu.com/user/profile/{user_id}"
    qparts = []
    if xsec_token: qparts.append(f"xsec_token={xsec_token}")
    qparts.append(f"xsec_source={xsec_source}")
    profile_url += "?" + "&".join(qparts)

    def _do():
        import json as _json
        with service.lock:
            scraper = service.get_persistent_scraper()
            scraper.start()
            ctx = scraper.context
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            pages_seen: list[dict[str, Any]] = []
            cursors_seen: set[str] = set()
            def _on_response(response):
                try:
                    if "/api/sns/web/v1/user_posted" in response.url and response.request.method == "GET":
                        body = ""
                        try: body = response.text()
                        except Exception: return
                        try: j = _json.loads(body)
                        except Exception: return
                        cur = ((j or {}).get("data") or {}).get("cursor", "")
                        # avoid double-counting same response
                        sig = f"{response.url}|{cur}|{len((j.get('data') or {}).get('notes') or [])}"
                        if sig in cursors_seen: return
                        cursors_seen.add(sig)
                        pages_seen.append(j)
                except Exception:
                    pass
            page.on("response", _on_response)
            try:
                page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
                # wait for first batch
                for _ in range(20):
                    if pages_seen: break
                    page.wait_for_timeout(500)
                last_count = -1
                stable_rounds = 0
                for round_i in range(req.max_pages):
                    # check has_more on the most recent batch
                    if pages_seen:
                        last = pages_seen[-1]
                        has_more = ((last or {}).get("data") or {}).get("has_more", True)
                        if not has_more:
                            break
                    try:
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    except Exception:
                        pass
                    # wait for another batch to arrive
                    waited = 0
                    cur_count = len(pages_seen)
                    while waited < 6000:
                        page.wait_for_timeout(500); waited += 500
                        if len(pages_seen) > cur_count: break
                    # if no progress for 2 rounds in a row, stop
                    if len(pages_seen) == last_count:
                        stable_rounds += 1
                        if stable_rounds >= 2: break
                    else:
                        stable_rounds = 0
                        last_count = len(pages_seen)
            finally:
                try: page.remove_listener("response", _on_response)
                except Exception: pass

            if not pages_seen:
                return {"ok": False, "error": "未捕获到任何 user_posted 响应", "page_url": page.url}
            all_notes: list[dict[str, Any]] = []
            seen_ids: set[str] = set()
            for j in pages_seen:
                for n in (((j or {}).get("data") or {}).get("notes") or []):
                    nid = n.get("note_id") or n.get("id")
                    if nid and nid in seen_ids: continue
                    if nid: seen_ids.add(nid)
                    all_notes.append(n)
            last = pages_seen[-1]
            result = {
                "ok": True,
                "user_id": user_id,
                "pages": len(pages_seen),
                "total_notes": len(all_notes),
                "has_more": ((last or {}).get("data") or {}).get("has_more", False),
                "last_cursor": ((last or {}).get("data") or {}).get("cursor", ""),
                "notes": all_notes,
                "page_url": page.url,
            }
            if req.save:
                from pathlib import Path as _P
                from datetime import datetime as _dt
                outdir = _P(__file__).resolve().parent / "outputs"
                outdir.mkdir(parents=True, exist_ok=True)
                ts = _dt.now().strftime("%Y%m%d_%H%M%S")
                fp = outdir / f"user_posted_{user_id}_{ts}.json"
                fp.write_text(_json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
                result["saved_to"] = str(fp)
            return result
    return service.run_pw(_do)


class RawEvalReq(BaseModel):
    code: str


@app.post("/api/v1/raw/eval")
def raw_eval(req: RawEvalReq) -> dict[str, Any]:
    def _do():
        with service.lock:
            scraper = service.get_persistent_scraper()
            scraper.start()
            ctx = scraper.context
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            try:
                if "xiaohongshu.com" not in (page.url or "") or "website-login/error" in (page.url or ""):
                    page.goto("https://www.xiaohongshu.com/explore", wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(1500)
            except Exception:
                pass
            try:
                return {"ok": True, "result": page.evaluate(req.code), "page_url": page.url}
            except Exception as e:
                return {"ok": False, "error": str(e), "page_url": page.url}
    return service.run_pw(_do)


class RawFetchReq(BaseModel):
    url: str
    method: str = "GET"
    body: str | None = None
    headers: dict[str, str] | None = None


@app.post("/api/v1/raw/fetch")
def raw_fetch(req: RawFetchReq) -> dict[str, Any]:
    """Run fetch() inside the persistent xiaohongshu.com page so its JS auto-signs (x-s/x-t)."""
    def _do():
        with service.lock:
            scraper = service.get_persistent_scraper()
            scraper.start()
            ctx = scraper.context
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            try:
                cur = page.url or ""
                if "xiaohongshu.com" not in cur or "website-login/error" in cur:
                    page.goto("https://www.xiaohongshu.com/explore", wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(1200)
            except Exception:
                pass
            result = page.evaluate(
                """
                ({url, method, body, headers}) => new Promise((resolve) => {
                  try {
                    const m = (method || 'GET').toUpperCase();
                    // Compute path+query relative to edith host so the sign matches
                    let signPath = url;
                    try {
                      const u = new URL(url, location.origin);
                      signPath = u.pathname + (u.search || '');
                    } catch(_) {}
                    let payload;
                    if (m === 'GET') payload = undefined;
                    else { try { payload = body ? JSON.parse(body) : {}; } catch(_) { payload = body || ''; } }

                    let sign = null, signErr = null;
                    try {
                      if (typeof window._webmsxyw === 'function') {
                        sign = window._webmsxyw(signPath, payload);
                      } else { signErr = 'no _webmsxyw'; }
                    } catch (e) { signErr = String(e); }

                    const xhr = new XMLHttpRequest();
                    xhr.open(m, url, true);
                    xhr.withCredentials = true;
                    if (m !== 'GET' && body && !(headers||{})['Content-Type']) {
                      xhr.setRequestHeader('Content-Type', 'application/json;charset=UTF-8');
                    }
                    if (sign) {
                      try { xhr.setRequestHeader('X-s', sign['X-s'] || sign.xs || ''); } catch(_) {}
                      try { xhr.setRequestHeader('X-t', String(sign['X-t'] || sign.xt || '')); } catch(_) {}
                      if (sign['X-S-Common']) { try { xhr.setRequestHeader('X-S-Common', sign['X-S-Common']); } catch(_) {} }
                      if (sign['X-B3-Traceid']) { try { xhr.setRequestHeader('X-B3-Traceid', sign['X-B3-Traceid']); } catch(_) {} }
                    }
                    Object.entries(headers || {}).forEach(([k, v]) => {
                      try { xhr.setRequestHeader(k, v); } catch(_) {}
                    });
                    xhr.onload = () => {
                      const text = xhr.responseText || '';
                      let json = null; try { json = text ? JSON.parse(text) : null; } catch(_) {}
                      resolve({ status: xhr.status, json, text: json ? null : text, error: null, page_url: location.href, sign_err: signErr, signed: !!sign });
                    };
                    xhr.onerror = () => resolve({ status: xhr.status || 0, json: null, text: xhr.responseText || '', error: 'XHR network error', page_url: location.href, sign_err: signErr });
                    xhr.ontimeout = () => resolve({ status: 0, json: null, text: '', error: 'XHR timeout', page_url: location.href, sign_err: signErr });
                    xhr.timeout = 25000;
                    xhr.send(m === 'GET' ? null : (body || null));
                  } catch (e) {
                    resolve({ status: 0, json: null, text: '', error: String(e), page_url: location.href });
                  }
                })
                """,
                {"url": req.url, "method": req.method, "body": req.body, "headers": req.headers or {}},
            )
            return {"ok": bool(result.get("status") and 200 <= result["status"] < 400 and not result.get("error")), **result}
    try:
        return service.run_pw(_do)
    except Exception as e:
        return {"ok": False, "error": f"raw_fetch 失败：{e}"}


class UserPostedReq(BaseModel):
    user_url_or_id: str
    cursor: str = ""
    num: int = 30
    xsec_token: str | None = None
    xsec_source: str = "pc_search"
    image_formats: str = "jpg,webp,avif"


@app.post("/api/v1/raw/user-posted")
def raw_user_posted(req: UserPostedReq) -> dict[str, Any]:
    """Navigate to a user's profile and capture the page's own user_posted XHR response.

    This works around xiaohongshu's full signing scheme by letting the page itself
    issue the request with all 6 sign headers (x-s, x-t, x-s-common, x-b3-traceid,
    x-rap-param, x-xray-traceid) and just observing the result.
    """
    from urllib.parse import urlparse, parse_qs
    s = req.user_url_or_id.strip()
    user_id = s
    xsec_token = req.xsec_token
    xsec_source = req.xsec_source
    if s.startswith("http"):
        try:
            u = urlparse(s)
            parts = [p for p in u.path.split("/") if p]
            if "profile" in parts:
                user_id = parts[parts.index("profile") + 1]
            elif parts:
                user_id = parts[-1]
            q = parse_qs(u.query)
            if not xsec_token and "xsec_token" in q:
                xsec_token = q["xsec_token"][0]
            if "xsec_source" in q:
                xsec_source = q["xsec_source"][0]
        except Exception as e:
            return {"ok": False, "error": f"解析 URL 失败：{e}"}

    profile_url = f"https://www.xiaohongshu.com/user/profile/{user_id}"
    qparts = []
    if xsec_token:
        qparts.append(f"xsec_token={xsec_token}")
    qparts.append(f"xsec_source={xsec_source}")
    profile_url += "?" + "&".join(qparts)

    def _do():
        with service.lock:
            scraper = service.get_persistent_scraper()
            scraper.start()
            ctx = scraper.context
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            captured: dict[str, Any] = {"resp": None, "all": []}
            def _on_response(response):
                try:
                    if "/api/sns/web/v1/user_posted" in response.url and response.request.method == "GET":
                        body_text = ""
                        try: body_text = response.text()
                        except Exception: body_text = ""
                        captured["all"].append({"url": response.url, "status": response.status, "body": body_text[:80000]})
                        if captured["resp"] is None:
                            captured["resp"] = captured["all"][-1]
                except Exception:
                    pass
            page.on("response", _on_response)
            try:
                page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
                # wait for the API to fire and resolve
                for _ in range(20):
                    if captured["resp"] is not None:
                        break
                    page.wait_for_timeout(500)
                # also scroll a bit in case of lazy load
                if captured["resp"] is None:
                    try:
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        page.wait_for_timeout(2000)
                    except Exception:
                        pass
            finally:
                try: page.remove_listener("response", _on_response)
                except Exception: pass

            r = captured["resp"]
            if r is None:
                return {"ok": False, "error": "页面未触发 user_posted 请求", "page_url": page.url}
            j = None
            try:
                import json as _json
                j = _json.loads(r["body"])
            except Exception:
                pass
            return {
                "ok": bool(j and j.get("success")),
                "status": r["status"],
                "json": j,
                "text": None if j else r["body"],
                "page_url": page.url,
                "captured_count": len(captured["all"]),
            }
    return service.run_pw(_do)


@app.post("/api/v1/auth/cookies/clear")
def cookies_clear() -> dict[str, Any]:
    def _do():
        with service.lock:
            scraper = service.get_persistent_scraper()
            scraper.start()
            try:
                scraper.context.clear_cookies()
                return {"ok": True, "message": "已清空当前浏览器 cookie"}
            except Exception as e:
                return {"ok": False, "message": f"清空 cookie 失败：{e}"}
    return service.run_pw(_do)


@app.get("/api/v1/auth/login/debug")
def login_debug() -> dict[str, Any]:
    def _do():
        with service.lock:
            scraper = service.login_scraper or XHSScraper(profile_dir=service.profile_dir, headless=service.headless)
            service.login_scraper = scraper
            return scraper.login_debug_snapshot()
    res = service.run_pw(_do)
    return {"ok": res.ok, "message": res.message, "data": res.data}


@app.post("/api/v1/note/detail")
def note_detail(req: MediaByNoteReq) -> dict[str, Any]:
    res = service.run("scrape_note", req.note_url)
    if not res.ok:
        raise HTTPException(status_code=400, detail=res.message)
    return {"ok": True, "message": res.message, "data": res.data}


@app.post("/api/v1/comment/collect")
def comment_collect(req: NoteLinksReq) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []
    for u in req.note_links:
        r = service.run("scrape_comments", u, req.max_comments_per_note)
        if r.ok and r.data:
            rows.extend(r.data.get("comments", []))
        else:
            failed.append({"note_url": u, "message": r.message})
            if not req.skip_failed:
                raise HTTPException(status_code=400, detail=r.message)
    return {
        "ok": True,
        "message": f"完成，评论 {len(rows)} 条，失败 {len(failed)} 条",
        "data": {"comments": rows, "failed": failed, "total_note_links": len(req.note_links)},
    }


@app.post("/api/v1/comment/export.xlsx")
def comment_export_excel(req: NoteLinksReq):
    raw_rows: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []
    for u in req.note_links:
        r = service.run("scrape_comments", u, req.max_comments_per_note)
        if r.ok and r.data:
            raw_rows.extend(r.data.get("comments", []))
        else:
            failed.append({"note_url": u, "message": r.message})
            if not req.skip_failed:
                raise HTTPException(status_code=400, detail=r.message)

    rows = build_comment_export_rows(raw_rows)
    if failed:
        for x in failed:
            row = {label: "" for label in COMMENT_EXPORT_FIELD_MAP.values()}
            row["笔记链接"] = x["note_url"]
            row["评论内容"] = f"[FAILED] {x['message']}"
            rows.append(row)

    excel = to_excel_bytes(rows, sheet_name="comments", columns=list(COMMENT_EXPORT_FIELD_MAP.values()))
    filename = f"xhs_comments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        io.BytesIO(excel),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=_attachment_headers(filename),
    )


@app.post("/api/v1/note/collect/by-links")
def note_collect_by_links(req: NoteLinksReq) -> dict[str, Any]:
    notes: list[dict[str, Any]] = []
    comments: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []

    for u in req.note_links:
        rn = service.run("scrape_note", u)
        if rn.ok and rn.data:
            notes.append(rn.data)
            if req.include_comments:
                rc = service.run("scrape_comments", u, req.max_comments_per_note)
                if rc.ok and rc.data:
                    comments.extend(rc.data.get("comments", []))
                else:
                    failed.append({"note_url": u, "message": f"评论失败: {rc.message}"})
                    if not req.skip_failed:
                        raise HTTPException(status_code=400, detail=rc.message)
        else:
            failed.append({"note_url": u, "message": f"笔记失败: {rn.message}"})
            if not req.skip_failed:
                raise HTTPException(status_code=400, detail=rn.message)

    return {
        "ok": True,
        "message": f"完成，笔记 {len(notes)} 篇，评论 {len(comments)} 条，失败 {len(failed)} 条",
        "data": {"notes": notes, "comments": comments, "failed": failed},
    }


@app.post("/api/v1/note/export/by-links.xlsx")
def note_export_by_links_excel(req: NoteLinksReq):
    raw_rows: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []

    for u in req.note_links:
        r = service.run("scrape_note", u)
        if r.ok and r.data:
            raw_rows.append(r.data)
        else:
            failed.append({"note_url": u, "message": r.message})
            if not req.skip_failed:
                raise HTTPException(status_code=400, detail=r.message)

    rows = build_note_export_rows(
        raw_rows,
        export_fields=req.export_fields,
        tag_categories=req.tag_categories,
    )
    selected_fields = req.export_fields or NOTE_EXPORT_DEFAULT_FIELDS
    template_cols = [NOTE_EXPORT_FIELD_MAP[f] for f in selected_fields if f in NOTE_EXPORT_FIELD_MAP]

    if failed:
        for x in failed:
            row = {c: "" for c in template_cols}
            if "笔记链接" in row:
                row["笔记链接"] = x["note_url"]
            if "笔记标题" in row:
                row["笔记标题"] = f"[FAILED] {x['message']}"
            else:
                row["错误信息"] = f"{x['note_url']} | {x['message']}"
            rows.append(row)

    excel = to_excel_bytes(rows, sheet_name="notes", columns=template_cols)
    filename = f"xhs_notes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        io.BytesIO(excel),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=_attachment_headers(filename),
    )


@app.post("/api/v1/blogger/detail")
def blogger_detail(req: BloggerLinksReq) -> dict[str, Any]:
    # for single detail use first link
    u = req.blogger_links[0]
    r = service.run("scrape_blogger", u, req.max_notes_per_blogger)
    if not r.ok:
        raise HTTPException(status_code=400, detail=r.message)
    return {"ok": True, "message": r.message, "data": r.data}


@app.post("/api/v1/blogger/collect/by-links")
def blogger_collect_by_links(req: BloggerLinksReq) -> dict[str, Any]:
    bloggers: list[dict[str, Any]] = []
    notes: list[dict[str, Any]] = []
    comments: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []

    for u in req.blogger_links:
        rb = service.run("scrape_blogger", u, req.max_notes_per_blogger)
        if rb.ok and rb.data:
            bloggers.append(rb.data)
            if req.include_notes:
                note_links = rb.data.get("note_links") or []
                for nurl in note_links:
                    rn = service.run("scrape_note", nurl)
                    if rn.ok and rn.data:
                        notes.append(rn.data)
                        if req.include_comments:
                            rc = service.run("scrape_comments", nurl, req.max_comments_per_note)
                            if rc.ok and rc.data:
                                comments.extend(rc.data.get("comments", []))
                    else:
                        failed.append({"blogger_url": u, "note_url": nurl, "message": rn.message})
        else:
            failed.append({"blogger_url": u, "message": rb.message})
            if not req.skip_failed:
                raise HTTPException(status_code=400, detail=rb.message)

    return {
        "ok": True,
        "message": f"完成，博主 {len(bloggers)} 位，笔记 {len(notes)} 篇，评论 {len(comments)} 条，失败 {len(failed)} 条",
        "data": {"bloggers": bloggers, "notes": notes, "comments": comments, "failed": failed},
    }


@app.post("/api/v1/blogger/export/by-links.xlsx")
def blogger_export_by_links_excel(req: BloggerLinksReq):
    raw_rows: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []

    for u in req.blogger_links:
        rb = service.run("scrape_blogger", u, req.max_notes_per_blogger)
        if rb.ok and rb.data:
            raw_rows.append(rb.data)
        else:
            failed.append({"blogger_url": u, "message": rb.message})
            if not req.skip_failed:
                raise HTTPException(status_code=400, detail=rb.message)

    rows = build_blogger_export_rows(raw_rows)
    if failed:
        for x in failed:
            row = {label: "" for label in BLOGGER_EXPORT_FIELD_MAP.values()}
            row["博主链接"] = x["blogger_url"]
            row["博主昵称"] = f"[FAILED] {x['message']}"
            rows.append(row)

    excel = to_excel_bytes(rows, sheet_name="bloggers", columns=list(BLOGGER_EXPORT_FIELD_MAP.values()))
    filename = f"xhs_bloggers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        io.BytesIO(excel),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=_attachment_headers(filename),
    )


@app.post("/api/v1/simple/current-page-comments")
def simple_current_page_comments(max_comments: int = Query(default=200, ge=5, le=500)) -> dict[str, Any]:
    """Fallback: manually open a note in Chromium, then extract comments from the current page."""
    with service.lock:
        scraper = service.get_persistent_scraper()
        try:
            r = scraper.scrape_comments_from_current_page(max_comments=max_comments, scroll_rounds=80)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"当前页面抓取失败: {e}")
        if not r.ok:
            raise HTTPException(status_code=400, detail=r.message)
        raw_comments = (r.data or {}).get("comments") or []
        rows = build_comment_export_rows(raw_comments)
        payload = {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "bloggers": [],
            "notes": [],
            "comments": raw_comments,
            "failed": [],
        }
        service.last_simple_result = payload
        counts = {"bloggers": 0, "notes": 0, "comments": len(raw_comments), "failed": 0}
        image_rows = sum(1 for row in rows if str(row.get("评论图片链接", "") or "").strip())
        return {
            "ok": True,
            "message": f"{r.message}；其中评论图片记录 {image_rows} 条。已缓存，可直接导出评论表。",
            "data": {
                "page_url": (r.data or {}).get("page_url", ""),
                "counts": counts,
                "comment_image_rows": image_rows,
                "comments_preview": rows[:10],
            },
        }


@app.post("/api/v1/search/notes")
def search_notes(req: KeywordReq) -> dict[str, Any]:
    r = service.run("search_notes", req.keyword.strip(), req.limit)
    if not r.ok:
        raise HTTPException(status_code=400, detail=r.message)
    return {"ok": True, "message": r.message, "data": r.data}


@app.post("/api/v1/search/bloggers")
def search_bloggers(req: KeywordReq) -> dict[str, Any]:
    r = service.run("search_bloggers", req.keyword.strip(), req.limit)
    if not r.ok:
        raise HTTPException(status_code=400, detail=r.message)
    return {"ok": True, "message": r.message, "data": r.data}


@app.post("/api/v1/note/media/download/by-note.zip")
def media_download_by_note(req: MediaByNoteReq):
    rn = service.run("scrape_note", req.note_url)
    if not rn.ok or not rn.data:
        raise HTTPException(status_code=400, detail=rn.message)
    urls = (rn.data.get("image_urls") or []) + (rn.data.get("video_urls") or [])
    if not urls:
        raise HTTPException(status_code=404, detail="未提取到可下载媒体链接")

    z = download_urls_to_zip(urls)
    note_id = rn.data.get("note_id") or "unknown"
    filename = f"xhs_note_media_{note_id}.zip"
    return StreamingResponse(io.BytesIO(z), media_type="application/zip", headers=_attachment_headers(filename))


@app.post("/api/v1/media/download/by-urls.zip")
def media_download_by_urls(req: MediaByUrlsReq):
    z = download_urls_to_zip(req.urls)
    filename = f"xhs_media_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    return StreamingResponse(io.BytesIO(z), media_type="application/zip", headers=_attachment_headers(filename))
