"""One-click bundle: user-posted → note-detail × N → comments × N → single JSON.

Runs in a background thread. State is exposed via JOBS dict for polling.
"""
from __future__ import annotations
import json
import threading
import time
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

JOBS: dict[str, dict[str, Any]] = {}
_LOCK = threading.Lock()


def _new_job(user_url: str, max_notes: int, max_comments: int) -> str:
    jid = uuid.uuid4().hex[:12]
    with _LOCK:
        JOBS[jid] = {
            "id": jid,
            "user_url": user_url,
            "params": {"max_notes": max_notes, "max_comments": max_comments},
            "status": "queued",
            "step": "等待中",
            "started_at": int(time.time()),
            "ended_at": None,
            "progress": {"notes_done": 0, "notes_total": 0, "comments_done": 0},
            "log": [],
            "error": None,
            "saved_to": None,
            "file": None,
        }
    return jid


def _log(jid: str, msg: str) -> None:
    with _LOCK:
        j = JOBS.get(jid)
        if not j:
            return
        ts = time.strftime("%H:%M:%S")
        j["log"].append(f"[{ts}] {msg}")
        if len(j["log"]) > 200:
            j["log"] = j["log"][-200:]


def _set(jid: str, **kw) -> None:
    with _LOCK:
        if jid in JOBS:
            JOBS[jid].update(kw)


def _set_progress(jid: str, **kw) -> None:
    with _LOCK:
        if jid in JOBS:
            JOBS[jid]["progress"].update(kw)


def get_job(jid: str) -> dict[str, Any] | None:
    with _LOCK:
        j = JOBS.get(jid)
        return None if j is None else dict(j)


def list_jobs() -> list[dict[str, Any]]:
    with _LOCK:
        return [dict(v) for v in JOBS.values()]


def _label(user_nick: str, n_notes: int, n_comments: int) -> str:
    nick = user_nick or "未知博主"
    return f"{nick}_一键全量_{n_notes}篇x{n_comments}条评论"


def run_bundle(
    jid: str,
    user_url: str,
    max_notes: int,
    max_comments: int,
    *,
    raw_user_posted_all: Callable,
    UserPostedAllReq,
    raw_note_detail: Callable,
    NoteDetailReq,
    raw_comments: Callable,
    CommentsReq,
    outdir: Path,
    upsert_bundle: Callable | None = None,
) -> None:
    """Worker: drives the 3 existing endpoints in sequence, aggregates, saves."""
    try:
        _set(jid, status="running", step="① 抓取笔记列表")
        _log(jid, f"开始一键抓取：max_notes={max_notes}, max_comments={max_comments}")

        # Step 1: user-posted-all (no individual save — we bundle at end)
        post_req = UserPostedAllReq(
            user_url_or_id=user_url,
            max_notes=max_notes,
            max_pages=200 if max_notes == 0 else max(5, (max_notes // 30 + 1) * 2),
            save=False,
        )
        post_res = raw_user_posted_all(post_req)
        if not post_res.get("ok"):
            raise RuntimeError(f"笔记列表抓取失败: {post_res.get('error', '未知错误')}")

        notes = post_res.get("notes") or []
        if max_notes and len(notes) > max_notes:
            notes = notes[:max_notes]
        _set_progress(jid, notes_total=len(notes))
        _log(jid, f"✅ 笔记列表完成，共 {len(notes)} 篇")

        # Resolve user info from the first note
        user_id = post_res.get("user_id", "")
        user_nick = ""
        user_avatar = ""
        if notes:
            u0 = notes[0].get("user") or {}
            user_nick = u0.get("nick_name") or u0.get("nickname") or ""
            user_avatar = u0.get("avatar", "")

        # Step 2 + 3 interleaved per note
        details: dict[str, dict] = {}
        comments_map: dict[str, dict] = {}
        per_note_errors: dict[str, str] = {}
        comments_done = 0

        for i, n in enumerate(notes, 1):
            nid = n.get("note_id") or n.get("id") or ""
            tok = n.get("xsec_token", "")
            title = n.get("display_title") or "(无标题)"
            short = (title[:18] + "…") if len(title) > 19 else title
            if not nid:
                continue

            _set(jid, step=f"② [{i}/{len(notes)}] 详情：{short}")
            try:
                d = raw_note_detail(NoteDetailReq(
                    note_id=nid, xsec_token=tok, xsec_source="pc_user", save=False
                ))
                if d.get("ok"):
                    details[nid] = d
                else:
                    per_note_errors[nid] = "详情:" + str(d.get("error", "?"))
                    _log(jid, f"⚠️ 详情失败 {nid[:8]}: {d.get('error','?')}")
            except Exception as e:
                per_note_errors[nid] = f"详情异常: {e}"
                _log(jid, f"❌ 详情异常 {nid[:8]}: {e}")

            if max_comments > 0:
                _set(jid, step=f"③ [{i}/{len(notes)}] 评论(目标{max_comments})：{short}")
                try:
                    c = raw_comments(CommentsReq(
                        note_id=nid, xsec_token=tok, xsec_source="pc_user",
                        max_comments=max_comments,
                        max_pages=min(50, max(3, (max_comments // 10 + 1) * 2)),
                        save=False,
                    ))
                    if c.get("ok"):
                        comments_map[nid] = {
                            "comments": c.get("comments") or [],
                            "pages": c.get("pages"),
                            "has_more": c.get("has_more"),
                        }
                        comments_done += len(c.get("comments") or [])
                    else:
                        per_note_errors[nid] = (per_note_errors.get(nid, "") +
                                                " | 评论:" + str(c.get("error", "?"))).strip(" |")
                        _log(jid, f"⚠️ 评论失败 {nid[:8]}: {c.get('error','?')}")
                except Exception as e:
                    per_note_errors[nid] = f"评论异常: {e}"
                    _log(jid, f"❌ 评论异常 {nid[:8]}: {e}")

            _set_progress(jid, notes_done=i, comments_done=comments_done)

        _set(jid, step="④ 保存文件")
        bundle = {
            "ok": True,
            "kind": "bundle",
            "user_id": user_id,
            "user": {"nickname": user_nick, "avatar": user_avatar, "user_id": user_id},
            "scrape_time": int(time.time()),
            "params": {
                "user_url_or_id": user_url,
                "max_notes": max_notes,
                "max_comments": max_comments,
            },
            "stats": {
                "notes": len(notes),
                "details_ok": len(details),
                "comments_ok": len(comments_map),
                "comments_total": comments_done,
                "errors": len(per_note_errors),
            },
            "notes": notes,
            "details": details,
            "comments": comments_map,
            "errors": per_note_errors,
        }
        outdir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_uid = (user_id or "user")[:24]
        fp = outdir / f"bundle_{safe_uid}_{ts}.json"
        fp.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")

        # Register in records
        if upsert_bundle:
            try:
                upsert_bundle(fp, bundle, JOBS[jid]["params"])
            except Exception as e:
                _log(jid, f"⚠️ 历史记录写入失败: {e}")

        _set(jid,
             status="done",
             step=f"✅ 完成：{len(notes)} 篇笔记 / {comments_done} 条评论",
             saved_to=str(fp),
             file=fp.name,
             ended_at=int(time.time()))
        _log(jid, f"🎉 完成 → {fp.name}")
    except Exception as e:
        _set(jid,
             status="failed",
             step=f"❌ 失败：{e}",
             error=str(e),
             ended_at=int(time.time()))
        _log(jid, "异常:\n" + traceback.format_exc())


def start(user_url: str, max_notes: int, max_comments: int, **deps) -> str:
    jid = _new_job(user_url, max_notes, max_comments)
    t = threading.Thread(
        target=run_bundle,
        args=(jid, user_url, max_notes, max_comments),
        kwargs=deps,
        daemon=True,
    )
    t.start()
    return jid
