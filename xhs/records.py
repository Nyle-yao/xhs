"""Lightweight registry for saved scrape outputs.

Stored as JSON at xhs/outputs/_records.json.
Each record:
{
  id: str (file basename without .json),
  kind: 'user_posts' | 'note' | 'comments',
  label: str (human-friendly Chinese name),
  nickname: str,
  user_id: str,
  note_id: str,
  count: int,            # notes or comments count
  captured_at: int,      # unix seconds
  file: str,             # filename in outputs/
  params: dict,          # original request params for re-fetch
}
"""
from __future__ import annotations
import json
import threading
import time
from pathlib import Path
from typing import Any

_LOCK = threading.Lock()


def _registry_path() -> Path:
    return Path(__file__).resolve().parent / "outputs" / "_records.json"


def _load() -> list[dict[str, Any]]:
    p = _registry_path()
    if not p.exists():
        return []
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []


def _save(items: list[dict[str, Any]]) -> None:
    p = _registry_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


def _short(s: str, n: int = 16) -> str:
    s = (s or "").strip().replace("\n", " ")
    return s if len(s) <= n else s[:n] + "…"


def _lookup_author_by_note_id(note_id: str) -> str:
    """Find the most recent author nickname for a note_id from existing records."""
    if not note_id:
        return ""
    for r in sorted(_load(), key=lambda x: x.get("captured_at", 0), reverse=True):
        if r.get("note_id") == note_id and r.get("nickname"):
            return r["nickname"]
        # also scan note records that wrap this id
        if r.get("kind") == "note" and r.get("note_id") == note_id:
            return r.get("nickname", "")
    # also try to read newest user_posts files containing this note
    outdir = _registry_path().parent
    for fp in sorted(outdir.glob("user_posted_*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
            for n in data.get("notes", []):
                if n.get("note_id") == note_id:
                    u = n.get("user") or {}
                    return u.get("nick_name") or u.get("nickname") or ""
        except Exception:
            continue
    return ""


def upsert_user_posts(file: Path, result: dict, params: dict) -> dict:
    notes = result.get("notes") or []
    user_id = result.get("user_id") or ""
    nickname = ""
    if notes:
        u = notes[0].get("user") or {}
        nickname = u.get("nick_name") or u.get("nickname") or ""
    label = f"{nickname or user_id[:8]}_全部笔记_{len(notes)}篇"
    rec = {
        "id": file.stem,
        "kind": "user_posts",
        "label": label,
        "nickname": nickname,
        "user_id": user_id,
        "note_id": "",
        "count": len(notes),
        "captured_at": int(time.time()),
        "file": file.name,
        "params": params,
    }
    return _upsert(rec)


def upsert_note(file: Path, result: dict, params: dict) -> dict:
    user = result.get("user") or {}
    nickname = user.get("nickname") or ""
    title = result.get("title") or ""
    label = f"{nickname or '?'}_笔记_{_short(title, 14)}"
    rec = {
        "id": file.stem,
        "kind": "note",
        "label": label,
        "nickname": nickname,
        "user_id": user.get("user_id", ""),
        "note_id": result.get("note_id", ""),
        "count": 1,
        "captured_at": int(time.time()),
        "file": file.name,
        "params": params,
    }
    return _upsert(rec)


def upsert_comments(file: Path, result: dict, params: dict) -> dict:
    note_id = result.get("note_id") or ""
    comments = result.get("comments") or []
    nickname = _lookup_author_by_note_id(note_id)
    label = f"{nickname or '?'}_评论_{note_id[:8]}_{len(comments)}条"
    rec = {
        "id": file.stem,
        "kind": "comments",
        "label": label,
        "nickname": nickname,
        "user_id": "",
        "note_id": note_id,
        "count": len(comments),
        "captured_at": int(time.time()),
        "file": file.name,
        "params": params,
    }
    return _upsert(rec)


def upsert_bundle(file: Path, result: dict, params: dict) -> dict:
    user = result.get("user") or {}
    nickname = user.get("nickname") or ""
    stats = result.get("stats") or {}
    n_notes = stats.get("notes", 0)
    n_cmts = stats.get("comments_total", 0)
    label = f"{nickname or '?'}_一键全量_{n_notes}篇_{n_cmts}评论"
    rec = {
        "id": file.stem,
        "kind": "bundle",
        "label": label,
        "nickname": nickname,
        "user_id": user.get("user_id", ""),
        "note_id": "",
        "count": n_notes,
        "captured_at": int(result.get("scrape_time") or time.time()),
        "file": file.name,
        "params": params,
        "extra": {"comments_total": n_cmts, "errors": stats.get("errors", 0)},
    }
    return _upsert(rec)


def _upsert(rec: dict) -> dict:
    with _LOCK:
        items = _load()
        items = [r for r in items if r.get("id") != rec["id"]]
        items.append(rec)
        _save(items)
    return rec


def list_records() -> list[dict]:
    items = _load()
    # also drop any that point to missing files
    outdir = _registry_path().parent
    items = [r for r in items if (outdir / r.get("file", "")).exists()]
    items.sort(key=lambda x: x.get("captured_at", 0), reverse=True)
    return items


def delete_record(rec_id: str, also_file: bool = False) -> bool:
    with _LOCK:
        items = _load()
        target = next((r for r in items if r.get("id") == rec_id), None)
        if not target:
            return False
        items = [r for r in items if r.get("id") != rec_id]
        _save(items)
        if also_file:
            fp = _registry_path().parent / target.get("file", "")
            try:
                if fp.exists():
                    fp.unlink()
            except Exception:
                pass
    return True
