"""Render saved JSON outputs as styled, self-contained HTML pages."""
from __future__ import annotations
import html
import json
import time
from pathlib import Path
from typing import Any


def _esc(s: Any) -> str:
    return html.escape("" if s is None else str(s), quote=True)


def _fmt_ts(ts: Any) -> str:
    try:
        ts = int(ts)
        if ts > 1e12:
            ts = ts // 1000
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
    except Exception:
        return _esc(ts)


def _pick_cover(note: dict) -> str:
    """Find the best cover image URL from a user_posted note."""
    cov = note.get("cover") or {}
    if isinstance(cov, dict):
        info = cov.get("info_list") or []
        for it in info:
            if it.get("image_scene") in ("WB_DFT", "WB_PRV") and it.get("url"):
                return it["url"]
        if cov.get("url"):
            return cov["url"]
    img_list = note.get("image_list") or note.get("imageList") or []
    if img_list:
        first = img_list[0]
        if isinstance(first, dict):
            return first.get("url_default") or first.get("urlDefault") or first.get("url") or ""
    return ""


def _img(url: str, **attrs) -> str:
    if not url:
        return ""
    extra = " ".join(f'{k}="{_esc(v)}"' for k, v in attrs.items())
    return (f'<img src="{_esc(url)}" loading="lazy" referrerpolicy="no-referrer" '
            f'onerror="this.style.opacity=0.2;this.alt=\'图片加载失败\'" {extra}/>')


_BASE_CSS = """
*{box-sizing:border-box}
body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif;background:#f5f5f7;color:#222;line-height:1.55}
.container{max-width:1180px;margin:0 auto;padding:18px}
.head{background:#fff;border-radius:12px;padding:16px 20px;margin-bottom:14px;box-shadow:0 2px 8px rgba(0,0,0,.04)}
.head h1{margin:0 0 6px 0;font-size:20px}
.head .meta{color:#666;font-size:13px}
.tag{display:inline-block;padding:2px 8px;background:#f0fdf4;color:#16a34a;border-radius:10px;font-size:12px;margin-right:6px}
.tag.warn{background:#fef3c7;color:#a16207}
.tag.info{background:#eff6ff;color:#1d4ed8}
a.back{color:#666;text-decoration:none;font-size:13px}
a.back:hover{color:#16a34a}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:14px}
.card{background:#fff;border-radius:10px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.06);transition:transform .15s, box-shadow .15s}
.card:hover{transform:translateY(-2px);box-shadow:0 4px 14px rgba(0,0,0,.08)}
.card .cov{width:100%;aspect-ratio:3/4;background:#eee;display:block;object-fit:cover}
.card .body{padding:10px 12px}
.card .title{font-size:13px;line-height:1.4;color:#222;margin:0 0 8px 0;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;min-height:2.8em}
.card .row{display:flex;justify-content:space-between;align-items:center;font-size:12px;color:#888}
.card .like{color:#ef4444;font-weight:600}
.card .typ{display:inline-block;padding:1px 6px;background:#f3f4f6;border-radius:8px;font-size:11px;color:#666}
.card .typ.video{background:#fef3c7;color:#a16207}
.note-detail{display:grid;grid-template-columns:minmax(0,1fr) 320px;gap:18px}
@media(max-width:780px){.note-detail{grid-template-columns:1fr}}
.note-imgs{background:#fff;border-radius:10px;padding:12px}
.note-imgs img,.note-imgs video{width:100%;display:block;margin-bottom:10px;border-radius:6px;background:#eee}
.note-side{background:#fff;border-radius:10px;padding:14px}
.user{display:flex;align-items:center;gap:10px;margin-bottom:10px}
.user img{width:40px;height:40px;border-radius:50%;background:#eee}
.user .nm{font-weight:600}
.user .uid{font-size:11px;color:#999}
.note-side h2{font-size:18px;margin:8px 0 6px 0}
.desc{white-space:pre-wrap;font-size:14px;color:#333;margin:8px 0;max-height:340px;overflow:auto}
.stats{display:flex;gap:14px;padding:8px 0;border-top:1px solid #f0f0f0;border-bottom:1px solid #f0f0f0;font-size:13px;color:#444}
.stats b{color:#ef4444;margin-left:3px}
.kv{font-size:12px;color:#666;margin-top:8px}
.kv div{padding:2px 0}
.tags{margin-top:8px}
.tags .t{display:inline-block;padding:2px 8px;background:#eff6ff;color:#1d4ed8;border-radius:10px;font-size:12px;margin:0 4px 4px 0}
.cmt-list{background:#fff;border-radius:10px;padding:6px 14px}
.cmt{display:flex;gap:10px;padding:12px 0;border-bottom:1px solid #f0f0f0}
.cmt:last-child{border:none}
.cmt img.av{width:36px;height:36px;border-radius:50%;background:#eee;flex-shrink:0}
.cmt .cb{flex:1;min-width:0}
.cmt .cb .nm{font-weight:600;font-size:13px}
.cmt .cb .ct{font-size:14px;margin:3px 0;white-space:pre-wrap;word-break:break-word}
.cmt .cb .mt{font-size:11px;color:#999;display:flex;gap:10px}
.cmt .cb .mt .lk{color:#ef4444}
.cmt .pics{display:flex;flex-wrap:wrap;gap:6px;margin-top:6px}
.cmt .pics img{width:80px;height:80px;object-fit:cover;border-radius:4px;background:#eee;cursor:pointer}
.sub{margin:6px 0 0 0;padding:6px 10px;background:#f9fafb;border-radius:6px;font-size:13px}
.sub .s{padding:4px 0;border-bottom:1px dashed #eee}
.sub .s:last-child{border:none}
.sub .nm{font-weight:600;color:#666;font-size:12px}
.lightbox{position:fixed;inset:0;background:rgba(0,0,0,.85);display:none;align-items:center;justify-content:center;z-index:9999;cursor:zoom-out}
.lightbox.show{display:flex}
.lightbox img{max-width:92vw;max-height:92vh;object-fit:contain}
"""

_LIGHTBOX = """
<div class="lightbox" id="lb" onclick="this.classList.remove('show')"><img id="lbi" src=""/></div>
<script>
document.addEventListener('click',e=>{
  const t=e.target;
  if(t.tagName==='IMG' && t.dataset.zoom!=='no' && !t.closest('.lightbox')){
    const lb=document.getElementById('lb'),lbi=document.getElementById('lbi');
    lbi.src=t.src; lb.classList.add('show');
  }
});
</script>
"""


def _wrap(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="zh-CN"><head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{_esc(title)}</title>
<style>{_BASE_CSS}</style>
</head><body>
<div class="container">{body}</div>
{_LIGHTBOX}
</body></html>"""


def render_user_posts(data: dict, name: str) -> str:
    notes = data.get("notes", [])
    user_id = data.get("user_id", "")
    total = data.get("total_notes", len(notes))
    pages = data.get("pages", "?")
    cards = []
    for i, n in enumerate(notes):
        cover = _pick_cover(n)
        title = n.get("display_title") or n.get("desc") or "(无标题)"
        likes = (n.get("interact_info") or {}).get("liked_count") or "0"
        typ = n.get("type") or "normal"
        nid = n.get("note_id") or ""
        tok = n.get("xsec_token") or ""
        ext_url = f"https://www.xiaohongshu.com/explore/{nid}?xsec_token={tok}&xsec_source=pc_user" if nid else "#"
        typ_cls = " video" if typ == "video" else ""
        cards.append(f"""
        <div class="card">
          <a href="{_esc(ext_url)}" target="_blank" rel="noreferrer">{_img(cover, **{'class':'cov','data-zoom':'no'})}</a>
          <div class="body">
            <p class="title" title="{_esc(title)}">{_esc(title)}</p>
            <div class="row">
              <span class="typ{typ_cls}">{_esc(typ)}</span>
              <span class="like">♡ {_esc(likes)}</span>
            </div>
          </div>
        </div>""")
    head = f"""<div class="head">
      <a href="/" class="back">← 返回仪表盘</a>
      <h1>📋 用户全部笔记</h1>
      <div class="meta">
        user_id: <code>{_esc(user_id)}</code> ·
        共 <b>{total}</b> 篇 · 翻页 {pages} ·
        <span class="tag">来源 {_esc(name)}</span>
      </div>
    </div>"""
    return _wrap(f"用户 {user_id} 的笔记", head + f'<div class="grid">{"".join(cards)}</div>')


def render_note(data: dict, name: str) -> str:
    note = data
    user = note.get("user") or {}
    interact = note.get("interact") or {}
    imgs = note.get("image_list") or []
    vids = note.get("video_urls") or []
    tags = note.get("tag_list") or []

    img_html = []
    for v in vids:
        if v:
            img_html.append(f'<video src="{_esc(v)}" controls preload="metadata"></video>')
            break
    for im in imgs:
        url = ""
        if isinstance(im, dict):
            url = (im.get("url_default") or im.get("urlDefault")
                   or im.get("url") or "")
            if not url:
                info = im.get("info_list") or []
                for it in info:
                    if it.get("url"):
                        url = it["url"]
                        break
        elif isinstance(im, str):
            url = im
        if url:
            img_html.append(_img(url))
    if not img_html:
        img_html.append('<div style="padding:40px;text-align:center;color:#999">无图片</div>')

    tag_html = "".join(f'<span class="t">#{_esc(t.get("name") if isinstance(t,dict) else t)}</span>' for t in tags)

    nid = note.get("note_id", "")
    tok = (user.get("xsec_token") or "")
    ext_url = f"https://www.xiaohongshu.com/explore/{nid}?xsec_token={tok}&xsec_source=pc_user"

    head = f"""<div class="head">
      <a href="/" class="back">← 返回仪表盘</a>
      <h1>📝 笔记详情</h1>
      <div class="meta">
        note_id: <code>{_esc(nid)}</code> ·
        <a href="{_esc(ext_url)}" target="_blank" rel="noreferrer">在小红书打开 ↗</a> ·
        <span class="tag">{_esc(note.get('type','normal'))}</span>
      </div>
    </div>"""

    side = f"""<div class="note-side">
      <div class="user">
        {_img(user.get('avatar',''), **{'data-zoom':'no'})}
        <div>
          <div class="nm">{_esc(user.get('nickname',''))}</div>
          <div class="uid">{_esc(user.get('user_id',''))}</div>
        </div>
      </div>
      <h2>{_esc(note.get('title','(无标题)'))}</h2>
      <div class="desc">{_esc(note.get('desc',''))}</div>
      <div class="tags">{tag_html}</div>
      <div class="stats">
        <span>❤️ <b>{_esc(interact.get('liked_count','0'))}</b></span>
        <span>⭐ <b>{_esc(interact.get('collected_count','0'))}</b></span>
        <span>💬 <b>{_esc(interact.get('comment_count','0'))}</b></span>
        <span>↗ <b>{_esc(interact.get('share_count','0'))}</b></span>
      </div>
      <div class="kv">
        <div>📅 发布：{_fmt_ts(note.get('time'))}</div>
        <div>🕒 更新：{_fmt_ts(note.get('last_update_time'))}</div>
        <div>📍 IP：{_esc(note.get('ip_location',''))}</div>
      </div>
    </div>"""

    body = head + f'<div class="note-detail"><div class="note-imgs">{"".join(img_html)}</div>{side}</div>'
    return _wrap(note.get("title", "笔记详情"), body)


def render_comments(data: dict, name: str) -> str:
    comments = data.get("comments", [])
    note_id = data.get("note_id", "")

    def _row(c: dict, sub: bool = False) -> str:
        u = c.get("user") or {}
        pics = "".join(_img(p.get("url_default") or p.get("url",""))
                       for p in (c.get("pictures") or []))
        sub_html = ""
        subs = c.get("sub_comments") or []
        if subs and not sub:
            sub_html = '<div class="sub">' + "".join(_row(s, True) for s in subs) + "</div>"
        if sub:
            return f"""<div class="s">
              <span class="nm">{_esc(u.get('nickname',''))}</span>
              <span style="color:#999;font-size:11px;margin-left:6px">{_fmt_ts(c.get('create_time'))} · {_esc(c.get('ip_location',''))}</span>
              <div>{_esc(c.get('content',''))}</div>
            </div>"""
        return f"""<div class="cmt">
          {_img(u.get('avatar',''), **{'class':'av','data-zoom':'no'})}
          <div class="cb">
            <div class="nm">{_esc(u.get('nickname',''))}</div>
            <div class="ct">{_esc(c.get('content',''))}</div>
            <div class="mt">
              <span>{_fmt_ts(c.get('create_time'))}</span>
              <span>📍 {_esc(c.get('ip_location',''))}</span>
              <span class="lk">❤️ {_esc(c.get('like_count','0'))}</span>
              {f'<span>↘ {len(subs)} 条回复</span>' if subs else ''}
            </div>
            {f'<div class="pics">{pics}</div>' if pics else ''}
            {sub_html}
          </div>
        </div>"""

    items = "".join(_row(c) for c in comments)
    empty_html = '<p style="color:#999;padding:20px">暂无评论</p>'
    head = f"""<div class="head">
      <a href="/" class="back">← 返回仪表盘</a>
      <h1>💬 笔记评论</h1>
      <div class="meta">
        note_id: <code>{_esc(note_id)}</code> ·
        共 <b>{len(comments)}</b> 条根评论 ·
        翻页 {data.get('captured_pages','?')} ·
        has_more: {data.get('has_more')}
      </div>
    </div>"""
    return _wrap(f"评论 {note_id}", head + f'<div class="cmt-list">{items or empty_html}</div>')


def render_top5(data: Any, name: str) -> str:
    notes = data if isinstance(data, list) else (data.get("notes") or [])
    cards = []
    for i, n in enumerate(notes):
        cover = _pick_cover(n)
        title = n.get("display_title") or n.get("title") or "(无标题)"
        likes = (n.get("interact_info") or n.get("interact") or {}).get("liked_count") or "0"
        nid = n.get("note_id") or ""
        cards.append(f"""<div class="card">
          {_img(cover, **{'class':'cov','data-zoom':'no'})}
          <div class="body">
            <p class="title" title="{_esc(title)}">⭐ TOP {i+1} · {_esc(title)}</p>
            <div class="row"><span class="typ">{_esc(n.get('type','normal'))}</span><span class="like">♡ {_esc(likes)}</span></div>
          </div>
        </div>""")
    head = f"""<div class="head">
      <a href="/" class="back">← 返回仪表盘</a>
      <h1>⭐ 前 5 笔记</h1>
      <div class="meta">共 {len(notes)} 篇 · <span class="tag">{_esc(name)}</span></div>
    </div>"""
    return _wrap("前 5 笔记", head + f'<div class="grid">{"".join(cards)}</div>')


def render_raw(data: Any, name: str) -> str:
    pretty = json.dumps(data, ensure_ascii=False, indent=2)
    head = f"""<div class="head">
      <a href="/" class="back">← 返回仪表盘</a>
      <h1>📄 原始 JSON</h1>
      <div class="meta">{_esc(name)}</div>
    </div>"""
    return _wrap(name, head + f'<pre style="background:#fff;padding:16px;border-radius:10px;overflow:auto;font-size:12px">{_esc(pretty)}</pre>')


def render(name: str, path: Path) -> str:
    data = json.loads(path.read_text(encoding="utf-8"))
    if name.startswith("user_posted_"):
        return render_user_posts(data, name)
    if name.startswith("note_"):
        return render_note(data, name)
    if name.startswith("comments_"):
        return render_comments(data, name)
    if name.startswith("notes_top5_"):
        return render_top5(data, name)
    return render_raw(data, name)
