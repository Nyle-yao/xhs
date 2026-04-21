from __future__ import annotations

import json
import hashlib
import re
import time
from urllib.parse import unquote
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.sync_api import BrowserContext, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright


NOTE_ID_RE = re.compile(r"(?:/explore/|/user/profile/[a-zA-Z0-9]+/)([a-zA-Z0-9]+)")
BLOGGER_ID_RE = re.compile(r"/user/profile/([a-zA-Z0-9]+)")
BLOGGER_LINK_RE = re.compile(r"https://www\\.xiaohongshu\\.com/user/profile/[a-zA-Z0-9]+")


@dataclass
class ScrapeResult:
    ok: bool
    message: str
    data: dict[str, Any] | None = None


class XHSScraper:
    """
    Best-effort Xiaohongshu scraper based on user's own logged-in browser profile.
    - Uses persistent context so user only logs in once.
    - Extracts data from DOM + hydration/state json where available.
    """

    def __init__(self, profile_dir: str = "./xhs_profile", headless: bool = False) -> None:
        self.profile_dir = str(Path(profile_dir).expanduser().resolve())
        self.headless = headless
        self._pw = None
        self.context: Optional[BrowserContext] = None

    def start(self) -> None:
        import os as _os
        if self.context:
            return
        Path(self.profile_dir).mkdir(parents=True, exist_ok=True)
        self._pw = sync_playwright().start()
        _exec_path = _os.environ.get("PLAYWRIGHT_CHROMIUM_PATH") or None
        _launch_kwargs = dict(
            user_data_dir=self.profile_dir,
            headless=self.headless,
            viewport={"width": 1400, "height": 900},
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        if _exec_path:
            _launch_kwargs["executable_path"] = _exec_path
        _proxy_server = _os.environ.get("XHS_PROXY_SERVER")
        if _proxy_server:
            if "://" not in _proxy_server:
                _proxy_server = "http://" + _proxy_server
            _proxy_cfg = {"server": _proxy_server}
            _pu = _os.environ.get("XHS_PROXY_USER")
            _pp = _os.environ.get("XHS_PROXY_PASS")
            if _pu:
                _proxy_cfg["username"] = _pu
            if _pp:
                _proxy_cfg["password"] = _pp
            _launch_kwargs["proxy"] = _proxy_cfg
            print(f"[scraper] using proxy {_proxy_server} (user={_pu or '-'})", flush=True)
        self.context = self._pw.chromium.launch_persistent_context(**_launch_kwargs)

    def close(self) -> None:
        if self.context:
            self.context.close()
            self.context = None
        if self._pw:
            self._pw.stop()
            self._pw = None

    def ensure_login(self, timeout_sec: int = 180, keep_open: bool = False) -> ScrapeResult:
        self.start()
        assert self.context is not None
        page = self.context.pages[0] if self.context.pages else self.context.new_page()
        try:
            page.goto("https://www.xiaohongshu.com/", wait_until="domcontentloaded")
        except Exception:
            # 持久化上下文偶发frame detach，重建页面继续检测。
            try:
                page.close()
            except Exception:
                pass
            page = self.context.new_page()
            try:
                page.goto("https://www.xiaohongshu.com/", wait_until="domcontentloaded")
            except Exception:
                pass
        try:
            page.bring_to_front()
        except Exception:
            pass

        try:
            self._trigger_login_ui(page)
        except Exception:
            pass

        start = time.time()
        ok = False
        last_reason = "等待扫码登录"
        while time.time() - start < timeout_sec:
            try:
                self._wait_for_page_settle(page, max_wait_sec=3)
                html = page.content()
                body_text = self._clean_text(page.inner_text("body", timeout=3000))
                logged_in, reason = self._detect_login_state(page, html, body_text)
                last_reason = reason
                if logged_in:
                    ok = True
                    break
            except Exception as e:
                last_reason = f"检测中：{e}"
            time.sleep(2)

        msg = f"登录检测通过：{last_reason}" if ok else f"登录检测未通过：{last_reason}。请在弹出的浏览器里扫码登录后重试"
        if not keep_open:
            page.close()
        return ScrapeResult(ok=ok, message=msg)

    def _trigger_login_ui(self, page: Page) -> None:
        """Open a visible login entry and leave the page stable for manual scan."""
        targets = [
            "https://www.xiaohongshu.com/explore",
            "https://www.xiaohongshu.com/",
            "https://www.xiaohongshu.com/login",
        ]
        for url in targets[:2]:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=45000)
                self._wait_for_page_settle(page, max_wait_sec=3)
                body = self._clean_text(page.inner_text("body", timeout=3000))
                html = page.content()
                logged_in, _ = self._detect_login_state(page, html, body)
                if logged_in:
                    return
                # Trigger common login buttons without assuming exact DOM.
                for selector in [
                    "text=登录",
                    "text=登陆",
                    "button:has-text('登录')",
                    "button:has-text('登陆')",
                    "[class*='login']",
                ]:
                    try:
                        loc = page.locator(selector).first
                        if loc.count() > 0 and loc.is_visible(timeout=800):
                            loc.click(timeout=1500)
                            time.sleep(1.5)
                            return
                    except Exception:
                        continue
            except Exception:
                continue
        try:
            page.goto(targets[-1], wait_until="domcontentloaded", timeout=45000)
            self._wait_for_page_settle(page, max_wait_sec=3)
        except Exception:
            pass

    def _detect_login_state(self, page: Page, html: str, body_text: str) -> tuple[bool, str]:
        final_url = page.url
        if self._is_login_or_blocked_url(final_url):
            return False, f"当前在登录/验证页面: {final_url}"
        if self._is_blocked_text(body_text):
            return False, "页面出现登录/验证/风控提示"
        try:
            cookies = self.context.cookies("https://www.xiaohongshu.com") if self.context else []
        except Exception:
            cookies = []
        cookie_names = {str(c.get("name", "")) for c in cookies}
        has_session_cookie = any(x in cookie_names for x in {"web_session", "web_session_id"})
        has_xhs_cookie = any(
            name in cookie_names
            for name in {
                "a1",
                "webId",
                "websectiga",
                "webBuild",
                "xsecappid",
                "gid",
                "abRequestId",
                "access-token",
                "customerClientId",
            }
        )
        has_login_button = bool(re.search(r"登录|验证码|扫码登录|手机号登录", body_text or html))
        has_logged_in_ui = bool(re.search(r"发布|消息|通知|我|小红书号|编辑资料|退出登录", body_text or html))
        # 小红书不同版本登录态字段不完全一致：优先认 session cookie；
        # 若仅有站点 cookie，也必须同时无登录入口且出现登录后 UI 信号，避免误判。
        if has_session_cookie and not has_login_button:
            return True, "检测到登录态 cookie，且页面无登录入口"
        if has_session_cookie:
            return False, "检测到部分登录 cookie，但页面仍显示登录入口，可能登录未完成"
        if has_xhs_cookie and has_logged_in_ui and not has_login_button:
            return True, "检测到小红书登录后页面信号，按已登录处理"
        if has_xhs_cookie:
            return False, f"检测到小红书站点 cookie，但页面登录信号不足: {sorted(cookie_names)}"
        return False, f"未检测到有效登录态 cookie，当前 cookie: {sorted(cookie_names)}"

    def login_debug_snapshot(self) -> ScrapeResult:
        self.start()
        assert self.context is not None
        page = self.context.pages[0] if self.context.pages else self.context.new_page()
        try:
            try:
                page.bring_to_front()
            except Exception:
                pass
            self._wait_for_page_settle(page, max_wait_sec=3)
            html = page.content() if "xiaohongshu.com" in (page.url or "") else ""
            body_text = self._clean_text(page.inner_text("body", timeout=3000)) if html else ""
            logged_in, reason = self._detect_login_state(page, html, body_text)
            cookies = self.context.cookies("https://www.xiaohongshu.com") if self.context else []
            cookie_names = sorted({str(c.get("name", "")) for c in cookies})
            return ScrapeResult(
                ok=True,
                message=reason,
                data={
                    "logged_in": logged_in,
                    "reason": reason,
                    "url": page.url,
                    "cookie_names": cookie_names,
                    "has_login_word": bool(re.search(r"登录|验证码|扫码登录|手机号登录", body_text or html)),
                    "has_logged_in_ui": bool(re.search(r"发布|消息|通知|我|小红书号|编辑资料|退出登录", body_text or html)),
                    "body_preview": body_text[:240],
                },
            )
        except Exception as e:
            return ScrapeResult(ok=False, message=f"登录诊断失败: {e}", data={})

    @staticmethod
    def _is_login_or_blocked_url(url: str) -> bool:
        u = (url or "").lower()
        blocked_signals = [
            "/login",
            "website-login/error",
            "website-login/captcha",
            "/captcha",
            "verifyuuid=",
            "verifybiz=",
            "security-verification",
        ]
        return any(s in u for s in blocked_signals)

    @staticmethod
    def _is_blocked_text(text: str) -> bool:
        t = (text or "").lower()
        blocked_signals = [
            "ip存在风险",
            "切换可靠网络环境后重试",
            "访问受限",
            "登录后查看",
            "登录即可查看",
            "请先登录",
            "验证",
            "captcha",
            "security verification",
            "feedback",
            "当前笔记暂时无法浏览",
            "访问频繁",
        ]
        return any(s in t for s in blocked_signals)

    @staticmethod
    def _looks_like_placeholder_text(text: str) -> bool:
        t = re.sub(r"\s+", "", str(text or ""))
        if not t:
            return False
        placeholders = {
            "发现直播发布通知",
            "发现直播发布消息通知",
            "发现直播",
        }
        if t in placeholders:
            return True
        # 登录态丢失时常见导航词拼接，且几乎没有正文
        nav_tokens = ["发现", "直播", "发布", "消息", "通知"]
        hit = sum(1 for x in nav_tokens if x in t)
        return hit >= 4 and len(t) <= 20

    @staticmethod
    def _extract_ld_json_objects(html: str) -> list[dict[str, Any]]:
        if not html:
            return []
        out: list[dict[str, Any]] = []
        blocks = re.findall(
            r'<script[^>]*type=["\']application/ld\\+json["\'][^>]*>(.*?)</script>',
            html,
            flags=re.I | re.S,
        )
        for b in blocks:
            s = (b or "").strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            if isinstance(obj, list):
                out.extend([x for x in obj if isinstance(x, dict)])
            elif isinstance(obj, dict):
                out.append(obj)
        return out

    @staticmethod
    def _parse_interaction_count(v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, (int, float)):
            return str(int(v))
        s = str(v).strip()
        if not s:
            return ""
        # 兼容 "1.2万" / "3k" 等
        return s

    @staticmethod
    def _sanitize_metric_count(v: str, publish_time: str = "") -> str:
        s = str(v or "").strip()
        if not s:
            return ""
        if "-" in s or "/" in s or "年" in s:
            return ""
        # 防止把日期年份误当成互动量（如 2025）
        if re.fullmatch(r"20\d{2}", s):
            if publish_time and s in str(publish_time):
                return ""
            # 纯四位年份默认判无效
            return ""
        return s

    @staticmethod
    def _infer_datetime_from_note_id(note_id: str) -> datetime | None:
        """小红书笔记ID前8位通常可按Unix时间戳反推发布时间，用于校准页面正文误提取的日期。"""
        s = str(note_id or "").strip()
        if len(s) < 8 or not re.fullmatch(r"[0-9a-fA-F]{8}.*", s):
            return None
        try:
            dt = datetime.fromtimestamp(int(s[:8], 16))
        except Exception:
            return None
        if 2020 <= dt.year <= datetime.now().year + 1:
            return dt
        return None

    @staticmethod
    def _parse_absolute_datetime(value: str) -> datetime | None:
        s = str(value or "").strip()
        if not s:
            return None
        s = s.replace("年", "-").replace("月", "-").replace("日", " ")
        s = s.replace("/", "-").replace(".", "-").replace("T", " ")
        m = re.search(r"(20\d{2})-(\d{1,2})-(\d{1,2})(?:\s+(\d{1,2}):(\d{1,2}))?", s)
        if not m:
            return None
        try:
            return datetime(
                int(m.group(1)),
                int(m.group(2)),
                int(m.group(3)),
                int(m.group(4) or 0),
                int(m.group(5) or 0),
            )
        except ValueError:
            return None

    def probe_profile_access(self, blogger_url: str) -> ScrapeResult:
        page = self._new_page()
        try:
            page.goto(blogger_url, wait_until="domcontentloaded", timeout=90000)
            self._wait_for_page_settle(page, max_wait_sec=10)
            final_url = page.url
            body = self._clean_text(page.inner_text("body"))
            if self._is_login_or_blocked_url(final_url):
                if ("website-login/error" in final_url) or ("website-login/captcha" in final_url) or self._is_blocked_text(body):
                    return ScrapeResult(ok=False, message=f"访问被风控/验证码拦截: {final_url}")
                return ScrapeResult(ok=False, message=f"需要登录后才可访问: {final_url}")
            if self._is_blocked_text(body):
                return ScrapeResult(ok=False, message="页面被风控/验证拦截")
            return ScrapeResult(ok=True, message="博主页访问预检通过", data={"final_url": final_url})
        except Exception as e:
            return ScrapeResult(ok=False, message=f"预检失败: {e}")
        finally:
            page.close()

    def _wait_for_page_settle(self, page: Page, max_wait_sec: int = 12) -> None:
        """Give client-rendered Xiaohongshu pages time to hydrate before extraction."""
        try:
            page.wait_for_load_state("networkidle", timeout=min(max_wait_sec, 15) * 1000)
        except Exception:
            pass
        deadline = time.time() + max(1, max_wait_sec)
        last_len = -1
        stable_rounds = 0
        while time.time() < deadline:
            try:
                txt = self._clean_text(page.inner_text("body", timeout=1500))
            except Exception:
                txt = ""
            cur_len = len(txt)
            if cur_len > 80 and abs(cur_len - last_len) < 20:
                stable_rounds += 1
            else:
                stable_rounds = 0
            if stable_rounds >= 2:
                break
            last_len = cur_len
            time.sleep(0.8)

    def _collect_profile_note_links(self, page: Page, max_notes: int, load_wait_sec: int = 18) -> tuple[str, list[str]]:
        """Collect note links after repeated hydration/scroll rounds.

        Profile pages often render note cards after a delay. Treating the first
        empty HTML snapshot as final causes false "no notes"/risk conclusions.
        """
        deadline = time.time() + max(2, load_wait_sec)
        note_links: list[str] = []
        html = ""
        scroll_round = 0
        while time.time() < deadline:
            try:
                html = page.content()
            except Exception:
                html = ""
            note_links = self._extract_note_links_from_profile_html(html, max_notes=max_notes)
            if len(note_links) >= max_notes:
                break
            try:
                hrefs: list[str] = page.eval_on_selector_all(
                    "a[href]",
                    "els => els.map(e => e.getAttribute('href') || '').filter(Boolean)",
                )
            except Exception:
                hrefs = []
            extra: list[str] = []
            for h in hrefs:
                s = (h or "").replace("&amp;", "&").strip()
                if not s:
                    continue
                if s.startswith("/user/profile/") and "xsec_source=pc_user" in s:
                    extra.append(f"https://www.xiaohongshu.com{s}")
                    continue
                mexp = re.search(r"/explore/([a-zA-Z0-9]+)", s)
                if mexp:
                    extra.append(f"https://www.xiaohongshu.com/explore/{mexp.group(1)}")
            merged: list[str] = []
            seen_merged: set[str] = set()
            for u in note_links + extra:
                if u in seen_merged:
                    continue
                seen_merged.add(u)
                merged.append(u)
                if len(merged) >= max_notes:
                    break
            note_links = merged
            if note_links:
                break
            try:
                page.mouse.wheel(0, 1200 + 300 * scroll_round)
            except Exception:
                pass
            scroll_round += 1
            time.sleep(1.0)
        return html, note_links

    def _new_page(self) -> Page:
        self.start()
        assert self.context is not None
        return self.context.new_page()

    def _active_page(self) -> Page:
        self.start()
        assert self.context is not None
        return self.context.pages[0] if self.context.pages else self.context.new_page()

    def _wait_for_manual_unblock(self, page: Page, timeout_sec: int = 180) -> tuple[bool, str]:
        """Wait for the user to manually finish login/verification in the visible page."""
        deadline = time.time() + timeout_sec
        last_reason = "等待人工完成登录/验证"
        try:
            page.bring_to_front()
        except Exception:
            pass
        while time.time() < deadline:
            try:
                self._wait_for_page_settle(page, max_wait_sec=3)
                body = self._clean_text(page.inner_text("body", timeout=3000))
                html = page.content()
                logged_in, reason = self._detect_login_state(page, html, body)
                last_reason = reason
                if logged_in and not self._is_blocked_text(body):
                    return True, reason
                if (not self._is_login_or_blocked_url(page.url)) and (not self._is_blocked_text(body)) and ("登录即可查看" not in body):
                    return True, "页面已脱离登录/验证提示"
            except Exception as e:
                last_reason = f"等待中：{e}"
            time.sleep(2)
        return False, last_reason

    @staticmethod
    def _clean_text(v: str | None) -> str:
        if not v:
            return ""
        return re.sub(r"\s+", " ", v).strip()

    @staticmethod
    def _normalize_comment_body(raw_text: str, user_name: str = "", comment_time: str = "") -> str:
        text = str(raw_text or "")
        if not text:
            return ""
        lines = [re.sub(r"\s+", " ", x).strip() for x in re.split(r"[\r\n]+", text) if str(x or "").strip()]
        body = " ".join(lines) if lines else re.sub(r"\s+", " ", text).strip()
        if not body:
            return ""

        # 去除开头用户名（常见“用户名 评论内容 ...”）
        uname = re.sub(r"\s+", "", str(user_name or ""))
        if uname:
            compact = re.sub(r"\s+", "", body)
            if compact.startswith(uname):
                body = compact[len(uname) :]
                body = re.sub(r"\s+", " ", body).strip()

        # 去除常见时间/地域/互动尾巴
        patterns = [
            r"(20\d{2}[./-]\d{1,2}[./-]\d{1,2}(?:\s+\d{1,2}:\d{1,2})?)",
            r"(?:\d{1,2}月\d{1,2}日(?:\s+\d{1,2}:\d{1,2})?)",
            r"(?:IP(?:属地|地址)[:：]?\s*[^\s，。]+)",
            r"(?:来自[^\s，。]+)",
            r"(?:赞\s*\d*)",
            r"(?:回复\s*\d*)",
            r"(?:展开\d*条回复)",
            r"(?:查看全部回复)",
        ]
        for pat in patterns:
            body = re.sub(pat, " ", body, flags=re.I)

        # 如果已识别评论时间，优先精确剔除
        ctime = str(comment_time or "").strip()
        if ctime:
            body = body.replace(ctime, " ")

        # 去掉“作者”前缀与常见省份+互动数字尾巴（如“山西 81 10”）
        body = re.sub(r"^\s*作者[:：]?", " ", body)
        body = re.sub(
            r"(?:北京|上海|天津|重庆|河北|山西|辽宁|吉林|黑龙江|江苏|浙江|安徽|福建|江西|山东|河南|湖北|湖南|广东|海南|四川|贵州|云南|陕西|甘肃|青海|台湾|内蒙古|广西|西藏|宁夏|新疆|香港|澳门)\s*(?:\d{1,6}(?:\s+\d{1,6})*)?\s*$",
            " ",
            body,
        )

        body = re.sub(r"\s+", " ", body).strip(" ，。:：|-")
        return body[:400].strip()

    def _extract_hydration_json(self, html: str) -> dict[str, Any]:
        candidates: list[str] = []
        # common patterns
        patterns = [
            r"window\.__INITIAL_STATE__\s*=\s*(\{.*?\})\s*;",
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        ]
        for pat in patterns:
            m = re.search(pat, html, flags=re.S)
            if m:
                candidates.append(m.group(1))
        for c in candidates:
            try:
                return json.loads(c)
            except Exception:
                continue
        return {}

    @staticmethod
    def _extract_media_urls_from_html(html: str) -> dict[str, list[str]]:
        # best-effort from page html; keep conservative and drop obvious static assets.
        image_urls = sorted(set(re.findall(r'https://[^"\']+?\.(?:jpg|jpeg|png|webp)(?:\?[^"\']*)?', html, flags=re.I)))
        video_urls = sorted(set(re.findall(r'https://[^"\']+?\.(?:mp4|m3u8)(?:\?[^"\']*)?', html, flags=re.I)))
        image_urls = [u for u in image_urls if not XHSScraper._is_bad_image_url(u)][:200]
        video_urls = [u for u in video_urls if "preview" not in u.lower()][:50]
        return {"images": image_urls, "videos": video_urls}

    @staticmethod
    def _is_bad_image_url(url: str) -> bool:
        u = (url or "").lower()
        bad_signals = [
            "/fe-platform/",
            "avatar",
            "emoji",
            "icon",
            "logo",
            "badge",
            "sprite",
            "favicon",
            "/comment/",
        ]
        return any(x in u for x in bad_signals)

    def _extract_media_urls_from_dom(self, page: Page) -> dict[str, list[str]]:
        """
        Prefer real note media in rendered DOM over raw HTML regex, because the
        raw HTML often contains many platform static assets (200x200 placeholders).
        """
        images: list[str] = []
        videos: list[str] = []
        try:
            rows = page.eval_on_selector_all(
                "img",
                """
                els => els.map(e => ({
                  src: (e.currentSrc || e.src || '').trim(),
                  w: Number(e.naturalWidth || 0),
                  h: Number(e.naturalHeight || 0),
                  cw: Number(e.clientWidth || 0),
                  ch: Number(e.clientHeight || 0),
                }))
                """,
            )
            if not isinstance(rows, list):
                rows = []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                src = str(r.get("src", "") or "").strip()
                if not src or not src.startswith("http"):
                    continue
                if self._is_bad_image_url(src):
                    continue
                w = int(r.get("w", 0) or 0)
                h = int(r.get("h", 0) or 0)
                cw = int(r.get("cw", 0) or 0)
                ch = int(r.get("ch", 0) or 0)
                # Gate tiny assets aggressively to avoid OCR noise.
                max_side = max(w, h, cw, ch)
                if max_side and max_side < 360:
                    continue
                images.append(src)
        except Exception:
            pass

        try:
            vrows = page.eval_on_selector_all(
                "video",
                """
                els => els.map(e => ({
                  src: (e.currentSrc || e.src || '').trim(),
                  poster: (e.poster || '').trim(),
                }))
                """,
            )
            if not isinstance(vrows, list):
                vrows = []
            for r in vrows:
                if not isinstance(r, dict):
                    continue
                for k in ("src", "poster"):
                    s = str(r.get(k, "") or "").strip()
                    if not s or not s.startswith("http"):
                        continue
                    if k == "src":
                        videos.append(s)
                    elif not self._is_bad_image_url(s):
                        images.append(s)
        except Exception:
            pass

        def _dedup_keep_order(xs: list[str], limit: int) -> list[str]:
            out: list[str] = []
            seen: set[str] = set()
            for x in xs:
                if x in seen:
                    continue
                seen.add(x)
                out.append(x)
                if len(out) >= limit:
                    break
            return out

        return {"images": _dedup_keep_order(images, 50), "videos": _dedup_keep_order(videos, 20)}

    def _extract_media_urls_with_carousel(self, page: Page, rounds: int = 12) -> dict[str, list[str]]:
        """Best-effort collection for multi-image notes.

        Xiaohongshu image notes may only render the current slide in DOM. We
        click likely "next image" controls and re-read visible media so later
        images are not missed. If no carousel exists, this safely returns the
        current DOM media only.
        """
        images: list[str] = []
        videos: list[str] = []

        def add_current() -> None:
            cur = self._extract_media_urls_from_dom(page)
            images.extend(cur.get("images", []))
            videos.extend(cur.get("videos", []))

        add_current()
        selectors = [
            "[class*='swiper-button-next']",
            "[class*='carousel'][class*='next']",
            "[class*='slider'][class*='next']",
            "[class*='arrow'][class*='right']",
            "[class*='right'][class*='arrow']",
            "button[aria-label*='下一']",
            "button[aria-label*='next' i]",
            "div[role='button'][aria-label*='下一']",
            "div[role='button'][aria-label*='next' i]",
        ]
        no_new_rounds = 0
        seen_before = set(images)
        for _ in range(max(0, rounds)):
            clicked = False
            for sel in selectors:
                try:
                    loc = page.locator(sel)
                    cnt = min(loc.count(), 4)
                    for i in range(cnt):
                        item = loc.nth(i)
                        try:
                            if not item.is_visible(timeout=300):
                                continue
                            item.click(timeout=800)
                            clicked = True
                            break
                        except Exception:
                            continue
                    if clicked:
                        break
                except Exception:
                    continue
            if not clicked:
                break
            time.sleep(0.7)
            add_current()
            seen_now = set(images)
            if len(seen_now) <= len(seen_before):
                no_new_rounds += 1
            else:
                no_new_rounds = 0
            seen_before = seen_now
            if no_new_rounds >= 2:
                break

        def dedup(xs: list[str], limit: int) -> list[str]:
            out: list[str] = []
            seen: set[str] = set()
            for x in xs:
                if x in seen:
                    continue
                seen.add(x)
                out.append(x)
                if len(out) >= limit:
                    break
            return out

        return {"images": dedup(images, 80), "videos": dedup(videos, 20)}

    @staticmethod
    def _extract_hash_tags(text: str) -> list[str]:
        if not text:
            return []
        tags = re.findall(r"#([^#\n\r]{1,40})#", text)
        out: list[str] = []
        seen: set[str] = set()
        for t in tags:
            s = re.sub(r"\s+", " ", t).strip()
            if not s or s in seen:
                continue
            out.append(s)
            seen.add(s)
        return out

    @staticmethod
    def _extract_topic_tags_from_html(html: str) -> list[str]:
        if not html:
            return []
        raw = re.findall(r"(?:https?://www\.xiaohongshu\.com)?/search_result\?keyword=([^\"'&<>]+)", html)
        out: list[str] = []
        seen: set[str] = set()

        def _decode_twice(x: str) -> str:
            s = str(x or "")
            for _ in range(2):
                try:
                    y = unquote(s)
                except Exception:
                    break
                if y == s:
                    break
                s = y
            return s

        for x in raw:
            s = _decode_twice(x).strip()
            if not s:
                continue
            s = s.replace("+", " ")
            # 过滤明显非标签参数
            if len(s) > 40 or "/" in s:
                continue
            if s in seen:
                continue
            out.append(s)
            seen.add(s)
        return out

    @staticmethod
    def _extract_note_links_from_profile_html(html: str, max_notes: int = 30) -> list[str]:
        """
        小红书博主页在不同版本里会出现：
        1) 绝对链接 https://www.xiaohongshu.com/explore/<note_id>
        2) 相对链接 /explore/<note_id>
        3) JSON 字段 "noteId": "<note_id>"
        统一归一化为绝对链接并去重。
        """
        if not html:
            return []

        # 1) 先抓取带 xsec_token 的签名链接（优先，稳定性更高）
        secure_links_raw = re.findall(
            r'/user/profile/[a-zA-Z0-9]+/[a-zA-Z0-9]+\?xsec_token=[^"\']+?xsec_source=pc_user',
            html,
        )
        secure_links: list[str] = []
        seen_secure: set[str] = set()
        for x in secure_links_raw:
            s = (x or "").replace("&amp;", "&").strip()
            if not s:
                continue
            full = s if s.startswith("http") else f"https://www.xiaohongshu.com{s}"
            if full in seen_secure:
                continue
            seen_secure.add(full)
            secure_links.append(full)
            if len(secure_links) >= max_notes:
                return secure_links

        # 2) 再回退到 /explore/ 显式链接
        ids: list[str] = []
        abs_links = re.findall(r"https://www\.xiaohongshu\.com/explore/([a-zA-Z0-9]+)", html)
        rel_links = re.findall(r"/explore/([a-zA-Z0-9]+)", html)
        ids.extend(abs_links)
        ids.extend(rel_links)

        # 3) 最后兜底才使用 noteId 字段，避免引入大量不可访问历史卡片ID
        if not ids:
            note_ids = re.findall(r'"noteId"\s*:\s*"([a-zA-Z0-9]+)"', html)
            ids.extend(note_ids[: max(1, min(10, max_notes))])

        out: list[str] = []
        seen: set[str] = set()
        for nid in ids:
            s = (nid or "").strip()
            if not s or s in seen:
                continue
            seen.add(s)
            out.append(f"https://www.xiaohongshu.com/explore/{s}")
            if len(out) >= max_notes:
                break
        return secure_links + out[: max(0, max_notes - len(secure_links))]

    def scrape_note(self, note_url: str) -> ScrapeResult:
        page = self._new_page()
        try:
            want_note_id = ""
            m = NOTE_ID_RE.search(note_url)
            if m:
                want_note_id = m.group(1)

            page.goto(note_url, wait_until="domcontentloaded", timeout=90000)
            time.sleep(2)
            final_url = page.url
            body_text = self._clean_text(page.inner_text("body"))
            if self._is_login_or_blocked_url(final_url):
                if "website-login/error" in final_url or self._is_blocked_text(body_text):
                    return ScrapeResult(ok=False, message=f"风控拦截: {final_url}")
                return ScrapeResult(ok=False, message=f"未登录或登录态失效: {final_url}")
            if self._is_blocked_text(body_text):
                return ScrapeResult(ok=False, message="页面被验证/风控拦截")
            # 防止被重定向到通用页但误判为笔记详情
            if want_note_id:
                on_note_path = (
                    f"/explore/{want_note_id}" in final_url
                    or re.search(rf"/user/profile/[a-zA-Z0-9]+/{want_note_id}(?:\\?|$)", final_url) is not None
                )
                if not on_note_path:
                    return ScrapeResult(ok=False, message=f"笔记重定向，未进入目标详情页: {final_url}")

            html = page.content()
            hydration = self._extract_hydration_json(html)
            ld_objs = self._extract_ld_json_objects(html)

            title = ""
            content = ""
            author_name = ""
            like_cnt = ""
            collect_cnt = ""
            comment_cnt = ""
            share_cnt = ""
            publish_time = ""

            selectors = [
                ("title", ["h1", "[class*='title']"]),
                ("content", ["[class*='desc']", "[class*='content']", "article"]),
                ("author_name", ["[class*='author'] [class*='name']", "[class*='user'] [class*='name']"]),
            ]
            for field, sel_list in selectors:
                for s in sel_list:
                    try:
                        text = page.locator(s).first.inner_text(timeout=1000)
                        if text:
                            if field == "title":
                                title = self._clean_text(text)
                            elif field == "content":
                                content = self._clean_text(text)
                            elif field == "author_name":
                                author_name = self._clean_text(text)
                            break
                    except Exception:
                        continue

            # 优先利用 ld+json 提升字段完整度
            for obj in ld_objs:
                if not title:
                    title = self._clean_text(
                        str(
                            obj.get("headline")
                            or obj.get("name")
                            or obj.get("title")
                            or ""
                        )
                    )
                if not content:
                    content = self._clean_text(
                        str(
                            obj.get("articleBody")
                            or obj.get("description")
                            or obj.get("text")
                            or ""
                        )
                    )
                if not publish_time:
                    publish_time = self._clean_text(
                        str(
                            obj.get("datePublished")
                            or obj.get("uploadDate")
                            or obj.get("dateCreated")
                            or ""
                        )
                    )
                inter = obj.get("interactionStatistic")
                if isinstance(inter, dict):
                    inter = [inter]
                if isinstance(inter, list):
                    for it in inter:
                        if not isinstance(it, dict):
                            continue
                        itype = str(it.get("interactionType") or "").lower()
                        cnt = self._parse_interaction_count(it.get("userInteractionCount"))
                        if not cnt:
                            continue
                        if ("like" in itype or "点赞" in itype) and not like_cnt:
                            like_cnt = cnt
                        elif ("comment" in itype or "评论" in itype) and not comment_cnt:
                            comment_cnt = cnt
                        elif ("share" in itype or "转发" in itype) and not share_cnt:
                            share_cnt = cnt

            # try interaction counters with broad regex in page text
            whole_text = body_text
            m_like = re.search(r"(?:赞|点赞)\s*([0-9\.万wWkK]+)", whole_text)
            m_collect = re.search(r"(?:收藏|收 藏)\s*([0-9\.万wWkK]+)", whole_text)
            m_comment = re.search(r"评论\s*([0-9\.万wWkK]+)", whole_text)
            m_share = re.search(r"(?:分享|转发)\s*([0-9\.万wWkK]+)", whole_text)
            if m_like:
                like_cnt = m_like.group(1)
            if m_collect:
                collect_cnt = m_collect.group(1)
            if m_comment:
                comment_cnt = m_comment.group(1)
            if m_share:
                share_cnt = m_share.group(1)

            m_time = re.search(r"(20\d{2}[-/.年]\d{1,2}[-/.月]\d{1,2}日?(?:\s*\d{1,2}:\d{1,2})?)", whole_text)
            if m_time:
                publish_time = m_time.group(1)
            if not publish_time:
                # 回退：仅月日格式，补当前年份
                m_md = re.search(r"(?<!\d)(\d{1,2})[-/.月](\d{1,2})日?(?:\s*(\d{1,2}:\d{1,2}))?", whole_text)
                if m_md:
                    y = datetime.now().year
                    mm = int(m_md.group(1))
                    dd = int(m_md.group(2))
                    hm = (m_md.group(3) or "").strip()
                    publish_time = f"{y:04d}-{mm:02d}-{dd:02d}" + (f" {hm}" if hm else "")
            like_cnt = self._sanitize_metric_count(like_cnt, publish_time)
            collect_cnt = self._sanitize_metric_count(collect_cnt, publish_time)
            comment_cnt = self._sanitize_metric_count(comment_cnt, publish_time)
            share_cnt = self._sanitize_metric_count(share_cnt, publish_time)
            update_time = publish_time

            note_id = want_note_id
            inferred_note_time = self._infer_datetime_from_note_id(note_id)
            if inferred_note_time:
                parsed_publish_time = self._parse_absolute_datetime(publish_time)
                if parsed_publish_time is None or abs((parsed_publish_time - inferred_note_time).days) > 3:
                    publish_time = inferred_note_time.strftime("%Y-%m-%d %H:%M")
                    update_time = publish_time

            media_html = self._extract_media_urls_from_html(html)
            media_dom = self._extract_media_urls_with_carousel(page)
            # DOM/carousel first (real rendered note media), HTML fallback.
            image_urls = media_dom.get("images", []) + media_html.get("images", [])
            video_urls = media_dom.get("videos", []) + media_html.get("videos", [])
            seen_i: set[str] = set()
            seen_v: set[str] = set()
            image_urls = [u for u in image_urls if not (u in seen_i or seen_i.add(u))][:120]
            video_urls = [u for u in video_urls if not (u in seen_v or seen_v.add(u))][:30]
            media = {"images": image_urls, "videos": video_urls}
            tags_text = self._extract_hash_tags(f"{title}\n{content}\n{whole_text}")
            tags_topic = self._extract_topic_tags_from_html(html)
            tags_all = []
            seen = set()
            for t in tags_text + tags_topic:
                if t in seen:
                    continue
                tags_all.append(t)
                seen.add(t)
            note_topic = "、".join(tags_all)

            blogger_url = ""
            mb = BLOGGER_LINK_RE.search(html)
            if mb:
                blogger_url = mb.group(0)
            blogger_id = ""
            mib = BLOGGER_ID_RE.search(blogger_url)
            if mib:
                blogger_id = mib.group(1)

            ip_address = ""
            mip = re.search(r"IP(?:地址|属地)[:：]?\s*([\\u4e00-\\u9fa5A-Za-z]+)", whole_text)
            if mip:
                ip_address = mip.group(1)

            note_type = "视频" if media["videos"] else "图文"
            cover_url = ""
            if media["images"]:
                cover_url = media["images"][0]
            elif media["videos"]:
                cover_url = media["videos"][0]

            data = {
                "note_id": note_id,
                "note_url": note_url,
                "note_type": note_type,
                "title": title,
                "content": content,
                "note_topic": note_topic,
                "author_name": author_name,
                "blogger_name": author_name,
                "blogger_id": blogger_id,
                "blogger_url": blogger_url,
                "publish_time": publish_time,
                "update_time": update_time,
                "like_count": like_cnt,
                "collect_count": collect_cnt,
                "comment_count": comment_cnt,
                "share_count": share_cnt,
                "ip_address": ip_address,
                "cover_url": cover_url,
                "image_urls": media["images"],
                "video_urls": media["videos"],
                "tags_text": tags_text,
                "tags_topic": tags_topic,
                "tags_all": tags_all,
                "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "raw_hydration_available": bool(hydration),
            }
            # 最低质量门槛：标题/正文/互动/媒体至少命中其一，否则视为无效详情页
            has_core = bool(title or content or like_cnt or collect_cnt or comment_cnt or media["images"] or media["videos"])
            if self._looks_like_placeholder_text(content) and not (like_cnt or collect_cnt or comment_cnt or publish_time):
                return ScrapeResult(ok=False, message="笔记详情疑似占位页（导航文本）")
            if self._looks_like_placeholder_text(title) and not (content or like_cnt or collect_cnt or comment_cnt):
                return ScrapeResult(ok=False, message="笔记标题疑似占位页（导航文本）")
            if not has_core:
                return ScrapeResult(ok=False, message="笔记详情缺失（疑似跳转页或受限页）")
            return ScrapeResult(ok=True, message="笔记抓取成功", data=data)
        except PlaywrightTimeoutError:
            return ScrapeResult(ok=False, message="打开笔记超时")
        except Exception as e:
            return ScrapeResult(ok=False, message=f"抓取失败: {e}")
        finally:
            page.close()

    def _extract_comment_image_url(self, comment_locator: Any) -> str:
        """Extract real image URLs from a single comment container.

        Image-only comments may have little/no text, so this is intentionally
        independent from text parsing and should run before text-based filters.
        """
        try:
            img_rows = comment_locator.eval_on_selector_all(
                "*",
                """
                els => els.map(e => ({
                  tag: String(e.tagName || ''),
                  src: (e.currentSrc || e.src || e.getAttribute('src') || '').trim(),
                  dataSrc: (e.getAttribute('data-src') || e.getAttribute('data-original') || e.getAttribute('data-lazy') || '').trim(),
                  styleBg: (window.getComputedStyle(e).backgroundImage || '').trim(),
                  className: String(e.className || ''),
                  alt: String(e.alt || ''),
                  w: Number(e.naturalWidth || e.clientWidth || 0),
                  h: Number(e.naturalHeight || e.clientHeight || 0),
                }))
                """,
            )
        except Exception:
            img_rows = []
        if not isinstance(img_rows, list):
            img_rows = []

        comment_image_urls: list[str] = []
        for row in img_rows:
            if not isinstance(row, dict):
                continue
            raw_sources = [
                str(row.get("src", "") or "").strip(),
                str(row.get("dataSrc", "") or "").strip(),
            ]
            style_bg = str(row.get("styleBg", "") or "").strip()
            raw_sources.extend(re.findall(r"url\\([\"']?(https?://[^)\"']+)[\"']?\\)", style_bg))
            meta = " ".join(raw_sources + [str(row.get("className", "") or ""), str(row.get("alt", "") or "")]).lower()
            if any(tok in meta for tok in ["avatar", "head", "icon", "emoji", "logo", "badge", "sprite", "favicon"]):
                continue
            w = int(row.get("w", 0) or 0)
            h = int(row.get("h", 0) or 0)
            if max(w, h) and max(w, h) < 120:
                continue
            for src in raw_sources:
                if not src or not src.startswith("http"):
                    continue
                if self._is_bad_image_url(src):
                    continue
                if src not in comment_image_urls:
                    comment_image_urls.append(src)
        return "\n".join(comment_image_urls[:8])

    def _collect_comments_from_loaded_page(self, page: Page, note_url: str, max_comments: int = 200, scroll_rounds: int = 30) -> list[dict[str, Any]]:
        comments: list[dict[str, Any]] = []

        # Try scroll for loading comments. If the user manually opened the page,
        # this works on the current DOM without forcing a risky navigation.
        for _ in range(scroll_rounds):
            page.mouse.wheel(0, 1200)
            time.sleep(0.8)
            if len(comments) >= max_comments:
                break

        selector_candidates = [
            "[data-e2e='comment-item']",
            "[class*='comment-item']",
            "[class*='commentItem']",
            "li[class*='comment']",
            "div[class*='comment'][class*='item']",
        ]

        picked = None
        picked_count = 0
        for sel in selector_candidates:
            try:
                loc = page.locator(sel)
                c = loc.count()
                if c > picked_count:
                    picked = loc
                    picked_count = c
            except Exception:
                continue

        if picked is None or picked_count == 0:
            picked = page.locator("[class*='comment']")
            picked_count = picked.count()

        count = min(picked_count, max_comments * 3)
        noise_tokens = ["共", "条评论", "展开", "查看更多", "发布", "通知", "发现", "直播"]
        for i in range(count):
            it = picked.nth(i)
            try:
                raw_txt = it.inner_text()
            except Exception:
                raw_txt = ""
            txt = self._clean_text(raw_txt)
            comment_image_url = self._extract_comment_image_url(it)
            if not txt and not comment_image_url:
                continue
            if (len(txt) < 3 or len(txt) > 450) and not comment_image_url:
                continue
            if sum(1 for n in noise_tokens if n in txt) >= 3 and not comment_image_url:
                continue

            user_name = ""
            for user_sel in ["[class*='name']", "[class*='author']", "a[href*='/user/profile/']"]:
                try:
                    v = self._clean_text(it.locator(user_sel).first.inner_text(timeout=300))
                    if v:
                        user_name = v[:40]
                        break
                except Exception:
                    continue

            user_url = ""
            user_id = ""
            try:
                href = it.locator("a[href*='/user/profile/']").first.get_attribute("href")
                if href:
                    h = str(href).strip().replace("&amp;", "&")
                    if h.startswith("/"):
                        user_url = f"https://www.xiaohongshu.com{h}"
                    elif h.startswith("http"):
                        user_url = h
                    if user_url:
                        m_uid = BLOGGER_ID_RE.search(user_url)
                        if m_uid:
                            user_id = m_uid.group(1)
            except Exception:
                pass

            like_count = ""
            m_like = re.search(r"(?:赞|点赞)\s*([0-9\.万wWkK]+)", txt)
            if m_like:
                like_count = m_like.group(1)

            comment_time = ""
            m_time = re.search(r"(20\d{2}[./-]\d{1,2}[./-]\d{1,2}(?:\s+\d{1,2}:\d{1,2})?)", txt)
            if m_time:
                comment_time = m_time.group(1)
            if not comment_time:
                m_md = re.search(r"(?<!\d)(\d{1,2})[-/.月](\d{1,2})日?(?:\s*(\d{1,2}:\d{1,2}))?", txt)
                if m_md:
                    y = datetime.now().year
                    mm = int(m_md.group(1))
                    dd = int(m_md.group(2))
                    hm = (m_md.group(3) or "").strip()
                    comment_time = f"{y:04d}-{mm:02d}-{dd:02d}" + (f" {hm}" if hm else "")

            comment_body = self._normalize_comment_body(raw_txt, user_name=user_name, comment_time=comment_time)
            if not comment_body:
                comment_body = txt
            if not comment_body and comment_image_url:
                comment_body = "[图片评论]"

            fingerprint = f"{note_url}|{user_id}|{user_name}|{comment_time}|{comment_body}|{comment_image_url}"
            comment_id = f"c_{hashlib.md5(fingerprint.encode('utf-8')).hexdigest()[:16]}"
            comments.append(
                {
                    "note_url": note_url,
                    "comment_index": i + 1,
                    "comment_id": comment_id,
                    "comment_text": comment_body,
                    "comment_content": comment_body,
                    "comment_text_raw": txt,
                    "user_name": user_name,
                    "user_url": user_url,
                    "user_id": user_id,
                    "comment_image_url": comment_image_url,
                    "like_count": like_count,
                    "comment_time": comment_time,
                }
            )
            if len(comments) >= max_comments:
                break

        seen = set()
        dedup = []
        for row in comments:
            key = (
                str(row.get("note_url", "")),
                str(row.get("user_id", "")),
                str(row.get("user_name", "")),
                str(row.get("comment_time", "")),
                str(row.get("comment_text", "")),
                str(row.get("comment_image_url", "")),
            )
            if key in seen:
                continue
            seen.add(key)
            dedup.append(row)
        return dedup

    def scrape_comments(self, note_url: str, max_comments: int = 200, scroll_rounds: int = 30) -> ScrapeResult:
        page = self._new_page()
        try:
            page.goto(note_url, wait_until="domcontentloaded", timeout=90000)
            time.sleep(2)
            comments = self._collect_comments_from_loaded_page(page, note_url, max_comments=max_comments, scroll_rounds=scroll_rounds)
            return ScrapeResult(ok=True, message=f"评论抓取完成，共{len(comments)}条", data={"comments": comments})
        except Exception as e:
            return ScrapeResult(ok=False, message=f"评论抓取失败: {e}")
        finally:
            page.close()

    def scrape_comments_from_current_page(self, max_comments: int = 200, scroll_rounds: int = 30) -> ScrapeResult:
        self.start()
        page = self._active_page()
        try:
            try:
                page.bring_to_front()
            except Exception:
                pass
            note_url = page.url
            if note_url.startswith("about:") or not (
                "/explore/" in note_url or re.search(r"/user/profile/[a-zA-Z0-9]+/[a-zA-Z0-9]+", note_url)
            ):
                return ScrapeResult(ok=False, message=f"当前页面不是笔记详情页，请先在 Chromium 里打开目标笔记: {note_url}")
            body_text = self._clean_text(page.inner_text("body", timeout=3000))
            if self._is_login_or_blocked_url(note_url) or self._is_blocked_text(body_text):
                return ScrapeResult(ok=False, message=f"当前页面是登录/风控页，无法抓评论: {note_url}")
            comments = self._collect_comments_from_loaded_page(page, note_url, max_comments=max_comments, scroll_rounds=scroll_rounds)
            return ScrapeResult(ok=True, message=f"当前页面评论抓取完成，共{len(comments)}条", data={"comments": comments, "page_url": note_url})
        except Exception as e:
            return ScrapeResult(ok=False, message=f"当前页面评论抓取失败: {e}")

    def scrape_blogger(
        self,
        blogger_url: str,
        max_notes: int = 30,
        load_wait_sec: int = 18,
        keep_page_open: bool = False,
    ) -> ScrapeResult:
        page = self._active_page() if keep_page_open else self._new_page()
        try:
            page.goto(blogger_url, wait_until="domcontentloaded", timeout=90000)
            self._wait_for_page_settle(page, max_wait_sec=min(max(6, load_wait_sec), 18))
            final_url = page.url
            body_text = self._clean_text(page.inner_text("body"))
            if self._is_login_or_blocked_url(final_url):
                if ("website-login/error" in final_url) or ("website-login/captcha" in final_url) or self._is_blocked_text(body_text):
                    ok, reason = self._wait_for_manual_unblock(page, timeout_sec=180)
                    if not ok:
                        return ScrapeResult(ok=False, message=f"风控/验证码拦截且等待超时: {final_url} | {reason}")
                    self._wait_for_page_settle(page, max_wait_sec=min(max(6, load_wait_sec), 18))
                    final_url = page.url
                    body_text = self._clean_text(page.inner_text("body"))
                else:
                    ok, reason = self._wait_for_manual_unblock(page, timeout_sec=180)
                    if not ok:
                        return ScrapeResult(ok=False, message=f"未登录或登录态失效且等待超时: {final_url} | {reason}")
                    self._wait_for_page_settle(page, max_wait_sec=min(max(6, load_wait_sec), 18))
                    final_url = page.url
                    body_text = self._clean_text(page.inner_text("body"))
            if self._is_blocked_text(body_text):
                ok, reason = self._wait_for_manual_unblock(page, timeout_sec=180)
                if not ok:
                    return ScrapeResult(ok=False, message=f"页面被验证/风控拦截且等待超时: {reason}")
                self._wait_for_page_settle(page, max_wait_sec=min(max(6, load_wait_sec), 18))
                final_url = page.url
                body_text = self._clean_text(page.inner_text("body"))
                if self._is_blocked_text(body_text):
                    return ScrapeResult(ok=False, message=f"页面仍被验证/风控拦截: {final_url}")
            html, note_links = self._collect_profile_note_links(page, max_notes=max_notes, load_wait_sec=load_wait_sec)
            # Re-read body after hydration/scroll; early snapshots can miss profile fields.
            try:
                body_text = self._clean_text(page.inner_text("body", timeout=3000))
            except Exception:
                pass

            nickname = ""
            xhs_id = ""
            bio = ""
            followers = ""
            following = ""
            likes_total = ""
            avatar_url = ""
            gender = ""
            ip_address = ""
            job_tag = ""
            region_tag = ""
            birthday_tag = ""
            school_tag = ""
            pugongying_url = ""

            # heuristic extraction
            try:
                nickname = self._clean_text(page.locator("h1, [class*='name']").first.inner_text(timeout=1000))
            except Exception:
                pass

            m_id = re.search(r"小红书号[:：]\s*([a-zA-Z0-9_\-]+)", body_text)
            if m_id:
                xhs_id = m_id.group(1)
            m_bio = re.search(r"简介[:：]?\s*([^。\n]{2,120})", body_text)
            if m_bio:
                bio = m_bio.group(1)

            m_followers = re.search(r"([0-9\.万wWkK]+)\s*粉丝", body_text)
            if m_followers:
                followers = m_followers.group(1)
            m_following = re.search(r"([0-9\.万wWkK]+)\s*关注", body_text)
            if m_following:
                following = m_following.group(1)
            m_likes = re.search(r"([0-9\.万wWkK]+)\s*获赞", body_text)
            if m_likes:
                likes_total = m_likes.group(1)

            m_gender = re.search(r"(?:性别[:：]?\s*)?(男|女)", body_text)
            if m_gender:
                gender = m_gender.group(1)
            m_ip = re.search(r"IP(?:地址|属地)[:：]?\s*([\\u4e00-\\u9fa5A-Za-z]+)", body_text)
            if m_ip:
                ip_address = m_ip.group(1)
            m_job = re.search(r"职业[:：]?\s*([^\\s，。]{1,20})", body_text)
            if m_job:
                job_tag = m_job.group(1)
            m_region = re.search(r"地区[:：]?\s*([^\\s，。]{1,20})", body_text)
            if m_region:
                region_tag = m_region.group(1)
            m_bday = re.search(r"(\\d{1,2}岁|[双白金狮巨处天摩水射牛羊鱼子蟹瓶秤]+座)", body_text)
            if m_bday:
                birthday_tag = m_bday.group(1)
            m_school = re.search(r"(?:学校|毕业于)[:：]?\s*([^\\s，。]{2,30})", body_text)
            if m_school:
                school_tag = m_school.group(1)

            m_avatar = re.search(r'https://[^"\\\']+?(?:avatar|sns-avatar|profile)[^"\\\']+\\.(?:jpg|jpeg|png|webp)(?:\\?[^"\\\']*)?', html, flags=re.I)
            if m_avatar:
                avatar_url = m_avatar.group(0)
            m_pgy = re.search(r"https?://pgy\\.xiaohongshu\\.com/[^\"]+", html)
            if m_pgy:
                pugongying_url = m_pgy.group(0).strip()

            blogger_id = ""
            m = BLOGGER_ID_RE.search(blogger_url)
            if m:
                blogger_id = m.group(1)

            data = {
                "blogger_id": blogger_id,
                "blogger_url": blogger_url,
                "nickname": nickname,
                "xhs_account": xhs_id,
                "bio": bio,
                "avatar_url": avatar_url,
                "gender": gender,
                "followers": followers,
                "following": following,
                "likes_total": likes_total,
                "ip_address": ip_address,
                "job_tag": job_tag,
                "region_tag": region_tag,
                "birthday_tag": birthday_tag,
                "school_tag": school_tag,
                "pugongying_url": pugongying_url,
                "note_links": note_links,
                "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            # 若核心字段全部为空，通常是验证码页/重定向页被误当成博主页。
            has_core = bool(nickname or xhs_id or bio or followers or following or likes_total or note_links)
            if not has_core:
                return ScrapeResult(ok=False, message=f"博主页字段提取为空（疑似验证码/风控页）: {final_url}")
            return ScrapeResult(ok=True, message="博主抓取成功", data=data)
        except Exception as e:
            return ScrapeResult(ok=False, message=f"博主抓取失败: {e}")
        finally:
            if not keep_page_open:
                page.close()

    def search_notes(self, keyword: str, limit: int = 30) -> ScrapeResult:
        page = self._new_page()
        try:
            url = f"https://www.xiaohongshu.com/search_result?keyword={keyword}"
            page.goto(url, wait_until="domcontentloaded", timeout=90000)
            time.sleep(2)
            links = []
            for _ in range(12):
                html = page.content()
                found = re.findall(r"https://www\.xiaohongshu\.com/explore/[a-zA-Z0-9]+", html)
                links.extend(found)
                links = sorted(set(links))
                if len(links) >= limit:
                    break
                page.mouse.wheel(0, 1600)
                time.sleep(0.8)
            return ScrapeResult(ok=True, message=f"搜索到{len(links[:limit])}条笔记链接", data={"keyword": keyword, "note_links": links[:limit]})
        except Exception as e:
            return ScrapeResult(ok=False, message=f"笔记搜索失败: {e}")
        finally:
            page.close()

    def search_bloggers(self, keyword: str, limit: int = 30) -> ScrapeResult:
        page = self._new_page()
        try:
            url = f"https://www.xiaohongshu.com/search_result?keyword={keyword}&type=user"
            page.goto(url, wait_until="domcontentloaded", timeout=90000)
            time.sleep(2)
            links = []
            for _ in range(12):
                html = page.content()
                found = re.findall(r"https://www\.xiaohongshu\.com/user/profile/[a-zA-Z0-9]+", html)
                links.extend(found)
                links = sorted(set(links))
                if len(links) >= limit:
                    break
                page.mouse.wheel(0, 1600)
                time.sleep(0.8)
            return ScrapeResult(ok=True, message=f"搜索到{len(links[:limit])}个博主链接", data={"keyword": keyword, "blogger_links": links[:limit]})
        except Exception as e:
            return ScrapeResult(ok=False, message=f"博主搜索失败: {e}")
        finally:
            page.close()
