from __future__ import annotations

import argparse
import time
from pathlib import Path

from playwright.sync_api import sync_playwright


def _is_blocked(url: str, body_text: str) -> bool:
    u = (url or "").lower()
    t = (body_text or "").lower()
    url_signals = ["/login", "website-login/error", "website-login/captcha", "captcha", "verifyuuid=", "verifybiz="]
    text_signals = ["验证", "captcha", "security verification", "访问受限", "请先登录", "ip存在风险", "反馈", "feedback"]
    return any(s in u for s in url_signals) or any(s in t for s in text_signals)


def main() -> None:
    p = argparse.ArgumentParser(description="验证码恢复助手（人工完成后自动返回成功）")
    p.add_argument("--profile-dir", default="./.xhs_profile")
    p.add_argument("--target-url", default="https://www.xiaohongshu.com/explore")
    p.add_argument(
        "--probe-profile-url",
        default="",
        help="用于校验登录态恢复是否可访问博主页（建议传一个可公开博主主页链接）",
    )
    p.add_argument("--wait-sec", type=int, default=180)
    args = p.parse_args()

    profile_dir = str(Path(args.profile_dir).expanduser().resolve())
    wait_sec = max(30, int(args.wait_sec))

    print("[captcha_recover] 已打开验证窗口，请在浏览器内完成验证码/登录验证，完成后会自动继续。")
    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=profile_dir,
            headless=False,
            viewport={"width": 1440, "height": 920},
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = ctx.new_page()
        page.goto(args.target_url, wait_until="domcontentloaded", timeout=90000)

        ok = False
        start = time.time()
        # 只做状态检测，不循环跳转，避免页面闪烁影响人工登录。
        while time.time() - start < wait_sec:
            try:
                cur_url = page.url
                body = page.inner_text("body") if page.locator("body").count() > 0 else ""
                if _is_blocked(cur_url, body):
                    time.sleep(2)
                    continue

                # 关键修复：不仅看当前页，可选再探测博主页是否可访问
                probe_url = (args.probe_profile_url or "").strip()
                if probe_url:
                    check = ctx.new_page()
                    try:
                        check.goto(probe_url, wait_until="domcontentloaded", timeout=30000)
                        check.wait_for_timeout(800)
                        p_url = check.url
                        p_body = check.inner_text("body") if check.locator("body").count() > 0 else ""
                        if _is_blocked(p_url, p_body):
                            time.sleep(2)
                            continue
                    finally:
                        try:
                            check.close()
                        except Exception:
                            pass

                ok = True
                break
            except Exception:
                pass
            time.sleep(2)

        try:
            page.close()
        except Exception:
            pass
        ctx.close()

    if ok:
        print("[captcha_recover] 验证通过，已恢复可访问状态。")
        raise SystemExit(0)
    print("[captcha_recover] 超时未恢复，请手动重试验证后继续。")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
