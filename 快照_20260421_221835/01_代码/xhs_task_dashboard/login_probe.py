from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright

from scraper import XHSScraper


def main() -> None:
    p = argparse.ArgumentParser(description="小红书登录与博主页访问预检")
    p.add_argument("--input", required=True, help="博主Excel路径")
    p.add_argument("--sheet", default="Sheet1", help="工作表名")
    p.add_argument("--link-column", default="博主链接", help="博主链接列名")
    p.add_argument("--profile-dir", default="./.xhs_profile", help="Playwright持久化目录")
    p.add_argument("--wait-sec", type=int, default=120, help="非交互环境下等待用户完成登录/验证的秒数")
    args = p.parse_args()

    xlsx = Path(args.input).expanduser().resolve()
    df = pd.read_excel(xlsx, sheet_name=args.sheet)
    links = [str(x).strip() for x in df[args.link_column].dropna().tolist() if str(x).strip().startswith("http")]
    if not links:
        raise SystemExit("未在Excel中找到可用博主链接")
    sample_link = links[0]

    # 先显式打开登录页，方便用户扫码登录
    profile_dir = str(Path(args.profile_dir).expanduser().resolve())
    print("正在打开小红书登录页，请在弹窗浏览器完成扫码登录。")
    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=profile_dir,
            headless=False,
            viewport={"width": 1400, "height": 900},
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = ctx.new_page()
        page.goto(
            "https://www.xiaohongshu.com/login?redirectPath=https%3A%2F%2Fwww.xiaohongshu.com%2Fexplore",
            wait_until="domcontentloaded",
            timeout=90000,
        )
        print("登录完成后，回到终端按回车继续检测；如果当前终端不可输入，会自动等待后继续：", end="", flush=True)
        try:
            input()
        except EOFError:
            wait_sec = max(30, int(args.wait_sec))
            print(f"\n[login_probe] 当前终端不可交互，改为等待 {wait_sec} 秒，避免浏览器闪退。")
            deadline = time.time() + wait_sec
            while time.time() < deadline:
                time.sleep(2)
        try:
            page.close()
        except Exception:
            pass
        ctx.close()

    scraper = XHSScraper(profile_dir=args.profile_dir, headless=False)
    try:
        login_res = scraper.ensure_login(timeout_sec=120)
        print(f"[login_check] ok={login_res.ok} msg={login_res.message}")

        probe_res = scraper.probe_profile_access(sample_link)
        print(f"[profile_probe] ok={probe_res.ok} msg={probe_res.message}")
        if probe_res.data:
            print(f"[profile_probe] data={probe_res.data}")
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
