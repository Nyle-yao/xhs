from __future__ import annotations

import argparse
import json
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import pandas as pd

from exporters import (
    BLOGGER_EXPORT_FIELD_MAP,
    COMMENT_EXPORT_FIELD_MAP,
    NOTE_EXPORT_FIELD_MAP,
    build_blogger_export_rows,
    build_comment_export_rows,
    build_note_export_rows,
)
from scraper import XHSScraper


PROFILE_ID_RE = re.compile(r"/user/profile/([a-zA-Z0-9]+)")

PROMOTE_KEYWORDS = [
    "主推",
    "重点",
    "推荐",
    "看好",
    "加仓",
    "上车",
    "定投",
    "核心",
    "布局",
    "继续拿",
    "可关注",
]
COMPARE_KEYWORDS = [
    "对比",
    "比较",
    "不如",
    "优于",
    "vs",
    "pk",
    "替代",
    "二选一",
    "横评",
]
POSITIVE_KEYWORDS = [
    "看好",
    "上涨",
    "机会",
    "稳",
    "回暖",
    "加仓",
    "增持",
    "突破",
    "反弹",
    "优选",
]
NEGATIVE_KEYWORDS = [
    "风险",
    "回撤",
    "下跌",
    "减仓",
    "卖出",
    "避雷",
    "亏损",
    "不建议",
    "谨慎",
    "震荡",
]

FUND_CONTEXT_KEYWORDS = [
    "基金",
    "债",
    "混合",
    "指数",
    "etf",
    "场外",
    "场内",
    "加仓",
    "减仓",
    "定投",
    "持有",
    "理财",
]

FUND_NAME_REGEX = re.compile(
    r"([A-Za-z\u4e00-\u9fa5]{2,30}(?:ETF联接[A-C]?|ETF|混合[A-C]?|债券[A-C]?|指数[A-C]?|基金[A-C]?|QDII[A-C]?))"
)
FUND_NAME_STOPWORDS = {
    "基金经理",
    "基金公司",
    "基金定投",
    "基金分析",
    "基金组合",
    "基金小白",
    "基金知识",
    "基金理财",
    "公募基金",
    "私募基金",
    "行业基金",
    "指数基金",
    "债券基金",
    "混债基金",
    "货币基金",
    "宽指基金",
    "优秀行业基金",
    "一般基金",
    "先把所有的基金",
    "基金",
    "etf基金",
    "ETF基金",
    "etf",
    "ETF",
    "场外基金",
    "场内基金",
    "A类基金",
    "C类基金",
    "A类基金和C类基金",
    "早点跟上指数",
}
NON_FUND_PHRASE_TOKENS = {
    "个人",
    "自己",
    "我们",
    "你们",
    "你说",
    "我说",
    "买了",
    "还有",
    "就是",
    "这个",
    "那个",
    "如果",
    "因为",
    "所以",
    "建议",
    "推荐",
    "收益",
    "回撤",
    "涨跌",
    "板块",
}
ETF_BENCHMARK_TOKENS = {
    "中证",
    "国证",
    "上证",
    "沪深",
    "纳指",
    "恒生",
    "标普",
    "创业板",
    "科创",
}
GENERIC_ALIAS_STOPWORDS = {
    "基金",
    "理财",
    "定投",
    "债券",
    "混合",
    "指数",
    "etf",
    "ETF",
    "qdii",
    "QDII",
}


@dataclass
class FundAliasItem:
    fund_code: str
    fund_name: str
    aliases: list[str]


FUND_MENTION_COLUMNS = [
    "record_id",
    "entity_type",
    "blogger_id",
    "blogger_name",
    "note_id",
    "comment_index",
    "fund_code",
    "fund_name",
    "alias_hit",
    "match_type",
    "confidence",
    "mention_role",
    "sentiment",
    "source_field",
    "snippet",
    "detected_at",
]


def load_fund_aliases(path: Path) -> list[FundAliasItem]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    out: list[FundAliasItem] = []
    for row in data:
        code = str(row.get("fund_code", "")).strip()
        name = str(row.get("fund_name", "")).strip()
        aliases = [str(x).strip() for x in (row.get("aliases") or []) if str(x).strip()]
        if not code and not name:
            continue
        out.append(FundAliasItem(fund_code=code, fund_name=name, aliases=aliases))
    return out


def normalize_profile_link(link: str | None, blogger_id: str | None = None) -> tuple[str | None, str | None]:
    raw = (link or "").strip()
    bid = (blogger_id or "").strip()
    if raw:
        m = PROFILE_ID_RE.search(raw)
        if m:
            bid = m.group(1)
            return f"https://www.xiaohongshu.com/user/profile/{bid}", bid
    if bid:
        return f"https://www.xiaohongshu.com/user/profile/{bid}", bid
    return None, None


def _normalize_for_match(v: str) -> str:
    s = str(v or "").strip().lower()
    s = unquote(s)
    s = s.replace("（", "(").replace("）", ")")
    s = re.sub(r"[^\w\u4e00-\u9fa5]+", "", s)
    return s


def _expand_aliases_for_match(code: str, name: str, aliases: list[str]) -> list[str]:
    raw = [name, code, *(aliases or [])]
    out: list[str] = []
    seen: set[str] = set()
    for x in raw:
        s = str(x or "").strip()
        if not s:
            continue
        vars_ = {s}
        vars_.add(re.sub(r"[A-E]$", "", s))
        vars_.add(re.sub(r"（?[A-E]类）?$", "", s))
        vars_.add(s.replace("ETF联接", "ETF"))
        vars_.add(s.replace("联接", ""))
        vars_.add(s.replace("基金", ""))
        for v in vars_:
            v = str(v or "").strip()
            if not v or v in seen:
                continue
            seen.add(v)
            out.append(v)
    return out


def detect_fund_mentions(
    text: str,
    source_field: str,
    note_id: str,
    blogger_id: str,
    blogger_name: str,
    fund_aliases: list[FundAliasItem],
) -> list[dict[str, Any]]:
    t_raw = (text or "").strip()
    if not t_raw:
        return []
    try:
        t = unquote(t_raw)
    except Exception:
        t = t_raw

    tl = t.lower()

    def _role_and_sentiment(txt_lower: str) -> tuple[str, str, float]:
        promote_score = sum(1 for k in PROMOTE_KEYWORDS if k in txt_lower)
        compare_score = sum(1 for k in COMPARE_KEYWORDS if k in txt_lower)
        pos_score = sum(1 for k in POSITIVE_KEYWORDS if k in txt_lower)
        neg_score = sum(1 for k in NEGATIVE_KEYWORDS if k in txt_lower)

        if promote_score >= compare_score + 1 and promote_score > 0:
            role = "主推"
        elif compare_score > 0:
            role = "对比"
        else:
            role = "提及"

        if pos_score >= neg_score + 1 and pos_score > 0:
            sentiment = "正向"
        elif neg_score >= pos_score + 1 and neg_score > 0:
            sentiment = "负向"
        else:
            sentiment = "中性"

        # 基础置信度：命中强度 + 语义信号强度
        signal = min(0.2, (promote_score + compare_score + pos_score + neg_score) * 0.03)
        confidence = min(0.98, 0.78 + signal)
        return role, sentiment, confidence

    role, sentiment, semantic_conf = _role_and_sentiment(tl)

    def _norm_code(v: str) -> str:
        s = str(v or "").strip()
        if s.isdigit():
            return s.zfill(6)
        return s

    code_name_map = {str(f.fund_code).zfill(6): str(f.fund_name or "").strip() for f in fund_aliases if f.fund_code}
    alias_to_fund: dict[str, tuple[str, str]] = {}
    alias_norm_to_fund: dict[str, tuple[str, str, str]] = {}
    for f in fund_aliases:
        code = _norm_code(str(f.fund_code or ""))
        name = str(f.fund_name or "").strip()
        if not code:
            continue
        candidates = _expand_aliases_for_match(code, name, f.aliases)
        for c in candidates:
            s = str(c or "").strip()
            if not s:
                continue
            alias_to_fund[s] = (code, name)
            sn = _normalize_for_match(s)
            if not sn:
                continue
            if sn in GENERIC_ALIAS_STOPWORDS:
                continue
            if len(sn) < 4 and not sn.isdigit():
                continue
            alias_norm_to_fund[sn] = (code, name, s)

    brand_tokens: set[str] = set()
    for f in fund_aliases:
        fn = str(f.fund_name or "").strip()
        m = re.match(r"^([\u4e00-\u9fa5]{2,4})", fn)
        if m:
            brand_tokens.add(m.group(1))

    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    matched_codes: set[str] = set()
    t_norm = _normalize_for_match(t)
    for fund in fund_aliases:
        candidates = _expand_aliases_for_match(str(fund.fund_code or ""), str(fund.fund_name or ""), fund.aliases)
        for a in candidates:
            alias = (a or "").strip()
            if not alias:
                continue
            # 避免短数字（如 86）在URL编码串里误命中
            if alias.isdigit() and len(alias) != 6:
                continue
            if alias in t:
                k = (fund.fund_code, alias, source_field)
                if k in seen:
                    continue
                seen.add(k)
                code_norm = _norm_code(fund.fund_code)
                out.append(
                    {
                        "record_id": f"{note_id}_{len(out)+1}",
                        "entity_type": "note",
                        "blogger_id": blogger_id,
                        "blogger_name": blogger_name,
                        "note_id": note_id,
                        "comment_index": "",
                        "fund_code": code_norm,
                        "fund_name": fund.fund_name,
                        "alias_hit": alias,
                        "match_type": "alias_exact",
                        "confidence": min(
                            0.99,
                            (0.95 if alias == fund.fund_code else 0.85) * 0.7 + semantic_conf * 0.3,
                        ),
                        "mention_role": role,
                        "sentiment": sentiment,
                        "source_field": source_field,
                        "snippet": t[:200],
                        "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
                if code_norm:
                    matched_codes.add(code_norm)

    # 弱匹配：归一化别名命中（容忍符号/空格差异）
    for an, (code, name, alias_raw) in alias_norm_to_fund.items():
        if code in matched_codes:
            continue
        if an.isdigit() or len(an) < 4:
            continue
        if an in t_norm:
            key = (code, alias_raw, source_field)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "record_id": f"{note_id}_{len(out)+1}",
                    "entity_type": "note",
                    "blogger_id": blogger_id,
                    "blogger_name": blogger_name,
                    "note_id": note_id,
                    "comment_index": "",
                    "fund_code": code,
                    "fund_name": name,
                    "alias_hit": alias_raw,
                    "match_type": "alias_norm",
                    "confidence": round(min(0.92, 0.58 * 0.7 + semantic_conf * 0.3), 4),
                    "mention_role": role,
                    "sentiment": sentiment,
                    "source_field": source_field,
                    "snippet": t[:200],
                    "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            matched_codes.add(code)

    # 兜底：正则提取基金名称（词库不足时仍产出可分析实体）
    name_seen: set[str] = set()
    for mname in FUND_NAME_REGEX.findall(t):
        fname = str(mname or "").strip()
        if not fname or fname in name_seen:
            continue
        if fname in FUND_NAME_STOPWORDS:
            continue
        if len(fname) < 4 or len(fname) > 20:
            continue
        if any(tok in fname for tok in NON_FUND_PHRASE_TOKENS):
            continue
        if not any(bt in fname for bt in brand_tokens):
            up = fname.upper()
            if "ETF" not in up and "QDII" not in up:
                continue
            if not any(k in fname for k in ETF_BENCHMARK_TOKENS):
                continue
        name_seen.add(fname)
        mapped = alias_to_fund.get(fname)
        mapped_code = mapped[0] if mapped else ""
        mapped_name = mapped[1] if mapped else fname
        key = (mapped_code, fname, source_field)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "record_id": f"{note_id}_{len(out)+1}",
                "entity_type": "note",
                "blogger_id": blogger_id,
                "blogger_name": blogger_name,
                "note_id": note_id,
                "comment_index": "",
                "fund_code": mapped_code,
                "fund_name": mapped_name,
                "alias_hit": fname,
                "match_type": "name_regex_mapped" if mapped else "name_regex",
                "confidence": 0.74 if mapped else 0.62,
                "mention_role": role,
                "sentiment": sentiment,
                "source_field": source_field,
                "snippet": t[:200],
                "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    # 兜底：识别6位基金代码（需要上下文）
    for m in re.finditer(r"(?<!\d)(\d{6})(?!\d)", t):
        code = _norm_code(m.group(1))
        if code in matched_codes:
            continue
        if code not in code_name_map:
            continue
        left = max(0, m.start() - 14)
        right = min(len(t), m.end() + 14)
        ctx = t[left:right].lower()
        if not any(k in ctx for k in FUND_CONTEXT_KEYWORDS):
            continue
        key = (code, code, source_field)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "record_id": f"{note_id}_{len(out)+1}",
                "entity_type": "note",
                "blogger_id": blogger_id,
                "blogger_name": blogger_name,
                "note_id": note_id,
                "comment_index": "",
                "fund_code": code,
                "fund_name": code_name_map.get(code, ""),
                "alias_hit": code,
                "match_type": "code_regex",
                "confidence": 0.72,
                "mention_role": role,
                "sentiment": sentiment,
                "source_field": source_field,
                "snippet": t[:200],
                "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    # 最末兜底：仅当完全空结果时，保守保留候选（用于词库扩充）
    if not out:
        patt = re.compile(r"([A-Za-z0-9\u4e00-\u9fa5]{2,24}(?:基金|债券|混合|指数|ETF))")
        blacklist = {"基金", "理财", "支付宝理财", "基金小白", "理财小白", "我的理财日记"}
        for m in patt.finditer(t):
            cand = str(m.group(1) or "").strip()
            if not cand or cand in blacklist:
                continue
            key = ("", cand, source_field)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "record_id": f"{note_id}_{len(out)+1}",
                    "entity_type": "note",
                    "blogger_id": blogger_id,
                    "blogger_name": blogger_name,
                    "note_id": note_id,
                    "comment_index": "",
                    "fund_code": "",
                    "fund_name": cand,
                    "alias_hit": cand,
                    "match_type": "candidate_pattern",
                    "confidence": 0.45,
                    "mention_role": role,
                    "sentiment": sentiment,
                    "source_field": source_field,
                    "snippet": t[:200],
                    "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="从博主Excel批量抓取博主/笔记/评论，并输出多表结果")
    p.add_argument("--input", required=True, help="输入Excel路径")
    p.add_argument("--sheet", default="Sheet1", help="工作表名")
    p.add_argument("--link-column", default="博主链接", help="博主链接列名")
    p.add_argument("--id-column", default="博主ID", help="博主ID列名")
    p.add_argument("--nickname-column", default="博主昵称", help="博主昵称列名")
    p.add_argument("--profile-dir", default="./.xhs_profile", help="登录态目录")
    p.add_argument("--headless", action="store_true", help="无头模式")
    p.add_argument("--max-bloggers", type=int, default=0, help="最多抓取博主数，0=全部")
    p.add_argument("--max-notes-per-blogger", type=int, default=10, help="每个博主最大笔记数")
    p.add_argument(
        "--include-comments",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="是否抓评论（默认开启，可用 --no-include-comments 关闭）",
    )
    p.add_argument("--max-comments-per-note", type=int, default=80, help="每篇评论最大数")
    p.add_argument("--comment-scroll-rounds", type=int, default=10, help="评论区滚动轮数（越大越慢）")
    p.add_argument("--max-note-age-days", type=int, default=0, help="只分析近N天笔记；0=不过滤")
    p.add_argument(
        "--stop-after-old-notes",
        type=int,
        default=3,
        help="同一博主连续命中过期笔记达到N篇后停止继续翻该博主笔记；0=不提前停止",
    )
    p.add_argument("--blogger-sleep-ms", type=int, default=900, help="每个博主抓取后的基础休眠毫秒")
    p.add_argument("--note-sleep-ms", type=int, default=450, help="每篇笔记抓取后的基础休眠毫秒")
    p.add_argument("--profile-load-wait-sec", type=int, default=18, help="博主页动态加载等待秒数，网络慢时可调高")
    p.add_argument("--retry-times", type=int, default=2, help="单个请求失败后的重试次数")
    p.add_argument("--retry-backoff-ms", type=int, default=1200, help="重试退避基础毫秒")
    p.add_argument("--block-cooldown-sec", type=float, default=8.0, help="命中访问频繁/风控后的基础冷却秒数")
    p.add_argument("--block-cooldown-jitter-sec", type=float, default=4.0, help="风控冷却抖动秒数")
    p.add_argument("--block-cooldown-long-sec", type=float, default=25.0, help="连续风控后的长冷却秒数")
    p.add_argument("--block-escalate-threshold", type=int, default=5, help="连续风控触发长冷却与浏览器上下文重建阈值")
    p.add_argument("--reprobe-every", type=int, default=12, help="每N个博主做一次访问预检，0=不预检")
    p.add_argument("--reprobe-cooldown-sec", type=float, default=20.0, help="预检失败后的冷却秒数")
    p.add_argument("--max-risk-fails-per-blogger", type=int, default=3, help="单博主触发风控失败上限，达到后跳过该博主剩余笔记")
    p.add_argument("--strict-preflight", action="store_true", help="严格预检：预检失败时直接退出")
    p.add_argument(
        "--allow-degraded-run",
        action="store_true",
        help="允许降级运行：当全部预检样本失败时仍继续（默认关闭，质量优先）",
    )
    p.add_argument("--max-runtime-sec", type=int, default=0, help="批次最大运行秒数，超时后保存已抓结果并结束；0=不限制")
    p.add_argument("--fund-aliases", default="./fund_aliases.json", help="基金别名库json")
    p.add_argument("--output-dir", default="./outputs", help="输出目录")
    args = p.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    fund_aliases = load_fund_aliases(Path(args.fund_aliases).expanduser().resolve())

    df = pd.read_excel(input_path, sheet_name=args.sheet)
    rows: list[dict[str, Any]] = df.to_dict(orient="records")

    source_list: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for r in rows:
        raw_link = str(r.get(args.link_column, "")).strip()
        raw_id = str(r.get(args.id_column, "")).strip()
        nick = str(r.get(args.nickname_column, "")).strip()
        profile_link, bid = normalize_profile_link(raw_link, raw_id)
        if not profile_link or not bid:
            continue
        if bid in seen_ids:
            continue
        seen_ids.add(bid)
        source_list.append(
            {
                "source_blogger_id": raw_id,
                "source_blogger_nickname": nick,
                "source_blogger_link": raw_link,
                "profile_link": profile_link,
                "profile_id": bid,
            }
        )
    if args.max_bloggers > 0:
        source_list = source_list[: args.max_bloggers]

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = f"blogger_batch_{ts}"

    crawler = XHSScraper(profile_dir=args.profile_dir, headless=args.headless)
    blogger_success: list[dict[str, Any]] = []
    blogger_failed: list[dict[str, Any]] = []
    note_raw: list[dict[str, Any]] = []
    comment_raw: list[dict[str, Any]] = []
    fund_mentions: list[dict[str, Any]] = []
    # 缓存与去重（提升效率，降低风控触发）
    note_cache: dict[str, tuple[bool, dict[str, Any] | None, str]] = {}
    comment_cache: dict[str, tuple[bool, list[dict[str, Any]], str]] = {}
    note_seen_links: set[str] = set()
    unique_note_link_seen: set[str] = set()
    note_cache_hit_count = 0
    comment_cache_hit_count = 0
    note_cache_miss_count = 0
    comment_cache_miss_count = 0
    skipped_duplicate_note_count = 0
    runtime_state: dict[str, int] = {
        "risk_hits_streak": 0,
        "risk_event_count": 0,
        "context_rebuild_count": 0,
        "reprobe_failed_count": 0,
    }

    total = len(source_list)
    if total == 0:
        raise SystemExit("输入表未解析出可用博主链接")

    # 抓取前预检：若样本全部失败，默认直接退出，避免产出低质量数据。
    preflight_sample = source_list[: min(5, len(source_list))]
    preflight_ok = False
    preflight_msgs: list[str] = []
    for sp in preflight_sample:
        pr = crawler.probe_profile_access(sp["profile_link"])
        preflight_msgs.append(f"{sp['profile_id']}: {'ok' if pr.ok else 'fail'} | {pr.message}")
        if pr.ok:
            preflight_ok = True
            break
    if not preflight_ok:
        msg = "; ".join(preflight_msgs[:3])
        if args.strict_preflight or (not args.allow_degraded_run):
            crawler.close()
            raise SystemExit(f"[preflight_failed] {msg}")
        print(f"[preflight_warn] 全部预检样本均失败，当前按降级模式继续。samples={msg}")

    def _norm_name(v: str) -> str:
        s = str(v or "").strip().lower()
        s = re.sub(r"\s+", "", s)
        s = re.sub(r"[^\w\u4e00-\u9fa5]", "", s)
        return s

    def _sleep_with_jitter(base_ms: int) -> None:
        if base_ms <= 0:
            return
        jitter = random.randint(0, max(80, int(base_ms * 0.3)))
        time.sleep((base_ms + jitter) / 1000.0)

    def _is_risk_message(msg: str) -> bool:
        m = (msg or "").lower()
        signals = [
            "风控",
            "访问频繁",
            "website-login/error",
            "验证",
            "受限",
            "error_code=300013",
        ]
        return any(s in m for s in signals)

    def _with_retry(action_name: str, fn):
        last = None
        attempts = max(0, args.retry_times) + 1
        for attempt in range(1, attempts + 1):
            try:
                r = fn()
            except Exception as e:
                r = None
                last = f"exception: {e}"
            else:
                if r and getattr(r, "ok", False):
                    runtime_state["risk_hits_streak"] = max(0, runtime_state["risk_hits_streak"] - 1)
                    return r
                last = getattr(r, "message", "unknown_error") if r else "empty_result"
            if attempt < attempts:
                if _is_risk_message(str(last)):
                    runtime_state["risk_event_count"] += 1
                    runtime_state["risk_hits_streak"] += 1
                    cooldown_sec = float(args.block_cooldown_sec) + random.uniform(
                        0, max(0.0, float(args.block_cooldown_jitter_sec))
                    )
                    if runtime_state["risk_hits_streak"] >= max(1, int(args.block_escalate_threshold)):
                        cooldown_sec = max(
                            cooldown_sec,
                            float(args.block_cooldown_long_sec)
                            + random.uniform(0, max(3.0, float(args.block_cooldown_jitter_sec))),
                        )
                        runtime_state["risk_hits_streak"] = 0
                        runtime_state["context_rebuild_count"] += 1
                        print(
                            f"[anti_block] {action_name} consecutive risk reached threshold; "
                            f"rebuild browser context and sleep={cooldown_sec:.1f}s"
                        )
                        crawler.close()
                    else:
                        print(
                            f"[anti_block] {action_name} risk failure attempt={attempt}/{attempts}; "
                            f"sleep={cooldown_sec:.1f}s"
                        )
                    time.sleep(cooldown_sec)
                else:
                    backoff_ms = args.retry_backoff_ms * attempt
                    print(f"[retry] {action_name} attempt={attempt}/{attempts} failed: {last}; sleep={backoff_ms}ms")
                    time.sleep(backoff_ms / 1000.0)
        return r if "r" in locals() and r else type("Tmp", (), {"ok": False, "message": str(last), "data": None})()

    def _is_note_low_quality(nd: dict[str, Any]) -> bool:
        title = str(nd.get("title", "") or "").strip()
        content = str(nd.get("content", "") or "").strip()
        like_cnt = str(nd.get("like_count", "") or "").strip()
        collect_cnt = str(nd.get("collect_count", "") or "").strip()
        comment_cnt = str(nd.get("comment_count", "") or "").strip()
        publish_time = str(nd.get("publish_time", "") or "").strip()
        note_id = str(nd.get("note_id", "") or "").strip()
        image_urls = nd.get("image_urls") or []
        video_urls = nd.get("video_urls") or []
        compact = re.sub(r"\\s+", "", content)
        if compact in {"发现直播发布通知", "发现直播发布消息通知", "发现直播"} and not (
            like_cnt or collect_cnt or comment_cnt or publish_time
        ):
            return True
        if (not note_id) and (not publish_time) and (not like_cnt) and (not collect_cnt) and (not comment_cnt):
            # 缺少标识与核心指标，通常为跳转页/低质页
            return True
        if (len(title) + len(content) < 6) and not (image_urls or video_urls):
            return True
        return False

    def _parse_note_datetime(value: str) -> datetime | None:
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

    def _infer_datetime_from_note_id_or_url(nd: dict[str, Any], note_url: str = "") -> datetime | None:
        raw = str(nd.get("note_id", "") or "").strip()
        if not raw:
            m = re.search(r"/(?:explore|profile/[a-zA-Z0-9]+)/([0-9a-fA-F]{8,})", str(note_url or ""))
            raw = m.group(1) if m else ""
        if len(raw) < 8 or not re.fullmatch(r"[0-9a-fA-F]{8}.*", raw):
            return None
        try:
            dt = datetime.fromtimestamp(int(raw[:8], 16))
        except Exception:
            return None
        if 2020 <= dt.year <= datetime.now().year + 1:
            return dt
        return None

    def _is_older_than_window(nd: dict[str, Any], note_url: str = "") -> bool:
        if int(args.max_note_age_days) <= 0:
            return False
        dt = _infer_datetime_from_note_id_or_url(nd, note_url) or _parse_note_datetime(str(nd.get("publish_time", "") or ""))
        # 取不到发布时间时先保留，避免误删近期但页面未解析到日期的笔记。
        if dt is None:
            return False
        cutoff = datetime.now() - timedelta(days=int(args.max_note_age_days))
        return dt < cutoff

    started_ts = time.time()
    for i, src in enumerate(source_list, start=1):
        if int(args.max_runtime_sec) > 0 and (time.time() - started_ts) >= int(args.max_runtime_sec):
            blogger_failed.append(
                {
                    "seq": i,
                    "profile_link": src.get("profile_link", ""),
                    "note_link": "",
                    "message": f"runtime_cutoff: reached max_runtime_sec={int(args.max_runtime_sec)}",
                }
            )
            print(f"[runtime_cutoff] reached max_runtime_sec={int(args.max_runtime_sec)}, stop at blogger #{i}")
            break
        if args.reprobe_every > 0 and i > 1 and (i - 1) % int(args.reprobe_every) == 0:
            rp = crawler.probe_profile_access(src["profile_link"])
            if not rp.ok:
                runtime_state["reprobe_failed_count"] += 1
                cooldown_sec = float(args.reprobe_cooldown_sec) + random.uniform(0, 3.0)
                print(
                    f"[reprobe] failed at blogger #{i} ({src['profile_id']}): {rp.message}; "
                    f"rebuild context and sleep={cooldown_sec:.1f}s"
                )
                crawler.close()
                time.sleep(cooldown_sec)

        link = src["profile_link"]
        rb = _with_retry(
            f"scrape_blogger:{src['profile_id']}",
            lambda: crawler.scrape_blogger(
                link,
                max_notes=args.max_notes_per_blogger,
                load_wait_sec=max(3, int(args.profile_load_wait_sec)),
            ),
        )
        bd = rb.data or {}
        nickname = str(bd.get("nickname", "")).strip()
        note_links = bd.get("note_links") or []
        useful = bool(nickname or note_links or bd.get("followers") or bd.get("likes_total"))
        if rb.ok and useful:
            record = {**src, **bd, "seq": i}
            blogger_success.append(record)
            note_links = list(dict.fromkeys([str(x).strip() for x in note_links if str(x).strip()]))
            blogger_risk_fails = 0
            old_note_streak = 0
            for nlink in note_links:
                unique_note_link_seen.add(nlink)
                if nlink in note_cache:
                    ok_cached, data_cached, msg_cached = note_cache[nlink]
                    note_cache_hit_count += 1
                    rn = type("Tmp", (), {"ok": ok_cached, "data": data_cached, "message": msg_cached})()
                else:
                    rn = _with_retry(f"scrape_note:{nlink}", lambda: crawler.scrape_note(nlink))
                    note_cache[nlink] = (bool(getattr(rn, "ok", False)), dict((rn.data or {})) if rn.data else None, str(rn.message))
                    note_cache_miss_count += 1

                if rn.ok and rn.data:
                    nd = rn.data
                    if _is_older_than_window(nd, nlink):
                        old_note_streak += 1
                        inferred_dt = _infer_datetime_from_note_id_or_url(nd, nlink)
                        shown_time = inferred_dt.strftime("%Y-%m-%d %H:%M") if inferred_dt else str(nd.get("publish_time", "") or "")
                        blogger_failed.append(
                            {
                                "seq": i,
                                "profile_link": link,
                                "note_link": nlink,
                                "message": f"note_skipped_out_of_window: 发布时间={shown_time}; window_days={int(args.max_note_age_days)}",
                            }
                        )
                        if int(args.stop_after_old_notes) > 0 and old_note_streak >= int(args.stop_after_old_notes):
                            print(
                                f"[recent_window] blogger={src['profile_id']} consecutive_old_notes={old_note_streak}; "
                                f"stop remaining notes for this blogger"
                            )
                            break
                        _sleep_with_jitter(args.note_sleep_ms)
                        continue
                    old_note_streak = 0
                    if nlink in note_seen_links:
                        skipped_duplicate_note_count += 1
                        continue
                    note_seen_links.add(nlink)
                    if _is_note_low_quality(nd):
                        blogger_failed.append(
                            {
                                "seq": i,
                                "profile_link": link,
                                "note_link": nlink,
                                "message": "note_skipped_low_quality: 笔记字段有效性不足，已跳过",
                            }
                        )
                        _sleep_with_jitter(args.note_sleep_ms)
                        continue
                    nd["blogger_id"] = nd.get("blogger_id") or bd.get("blogger_id") or src["profile_id"]
                    nd["blogger_name"] = nd.get("blogger_name") or nickname or src["source_blogger_nickname"]
                    nd["blogger_url"] = nd.get("blogger_url") or link
                    nd["source_keyword"] = ""
                    note_raw.append(nd)

                    nid = str(nd.get("note_id", ""))
                    bname = str(nd.get("blogger_name", ""))
                    bid = str(nd.get("blogger_id", ""))
                    for field in ["title", "content", "note_topic"]:
                        fund_mentions.extend(
                            detect_fund_mentions(
                                str(nd.get(field, "")),
                                field,
                                nid,
                                bid,
                                bname,
                                fund_aliases,
                            )
                        )
                    if args.include_comments:
                        if nlink in comment_cache:
                            okc, rowsc, msgc = comment_cache[nlink]
                            comment_cache_hit_count += 1
                            rc = type("Tmp", (), {"ok": okc, "data": {"comments": rowsc}, "message": msgc})()
                        else:
                            rc = _with_retry(
                                f"scrape_comments:{nlink}",
                                lambda: crawler.scrape_comments(
                                    nlink,
                                    max_comments=args.max_comments_per_note,
                                    scroll_rounds=max(2, args.comment_scroll_rounds),
                                ),
                            )
                            comment_cache[nlink] = (
                                bool(getattr(rc, "ok", False)),
                                list((rc.data or {}).get("comments") or []),
                                str(rc.message),
                            )
                            comment_cache_miss_count += 1
                        if rc.ok and rc.data:
                            cl = rc.data.get("comments") or []
                            for ci, c in enumerate(cl, start=1):
                                c = dict(c)
                                c["blogger_id"] = bid
                                c["blogger_name"] = bname
                                c["comment_body_guess"] = c.get("comment_content") or c.get("comment_text", "")
                                uname = c.get("user_name", "")
                                uid = str(c.get("user_id", "")).strip()
                                is_self = False
                                if uid and bid and uid == bid:
                                    is_self = True
                                elif _norm_name(uname) and _norm_name(bname) and _norm_name(uname) == _norm_name(bname):
                                    is_self = True
                                c["is_blogger_self_guess"] = "是" if is_self else "否"
                                comment_raw.append(c)
                                hits = detect_fund_mentions(
                                    str(c.get("comment_text", "")),
                                    "comment_text",
                                    nid,
                                    bid,
                                    bname,
                                    fund_aliases,
                                )
                                for h in hits:
                                    h["entity_type"] = "comment"
                                    h["comment_index"] = ci
                                fund_mentions.extend(hits)
                        _sleep_with_jitter(args.note_sleep_ms)
                    else:
                        _sleep_with_jitter(args.note_sleep_ms)
                else:
                    blogger_failed.append(
                        {
                            "seq": i,
                            "profile_link": link,
                            "note_link": nlink,
                            "message": f"note_failed: {rn.message}",
                        }
                    )
                    if _is_risk_message(str(rn.message)):
                        blogger_risk_fails += 1
                        if blogger_risk_fails >= max(1, int(args.max_risk_fails_per_blogger)):
                            blogger_failed.append(
                                {
                                    "seq": i,
                                    "profile_link": link,
                                    "note_link": "",
                                    "message": (
                                        f"risk_skip: 单博主风控失败达到阈值 {args.max_risk_fails_per_blogger}，"
                                        "已跳过该博主剩余笔记"
                                    ),
                                }
                            )
                            break
                    _sleep_with_jitter(args.note_sleep_ms)
        else:
            fail_msg = rb.message if rb.message else "empty_data"
            if rb.ok and not useful:
                fail_msg = "博主可访问但未提取到有效字段（可能无公开内容/风控降级）"
            blogger_failed.append(
                {
                    "seq": i,
                    "profile_link": link,
                    "note_link": "",
                    "message": fail_msg,
                }
            )
        _sleep_with_jitter(args.blogger_sleep_ms)
        if i % 10 == 0 or i == total:
            print(f"[progress] {i}/{total} bloggers done")

    crawler.close()

    note_export = build_note_export_rows(note_raw)
    comment_export = build_comment_export_rows(comment_raw)
    blogger_export = build_blogger_export_rows(blogger_success)
    note_export_cols = list(NOTE_EXPORT_FIELD_MAP.values())
    comment_export_cols = list(COMMENT_EXPORT_FIELD_MAP.values())
    blogger_export_cols = list(BLOGGER_EXPORT_FIELD_MAP.values())
    failed_cols = ["seq", "profile_link", "note_link", "message"]

    note_export_df = pd.DataFrame(note_export, columns=note_export_cols)
    comment_export_df = pd.DataFrame(comment_export, columns=comment_export_cols)
    blogger_export_df = pd.DataFrame(blogger_export, columns=blogger_export_cols)
    failed_df = pd.DataFrame(blogger_failed, columns=failed_cols)
    fund_mentions_df = pd.DataFrame(fund_mentions, columns=FUND_MENTION_COLUMNS)

    out_xlsx = output_dir / f"{base}_result.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        pd.DataFrame(source_list).to_excel(writer, index=False, sheet_name="source_blogger_list")
        blogger_export_df.to_excel(writer, index=False, sheet_name="blogger_export")
        note_export_df.to_excel(writer, index=False, sheet_name="note_export")
        comment_export_df.to_excel(writer, index=False, sheet_name="comment_export")
        fund_mentions_df.to_excel(writer, index=False, sheet_name="fund_mentions")
        failed_df.to_excel(writer, index=False, sheet_name="failed")

    summary = {
        "input_file": str(input_path),
        "sheet": args.sheet,
        "source_unique_bloggers": len(source_list),
        "blogger_success_count": len(blogger_success),
        "blogger_failed_count": len(blogger_failed),
        "note_raw_count": len(note_raw),
        "note_export_count": len(note_export),
        "comment_raw_count": len(comment_raw),
        "comment_export_count": len(comment_export),
        "fund_mention_count": len(fund_mentions),
        "unique_note_link_count": len(unique_note_link_seen),
        "note_cache_hit_count": note_cache_hit_count,
        "note_cache_miss_count": note_cache_miss_count,
        "comment_cache_hit_count": comment_cache_hit_count,
        "comment_cache_miss_count": comment_cache_miss_count,
        "skipped_duplicate_note_count": skipped_duplicate_note_count,
        "risk_event_count": runtime_state["risk_event_count"],
        "context_rebuild_count": runtime_state["context_rebuild_count"],
        "reprobe_failed_count": runtime_state["reprobe_failed_count"],
        "output_excel": str(out_xlsx),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    out_json = output_dir / f"{base}_summary.json"
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
