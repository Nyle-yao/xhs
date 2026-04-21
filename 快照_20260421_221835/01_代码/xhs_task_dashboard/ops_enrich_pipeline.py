from __future__ import annotations

import argparse
import base64
import json
import math
import os
import re
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote
from urllib.parse import urlparse

import pandas as pd
import requests

from scraper import XHSScraper

try:
    import cv2  # type: ignore
    import numpy as np  # type: ignore

    CV_RUNTIME_OK = True
except Exception:
    cv2 = None  # type: ignore
    np = None  # type: ignore
    CV_RUNTIME_OK = False

try:
    from rapidocr_onnxruntime import RapidOCR  # type: ignore

    RAPID_OCR_RUNTIME_OK = True
except Exception:
    RapidOCR = None  # type: ignore
    RAPID_OCR_RUNTIME_OK = False

try:
    from openai import OpenAI  # type: ignore

    QIANFAN_SDK_OK = True
except Exception:
    OpenAI = None  # type: ignore
    QIANFAN_SDK_OK = False

OCR_RUNTIME_OK = RAPID_OCR_RUNTIME_OK

QIANFAN_DEFAULT_BASE_URL = "https://qianfan.baidubce.com/v2"
QIANFAN_DEFAULT_MODEL = "ernie-4.5-turbo-vl"
QIANFAN_API_KEY_ENV_KEYS = (
    "BAIDU_QIANFAN_API_KEY",
    "QIANFAN_API_KEY",
    "ERNIE_API_KEY",
)


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


@dataclass
class FundAliasItem:
    fund_code: str
    fund_name: str
    aliases: list[str]


MENTION_COLUMNS = [
    "record_id",
    "entity_type",
    "blogger_id",
    "blogger_name",
    "note_id",
    "comment_index",
    "fund_code",
    "fund_code_text",
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

FUND_SUMMARY_COLUMNS = [
    "fund_code",
    "fund_name",
    "提及次数",
    "主推次数",
    "对比次数",
    "正向次数",
    "负向次数",
    "涉及博主数",
    "涉及笔记数",
    "平均置信度",
    "主推占比",
    "正向占比",
    "负向占比",
    "对比占比",
    "综合热度分",
    "运营建议",
]

BLOGGER_SUMMARY_COLUMNS = [
    "blogger_id",
    "blogger_name",
    "提及次数",
    "提及基金数",
    "主推次数",
    "对比次数",
    "正向次数",
    "负向次数",
    "主推占比",
    "正向占比",
    "负向占比",
    "运营建议",
]

OCR_IMAGE_COLUMNS = [
    "image_source",
    "comment_id",
    "note_id",
    "blogger_id",
    "blogger_name",
    "image_index",
    "image_url",
    "ocr_provider",
    "ocr_text",
    "ocr_char_count",
    "ocr_ok",
    "ocr_error",
]

IMAGE_AUDIT_COLUMNS = [
    "note_id",
    "blogger_id",
    "blogger_name",
    "image_index",
    "image_url",
    "image_domain",
    "http_status",
    "img_width",
    "img_height",
    "img_bytes",
    "is_valid",
    "invalid_reason",
    "kept_for_ocr",
    "checked_at",
]

UNMAPPED_CANDIDATE_COLUMNS = [
    "候选基金名",
    "提及次数",
    "涉及博主数",
    "涉及笔记数",
    "主要来源",
    "样例片段",
]
ALIAS_SUGGEST_COLUMNS = [
    "候选基金名",
    "提及次数",
    "建议匹配fund_code",
    "建议匹配fund_name",
    "相似度",
    "匹配依据",
]

SIGNAL_TAG_COLUMNS = ["信号标签", "命中次数", "命中笔记数", "命中博主数", "说明"]
SIGNAL_TAG_RULES = [
    ("加仓", "用户偏积极配置信号"),
    ("减仓", "用户偏防守/止盈信号"),
    ("定投", "长期配置行为信号"),
    ("主推", "博主强推荐信号"),
    ("对比", "竞品比较信号"),
    ("风险", "风险提示/负面舆情信号"),
    ("回撤", "波动承压信号"),
    ("稳健", "稳健偏好信号"),
    ("债券", "固收偏好信号"),
    ("指数", "指数化配置信号"),
    ("ETF", "ETF资产偏好信号"),
    ("合作", "疑似商业合作信号"),
    ("推广", "疑似投放推广信号"),
    ("广告", "疑似广告信号"),
]

SPONSOR_SIGNAL_WEIGHTS: dict[str, float] = {
    "商务合作": 4.2,
    "合作": 2.1,
    "广告": 3.8,
    "推广": 3.2,
    "投放": 3.0,
    "恰饭": 4.5,
    "种草": 2.0,
    "福利": 1.6,
    "专属": 1.4,
    "优惠": 1.5,
    "费率": 1.4,
    "申购": 1.1,
    "赎回": 1.1,
    "上车": 1.0,
    "点击": 2.0,
    "链接": 2.0,
    "私信": 2.2,
    "抽奖": 1.8,
    "置顶": 1.2,
    "非投资建议": 1.6,
    "仅供参考": 1.2,
    "风险自担": 1.0,
}

SPONSOR_NOTE_COLUMNS = [
    "笔记ID",
    "博主ID",
    "博主昵称",
    "基金提及数",
    "提及基金列表",
    "广告信号命中次数",
    "广告信号关键词",
    "广告可能性分",
    "风险等级",
    "运营建议",
]

SPONSOR_BLOGGER_COLUMNS = [
    "博主ID",
    "博主昵称",
    "样本笔记数",
    "高风险笔记数",
    "中风险笔记数",
    "平均广告分",
    "最大广告分",
    "涉及基金数",
    "核心信号词",
    "运营建议",
]

SPONSOR_FUND_COLUMNS = [
    "fund_code",
    "fund_name",
    "涉及笔记数",
    "高风险关联笔记数",
    "平均关联广告分",
    "主推次数",
    "对比次数",
    "负向占比",
    "运营建议",
]

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


def load_fund_aliases(path: Path) -> list[FundAliasItem]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    out: list[FundAliasItem] = []
    for row in data:
        code = str(row.get("fund_code", "")).strip()
        name = str(row.get("fund_name", "")).strip()
        aliases = [str(x).strip() for x in (row.get("aliases") or []) if str(x).strip()]
        if not (code or name):
            continue
        out.append(FundAliasItem(fund_code=code, fund_name=name, aliases=aliases))
    return out


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
        # 去份额后缀（A/C/E等），适配帖子只写主名称
        vars_.add(re.sub(r"[A-E]$", "", s))
        vars_.add(re.sub(r"（?[A-E]类）?$", "", s))
        # 去“联接”以适配写ETF简称
        vars_.add(s.replace("ETF联接", "ETF"))
        vars_.add(s.replace("联接", ""))
        vars_.add(s.replace("基金", ""))
        for v in vars_:
            v = str(v or "").strip()
            if not v:
                continue
            if v in seen:
                continue
            seen.add(v)
            out.append(v)
    return out


def role_and_sentiment(text: str) -> tuple[str, str, float]:
    t = (text or "").lower()
    promote_score = sum(1 for k in PROMOTE_KEYWORDS if k in t)
    compare_score = sum(1 for k in COMPARE_KEYWORDS if k in t)
    pos_score = sum(1 for k in POSITIVE_KEYWORDS if k in t)
    neg_score = sum(1 for k in NEGATIVE_KEYWORDS if k in t)

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

    signal = min(0.2, (promote_score + compare_score + pos_score + neg_score) * 0.03)
    confidence = min(0.98, 0.78 + signal)
    return role, sentiment, confidence


def _collect_weighted_signals(text: str, signal_weights: dict[str, float]) -> tuple[float, int, Counter]:
    t = str(text or "").lower()
    if not t:
        return 0.0, 0, Counter()
    raw = 0.0
    cnt = 0
    hit = Counter()
    for k, w in signal_weights.items():
        c = t.count(str(k).lower())
        if c <= 0:
            continue
        cnt += c
        raw += float(w) * c
        hit[k] += c
    return raw, cnt, hit


def _risk_level_by_score(score: float) -> str:
    s = float(score or 0.0)
    if s >= 70:
        return "高"
    if s >= 45:
        return "中"
    if s >= 25:
        return "低"
    return "无"


def _advice_by_risk(level: str, mention_cnt: int, score: float) -> str:
    if level == "高":
        return "疑似投放密集：优先纳入重点竞品观察清单"
    if level == "中":
        if mention_cnt >= 2:
            return "中度疑似投放：建议结合达人历史行为复核"
        return "中度营销信号：继续跟踪后续笔记是否延续"
    if level == "低":
        if score >= 30:
            return "轻度营销信号：建议观察是否转为持续投放"
        return "轻度信号：暂不作为重点判断依据"
    return "暂无明显营销信号"


def _name_similarity(a: str, b: str) -> float:
    aa = _normalize_for_match(a)
    bb = _normalize_for_match(b)
    if not aa or not bb:
        return 0.0
    if aa == bb:
        return 1.0
    if aa in bb or bb in aa:
        shorter = min(len(aa), len(bb))
        longer = max(len(aa), len(bb))
        return min(0.99, 0.78 + shorter / max(longer, 1) * 0.18)
    sa, sb = set(aa), set(bb)
    jac = len(sa & sb) / max(1, len(sa | sb))
    # 公共连续片段加分
    common = 0
    for i in range(len(aa)):
        for j in range(i + 2, len(aa) + 1):
            seg = aa[i:j]
            if seg in bb:
                common = max(common, len(seg))
    cont = common / max(1, min(len(aa), len(bb)))
    return round(min(0.97, jac * 0.55 + cont * 0.45), 4)


def detect_mentions(
    text: str,
    source_field: str,
    entity_type: str,
    note_id: str,
    blogger_id: str,
    blogger_name: str,
    comment_index: int | str,
    fund_aliases: list[FundAliasItem],
) -> list[dict[str, Any]]:
    t_raw = (text or "").strip()
    # 解码URL编码文本，提升中文标签/文案命中率
    try:
        t = unquote(t_raw)
    except Exception:
        t = t_raw
    if not t:
        return []
    role, sentiment, semantic_conf = role_and_sentiment(t)
    t_norm = _normalize_for_match(t)

    def _with_source_boost(conf: float) -> float:
        sf = str(source_field or "")
        boost = 0.0
        if sf == "comment_body_self":
            # 博主本人评论常用于补充操作、仓位和隐晦基金名，证据优先级高于普通评论。
            boost += 0.08
        elif sf in {"note_title", "note_content", "note_topic"}:
            boost += 0.03
        elif sf in {"ocr_image_text", "ocr_comment_image_text"}:
            boost += 0.02
        return min(0.99, float(conf or 0.0) + boost)

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
            # 归一化候选，后续做弱匹配
            alias_norm_to_fund[sn] = (code, name, s)
    brand_tokens: set[str] = set()
    for f in fund_aliases:
        fn = str(f.fund_name or "").strip()
        m = re.match(r"^([\u4e00-\u9fa5]{2,4})", fn)
        if m:
            brand_tokens.add(m.group(1))
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    matched_codes: set[str] = set()
    for fund in fund_aliases:
        candidates = _expand_aliases_for_match(str(fund.fund_code or ""), str(fund.fund_name or ""), fund.aliases)
        for alias in candidates:
            a = (alias or "").strip()
            if not a:
                continue
            # 避免短数字（如 86）在URL编码串里误命中
            if a.isdigit() and len(a) != 6:
                continue
            if a in t:
                key = (fund.fund_code, a, source_field, str(comment_index))
                if key in seen:
                    continue
                seen.add(key)
                base = 0.95 if a == fund.fund_code else 0.85
                conf = _with_source_boost(min(0.99, base * 0.7 + semantic_conf * 0.3))
                out.append(
                    {
                        "record_id": f"{note_id}_{entity_type}_{len(out)+1}",
                        "entity_type": entity_type,
                        "blogger_id": blogger_id,
                        "blogger_name": blogger_name,
                        "note_id": note_id,
                        "comment_index": comment_index,
                        "fund_code": _norm_code(fund.fund_code),
                        "fund_code_text": _norm_code(fund.fund_code),
                        "fund_name": fund.fund_name,
                        "alias_hit": a,
                        "match_type": "alias_exact",
                        "confidence": round(conf, 4),
                        "mention_role": role,
                        "sentiment": sentiment,
                        "source_field": source_field,
                        "snippet": t[:220],
                        "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
                if fund.fund_code:
                    matched_codes.add(_norm_code(fund.fund_code))

    # 弱匹配：归一化别名命中（容忍空格/符号差异）
    for an, (code, name, alias_raw) in alias_norm_to_fund.items():
        if code in matched_codes:
            continue
        if an.isdigit():
            # 数字代码交给code_regex兜底
            continue
        if len(an) < 4:
            continue
        if an in t_norm:
            key = (code, alias_raw, source_field, str(comment_index))
            if key in seen:
                continue
            seen.add(key)
            conf = _with_source_boost(min(0.92, 0.58 * 0.7 + semantic_conf * 0.3))
            out.append(
                {
                    "record_id": f"{note_id}_{entity_type}_norm_{len(out)+1}",
                    "entity_type": entity_type,
                    "blogger_id": blogger_id,
                    "blogger_name": blogger_name,
                    "note_id": note_id,
                    "comment_index": comment_index,
                    "fund_code": code,
                    "fund_code_text": code,
                    "fund_name": name,
                    "alias_hit": alias_raw,
                    "match_type": "alias_norm",
                    "confidence": round(conf, 4),
                    "mention_role": role,
                    "sentiment": sentiment,
                    "source_field": source_field,
                    "snippet": t[:220],
                    "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            matched_codes.add(code)

    # 兜底：正则提取基金名称（在词库不足时仍可产出可分析实体）
    name_seen: set[str] = set()
    for mname in FUND_NAME_REGEX.findall(t):
        fname = str(mname or "").strip()
        if not fname or fname in name_seen:
            continue
        if fname in FUND_NAME_STOPWORDS:
            continue
        if len(fname) < 4:
            continue
        if len(fname) > 20:
            continue
        if any(tok in fname for tok in NON_FUND_PHRASE_TOKENS):
            continue
        # 约束：优先保留带基金公司前缀的实体，减少泛词误识别
        if not any(bt in fname for bt in brand_tokens):
            up = fname.upper()
            # 非品牌词仅允许“指数/境外指数”类ETF候选，避免句子误识别
            if "ETF" not in up and "QDII" not in up:
                continue
            if not any(k in fname for k in ETF_BENCHMARK_TOKENS):
                continue
        name_seen.add(fname)
        mapped = alias_to_fund.get(fname)
        mapped_code = mapped[0] if mapped else ""
        mapped_name = mapped[1] if mapped else fname
        out.append(
            {
                "record_id": f"{note_id}_{entity_type}_name_{len(out)+1}",
                "entity_type": entity_type,
                "blogger_id": blogger_id,
                "blogger_name": blogger_name,
                "note_id": note_id,
                "comment_index": comment_index,
                "fund_code": mapped_code,
                "fund_code_text": mapped_code,
                "fund_name": mapped_name,
                "alias_hit": fname,
                "match_type": "name_regex_mapped" if mapped else "name_regex",
                "confidence": round(_with_source_boost(0.74 if mapped else 0.62), 4),
                "mention_role": role,
                "sentiment": sentiment,
                "source_field": source_field,
                "snippet": t[:220],
                "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    # 兜底：识别6位基金代码（适配未入别名库的新基金）
    for m in re.finditer(r"(?<!\d)(\d{6})(?!\d)", t):
        code = m.group(1)
        code = _norm_code(code)
        if code in matched_codes:
            continue
        if code not in code_name_map:
            continue
        # 仅在代码附近存在基金语义上下文时，才启用兜底识别
        left = max(0, m.start() - 14)
        right = min(len(t), m.end() + 14)
        ctx = t[left:right].lower()
        if not any(k in ctx for k in FUND_CONTEXT_KEYWORDS):
            continue
        out.append(
            {
                "record_id": f"{note_id}_{entity_type}_code_{code}_{len(out)+1}",
                "entity_type": entity_type,
                "blogger_id": blogger_id,
                "blogger_name": blogger_name,
                "note_id": note_id,
                "comment_index": comment_index,
                "fund_code": code,
                "fund_code_text": code,
                "fund_name": code_name_map.get(code, ""),
                "alias_hit": code,
                "match_type": "code_regex",
                "confidence": round(_with_source_boost(0.72), 4),
                "mention_role": role,
                "sentiment": sentiment,
                "source_field": source_field,
                "snippet": t[:220],
                "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return out


def guess_comment_author_and_body(raw_text: str) -> tuple[str, str]:
    s = (raw_text or "").strip()
    if not s:
        return "", ""
    lines = [x.strip() for x in re.split(r"[\r\n]+", s) if x and x.strip()]
    if not lines:
        return "", ""
    if len(lines) == 1:
        return "", lines[0]
    author = lines[0][:50]
    body = " ".join(lines[1:]).strip()
    return author, body


def safe_sheet(x: str) -> str:
    return str(x or "sheet")[:31]


def _norm_name(v: str) -> str:
    s = str(v or "").strip().lower()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^\w\u4e00-\u9fa5]", "", s)
    return s


def _split_image_urls(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v).strip()
    if not s:
        return []
    parts = re.split(r"[\n\r,，、]+", s)
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        u = str(p or "").strip()
        if not u:
            continue
        if not u.startswith("http"):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _read_table_any(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() in {".xlsx", ".xls"}:
        try:
            return pd.read_excel(path)
        except Exception:
            return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _pick_col(cols: list[str], candidates: list[str]) -> str:
    lowered = {str(c).strip().lower(): c for c in cols}
    for c in candidates:
        x = lowered.get(str(c).strip().lower())
        if x:
            return str(x)
    return ""


def _build_dual_tag_bridge(fund_sum: pd.DataFrame, leshu_tag_df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "fund_code",
        "fund_name",
        "博主侧标签",
        "博主侧依据",
        "外部标签",
        "标签一致性",
        "运营建议",
    ]
    if fund_sum.empty:
        return pd.DataFrame(columns=cols)

    external_map_by_code: dict[str, str] = {}
    external_map_by_name: dict[str, str] = {}
    if not leshu_tag_df.empty:
        ext_cols = [str(c) for c in leshu_tag_df.columns]
        code_col = _pick_col(ext_cols, ["fund_code", "基金代码", "代码", "产品代码"])
        name_col = _pick_col(ext_cols, ["fund_name", "基金名称", "产品名称", "简称"])
        tag_candidates = [c for c in ext_cols if ("标签" in c or "tag" in c.lower()) and c not in {code_col, name_col}]
        if not tag_candidates:
            tag_candidates = [c for c in ext_cols if c not in {code_col, name_col}][:2]

        for _, rr in leshu_tag_df.iterrows():
            code = str(rr.get(code_col, "")).strip() if code_col else ""
            code = re.sub(r"[^\d]", "", code).zfill(6) if code else ""
            name = str(rr.get(name_col, "")).strip() if name_col else ""
            tags = []
            for tc in tag_candidates:
                tv = str(rr.get(tc, "")).strip()
                if tv:
                    tags.append(tv)
            ext_tag = "；".join(dict.fromkeys(tags))
            if not ext_tag:
                continue
            if code:
                external_map_by_code[code] = ext_tag
            if name:
                external_map_by_name[name] = ext_tag

    rows: list[dict[str, Any]] = []
    for _, rr in fund_sum.iterrows():
        code = str(rr.get("fund_code", "")).strip()
        name = str(rr.get("fund_name", "")).strip()
        mention_times = int(rr.get("提及次数", 0) or 0)
        promote_ratio = float(rr.get("主推占比", 0) or 0)
        neg_ratio = float(rr.get("负向占比", 0) or 0)
        cmp_ratio = float(rr.get("对比占比", 0) or 0)

        if neg_ratio >= 0.35:
            blogger_tag = "风险讨论"
            blogger_basis = f"负向占比{neg_ratio:.2%}"
        elif promote_ratio >= 0.40 and mention_times >= 3:
            blogger_tag = "主推热度"
            blogger_basis = f"主推占比{promote_ratio:.2%}，提及{mention_times}次"
        elif cmp_ratio >= 0.30:
            blogger_tag = "竞品对比"
            blogger_basis = f"对比占比{cmp_ratio:.2%}"
        else:
            blogger_tag = "常规提及"
            blogger_basis = f"提及{mention_times}次"

        ext_tag = external_map_by_code.get(code) or external_map_by_name.get(name) or ""
        if not ext_tag:
            consistency = "外部标签缺失"
            advice = "补齐外部标签映射后再做一致性判断"
        else:
            hit = any(x in ext_tag for x in ["稳健", "债", "固收"]) and ("风险" not in blogger_tag)
            if blogger_tag == "风险讨论":
                consistency = "偏离"
                advice = "内容侧风险讨论升温，建议重点复核投放素材"
            elif hit or blogger_tag in ext_tag:
                consistency = "一致"
                advice = "标签一致，可延续当前运营策略"
            else:
                consistency = "待观察"
                advice = "标签与内容信号存在差异，建议分场景观察"

        rows.append(
            {
                "fund_code": code,
                "fund_name": name,
                "博主侧标签": blogger_tag,
                "博主侧依据": blogger_basis,
                "外部标签": ext_tag,
                "标签一致性": consistency,
                "运营建议": advice,
            }
        )
    return pd.DataFrame(rows, columns=cols)


@dataclass
class OcrRuntime:
    provider: str
    client: Any = None
    rapid_engine: Any = None
    model: str = ""
    temperature: float = 0.1
    top_p: float = 0.2


def _load_local_env_file() -> None:
    """Load .env/.env.local lightly without adding another runtime dependency."""
    candidates = [
        Path.cwd() / ".env.local",
        Path.cwd() / ".env",
        Path(__file__).resolve().parent / ".env.local",
        Path(__file__).resolve().parent / ".env",
    ]
    seen: set[Path] = set()
    for env_path in candidates:
        env_path = env_path.resolve()
        if env_path in seen or not env_path.exists():
            continue
        seen.add(env_path)
        try:
            for raw_line in env_path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[len("export ") :].strip()
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("'\"")
                if key and key not in os.environ:
                    os.environ[key] = value
        except Exception:
            continue


def _qianfan_api_key(cli_value: str = "") -> str:
    if str(cli_value or "").strip():
        return str(cli_value or "").strip()
    for env_key in QIANFAN_API_KEY_ENV_KEYS:
        value = os.getenv(env_key, "").strip()
        if value:
            return value
    return ""


def _safe_ocr_error(err: Any) -> str:
    msg = str(err or "").strip()
    for env_key in QIANFAN_API_KEY_ENV_KEYS:
        secret = os.getenv(env_key, "").strip()
        if secret:
            msg = msg.replace(secret, "***")
    msg = re.sub(r"bce-v3/[^\s'\"]+", "bce-v3/***", msg)
    msg = re.sub(r"\s+", " ", msg)
    return msg[:260]


def _init_ocr_runtime(args: argparse.Namespace) -> tuple[OcrRuntime | None, str]:
    provider = str(getattr(args, "ocr_provider", "") or "qianfan").strip().lower()
    api_key = _qianfan_api_key(str(getattr(args, "qianfan_api_key", "") or ""))
    if provider == "auto":
        provider = "qianfan" if (api_key and QIANFAN_SDK_OK) else "rapidocr"

    if provider == "qianfan":
        if not QIANFAN_SDK_OK or OpenAI is None:
            return None, "qianfan_sdk_unavailable: 请先安装 openai SDK"
        if not api_key:
            return None, "qianfan_api_key_missing: 请设置 BAIDU_QIANFAN_API_KEY"
        try:
            client = OpenAI(
                base_url=str(getattr(args, "qianfan_base_url", "") or QIANFAN_DEFAULT_BASE_URL),
                api_key=api_key,
                timeout=max(10, int(getattr(args, "qianfan_request_timeout_sec", 60) or 60)),
            )
            return (
                OcrRuntime(
                    provider="qianfan",
                    client=client,
                    model=str(getattr(args, "qianfan_model", "") or QIANFAN_DEFAULT_MODEL),
                    temperature=float(getattr(args, "qianfan_temperature", 0.1)),
                    top_p=float(getattr(args, "qianfan_top_p", 0.2)),
                ),
                "",
            )
        except Exception as e:
            return None, f"qianfan_init_exception:{_safe_ocr_error(e)}"

    if provider == "rapidocr":
        if not (CV_RUNTIME_OK and cv2 is not None and np is not None):
            return None, "opencv_unavailable"
        if not (RAPID_OCR_RUNTIME_OK and RapidOCR is not None):
            return None, "rapidocr_runtime_unavailable"
        try:
            return OcrRuntime(provider="rapidocr", rapid_engine=RapidOCR()), ""
        except Exception as e:
            return None, f"rapidocr_init_exception:{_safe_ocr_error(e)}"

    return None, f"unknown_ocr_provider:{provider}"


def _normalize_ocr_text(text: Any) -> str:
    t = str(text or "").strip()
    if not t:
        return ""
    t = re.sub(r"^```(?:text|txt)?", "", t, flags=re.I).strip()
    t = re.sub(r"```$", "", t).strip()
    no_text_patterns = [
        "无",
        "没有文字",
        "未识别到文字",
        "图片中没有文字",
        "图片中没有可见文字",
        "未发现可见文字",
    ]
    if t.replace("。", "").replace(".", "").strip() in no_text_patterns:
        return ""
    return re.sub(r"\s+", " ", t).strip()


def _response_text(response: Any) -> str:
    try:
        content = response.choices[0].message.content
    except Exception:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                txt = item.get("text") or item.get("content") or ""
            else:
                txt = getattr(item, "text", "") or getattr(item, "content", "")
            if txt:
                parts.append(str(txt))
        return "\n".join(parts)
    return str(content or "")


def _image_to_data_url(img: Any, max_side: int = 1800) -> tuple[str, str]:
    if img is None or cv2 is None:
        return "", "empty_image"
    try:
        h, w = img.shape[:2]
        prepared = img
        if max(h, w) > max_side:
            scale = max_side / float(max(h, w))
            prepared = cv2.resize(img, (max(1, int(w * scale)), max(1, int(h * scale))), interpolation=cv2.INTER_AREA)
        ok, buf = cv2.imencode(".jpg", prepared, [int(cv2.IMWRITE_JPEG_QUALITY), 92])
        if not ok:
            return "", "image_encode_failed"
        b64 = base64.b64encode(buf.tobytes()).decode("ascii")
        return f"data:image/jpeg;base64,{b64}", ""
    except Exception as e:
        return "", f"image_encode_exception:{_safe_ocr_error(e)}"


def _is_static_or_bad_image_url(url: str) -> bool:
    u = str(url or "").lower()
    bad_tokens = [
        "/fe-platform/",
        "avatar",
        "emoji",
        "icon",
        "logo",
        "badge",
        "sprite",
        "favicon",
    ]
    return any(x in u for x in bad_tokens)


def _download_image_best_effort(url: str, timeout_sec: int = 12) -> tuple[Any | None, dict[str, Any]]:
    """
    返回: (decoded_image_or_none, meta)
    meta: http_status/img_bytes/error
    """
    meta = {"http_status": "", "img_bytes": 0, "error": ""}
    if cv2 is None or np is None:
        meta["error"] = "opencv_unavailable"
        return None, meta

    try:
        candidates = [str(url or "").strip()]
        u = candidates[0]
        if "imageview2/2/w/360" in u.lower():
            candidates.append(re.sub(r"(?i)imageview2/2/w/360", "imageView2/2/w/1440", u))
        if "imageview2/2/w/540" in u.lower():
            candidates.append(re.sub(r"(?i)imageview2/2/w/540", "imageView2/2/w/1440", u))
        if "format/webp" in u.lower():
            candidates.append(re.sub(r"(?i)format/webp", "format/jpg", u))

        last_resp = None
        for cu in candidates:
            try:
                r = requests.get(cu, timeout=timeout_sec, headers={"User-Agent": "Mozilla/5.0"})
                last_resp = r
                meta["http_status"] = int(r.status_code)
                if r.status_code != 200 or not r.content:
                    continue
                meta["img_bytes"] = int(len(r.content))
                arr = np.frombuffer(r.content, dtype=np.uint8)
                img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
                if img is not None:
                    return img, meta
            except Exception as e:
                em = str(e or "").lower()
                if "ssl" in em:
                    meta["error"] = "network_ssl"
                elif "timed out" in em or "timeout" in em:
                    meta["error"] = "network_timeout"
                elif "connection" in em:
                    meta["error"] = "network_connection"
                else:
                    meta["error"] = "network_exception"
                continue

        if last_resp is not None and not meta["error"]:
            meta["error"] = f"http_status_{last_resp.status_code}"
        elif not meta["error"]:
            meta["error"] = "download_failed"
        return None, meta
    except Exception as e:
        em = str(e or "").lower()
        if "ssl" in em:
            meta["error"] = "network_ssl"
        elif "timeout" in em:
            meta["error"] = "network_timeout"
        else:
            meta["error"] = "download_exception"
        return None, meta


def _audit_image_url(url: str, timeout_sec: int = 12) -> tuple[dict[str, Any], Any | None]:
    domain = ""
    try:
        domain = urlparse(str(url or "")).netloc
    except Exception:
        domain = ""

    rec = {
        "image_url": str(url or "").strip(),
        "image_domain": domain,
        "http_status": "",
        "img_width": 0,
        "img_height": 0,
        "img_bytes": 0,
        "is_valid": "否",
        "invalid_reason": "",
    }

    if not rec["image_url"] or not rec["image_url"].startswith("http"):
        rec["invalid_reason"] = "invalid_url"
        return rec, None
    if _is_static_or_bad_image_url(rec["image_url"]):
        rec["invalid_reason"] = "static_asset_url"
        return rec, None

    img, meta = _download_image_best_effort(rec["image_url"], timeout_sec=timeout_sec)
    rec["http_status"] = meta.get("http_status", "")
    rec["img_bytes"] = meta.get("img_bytes", 0)

    if img is None:
        rec["invalid_reason"] = str(meta.get("error", "") or "download_or_decode_failed")
        return rec, None

    h, w = img.shape[:2]
    rec["img_width"] = int(w)
    rec["img_height"] = int(h)

    if max(h, w) < 360:
        rec["invalid_reason"] = f"small_resolution_{w}x{h}"
        return rec, None
    if min(h, w) < 240:
        rec["invalid_reason"] = f"narrow_resolution_{w}x{h}"
        return rec, None
    if int(rec["img_bytes"] or 0) < 8000:
        rec["invalid_reason"] = "image_bytes_too_small"
        return rec, None

    try:
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        stdv = float(gray.std())
        if stdv < 8.0:
            rec["invalid_reason"] = f"low_texture_std_{stdv:.2f}"
            return rec, None
    except Exception:
        pass

    rec["is_valid"] = "是"
    rec["invalid_reason"] = ""
    return rec, img


def _rapid_ocr_text_from_image(img: Any, ocr_engine: Any) -> tuple[str, str]:
    if not (RAPID_OCR_RUNTIME_OK and ocr_engine and cv2 is not None and np is not None):
        return "", "rapidocr_runtime_unavailable"
    try:
        if img is None:
            return "", "empty_image"
        h, w = img.shape[:2]
        if max(h, w) < 360:
            return "", f"image_too_small_{w}x{h}"

        def _read(im) -> str:
            rs, _ = ocr_engine(im)
            if not rs:
                return ""
            txts: list[str] = []
            for row in rs:
                if not row or len(row) < 2:
                    continue
                t = str(row[1] or "").strip()
                if t:
                    txts.append(t)
            return " ".join(txts).strip()

        candidates = []
        candidates.append(_read(img))
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        candidates.append(_read(gray))
        _, th1 = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        candidates.append(_read(th1))
        th2 = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 5)
        candidates.append(_read(th2))
        up = cv2.resize(gray, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
        candidates.append(_read(up))
        blur = cv2.GaussianBlur(gray, (3, 3), 0)
        candidates.append(_read(blur))
        up2 = cv2.resize(gray, None, fx=3.0, fy=3.0, interpolation=cv2.INTER_CUBIC)
        candidates.append(_read(up2))

        def _score(s: str) -> int:
            t = str(s or "").strip()
            if not t:
                return 0
            cjk = len(re.findall(r"[\u4e00-\u9fa5]", t))
            alnum = len(re.findall(r"[A-Za-z0-9]", t))
            punct = len(re.findall(r"[^\u4e00-\u9fa5A-Za-z0-9\s]", t))
            return cjk * 3 + alnum - punct

        best = max(candidates, key=_score) if candidates else ""
        best = re.sub(r"\s+", " ", str(best or "")).strip()
        cjk = len(re.findall(r"[\u4e00-\u9fa5]", best))
        alnum = len(re.findall(r"[A-Za-z0-9]", best))
        if best and cjk == 0 and alnum <= 4:
            return "", "ocr_noise_filtered"
        if best and cjk < 2 and (cjk + alnum) < 5:
            return "", "ocr_low_info_filtered"
        return best, ""
    except Exception as e:
        return "", f"rapidocr_exception:{_safe_ocr_error(e)}"


def _qianfan_ocr_text_from_image(img: Any, ocr_runtime: OcrRuntime) -> tuple[str, str]:
    if not (QIANFAN_SDK_OK and ocr_runtime and ocr_runtime.client):
        return "", "qianfan_runtime_unavailable"
    data_url, enc_err = _image_to_data_url(img)
    if enc_err:
        return "", enc_err
    prompt = (
        "请对这张小红书图片做高召回OCR。只输出图片中的可见文字，保留中文、英文、数字、"
        "基金代码、百分比、金额、日期；按自然阅读顺序排列。不要解释，不要总结，不要猜测。"
        "如果没有可见文字，只输出“无”。"
    )
    try:
        response = ocr_runtime.client.chat.completions.create(
            model=ocr_runtime.model or QIANFAN_DEFAULT_MODEL,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                }
            ],
            temperature=ocr_runtime.temperature,
            top_p=ocr_runtime.top_p,
            extra_body={
                "penalty_score": 1,
                "stop": [],
                "compression": True,
            },
        )
        txt = _normalize_ocr_text(_response_text(response))
        if not txt:
            return "", "qianfan_empty_response"
        return txt, ""
    except Exception as e:
        return "", f"qianfan_exception:{_safe_ocr_error(e)}"


def _ocr_text_from_url(url: str, ocr_runtime: OcrRuntime | None, timeout_sec: int = 12) -> tuple[str, str]:
    if ocr_runtime is None:
        return "", "ocr_runtime_unavailable"
    rec, img = _audit_image_url(url, timeout_sec=timeout_sec)
    if rec.get("is_valid") != "是":
        return "", str(rec.get("invalid_reason", "") or "image_invalid")
    if ocr_runtime.provider == "qianfan":
        return _qianfan_ocr_text_from_image(img, ocr_runtime)
    if ocr_runtime.provider == "rapidocr":
        return _rapid_ocr_text_from_image(img, ocr_runtime.rapid_engine)
    return "", f"unknown_ocr_provider:{ocr_runtime.provider}"


def main() -> None:
    _load_local_env_file()
    p = argparse.ArgumentParser(description="小红书二轮增强：评论补抓 + 提及语义增强 + 运营汇总")
    p.add_argument("--input-result", required=True, help="第一轮结果 xlsx（含 note_export）")
    p.add_argument("--fund-aliases", default="./fund_aliases.json", help="基金别名库")
    p.add_argument("--profile-dir", default="./.xhs_profile", help="登录态目录")
    p.add_argument("--crawl-comments", action="store_true", help="是否抓评论")
    p.add_argument("--headed", action="store_true", help="评论抓取使用有界面浏览器（便于手动登录）")
    p.add_argument("--max-notes-for-comments", type=int, default=80, help="评论补抓最多处理多少篇笔记")
    p.add_argument("--max-comments-per-note", type=int, default=60, help="每篇笔记评论上限")
    p.add_argument("--sleep-ms", type=int, default=300, help="每篇笔记评论抓取间隔（毫秒）")
    p.add_argument("--ocr-images", action="store_true", help="是否对笔记图片做OCR并参与基金提及识别")
    p.add_argument(
        "--ocr-provider",
        choices=["qianfan", "rapidocr", "auto"],
        default=os.getenv("XHS_OCR_PROVIDER", "qianfan"),
        help="OCR引擎：qianfan=百度千帆视觉大模型（默认），rapidocr=本地OCR，auto=优先千帆否则本地",
    )
    p.add_argument("--qianfan-api-key", default="", help="百度千帆API Key；建议使用BAIDU_QIANFAN_API_KEY环境变量")
    p.add_argument("--qianfan-base-url", default=os.getenv("QIANFAN_BASE_URL", QIANFAN_DEFAULT_BASE_URL))
    p.add_argument("--qianfan-model", default=os.getenv("QIANFAN_MODEL", QIANFAN_DEFAULT_MODEL))
    p.add_argument("--qianfan-temperature", type=float, default=float(os.getenv("QIANFAN_TEMPERATURE", "0.1")))
    p.add_argument("--qianfan-top-p", type=float, default=float(os.getenv("QIANFAN_TOP_P", "0.2")))
    p.add_argument("--qianfan-request-timeout-sec", type=int, default=int(os.getenv("QIANFAN_REQUEST_TIMEOUT_SEC", "60")))
    p.add_argument("--ocr-max-notes", type=int, default=80, help="OCR最多处理多少篇笔记")
    p.add_argument("--ocr-max-images-per-note", type=int, default=0, help="每篇笔记最多OCR几张图；0=全图")
    p.add_argument(
        "--ocr-comment-images",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="是否对评论图片做OCR（默认开启）",
    )
    p.add_argument("--ocr-max-comments", type=int, default=200, help="评论图片OCR最多处理多少条评论")
    p.add_argument("--ocr-max-images-per-comment", type=int, default=1, help="每条评论最多OCR几张图")
    p.add_argument("--ocr-timeout-sec", type=int, default=12, help="OCR下载图片超时时间")
    p.add_argument("--image-audit-timeout-sec", type=int, default=12, help="图片有效性审计超时时间")
    p.add_argument("--image-audit-max-notes", type=int, default=120, help="图片审计最多处理多少篇笔记，0=全部")
    p.add_argument("--image-audit-max-images-per-note", type=int, default=0, help="单篇笔记最多审计多少张图片，0=全部")
    p.add_argument("--leshu-tag-file", default="", help="外部标签对照表（可选，支持xlsx/csv）")
    p.add_argument("--output-dir", default="./outputs", help="输出目录")
    args = p.parse_args()

    input_path = Path(args.input_result).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    fund_aliases = load_fund_aliases(Path(args.fund_aliases).expanduser().resolve())

    xls = pd.ExcelFile(input_path)
    note_df = pd.read_excel(input_path, sheet_name="note_export") if "note_export" in xls.sheet_names else pd.DataFrame()
    comment_df_existing = (
        pd.read_excel(input_path, sheet_name="comment_export") if "comment_export" in xls.sheet_names else pd.DataFrame()
    )
    failed_df = pd.read_excel(input_path, sheet_name="failed") if "failed" in xls.sheet_names else pd.DataFrame()
    blogger_df = pd.read_excel(input_path, sheet_name="blogger_export") if "blogger_export" in xls.sheet_names else pd.DataFrame()

    if note_df.empty:
        raise SystemExit("输入结果缺少 note_export 或 note_export 为空，无法增强。")

    # note map
    note_map: dict[str, dict[str, str]] = {}
    note_rows = note_df.to_dict(orient="records")
    for r in note_rows:
        nid = str(r.get("笔记ID", "")).strip()
        nurl = str(r.get("笔记链接", "")).strip()
        bid = str(r.get("博主ID", "")).strip()
        bname = str(r.get("博主昵称", "")).strip()
        if nid:
            note_map[nid] = {"note_url": nurl, "blogger_id": bid, "blogger_name": bname}
        if nurl and nurl not in note_map:
            note_map[nurl] = {"note_url": nurl, "blogger_id": bid, "blogger_name": bname}

    # 图片审计：归档无效图片，并在主分析流程中仅保留有效图片
    image_audit_rows: list[dict[str, Any]] = []
    image_audit_cache: dict[str, dict[str, Any]] = {}
    note_valid_urls_map: dict[str, list[str]] = {}
    checked_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    audit_note_limit = max(0, int(args.image_audit_max_notes))
    audit_img_limit = max(0, int(args.image_audit_max_images_per_note))
    for note_idx, r in enumerate(note_rows, start=1):
        nid = str(r.get("笔记ID", "")).strip()
        nurl = str(r.get("笔记链接", "")).strip()
        bid = str(r.get("博主ID", "")).strip()
        bname = str(r.get("博主昵称", "")).strip()
        key = nid or nurl
        raw_urls = _split_image_urls(r.get("笔记图片链接", ""))
        should_audit_note = (audit_note_limit == 0) or (note_idx <= audit_note_limit)
        if not should_audit_note:
            # 超过审计上限的笔记默认保留原图链接，避免“未审计即丢数”。
            note_valid_urls_map[key] = list(raw_urls)
            for j, u in enumerate(raw_urls, start=1):
                image_audit_rows.append(
                    {
                        "note_id": nid,
                        "blogger_id": bid,
                        "blogger_name": bname,
                        "image_index": j,
                        "image_url": u,
                        "image_domain": "",
                        "http_status": "",
                        "img_width": 0,
                        "img_height": 0,
                        "img_bytes": 0,
                        "is_valid": "未审计",
                        "invalid_reason": "not_audited_note_limit",
                        "kept_for_ocr": "否",
                        "checked_at": checked_at,
                    }
                )
            continue
        valid_urls: list[str] = []
        for j, u in enumerate(raw_urls, start=1):
            if audit_img_limit > 0 and j > audit_img_limit:
                # 超过单笔记审计张数上限的图片同样保留，且标记未审计。
                valid_urls.append(u)
                image_audit_rows.append(
                    {
                        "note_id": nid,
                        "blogger_id": bid,
                        "blogger_name": bname,
                        "image_index": j,
                        "image_url": u,
                        "image_domain": "",
                        "http_status": "",
                        "img_width": 0,
                        "img_height": 0,
                        "img_bytes": 0,
                        "is_valid": "未审计",
                        "invalid_reason": "not_audited_image_limit",
                        "kept_for_ocr": "否",
                        "checked_at": checked_at,
                    }
                )
                continue
            if u in image_audit_cache:
                base = dict(image_audit_cache[u])
            else:
                rec0, _ = _audit_image_url(u, timeout_sec=max(5, int(args.image_audit_timeout_sec)))
                base = {
                    "image_url": rec0.get("image_url", ""),
                    "image_domain": rec0.get("image_domain", ""),
                    "http_status": rec0.get("http_status", ""),
                    "img_width": rec0.get("img_width", 0),
                    "img_height": rec0.get("img_height", 0),
                    "img_bytes": rec0.get("img_bytes", 0),
                    "is_valid": rec0.get("is_valid", "否"),
                    "invalid_reason": rec0.get("invalid_reason", ""),
                }
                image_audit_cache[u] = dict(base)
            rec = {
                "note_id": nid,
                "blogger_id": bid,
                "blogger_name": bname,
                "image_index": j,
                "image_url": base.get("image_url", ""),
                "image_domain": base.get("image_domain", ""),
                "http_status": base.get("http_status", ""),
                "img_width": base.get("img_width", 0),
                "img_height": base.get("img_height", 0),
                "img_bytes": base.get("img_bytes", 0),
                "is_valid": base.get("is_valid", "否"),
                "invalid_reason": base.get("invalid_reason", ""),
                "kept_for_ocr": "否",
                "checked_at": checked_at,
            }
            if rec["is_valid"] == "是":
                valid_urls.append(u)
            image_audit_rows.append(rec)
        note_valid_urls_map[key] = valid_urls

    # note_export增强：主列只保留有效图，同时保留原始图列供追溯
    note_df_valid = note_df.copy()
    if not note_df_valid.empty:
        if "笔记图片链接(原始)" not in note_df_valid.columns:
            note_df_valid["笔记图片链接(原始)"] = note_df_valid.get("笔记图片链接", "")
        if "图片数量(原始)" not in note_df_valid.columns:
            note_df_valid["图片数量(原始)"] = note_df_valid.get("图片数量", 0)
        note_df_valid["图片数量(有效)"] = 0
        note_df_valid["无效图片数量"] = 0
        note_df_valid["笔记图片链接(有效)"] = ""
        for ix, rr in note_df_valid.iterrows():
            nid = str(rr.get("笔记ID", "")).strip()
            nurl = str(rr.get("笔记链接", "")).strip()
            key = nid or nurl
            raw_urls = _split_image_urls(rr.get("笔记图片链接(原始)", rr.get("笔记图片链接", "")))
            valid_urls = note_valid_urls_map.get(key, [])
            note_df_valid.at[ix, "图片数量(有效)"] = len(valid_urls)
            note_df_valid.at[ix, "无效图片数量"] = max(0, len(raw_urls) - len(valid_urls))
            note_df_valid.at[ix, "笔记图片链接(有效)"] = "\n".join(valid_urls)
            # 主流程字段仅保留有效图
            note_df_valid.at[ix, "图片数量"] = len(valid_urls)
            note_df_valid.at[ix, "笔记图片链接"] = "\n".join(valid_urls)

    note_rows_valid = note_df_valid.to_dict(orient="records")

    # 评论补抓
    comment_rows: list[dict[str, Any]] = []
    if not comment_df_existing.empty:
        comment_rows.extend(comment_df_existing.to_dict(orient="records"))

    crawled_comment_rows: list[dict[str, Any]] = []
    crawled_note_count = 0
    crawl_failed_count = 0
    if args.crawl_comments:
        scraper = XHSScraper(profile_dir=args.profile_dir, headless=not args.headed)
        try:
            note_links = []
            seen_links: set[str] = set()
            for r in note_rows_valid:
                nurl = str(r.get("笔记链接", "")).strip()
                if not nurl or nurl in seen_links:
                    continue
                seen_links.add(nurl)
                note_links.append(nurl)
                if len(note_links) >= args.max_notes_for_comments:
                    break

            for idx, nurl in enumerate(note_links, start=1):
                rc = scraper.scrape_comments(nurl, max_comments=args.max_comments_per_note)
                if rc.ok and rc.data:
                    rows = rc.data.get("comments") or []
                    for i, c in enumerate(rows, start=1):
                        raw_text = str(c.get("comment_text", "")).strip()
                        m = re.search(r"(?:/explore/|/user/profile/[a-zA-Z0-9]+/)([a-zA-Z0-9]+)", nurl)
                        nid = m.group(1) if m else str(c.get("note_id", "")).strip()
                        mp = note_map.get(nid, note_map.get(nurl, {}))
                        bname = str(mp.get("blogger_name", "")).strip()
                        bid = str(mp.get("blogger_id", "")).strip()
                        author_guess, body_guess = guess_comment_author_and_body(raw_text)
                        user_name = str(c.get("user_name", "")).strip() or author_guess
                        user_id = str(c.get("user_id", "")).strip()
                        user_url = str(c.get("user_url", "")).strip()
                        is_self = False
                        if user_id and bid and user_id == bid:
                            is_self = True
                        elif _norm_name(user_name) and _norm_name(bname) and _norm_name(user_name) == _norm_name(bname):
                            is_self = True
                        crawled_comment_rows.append(
                            {
                                "评论ID": f"{nid}_{idx}_{i}",
                                "评论内容": raw_text,
                                "评论正文猜测": body_guess or raw_text,
                                "评论作者猜测": author_guess,
                                "是否博主本人评论(猜测)": "是" if is_self else "否",
                                "评论图片链接": c.get("comment_image_url", ""),
                                "点赞量": c.get("like_count", ""),
                                "评论时间": c.get("comment_time", ""),
                                "IP地址": c.get("ip_address", ""),
                                "子评论数": c.get("reply_count", ""),
                                "笔记ID": nid,
                                "笔记链接": nurl,
                                "用户ID": user_id,
                                "用户链接": user_url,
                                "用户名称": user_name,
                                "一级评论ID": c.get("parent_comment_id", ""),
                                "一级评论内容": c.get("parent_comment_content", ""),
                                "引用的评论ID": c.get("quoted_comment_id", ""),
                                "引用的评论内容": c.get("quoted_comment_content", ""),
                                "一级评论用户ID": c.get("parent_user_id", ""),
                                "一级评论用户名称": c.get("parent_user_name", ""),
                                "引用的用户ID": c.get("quoted_user_id", ""),
                                "引用的用户名称": c.get("quoted_user_name", ""),
                                "博主ID": bid,
                                "博主昵称": bname,
                            }
                        )
                    crawled_note_count += 1
                else:
                    crawl_failed_count += 1
                if idx % 10 == 0:
                    print(f"[comment_crawl_progress] {idx}/{len(note_links)}")
                time.sleep(max(0, args.sleep_ms) / 1000.0)
        finally:
            scraper.close()

    # 合并评论
    if crawled_comment_rows:
        comment_rows.extend(crawled_comment_rows)
    comment_all_df = pd.DataFrame(comment_rows)
    if comment_all_df.empty:
        comment_all_df = pd.DataFrame(
            columns=[
                "评论ID",
                "评论内容",
                "评论正文猜测",
                "评论作者猜测",
                "是否博主本人评论(猜测)",
                "评论图片链接",
                "点赞量",
                "评论时间",
                "IP地址",
                "子评论数",
                "笔记ID",
                "笔记链接",
                "用户ID",
                "用户链接",
                "用户名称",
                "一级评论ID",
                "一级评论内容",
                "引用的评论ID",
                "引用的评论内容",
                "一级评论用户ID",
                "一级评论用户名称",
                "引用的用户ID",
                "引用的用户名称",
                "博主ID",
                "博主昵称",
            ]
        )
    if not comment_all_df.empty:
        dedup_cols = [c for c in ["笔记链接", "评论内容", "用户ID", "用户名称", "评论时间", "评论图片链接"] if c in comment_all_df.columns]
        if dedup_cols:
            comment_all_df = comment_all_df.drop_duplicates(subset=dedup_cols, keep="first")
        else:
            comment_all_df = comment_all_df.drop_duplicates(keep="first")
        # 标准化评论正文列
        if "评论正文猜测" not in comment_all_df.columns:
            if "评论内容" in comment_all_df.columns:
                comment_all_df["评论正文猜测"] = comment_all_df["评论内容"]
            else:
                comment_all_df["评论正文猜测"] = ""
        else:
            comment_all_df["评论正文猜测"] = comment_all_df["评论正文猜测"].fillna("")
            if "评论内容" in comment_all_df.columns:
                mask = comment_all_df["评论正文猜测"].astype(str).str.strip() == ""
                comment_all_df.loc[mask, "评论正文猜测"] = comment_all_df.loc[mask, "评论内容"]

        # 标准化“博主本人评论(猜测)”
        if "是否博主本人评论(猜测)" not in comment_all_df.columns:
            comment_all_df["是否博主本人评论(猜测)"] = ""
        if "用户名称" not in comment_all_df.columns:
            comment_all_df["用户名称"] = ""
        if "用户ID" not in comment_all_df.columns:
            comment_all_df["用户ID"] = ""
        if "博主昵称" not in comment_all_df.columns:
            comment_all_df["博主昵称"] = ""
        if "博主ID" not in comment_all_df.columns:
            comment_all_df["博主ID"] = ""
        guess_vals = []
        for _, rr in comment_all_df.iterrows():
            uid = str(rr.get("用户ID", "")).strip()
            bid = str(rr.get("博主ID", "")).strip()
            uname = _norm_name(str(rr.get("用户名称", "")))
            bname = _norm_name(str(rr.get("博主昵称", "")))
            is_self = False
            if uid and bid and uid == bid:
                is_self = True
            elif uname and bname and uname == bname:
                is_self = True
            guess_vals.append("是" if is_self else "否")
        comment_all_df["是否博主本人评论(猜测)"] = guess_vals

    comment_self_df = (
        comment_all_df[comment_all_df["是否博主本人评论(猜测)"] == "是"].copy()
        if (not comment_all_df.empty and "是否博主本人评论(猜测)" in comment_all_df.columns)
        else pd.DataFrame(columns=comment_all_df.columns if not comment_all_df.empty else [])
    )

    # OCR: 笔记图片识别（可选）
    ocr_rows: list[dict[str, Any]] = []
    ocr_kept_pairs: set[tuple[str, str]] = set()
    ocr_runtime: OcrRuntime | None = None
    if args.ocr_images:
        ocr_runtime, ocr_init_err = _init_ocr_runtime(args)
        if ocr_runtime is None:
            print(f"[ocr_warn] {ocr_init_err}，跳过OCR。")
        else:
            print(f"[ocr_info] 使用OCR引擎: {ocr_runtime.provider}")

    if args.ocr_images and ocr_runtime is not None:
        for idx, r in enumerate(note_rows_valid, start=1):
            if idx > max(0, args.ocr_max_notes):
                break
            nid = str(r.get("笔记ID", "")).strip()
            bid = str(r.get("博主ID", "")).strip()
            bname = str(r.get("博主昵称", "")).strip()
            urls = _split_image_urls(r.get("笔记图片链接", ""))
            if not urls:
                continue
            note_ocr_limit = int(args.ocr_max_images_per_note)
            urls_for_ocr = urls if note_ocr_limit <= 0 else urls[: max(1, note_ocr_limit)]
            for j, u in enumerate(urls_for_ocr, start=1):
                ocr_kept_pairs.add((nid, u))
                txt, err = _ocr_text_from_url(u, ocr_runtime, timeout_sec=max(5, args.ocr_timeout_sec))
                ocr_rows.append(
                    {
                        "image_source": "note_image",
                        "comment_id": "",
                        "note_id": nid,
                        "blogger_id": bid,
                        "blogger_name": bname,
                        "image_index": j,
                        "image_url": u,
                        "ocr_provider": ocr_runtime.provider,
                        "ocr_text": txt,
                        "ocr_char_count": len(txt or ""),
                        "ocr_ok": "是" if (txt and not err) else "否",
                        "ocr_error": err,
                    }
                )

    # OCR: 评论图片识别（可选）
    if (
        args.ocr_images
        and args.ocr_comment_images
        and ocr_runtime is not None
        and (not comment_all_df.empty)
        and ("评论图片链接" in comment_all_df.columns)
    ):
        comment_rows_for_ocr = comment_all_df.to_dict(orient="records")
        max_comments = max(0, int(args.ocr_max_comments))
        max_imgs_per_comment = max(1, int(args.ocr_max_images_per_comment))
        for cidx, rr in enumerate(comment_rows_for_ocr, start=1):
            if max_comments > 0 and cidx > max_comments:
                break
            note_id = str(rr.get("笔记ID", "")).strip()
            blogger_id = str(rr.get("博主ID", "")).strip()
            blogger_name = str(rr.get("博主昵称", "")).strip()
            comment_id = str(rr.get("评论ID", "")).strip() or f"comment_{cidx}"
            urls = _split_image_urls(rr.get("评论图片链接", ""))
            if not urls:
                continue
            for j, u in enumerate(urls[:max_imgs_per_comment], start=1):
                txt, err = _ocr_text_from_url(u, ocr_runtime, timeout_sec=max(5, args.ocr_timeout_sec))
                ocr_rows.append(
                    {
                        "image_source": "comment_image",
                        "comment_id": comment_id,
                        "note_id": note_id,
                        "blogger_id": blogger_id,
                        "blogger_name": blogger_name,
                        "image_index": j,
                        "image_url": u,
                        "ocr_provider": ocr_runtime.provider,
                        "ocr_text": txt,
                        "ocr_char_count": len(txt or ""),
                        "ocr_ok": "是" if (txt and not err) else "否",
                        "ocr_error": err,
                    }
                )

    # 提及增强：notes + comments
    mentions: list[dict[str, Any]] = []
    for r in note_rows_valid:
        nid = str(r.get("笔记ID", "")).strip()
        bid = str(r.get("博主ID", "")).strip()
        bname = str(r.get("博主昵称", "")).strip()
        mentions.extend(detect_mentions(str(r.get("笔记标题", "")), "note_title", "note", nid, bid, bname, "", fund_aliases))
        mentions.extend(detect_mentions(str(r.get("笔记内容", "")), "note_content", "note", nid, bid, bname, "", fund_aliases))
        mentions.extend(detect_mentions(str(r.get("笔记话题", "")), "note_topic", "note", nid, bid, bname, "", fund_aliases))

    if not comment_all_df.empty:
        comment_rows2 = comment_all_df.to_dict(orient="records")
        for i, r in enumerate(comment_rows2, start=1):
            nid = str(r.get("笔记ID", "")).strip()
            bid = str(r.get("博主ID", "")).strip()
            bname = str(r.get("博主昵称", "")).strip()
            text_for_detect = str(r.get("评论正文猜测", "")) or str(r.get("评论内容", ""))
            source_field = "comment_body_self" if str(r.get("是否博主本人评论(猜测)", "")) == "是" else "comment_body"
            mentions.extend(
                detect_mentions(text_for_detect, source_field, "comment", nid, bid, bname, i, fund_aliases)
            )

    if ocr_rows:
        for i, rr in enumerate(ocr_rows, start=1):
            txt = str(rr.get("ocr_text", "")).strip()
            if not txt:
                continue
            image_source = str(rr.get("image_source", "")).strip()
            source_field = "ocr_comment_image_text" if image_source == "comment_image" else "ocr_image_text"
            entity_type = "comment_ocr" if image_source == "comment_image" else "note_ocr"
            comment_index = str(rr.get("comment_id", "")).strip() or i
            mentions.extend(
                detect_mentions(
                    txt,
                    source_field,
                    entity_type,
                    str(rr.get("note_id", "")),
                    str(rr.get("blogger_id", "")),
                    str(rr.get("blogger_name", "")),
                    comment_index,
                    fund_aliases,
                )
            )

    mentions_df = pd.DataFrame(mentions, columns=MENTION_COLUMNS)
    if not mentions_df.empty:
        mentions_df = mentions_df.drop_duplicates(
            subset=["entity_type", "note_id", "comment_index", "fund_code", "alias_hit", "source_field"], keep="first"
        )

    image_audit_df = pd.DataFrame(image_audit_rows, columns=IMAGE_AUDIT_COLUMNS)
    if not image_audit_df.empty:
        image_audit_df["kept_for_ocr"] = image_audit_df.apply(
            lambda rr: "是"
            if (
                str(rr.get("is_valid", "")) == "是"
                and (str(rr.get("note_id", "")), str(rr.get("image_url", ""))) in ocr_kept_pairs
            )
            else "否",
            axis=1,
        )
    image_valid_df = image_audit_df[image_audit_df["is_valid"] == "是"].copy() if not image_audit_df.empty else pd.DataFrame(columns=IMAGE_AUDIT_COLUMNS)
    image_invalid_df = image_audit_df[image_audit_df["is_valid"] != "是"].copy() if not image_audit_df.empty else pd.DataFrame(columns=IMAGE_AUDIT_COLUMNS)

    if not mentions_df.empty:
        cand_df = mentions_df[
            (mentions_df["fund_code_text"].fillna("").astype(str).str.strip() == "")
            & (mentions_df["fund_name"].fillna("").astype(str).str.strip() != "")
        ].copy()
        if not cand_df.empty:
            cand_df["主要来源"] = cand_df["source_field"].fillna("").astype(str)
            candidate_unmapped_df = (
                cand_df.groupby("fund_name", dropna=False)
                .agg(
                    提及次数=("record_id", "count"),
                    涉及博主数=("blogger_id", pd.Series.nunique),
                    涉及笔记数=("note_id", pd.Series.nunique),
                    主要来源=("主要来源", lambda s: "、".join(sorted(set([str(x) for x in s if str(x)]))[:3])),
                    样例片段=("snippet", lambda s: next((str(x)[:80] for x in s if str(x).strip()), "")),
                )
                .reset_index()
                .rename(columns={"fund_name": "候选基金名"})
                .sort_values(["提及次数", "涉及博主数"], ascending=False)
            )
        else:
            candidate_unmapped_df = pd.DataFrame(columns=UNMAPPED_CANDIDATE_COLUMNS)
    else:
        candidate_unmapped_df = pd.DataFrame(columns=UNMAPPED_CANDIDATE_COLUMNS)

    # 未映射候选基金名 -> 自动匹配建议（用于词典补齐）
    alias_suggest_rows: list[dict[str, Any]] = []
    known_funds = []
    for ff in fund_aliases:
        code = str(ff.fund_code or "").strip().zfill(6) if str(ff.fund_code or "").strip().isdigit() else str(ff.fund_code or "").strip()
        name = str(ff.fund_name or "").strip()
        if not name:
            continue
        known_funds.append((code, name))
    if not candidate_unmapped_df.empty and known_funds:
        for _, rr in candidate_unmapped_df.iterrows():
            cand = str(rr.get("候选基金名", "")).strip()
            mention_times = int(rr.get("提及次数", 0) or 0)
            if not cand:
                continue
            best_code = ""
            best_name = ""
            best_score = 0.0
            best_reason = ""
            for code, name in known_funds:
                s = _name_similarity(cand, name)
                if s > best_score:
                    best_score = s
                    best_code = code
                    best_name = name
            if best_score >= 0.45:
                c_norm = _normalize_for_match(cand)
                n_norm = _normalize_for_match(best_name)
                if c_norm == n_norm:
                    best_reason = "同名归一化完全一致"
                elif c_norm in n_norm or n_norm in c_norm:
                    best_reason = "名称包含关系"
                else:
                    best_reason = "字符集合与连续片段相近"
                alias_suggest_rows.append(
                    {
                        "候选基金名": cand,
                        "提及次数": mention_times,
                        "建议匹配fund_code": best_code,
                        "建议匹配fund_name": best_name,
                        "相似度": round(float(best_score), 4),
                        "匹配依据": best_reason,
                    }
                )
    alias_suggest_df = pd.DataFrame(alias_suggest_rows, columns=ALIAS_SUGGEST_COLUMNS)
    if not alias_suggest_df.empty:
        alias_suggest_df = alias_suggest_df.sort_values(["相似度", "提及次数"], ascending=False)

    signal_rows: list[dict[str, Any]] = []
    signal_corpus: list[tuple[str, str, str]] = []  # (note_id, blogger_id, text)
    for r in note_rows_valid:
        nid = str(r.get("笔记ID", "")).strip()
        bid = str(r.get("博主ID", "")).strip()
        txt = " ".join(
            [
                str(r.get("笔记标题", "") or ""),
                str(r.get("笔记内容", "") or ""),
                str(r.get("笔记话题", "") or ""),
            ]
        )
        signal_corpus.append((nid, bid, txt))
    if not comment_all_df.empty:
        for _, r in comment_all_df.iterrows():
            nid = str(r.get("笔记ID", "")).strip()
            bid = str(r.get("博主ID", "")).strip()
            txt = str(r.get("评论正文猜测", "") or r.get("评论内容", "") or "")
            signal_corpus.append((nid, bid, txt))
    if ocr_rows:
        for r in ocr_rows:
            nid = str(r.get("note_id", "")).strip()
            bid = str(r.get("blogger_id", "")).strip()
            txt = str(r.get("ocr_text", "") or "")
            signal_corpus.append((nid, bid, txt))

    for tag, desc in SIGNAL_TAG_RULES:
        total_hits = 0
        note_hit: set[str] = set()
        blogger_hit: set[str] = set()
        for nid, bid, txt in signal_corpus:
            t = str(txt or "")
            if not t:
                continue
            c = t.count(tag)
            if c <= 0:
                continue
            total_hits += c
            if nid:
                note_hit.add(nid)
            if bid:
                blogger_hit.add(bid)
        signal_rows.append(
            {
                "信号标签": tag,
                "命中次数": int(total_hits),
                "命中笔记数": int(len(note_hit)),
                "命中博主数": int(len(blogger_hit)),
                "说明": desc,
            }
        )
    signal_tag_df = pd.DataFrame(signal_rows, columns=SIGNAL_TAG_COLUMNS).sort_values("命中次数", ascending=False)

    # 广告/投放识别：按笔记、博主、基金三层输出可操作信号
    mention_note_map: dict[str, set[str]] = {}
    mention_cnt_map: dict[str, int] = {}
    blogger_fund_set_map: dict[str, set[str]] = {}
    if not mentions_df.empty:
        for _, rr in mentions_df.iterrows():
            nid = str(rr.get("note_id", "")).strip()
            bid = str(rr.get("blogger_id", "")).strip()
            fname = str(rr.get("fund_name", "")).strip()
            if nid and fname:
                mention_note_map.setdefault(nid, set()).add(fname)
            if nid:
                mention_cnt_map[nid] = mention_cnt_map.get(nid, 0) + 1
            if bid and fname:
                blogger_fund_set_map.setdefault(bid, set()).add(fname)

    comment_self_map: dict[str, list[str]] = {}
    if not comment_self_df.empty:
        for _, rr in comment_self_df.iterrows():
            nid = str(rr.get("笔记ID", "")).strip()
            txt = str(rr.get("评论正文猜测", "") or rr.get("评论内容", "") or "").strip()
            if nid and txt:
                comment_self_map.setdefault(nid, []).append(txt)

    ocr_text_map: dict[str, list[str]] = {}
    if ocr_rows:
        for rr in ocr_rows:
            nid = str(rr.get("note_id", "")).strip()
            txt = str(rr.get("ocr_text", "")).strip()
            if nid and txt:
                ocr_text_map.setdefault(nid, []).append(txt)

    note_sponsor_rows: list[dict[str, Any]] = []
    blogger_signal_counter: dict[str, Counter] = {}
    for rr in note_rows_valid:
        nid = str(rr.get("笔记ID", "")).strip()
        bid = str(rr.get("博主ID", "")).strip()
        bname = str(rr.get("博主昵称", "")).strip()
        note_text = " ".join(
            [
                str(rr.get("笔记标题", "") or ""),
                str(rr.get("笔记内容", "") or ""),
                str(rr.get("笔记话题", "") or ""),
            ]
        )
        self_text = " ".join(comment_self_map.get(nid, []))
        ocr_text = " ".join(ocr_text_map.get(nid, []))
        r_note, c_note, h_note = _collect_weighted_signals(note_text, SPONSOR_SIGNAL_WEIGHTS)
        r_self, c_self, h_self = _collect_weighted_signals(self_text, SPONSOR_SIGNAL_WEIGHTS)
        r_ocr, c_ocr, h_ocr = _collect_weighted_signals(ocr_text, SPONSOR_SIGNAL_WEIGHTS)
        raw_total = r_note + r_self * 0.9 + r_ocr * 0.65
        hit_total = int(c_note + c_self + c_ocr)
        hit_counter = h_note + h_self + h_ocr
        score = round((1 - math.exp(-raw_total / 5.2)) * 100, 2) if raw_total > 0 else 0.0
        level = _risk_level_by_score(score)
        fund_list = sorted(list(mention_note_map.get(nid, set())))
        fund_list_text = "、".join(fund_list[:8])
        mention_cnt = int(mention_cnt_map.get(nid, 0))
        kws = "、".join([f"{k}({v})" for k, v in hit_counter.most_common(5)])
        note_sponsor_rows.append(
            {
                "笔记ID": nid,
                "博主ID": bid,
                "博主昵称": bname,
                "基金提及数": mention_cnt,
                "提及基金列表": fund_list_text,
                "广告信号命中次数": hit_total,
                "广告信号关键词": kws,
                "广告可能性分": score,
                "风险等级": level,
                "运营建议": _advice_by_risk(level, mention_cnt, score),
            }
        )
        if bid and hit_counter:
            blogger_signal_counter.setdefault(bid, Counter()).update(hit_counter)

    note_sponsor_df = pd.DataFrame(note_sponsor_rows, columns=SPONSOR_NOTE_COLUMNS)
    if not note_sponsor_df.empty:
        note_sponsor_df = note_sponsor_df.sort_values(["广告可能性分", "基金提及数"], ascending=False)

    if not note_sponsor_df.empty:
        blogger_sponsor_df = (
            note_sponsor_df.groupby(["博主ID", "博主昵称"], dropna=False)
            .agg(
                样本笔记数=("笔记ID", "count"),
                高风险笔记数=("风险等级", lambda s: int((s == "高").sum())),
                中风险笔记数=("风险等级", lambda s: int((s == "中").sum())),
                平均广告分=("广告可能性分", "mean"),
                最大广告分=("广告可能性分", "max"),
            )
            .reset_index()
        )
        blogger_sponsor_df["平均广告分"] = blogger_sponsor_df["平均广告分"].round(2)
        blogger_sponsor_df["最大广告分"] = blogger_sponsor_df["最大广告分"].round(2)
        blogger_sponsor_df["涉及基金数"] = blogger_sponsor_df["博主ID"].astype(str).map(
            lambda x: len(blogger_fund_set_map.get(x, set()))
        )
        blogger_sponsor_df["核心信号词"] = blogger_sponsor_df["博主ID"].astype(str).map(
            lambda x: "、".join([f"{k}({v})" for k, v in blogger_signal_counter.get(x, Counter()).most_common(4)])
        )

        def _blogger_sponsor_advice(r: pd.Series) -> str:
            if r["高风险笔记数"] >= 2 or r["最大广告分"] >= 80:
                return "优先观察：疑似投放密集达人"
            if r["中风险笔记数"] >= 2 or r["平均广告分"] >= 45:
                return "重点跟踪：存在持续营销信号"
            if r["平均广告分"] >= 25:
                return "常规跟踪：有轻度营销线索"
            return "低优先级：暂未见显著营销信号"

        blogger_sponsor_df["运营建议"] = blogger_sponsor_df.apply(_blogger_sponsor_advice, axis=1)
        blogger_sponsor_df = blogger_sponsor_df[SPONSOR_BLOGGER_COLUMNS].sort_values(
            ["高风险笔记数", "平均广告分", "样本笔记数"], ascending=False
        )
    else:
        blogger_sponsor_df = pd.DataFrame(columns=SPONSOR_BLOGGER_COLUMNS)

    if not mentions_df.empty and not note_sponsor_df.empty:
        score_map = (
            note_sponsor_df[["笔记ID", "广告可能性分", "风险等级"]]
            .drop_duplicates(subset=["笔记ID"])
            .rename(columns={"笔记ID": "note_id"})
        )
        fund_note = (
            mentions_df[["note_id", "fund_code_text", "fund_name", "mention_role", "sentiment"]]
            .copy()
            .merge(score_map, on="note_id", how="left")
        )
        fund_note["fund_code_text"] = fund_note["fund_code_text"].fillna("").astype(str)
        fund_note["fund_name"] = fund_note["fund_name"].fillna("").astype(str)
        fund_note = fund_note[(fund_note["fund_code_text"] != "") | (fund_note["fund_name"] != "")]

        if not fund_note.empty:
            fund_dedup = fund_note.drop_duplicates(subset=["note_id", "fund_code_text", "fund_name"])
            fund_sponsor_df = (
                fund_dedup.groupby(["fund_code_text", "fund_name"], dropna=False)
                .agg(
                    涉及笔记数=("note_id", pd.Series.nunique),
                    高风险关联笔记数=("风险等级", lambda s: int((s == "高").sum())),
                    平均关联广告分=("广告可能性分", "mean"),
                )
                .reset_index()
                .rename(columns={"fund_code_text": "fund_code"})
            )
            role_stats = (
                fund_note.groupby(["fund_code_text", "fund_name"], dropna=False)
                .agg(
                    主推次数=("mention_role", lambda s: int((s == "主推").sum())),
                    对比次数=("mention_role", lambda s: int((s == "对比").sum())),
                    负向占比=("sentiment", lambda s: round(float((s == "负向").mean()), 4)),
                )
                .reset_index()
                .rename(columns={"fund_code_text": "fund_code"})
            )
            fund_sponsor_df = fund_sponsor_df.merge(
                role_stats, on=["fund_code", "fund_name"], how="left"
            )
            fund_sponsor_df["平均关联广告分"] = fund_sponsor_df["平均关联广告分"].fillna(0).round(2)
            fund_sponsor_df["主推次数"] = fund_sponsor_df["主推次数"].fillna(0).astype(int)
            fund_sponsor_df["对比次数"] = fund_sponsor_df["对比次数"].fillna(0).astype(int)
            fund_sponsor_df["负向占比"] = fund_sponsor_df["负向占比"].fillna(0).round(4)

            def _fund_sponsor_advice(r: pd.Series) -> str:
                if r["高风险关联笔记数"] >= 2 and r["平均关联广告分"] >= 55:
                    return "高关注：疑似投放关联较强，建议重点竞品观察"
                if r["高风险关联笔记数"] >= 1 or r["平均关联广告分"] >= 45:
                    return "中关注：存在营销关联，建议纳入周跟踪"
                if r["平均关联广告分"] >= 25:
                    return "低关注：轻度营销线索，持续观察"
                return "常规观察"

            fund_sponsor_df["运营建议"] = fund_sponsor_df.apply(_fund_sponsor_advice, axis=1)
            fund_sponsor_df = fund_sponsor_df[SPONSOR_FUND_COLUMNS].sort_values(
                ["高风险关联笔记数", "平均关联广告分", "涉及笔记数"], ascending=False
            )
        else:
            fund_sponsor_df = pd.DataFrame(columns=SPONSOR_FUND_COLUMNS)
    else:
        fund_sponsor_df = pd.DataFrame(columns=SPONSOR_FUND_COLUMNS)

    # 运营汇总
    if not mentions_df.empty:
        fund_sum = (
            mentions_df.groupby(["fund_code_text", "fund_name"], dropna=False)
            .agg(
                提及次数=("record_id", "count"),
                主推次数=("mention_role", lambda s: int((s == "主推").sum())),
                对比次数=("mention_role", lambda s: int((s == "对比").sum())),
                正向次数=("sentiment", lambda s: int((s == "正向").sum())),
                负向次数=("sentiment", lambda s: int((s == "负向").sum())),
                涉及博主数=("blogger_id", pd.Series.nunique),
                涉及笔记数=("note_id", pd.Series.nunique),
                平均置信度=("confidence", "mean"),
            )
            .reset_index()
            .sort_values(["提及次数", "主推次数", "正向次数"], ascending=False)
        )
        fund_sum = fund_sum.rename(columns={"fund_code_text": "fund_code"})
        fund_sum["平均置信度"] = fund_sum["平均置信度"].round(4)
        fund_sum["主推占比"] = (fund_sum["主推次数"] / fund_sum["提及次数"]).round(4)
        fund_sum["正向占比"] = (fund_sum["正向次数"] / fund_sum["提及次数"]).round(4)
        fund_sum["负向占比"] = (fund_sum["负向次数"] / fund_sum["提及次数"]).round(4)
        fund_sum["对比占比"] = (fund_sum["对比次数"] / fund_sum["提及次数"]).round(4)
        fund_sum["综合热度分"] = (
            fund_sum["提及次数"] * 0.4
            + fund_sum["主推占比"] * 100 * 0.3
            + fund_sum["正向占比"] * 100 * 0.3
        ).round(2)

        def _fund_action(r: pd.Series) -> str:
            if r["提及次数"] >= 5 and r["主推占比"] >= 0.4 and r["正向占比"] >= 0.4:
                return "优先跟进：可做重点内容/投放观察"
            if r["负向占比"] >= 0.35:
                return "风险观察：关注负面反馈与回撤话术"
            if r["对比占比"] >= 0.3:
                return "竞品对比高：建议补充差异化话术"
            if r["提及次数"] >= 3 and r["主推占比"] >= 0.3:
                return "跟进观察：已出现持续提及，建议纳入周跟踪池"
            return "常规观察：继续累计样本"

        fund_sum["运营建议"] = fund_sum.apply(_fund_action, axis=1)

        blogger_sum = (
            mentions_df.groupby(["blogger_id", "blogger_name"], dropna=False)
            .agg(
                提及次数=("record_id", "count"),
                提及基金数=("fund_code", pd.Series.nunique),
                主推次数=("mention_role", lambda s: int((s == "主推").sum())),
                对比次数=("mention_role", lambda s: int((s == "对比").sum())),
                正向次数=("sentiment", lambda s: int((s == "正向").sum())),
                负向次数=("sentiment", lambda s: int((s == "负向").sum())),
            )
            .reset_index()
            .sort_values(["提及次数", "主推次数"], ascending=False)
        )
        blogger_sum["主推占比"] = (blogger_sum["主推次数"] / blogger_sum["提及次数"]).round(4)
        blogger_sum["正向占比"] = (blogger_sum["正向次数"] / blogger_sum["提及次数"]).round(4)
        blogger_sum["负向占比"] = (blogger_sum["负向次数"] / blogger_sum["提及次数"]).round(4)

        def _blogger_action(r: pd.Series) -> str:
            if r["提及次数"] >= 5 and r["提及基金数"] >= 3 and r["主推占比"] >= 0.35:
                return "高价值达人：可重点跟踪合作/竞品投放动向"
            if r["负向占比"] >= 0.35:
                return "争议达人：关注负面舆情扩散风险"
            return "常规达人：持续观察"

        blogger_sum["运营建议"] = blogger_sum.apply(_blogger_action, axis=1)

        ops_action_fund = fund_sum[
            [
                "fund_code",
                "fund_name",
                "提及次数",
                "主推次数",
                "对比次数",
                "正向次数",
                "负向次数",
                "主推占比",
                "正向占比",
                "负向占比",
                "综合热度分",
                "运营建议",
            ]
        ].sort_values(["综合热度分", "提及次数"], ascending=False)

        ops_action_blogger = blogger_sum[
            [
                "blogger_id",
                "blogger_name",
                "提及次数",
                "提及基金数",
                "主推次数",
                "对比次数",
                "正向次数",
                "负向次数",
                "主推占比",
                "正向占比",
                "负向占比",
                "运营建议",
            ]
        ].sort_values(["提及次数", "提及基金数"], ascending=False)
    else:
        fund_sum = pd.DataFrame(columns=FUND_SUMMARY_COLUMNS)
        blogger_sum = pd.DataFrame(columns=BLOGGER_SUMMARY_COLUMNS)
        ops_action_fund = pd.DataFrame(columns=FUND_SUMMARY_COLUMNS[:])
        ops_action_blogger = pd.DataFrame(columns=BLOGGER_SUMMARY_COLUMNS[:])

    leshu_tag_df = pd.DataFrame()
    if args.leshu_tag_file:
        leshu_path = Path(args.leshu_tag_file).expanduser().resolve()
        leshu_tag_df = _read_table_any(leshu_path)
    fund_tag_bridge_df = _build_dual_tag_bridge(fund_sum, leshu_tag_df)

    run_meta = pd.DataFrame(
        [
            {
                "input_result": str(input_path),
                "crawl_comments": bool(args.crawl_comments),
                "max_notes_for_comments": args.max_notes_for_comments,
                "max_comments_per_note": args.max_comments_per_note,
                "note_count": len(note_rows),
                "existing_comment_count": 0 if comment_df_existing.empty else len(comment_df_existing),
                "new_crawled_comment_count": len(crawled_comment_rows),
                "merged_comment_count": 0 if comment_all_df.empty else len(comment_all_df),
                "self_comment_count": 0 if comment_self_df.empty else len(comment_self_df),
                "ocr_row_count": len(ocr_rows),
                "ocr_hit_count": int(sum(1 for x in ocr_rows if str(x.get("ocr_text", "")).strip())),
                "ocr_comment_row_count": int(sum(1 for x in ocr_rows if str(x.get("image_source", "")) == "comment_image")),
                "ocr_comment_hit_count": int(
                    sum(
                        1
                        for x in ocr_rows
                        if str(x.get("image_source", "")) == "comment_image" and str(x.get("ocr_text", "")).strip()
                    )
                ),
                "image_audit_count": 0 if image_audit_df.empty else len(image_audit_df),
                "valid_image_count": 0 if image_valid_df.empty else len(image_valid_df),
                "invalid_image_count": 0 if image_invalid_df.empty else len(image_invalid_df),
                "invalid_image_ratio": round(
                    (0 if image_audit_df.empty else len(image_invalid_df) / max(1, len(image_audit_df))), 4
                ),
                "mention_count": 0 if mentions_df.empty else len(mentions_df),
                "fund_summary_count": 0 if fund_sum.empty else len(fund_sum),
                "blogger_summary_count": 0 if blogger_sum.empty else len(blogger_sum),
                "dual_tag_bridge_count": 0 if fund_tag_bridge_df.empty else len(fund_tag_bridge_df),
                "external_tag_source_count": 0 if leshu_tag_df.empty else len(leshu_tag_df),
                "sponsor_note_count": 0 if note_sponsor_df.empty else len(note_sponsor_df),
                "sponsor_high_note_count": 0
                if note_sponsor_df.empty
                else int((note_sponsor_df["风险等级"] == "高").sum()),
                "sponsor_blogger_count": 0 if blogger_sponsor_df.empty else len(blogger_sponsor_df),
                "sponsor_fund_count": 0 if fund_sponsor_df.empty else len(fund_sponsor_df),
                "unmapped_candidate_count": 0 if candidate_unmapped_df.empty else len(candidate_unmapped_df),
                "alias_suggest_count": 0 if alias_suggest_df.empty else len(alias_suggest_df),
                "signal_tag_nonzero_count": int((signal_tag_df["命中次数"].fillna(0) > 0).sum()) if not signal_tag_df.empty else 0,
                "failed_sheet_count": 0 if failed_df.empty else len(failed_df),
                "comment_crawl_success_note_count": crawled_note_count,
                "comment_crawl_failed_note_count": crawl_failed_count,
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        ]
    )

    digest_rows = [
        {"模块": "采集规模", "指标": "笔记数", "数值": int(len(note_rows_valid)), "说明": "进入增强分析的笔记总数"},
        {
            "模块": "图片质量",
            "指标": "图片审计总数",
            "数值": int(0 if image_audit_df.empty else len(image_audit_df)),
            "说明": "参与有效性审计的图片条数",
        },
        {
            "模块": "图片质量",
            "指标": "有效图片数",
            "数值": int(0 if image_valid_df.empty else len(image_valid_df)),
            "说明": "通过审计并保留用于分析的图片",
        },
        {
            "模块": "图片质量",
            "指标": "无效图片数",
            "数值": int(0 if image_invalid_df.empty else len(image_invalid_df)),
            "说明": "已归档（不会进入OCR和后续识别）",
        },
        {
            "模块": "图片质量",
            "指标": "无效图片占比",
            "数值": round((0 if image_audit_df.empty else len(image_invalid_df) / max(1, len(image_audit_df))) * 100, 2),
            "说明": "%",
        },
        {
            "模块": "文本识别",
            "指标": "OCR命中图数",
            "数值": int(sum(1 for x in ocr_rows if str(x.get('ocr_text', '')).strip())),
            "说明": "识别到有效文本的图片数",
        },
        {
            "模块": "文本识别",
            "指标": "评论图片OCR命中图数",
            "数值": int(
                sum(
                    1
                    for x in ocr_rows
                    if str(x.get("image_source", "")) == "comment_image" and str(x.get("ocr_text", "")).strip()
                )
            ),
            "说明": "识别到有效文本的评论图片数",
        },
        {
            "模块": "基金识别",
            "指标": "提及记录数",
            "数值": int(0 if mentions_df.empty else len(mentions_df)),
            "说明": "含标题/正文/话题/评论/OCR来源",
        },
        {
            "模块": "基金识别",
            "指标": "未映射候选数",
            "数值": int(0 if candidate_unmapped_df.empty else len(candidate_unmapped_df)),
            "说明": "可用于补充别名词典",
        },
        {
            "模块": "基金识别",
            "指标": "自动匹配建议数",
            "数值": int(0 if alias_suggest_df.empty else len(alias_suggest_df)),
            "说明": "未映射候选与已有基金名称的相似匹配建议",
        },
        {
            "模块": "标签对齐",
            "指标": "双标签桥接数",
            "数值": int(0 if fund_tag_bridge_df.empty else len(fund_tag_bridge_df)),
            "说明": "博主侧标签与外部标签的基金级对照条数",
        },
        {
            "模块": "投放识别",
            "指标": "高风险笔记数",
            "数值": int(0 if note_sponsor_df.empty else (note_sponsor_df["风险等级"] == "高").sum()),
            "说明": "广告可能性分>=70的笔记数量",
        },
        {
            "模块": "投放识别",
            "指标": "高风险达人数",
            "数值": int(
                0
                if blogger_sponsor_df.empty
                else (blogger_sponsor_df["高风险笔记数"].fillna(0).astype(int) > 0).sum()
            ),
            "说明": "至少含1条高风险笔记的达人数",
        },
    ]
    if not signal_tag_df.empty:
        nz = signal_tag_df[signal_tag_df["命中次数"] > 0].head(5)
        digest_rows.append(
            {
                "模块": "内容信号",
                "指标": "信号Top5",
                "数值": "；".join([f"{r['信号标签']}:{int(r['命中次数'])}" for _, r in nz.iterrows()]) if len(nz) else "",
                "说明": "内容方向快速判断",
            }
        )
    if not image_invalid_df.empty:
        top_invalid = (
            image_invalid_df["invalid_reason"].fillna("").astype(str).value_counts().head(3).to_dict()
        )
        digest_rows.append(
            {
                "模块": "图片质量",
                "指标": "无效主因Top3",
                "数值": "；".join([f"{k}:{v}" for k, v in top_invalid.items()]),
                "说明": "用于定向修复",
            }
        )
    ops_digest_df = pd.DataFrame(digest_rows, columns=["模块", "指标", "数值", "说明"])

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = output_dir / f"ops_enriched_{ts}.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        note_df_valid.to_excel(writer, index=False, sheet_name=safe_sheet("note_export"))
        blogger_df.to_excel(writer, index=False, sheet_name=safe_sheet("blogger_export"))
        failed_df.to_excel(writer, index=False, sheet_name=safe_sheet("failed"))
        comment_all_df.to_excel(writer, index=False, sheet_name=safe_sheet("comment_export_enhanced"))
        comment_self_df.to_excel(writer, index=False, sheet_name=safe_sheet("comment_self_only"))
        pd.DataFrame(ocr_rows, columns=OCR_IMAGE_COLUMNS).to_excel(writer, index=False, sheet_name=safe_sheet("ocr_note_images"))
        image_audit_df.to_excel(writer, index=False, sheet_name=safe_sheet("image_audit_all"))
        image_invalid_df.to_excel(writer, index=False, sheet_name=safe_sheet("image_invalid_archive"))
        image_valid_df.to_excel(writer, index=False, sheet_name=safe_sheet("image_valid_kept"))
        mentions_df.to_excel(writer, index=False, sheet_name=safe_sheet("fund_mentions_enhanced"))
        candidate_unmapped_df.to_excel(writer, index=False, sheet_name=safe_sheet("fund_unmapped_candidates"))
        alias_suggest_df.to_excel(writer, index=False, sheet_name=safe_sheet("fund_alias_suggest"))
        signal_tag_df.to_excel(writer, index=False, sheet_name=safe_sheet("signal_tag_summary"))
        note_sponsor_df.to_excel(writer, index=False, sheet_name=safe_sheet("ops_sponsor_note"))
        blogger_sponsor_df.to_excel(writer, index=False, sheet_name=safe_sheet("ops_sponsor_blogger"))
        fund_sponsor_df.to_excel(writer, index=False, sheet_name=safe_sheet("ops_sponsor_fund"))
        fund_sum.to_excel(writer, index=False, sheet_name=safe_sheet("ops_summary_fund"))
        blogger_sum.to_excel(writer, index=False, sheet_name=safe_sheet("ops_summary_blogger"))
        fund_tag_bridge_df.to_excel(writer, index=False, sheet_name=safe_sheet("fund_tag_bridge"))
        leshu_tag_df.to_excel(writer, index=False, sheet_name=safe_sheet("external_tag_source"))
        ops_action_fund.to_excel(writer, index=False, sheet_name=safe_sheet("ops_action_fund"))
        ops_action_blogger.to_excel(writer, index=False, sheet_name=safe_sheet("ops_action_blogger"))
        ops_digest_df.to_excel(writer, index=False, sheet_name=safe_sheet("ops_digest"))
        run_meta.to_excel(writer, index=False, sheet_name=safe_sheet("meta"))

    out_json = output_dir / f"ops_enriched_{ts}_summary.json"
    summary = run_meta.to_dict(orient="records")[0]
    summary["output_excel"] = str(out_xlsx)
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
