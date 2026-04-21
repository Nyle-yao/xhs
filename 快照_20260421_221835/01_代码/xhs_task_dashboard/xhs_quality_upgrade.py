from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from copy import copy
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_INPUT = Path("/Users/yaoruanxingchen/c/xhs_task_dashboard/outputs/xhs_batch_merged_20260419_160016.xlsx")
DEFAULT_ALIAS_JSON = Path("/Users/yaoruanxingchen/c/xhs_task_dashboard/fund_aliases_expanded.json")
DEFAULT_FUND_TAG_XLSX = Path("/Users/yaoruanxingchen/c/exports/addsub/基金名称-标签整理_全历史池_修复清洗版_20260420.xlsx")
DEFAULT_OUTPUT_DIR = Path("/Users/yaoruanxingchen/Desktop/小红书爬虫/04_输出数据_当前")


FUND_KEYWORDS = [
    "基金", "债券", "混合", "指数", "ETF", "etf", "QDII", "qdii", "联接",
    "定投", "加仓", "减仓", "持有", "申购", "赎回", "收益", "回撤", "净值",
]

PROMOTE_KEYWORDS = ["主推", "推荐", "看好", "加仓", "上车", "定投", "布局", "值得关注", "可关注", "继续拿"]
COMPARE_KEYWORDS = ["对比", "比较", "VS", "vs", "pk", "PK", "不如", "优于", "替代", "二选一", "横评"]
NEGATIVE_KEYWORDS = ["风险", "回撤", "下跌", "减仓", "卖出", "避雷", "亏损", "谨慎", "不建议", "踩雷"]
AD_KEYWORDS = ["合作", "推广", "广告", "商务", "投放", "恰饭", "种草", "福利", "专属", "链接", "私信", "置顶"]

GENERIC_CANDIDATE_STOPWORDS = {
    "基金",
    "指数基金",
    "债券基金",
    "混合基金",
    "ETF基金",
    "etf基金",
    "QDII基金",
    "股票基金",
    "公募基金",
    "私募基金",
    "行业基金",
    "主动基金",
    "场内基金",
    "场外基金",
    "货币基金",
    "理财基金",
    "人生自由基金",
    "自由基金",
}

PLACEHOLDER_PATTERNS = [
    r"发现\s*直播\s*发布\s*通知",
    r"发现直播发布通知",
]

SHEETS = {
    "source": "source_blogger_list",
    "blogger": "blogger_export",
    "note": "note_export",
    "comment": "comment_export",
    "mention": "fund_mentions",
    "failed": "failed",
    "meta": "meta",
}


def norm_code(v: Any) -> str:
    s = str(v or "").strip()
    if not s or s.lower() == "nan":
        return ""
    s = re.sub(r"\.0$", "", s)
    s = re.sub(r"[^\d]", "", s)
    if not s:
        return ""
    if len(s) < 6:
        return s.zfill(6)
    if len(s) > 6:
        return s[-6:]
    return s


def clean_text(v: Any) -> str:
    s = str(v or "").replace("\u3000", " ").strip()
    if s.lower() == "nan":
        return ""
    return re.sub(r"\s+", " ", s)


def compact_text(v: Any) -> str:
    return re.sub(r"\s+", "", clean_text(v))


def split_urls(v: Any) -> list[str]:
    s = clean_text(v)
    if not s:
        return []
    parts = re.split(r"[\n\r,，、;；]+", s)
    out = []
    seen = set()
    for p in parts:
        p = p.strip()
        if p.startswith("http") and p not in seen:
            seen.add(p)
            out.append(p)
    return out


def parse_num(v: Any) -> float:
    s = clean_text(v)
    if not s:
        return 0.0
    s = s.replace(",", "")
    mult = 1.0
    if "万" in s:
        mult = 10000.0
    if "千" in s:
        mult = 1000.0
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group(0)) * mult if m else 0.0


def safe_read(path: Path, sheet: str) -> pd.DataFrame:
    try:
        xl = pd.ExcelFile(path)
        if sheet not in xl.sheet_names:
            return pd.DataFrame()
        return pd.read_excel(path, sheet_name=sheet, dtype=str)
    except Exception:
        return pd.DataFrame()


def name_variants(name: str) -> list[str]:
    s = clean_text(name)
    if not s:
        return []
    variants = {
        s,
        s.replace("（", "(").replace("）", ")"),
        re.sub(r"[A-E]$", "", s),
        re.sub(r"(A|B|C|D|E)类$", "", s),
        s.replace("ETF联接", "ETF"),
        s.replace("联接", ""),
    }
    if "(" in s:
        variants.add(re.sub(r"\([^)]*\)", "", s))
    if "（" in s:
        variants.add(re.sub(r"（[^）]*）", "", s))
    out = []
    for v in variants:
        v = clean_text(v)
        if v and v not in out:
            out.append(v)
    return out


def load_aliases(alias_json: Path, fund_tag_xlsx: Path) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    if alias_json.exists():
        try:
            for r in json.loads(alias_json.read_text(encoding="utf-8")):
                code = norm_code(r.get("fund_code"))
                name = clean_text(r.get("fund_name"))
                if not code and not name:
                    continue
                key = code or name
                aliases = [clean_text(x) for x in (r.get("aliases") or []) if clean_text(x)]
                merged[key] = {"fund_code": code, "fund_name": name, "aliases": aliases}
        except Exception:
            pass

    if fund_tag_xlsx.exists():
        for sheet in ["基金名称-标签汇总", "基金名称-标签明细"]:
            df = safe_read(fund_tag_xlsx, sheet)
            if df.empty:
                continue
            for _, r in df.iterrows():
                code = norm_code(r.get("基金代码"))
                name = clean_text(r.get("基金名称"))
                if not code and not name:
                    continue
                key = code or name
                if key not in merged:
                    merged[key] = {"fund_code": code, "fund_name": name, "aliases": []}
                item = merged[key]
                if not item.get("fund_code") and code:
                    item["fund_code"] = code
                if not item.get("fund_name") and name:
                    item["fund_name"] = name
                for a in name_variants(name):
                    if a and a not in item["aliases"]:
                        item["aliases"].append(a)
                if code and code not in item["aliases"]:
                    item["aliases"].append(code)

    out = list(merged.values())
    out.sort(key=lambda x: (x.get("fund_code") or "", x.get("fund_name") or ""))
    return out


def classify_role_sentiment(text: str) -> tuple[str, str, str]:
    t = clean_text(text)
    promote = sum(k in t for k in PROMOTE_KEYWORDS)
    compare = sum(k in t for k in COMPARE_KEYWORDS)
    neg = sum(k in t for k in NEGATIVE_KEYWORDS)
    ad = sum(k in t for k in AD_KEYWORDS)
    role = "主推" if promote > compare and promote > 0 else ("对比" if compare > 0 else "提及")
    sentiment = "负向" if neg > 0 and neg >= promote else ("正向" if promote > 0 else "中性")
    ad_level = "高" if ad >= 2 else ("中" if ad == 1 else "低")
    return role, sentiment, ad_level


def build_alias_index(aliases: list[dict[str, Any]]) -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    alias_index: dict[str, dict[str, str]] = {}
    code_to_name: dict[str, str] = {}
    for item in aliases:
        code = norm_code(item.get("fund_code"))
        name = clean_text(item.get("fund_name"))
        if code:
            code_to_name[code] = name
        all_aliases = []
        all_aliases.extend(name_variants(name))
        all_aliases.extend([clean_text(x) for x in item.get("aliases", [])])
        if code:
            all_aliases.append(code)
        for a in all_aliases:
            a = clean_text(a)
            if not a:
                continue
            key = compact_text(a).lower()
            if not key:
                continue
            if len(key) < 4 and not key.isdigit():
                continue
            if key in {"基金", "债券", "指数基金", "混合基金", "etf基金"}:
                continue
            alias_index[key] = {"fund_code": code, "fund_name": name, "alias_hit": a}
    return alias_index, code_to_name


def detect_funds(text: str, alias_index: dict[str, dict[str, str]], code_to_name: dict[str, str]) -> list[dict[str, Any]]:
    raw = clean_text(text)
    if not raw:
        return []
    comp = compact_text(raw).lower()
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    has_context = any(k.lower() in raw.lower() for k in FUND_KEYWORDS)

    for alias_key, item in alias_index.items():
        if alias_key.isdigit():
            continue
        if alias_key in comp:
            key = (item.get("fund_code", ""), item.get("fund_name", "") or item.get("alias_hit", ""))
            if key in seen:
                continue
            seen.add(key)
            role, sentiment, ad_level = classify_role_sentiment(raw)
            conf = 0.88 if len(alias_key) >= 6 else 0.76
            out.append({
                **item,
                "match_type": "alias",
                "confidence": conf,
                "mention_role": role,
                "sentiment": sentiment,
                "ad_signal_level": ad_level,
            })

    for m in re.finditer(r"(?<!\d)(\d{6})(?!\d)", raw):
        code = norm_code(m.group(1))
        if not has_context or code not in code_to_name:
            continue
        key = (code, code_to_name.get(code, ""))
        if key in seen:
            continue
        seen.add(key)
        role, sentiment, ad_level = classify_role_sentiment(raw)
        out.append({
            "fund_code": code,
            "fund_name": code_to_name.get(code, ""),
            "alias_hit": code,
            "match_type": "code",
            "confidence": 0.72,
            "mention_role": role,
            "sentiment": sentiment,
            "ad_signal_level": ad_level,
        })

    # 词库未覆盖时保留候选实体，方便人工补别名，不直接当确定基金。
    for m in re.finditer(r"([\u4e00-\u9fa5A-Za-z]{2,24}(?:ETF联接|ETF|混合|债券|指数|QDII|基金)[A-E]?)", raw):
        name = clean_text(m.group(1))
        if name in GENERIC_CANDIDATE_STOPWORDS:
            continue
        if any(tok in name for tok in ["基金经理", "基金公司", "基金定投", "基金知识"]):
            continue
        key = ("", name)
        if key in seen:
            continue
        seen.add(key)
        role, sentiment, ad_level = classify_role_sentiment(raw)
        out.append({
            "fund_code": "",
            "fund_name": name,
            "alias_hit": name,
            "match_type": "candidate_name",
            "confidence": 0.48,
            "mention_role": role,
            "sentiment": sentiment,
            "ad_signal_level": ad_level,
        })
    return out


def is_placeholder_note(row: pd.Series) -> bool:
    text = compact_text(" ".join([clean_text(row.get("笔记标题")), clean_text(row.get("笔记内容")), clean_text(row.get("笔记话题"))]))
    if not text:
        return True
    return any(re.search(p, text) for p in PLACEHOLDER_PATTERNS)


def quality_label(score: float) -> str:
    if score >= 80:
        return "可分析"
    if score >= 55:
        return "需复核"
    return "需补抓"


def build_note_quality(note_df: pd.DataFrame, comment_df: pd.DataFrame, mention_df: pd.DataFrame) -> pd.DataFrame:
    comment_count_by_note = Counter(comment_df.get("笔记ID", pd.Series(dtype=str)).fillna("").astype(str))
    mention_count_by_note = Counter(mention_df.get("note_id", pd.Series(dtype=str)).fillna("").astype(str))
    rows = []
    for _, r in note_df.iterrows():
        note_id = clean_text(r.get("笔记ID"))
        title = clean_text(r.get("笔记标题"))
        content = clean_text(r.get("笔记内容"))
        topics = clean_text(r.get("笔记话题"))
        image_urls = split_urls(r.get("笔记图片链接")) or split_urls(r.get("笔记封面链接"))
        has_text = bool(title or content or topics)
        placeholder = is_placeholder_note(r)
        has_fund_kw = any(k.lower() in (title + content + topics).lower() for k in FUND_KEYWORDS)
        has_interaction = any(parse_num(r.get(c)) > 0 for c in ["点赞量", "收藏量", "评论量", "分享量"] if c in note_df.columns)
        comment_count = int(comment_count_by_note.get(note_id, 0))
        mention_count = int(mention_count_by_note.get(note_id, 0))
        score = 0
        score += 25 if has_text else 0
        score += 20 if not placeholder else -20
        score += 15 if has_interaction else 0
        score += 15 if image_urls else 0
        score += 15 if comment_count > 0 else 0
        score += 10 if mention_count > 0 or has_fund_kw else 0
        score = max(0, min(100, score))
        issues = []
        if placeholder:
            issues.append("占位内容")
        if not has_interaction:
            issues.append("互动字段缺失")
        if not image_urls:
            issues.append("图片缺失")
        if parse_num(r.get("评论量")) > 0 and comment_count == 0:
            issues.append("声明有评论但未抓到")
        if has_fund_kw and mention_count == 0:
            issues.append("有基金语义但未识别基金")
        rows.append({
            "博主ID": clean_text(r.get("博主ID")),
            "博主昵称": clean_text(r.get("博主昵称")),
            "笔记ID": note_id,
            "笔记链接": clean_text(r.get("笔记链接")),
            "笔记标题": title,
            "发布时间": clean_text(r.get("发布时间")),
            "图片数量": len(image_urls),
            "声明评论量": clean_text(r.get("评论量")),
            "实抓评论数": comment_count,
            "基金提及数": mention_count,
            "是否占位/无效内容": "是" if placeholder else "否",
            "是否含基金语义": "是" if has_fund_kw else "否",
            "质量分": score,
            "质量标签": quality_label(score),
            "主要问题": "；".join(issues),
        })
    return pd.DataFrame(rows)


def build_comment_quality(comment_df: pd.DataFrame, note_ids: set[str]) -> pd.DataFrame:
    rows = []
    for _, r in comment_df.iterrows():
        content = clean_text(r.get("评论内容") or r.get("评论正文猜测"))
        note_id = clean_text(r.get("笔记ID"))
        has_fund_kw = any(k.lower() in content.lower() for k in FUND_KEYWORDS)
        author_self = clean_text(r.get("是否博主本人评论(猜测)"))
        rows.append({
            "博主ID": clean_text(r.get("博主ID")),
            "博主昵称": clean_text(r.get("博主昵称")),
            "笔记ID": note_id,
            "评论ID": clean_text(r.get("评论ID")),
            "评论内容": content,
            "用户名称": clean_text(r.get("用户名称")),
            "是否博主本人评论": author_self,
            "是否关联已抓笔记": "是" if note_id in note_ids else "否",
            "是否含基金语义": "是" if has_fund_kw else "否",
            "评论时间": clean_text(r.get("评论时间")),
            "点赞量": clean_text(r.get("点赞量")),
        })
    return pd.DataFrame(rows)


def build_enhanced_mentions(note_df: pd.DataFrame, comment_df: pd.DataFrame, existing_mentions: pd.DataFrame, alias_index: dict[str, dict[str, str]], code_to_name: dict[str, str]) -> pd.DataFrame:
    rows = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    seen = set()

    def add_records(records: list[dict[str, Any]], base: dict[str, Any], source_field: str, text: str) -> None:
        for rec in records:
            key = (base.get("note_id", ""), base.get("comment_id", ""), rec.get("fund_code", ""), rec.get("fund_name", ""), source_field, rec.get("alias_hit", ""))
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                "record_id": f"{base.get('note_id','')}_{base.get('comment_id','')}_{len(rows)+1}",
                "来源层级": base.get("entity_type", ""),
                "博主ID": base.get("blogger_id", ""),
                "博主昵称": base.get("blogger_name", ""),
                "笔记ID": base.get("note_id", ""),
                "评论ID": base.get("comment_id", ""),
                "基金代码": rec.get("fund_code", ""),
                "基金名称": rec.get("fund_name", ""),
                "命中词": rec.get("alias_hit", ""),
                "匹配方式": rec.get("match_type", ""),
                "确认程度": "确认" if rec.get("fund_code") else ("候选" if rec.get("match_type") == "candidate_name" else "待确认"),
                "置信度": rec.get("confidence", 0),
                "提及角色": rec.get("mention_role", ""),
                "情绪": rec.get("sentiment", ""),
                "广告信号": rec.get("ad_signal_level", ""),
                "证据字段": source_field,
                "证据片段": clean_text(text)[:260],
                "检测时间": now,
            })

    for _, r in note_df.iterrows():
        base = {
            "entity_type": "笔记",
            "blogger_id": clean_text(r.get("博主ID")),
            "blogger_name": clean_text(r.get("博主昵称")),
            "note_id": clean_text(r.get("笔记ID")),
            "comment_id": "",
        }
        for field, col in [("标题", "笔记标题"), ("正文", "笔记内容"), ("话题", "笔记话题")]:
            text = clean_text(r.get(col))
            add_records(detect_funds(text, alias_index, code_to_name), base, field, text)

    for _, r in comment_df.iterrows():
        base = {
            "entity_type": "评论",
            "blogger_id": clean_text(r.get("博主ID")),
            "blogger_name": clean_text(r.get("博主昵称")),
            "note_id": clean_text(r.get("笔记ID")),
            "comment_id": clean_text(r.get("评论ID")),
        }
        text = clean_text(r.get("评论内容") or r.get("评论正文猜测"))
        add_records(detect_funds(text, alias_index, code_to_name), base, "评论", text)

    # 合入旧识别结果，保留证据。
    if not existing_mentions.empty:
        for _, r in existing_mentions.iterrows():
            key = (clean_text(r.get("note_id")), clean_text(r.get("comment_index")), norm_code(r.get("fund_code")), clean_text(r.get("fund_name")), clean_text(r.get("source_field")), clean_text(r.get("alias_hit")))
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                "record_id": clean_text(r.get("record_id")),
                "来源层级": clean_text(r.get("entity_type")),
                "博主ID": clean_text(r.get("blogger_id")),
                "博主昵称": clean_text(r.get("blogger_name")),
                "笔记ID": clean_text(r.get("note_id")),
                "评论ID": clean_text(r.get("comment_index")),
                "基金代码": norm_code(r.get("fund_code")),
                "基金名称": clean_text(r.get("fund_name")),
                "命中词": clean_text(r.get("alias_hit")),
                "匹配方式": clean_text(r.get("match_type")) or "历史识别",
                "确认程度": "确认" if norm_code(r.get("fund_code")) else "候选",
                "置信度": parse_num(r.get("confidence")),
                "提及角色": clean_text(r.get("mention_role")),
                "情绪": clean_text(r.get("sentiment")),
                "广告信号": "",
                "证据字段": clean_text(r.get("source_field")),
                "证据片段": clean_text(r.get("snippet")),
                "检测时间": clean_text(r.get("detected_at")) or now,
            })

    return pd.DataFrame(rows)


def build_summaries(blogger_df: pd.DataFrame, note_quality: pd.DataFrame, comment_quality: pd.DataFrame, mentions: pd.DataFrame, failed_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    blogger_rows = []
    blogger_ids = sorted(set(blogger_df.get("博主ID", pd.Series(dtype=str)).fillna("").astype(str)) | set(note_quality.get("博主ID", pd.Series(dtype=str)).fillna("").astype(str)))
    for bid in blogger_ids:
        if not bid:
            continue
        b0 = blogger_df[blogger_df.get("博主ID", pd.Series(dtype=str)).fillna("").astype(str) == bid]
        n0 = note_quality[note_quality["博主ID"].astype(str) == bid] if not note_quality.empty else pd.DataFrame()
        c0 = comment_quality[comment_quality["博主ID"].astype(str) == bid] if not comment_quality.empty else pd.DataFrame()
        m0 = mentions[mentions["博主ID"].astype(str) == bid] if not mentions.empty else pd.DataFrame()
        failed0 = failed_df[failed_df.get("profile_link", pd.Series(dtype=str)).fillna("").astype(str).str.contains(bid, regex=False)] if not failed_df.empty else pd.DataFrame()
        note_count = len(n0)
        analyzable = int((n0.get("质量标签", pd.Series(dtype=str)) == "可分析").sum()) if note_count else 0
        need_recrawl = int((n0.get("质量标签", pd.Series(dtype=str)) == "需补抓").sum()) if note_count else 0
        comment_count = len(c0)
        confirmed_m0 = m0[m0.get("确认程度", pd.Series(dtype=str)).astype(str).eq("确认")] if not m0.empty else pd.DataFrame()
        mention_count = len(confirmed_m0)
        score = 0
        score += min(35, note_count * 2)
        score += min(20, comment_count * 0.2)
        score += min(25, mention_count * 2)
        score += 20 * (analyzable / max(note_count, 1)) if note_count else 0
        status = "优先复核" if mention_count > 0 or need_recrawl >= max(3, note_count * 0.5) else ("可继续补抓" if note_count > 0 else "主页待补抓")
        blogger_rows.append({
            "博主ID": bid,
            "博主昵称": clean_text(b0.iloc[0].get("博主昵称")) if not b0.empty else clean_text(n0.iloc[0].get("博主昵称")) if not n0.empty else "",
            "博主链接": clean_text(b0.iloc[0].get("博主链接")) if not b0.empty else "",
            "抓取笔记数": note_count,
            "可分析笔记数": analyzable,
            "需补抓笔记数": need_recrawl,
            "评论数": comment_count,
            "确认基金提及数": mention_count,
            "候选基金词数": int((m0.get("确认程度", pd.Series(dtype=str)).astype(str) != "确认").sum()) if not m0.empty else 0,
            "涉及确认基金数": int(confirmed_m0["基金名称"].replace("", pd.NA).dropna().nunique()) if not confirmed_m0.empty else 0,
            "失败记录数": len(failed0),
            "运营优先级分": round(score, 2),
            "状态标签": status,
        })

    fund_rows = []
    candidate_summary = pd.DataFrame()
    confirmed_mentions = mentions[mentions.get("确认程度", pd.Series(dtype=str)).astype(str).eq("确认")].copy() if not mentions.empty else pd.DataFrame()
    candidate_mentions = mentions[~mentions.get("确认程度", pd.Series(dtype=str)).astype(str).eq("确认")].copy() if not mentions.empty else pd.DataFrame()
    if not confirmed_mentions.empty:
        for (code, name), g in confirmed_mentions.groupby(["基金代码", "基金名称"], dropna=False):
            name = clean_text(name)
            if not name:
                continue
            cnt = len(g)
            promote = int((g["提及角色"] == "主推").sum())
            compare = int((g["提及角色"] == "对比").sum())
            neg = int((g["情绪"] == "负向").sum())
            high_ad = int((g["广告信号"] == "高").sum())
            fund_rows.append({
                "基金代码": norm_code(code),
                "基金名称": name,
                "提及次数": cnt,
                "涉及博主数": g["博主ID"].replace("", pd.NA).dropna().nunique(),
                "涉及笔记数": g["笔记ID"].replace("", pd.NA).dropna().nunique(),
                "主推次数": promote,
                "对比次数": compare,
                "负向次数": neg,
                "高广告信号次数": high_ad,
                "平均置信度": round(pd.to_numeric(g["置信度"], errors="coerce").fillna(0).mean(), 3),
                "运营解读": "疑似重点投放/主推" if promote + high_ad >= 2 else ("存在负面讨论" if neg > 0 else "常规提及/待观察"),
            })
    fund_summary = pd.DataFrame(fund_rows).sort_values(["提及次数", "涉及博主数"], ascending=False) if fund_rows else pd.DataFrame()

    if not candidate_mentions.empty:
        cand_rows = []
        for name, g in candidate_mentions.groupby("基金名称", dropna=False):
            name = clean_text(name)
            if not name:
                continue
            cand_rows.append({
                "候选基金词": name,
                "候选提及次数": len(g),
                "涉及博主数": g["博主ID"].replace("", pd.NA).dropna().nunique(),
                "涉及笔记数": g["笔记ID"].replace("", pd.NA).dropna().nunique(),
                "最高置信度": round(pd.to_numeric(g["置信度"], errors="coerce").fillna(0).max(), 3),
                "主要命中词": "；".join(list(dict.fromkeys(g["命中词"].fillna("").astype(str).head(5)))),
                "样例证据": clean_text(g.iloc[0].get("证据片段")),
                "处理建议": "人工确认是否为真实基金/化名；若确认，加入基金别名库",
            })
        candidate_summary = pd.DataFrame(cand_rows).sort_values(["候选提及次数", "涉及博主数"], ascending=False)

    gap_rows = []
    if not note_quality.empty:
        gap = note_quality[(note_quality["质量标签"] == "需补抓") | (note_quality["主要问题"].astype(str).str.contains("声明有评论但未抓到|有基金语义但未识别基金", regex=True, na=False))]
        gap_rows = gap.sort_values(["质量分"], ascending=True).to_dict("records")

    failed_summary = pd.DataFrame()
    if not failed_df.empty and "message" in failed_df.columns:
        failed_summary = failed_df["message"].fillna("").astype(str).value_counts().reset_index()
        failed_summary.columns = ["失败/状态信息", "条数"]

    return pd.DataFrame(blogger_rows).sort_values("运营优先级分", ascending=False), fund_summary, candidate_summary, pd.DataFrame(gap_rows), failed_summary


def build_overview(source_df: pd.DataFrame, blogger_df: pd.DataFrame, note_df: pd.DataFrame, comment_df: pd.DataFrame, mentions: pd.DataFrame, note_quality: pd.DataFrame) -> pd.DataFrame:
    total_notes = len(note_df)
    analyzable = int((note_quality.get("质量标签", pd.Series(dtype=str)) == "可分析").sum()) if not note_quality.empty else 0
    recrawl = int((note_quality.get("质量标签", pd.Series(dtype=str)) == "需补抓").sum()) if not note_quality.empty else 0
    confirmed = mentions[mentions.get("确认程度", pd.Series(dtype=str)).astype(str).eq("确认")] if not mentions.empty else pd.DataFrame()
    candidate = mentions[~mentions.get("确认程度", pd.Series(dtype=str)).astype(str).eq("确认")] if not mentions.empty else pd.DataFrame()
    rows = [
        ("输入博主数", len(source_df), "来自 source_blogger_list"),
        ("成功识别博主数", len(blogger_df), "来自 blogger_export"),
        ("抓取笔记数", total_notes, "来自 note_export"),
        ("可分析笔记数", analyzable, "质量分 >= 80"),
        ("需补抓笔记数", recrawl, "质量分 < 55 或关键字段缺失"),
        ("抓取评论数", len(comment_df), "来自 comment_export"),
        ("增强后全部提及数", len(mentions), "确认基金 + 候选基金词"),
        ("确认基金提及数", len(confirmed), "有基金代码或高置信别名命中"),
        ("候选基金词提及数", len(candidate), "可能是基金简称/化名，需人工确认"),
        ("涉及确认基金数", confirmed["基金名称"].replace("", pd.NA).dropna().nunique() if not confirmed.empty else 0, "按确认基金名称去重"),
        ("生成时间", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "本次升级分析生成时间"),
    ]
    return pd.DataFrame(rows, columns=["指标", "数值", "说明"])


def autosize(writer: pd.ExcelWriter, sheet_names: list[str]) -> None:
    wb = writer.book
    for name in sheet_names:
        ws = wb[name]
        ws.freeze_panes = "A2"
        for cell in ws[1]:
            new_font = copy(cell.font)
            new_font.bold = True
            cell.font = new_font
        for col in ws.columns:
            max_len = 8
            letter = col[0].column_letter
            header = str(col[0].value or "")
            if any(k in header for k in ["代码", "ID", "id"]):
                for cell in col:
                    cell.number_format = "@"
            for cell in col[:200]:
                val = "" if cell.value is None else str(cell.value)
                max_len = max(max_len, min(48, len(val) + 2))
            ws.column_dimensions[letter].width = max_len


def main() -> int:
    parser = argparse.ArgumentParser(description="升级小红书爬虫结果：质量诊断、基金识别增强、运营分析输出")
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--alias-json", default=str(DEFAULT_ALIAS_JSON))
    parser.add_argument("--fund-tag-xlsx", default=str(DEFAULT_FUND_TAG_XLSX))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    source_df = safe_read(input_path, SHEETS["source"])
    blogger_df = safe_read(input_path, SHEETS["blogger"])
    note_df = safe_read(input_path, SHEETS["note"])
    comment_df = safe_read(input_path, SHEETS["comment"])
    old_mentions = safe_read(input_path, SHEETS["mention"])
    failed_df = safe_read(input_path, SHEETS["failed"])
    aliases = load_aliases(Path(args.alias_json), Path(args.fund_tag_xlsx))
    alias_index, code_to_name = build_alias_index(aliases)

    enhanced_mentions = build_enhanced_mentions(note_df, comment_df, old_mentions, alias_index, code_to_name)
    note_quality = build_note_quality(note_df, comment_df, enhanced_mentions.rename(columns={"笔记ID": "note_id"}))
    comment_quality = build_comment_quality(comment_df, set(note_df.get("笔记ID", pd.Series(dtype=str)).fillna("").astype(str)))
    blogger_summary, fund_summary, candidate_summary, gap_focus, failed_summary = build_summaries(blogger_df, note_quality, comment_quality, enhanced_mentions, failed_df)
    overview = build_overview(source_df, blogger_df, note_df, comment_df, enhanced_mentions, note_quality)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = output_dir / f"小红书爬虫_质量诊断与运营分析_{ts}.xlsx"
    out_json = output_dir / f"小红书爬虫_质量诊断与运营分析_{ts}_summary.json"

    sheets = {
        "00_总览": overview,
        "01_博主质量总览": blogger_summary,
        "02_笔记质量诊断": note_quality,
        "03_评论质量诊断": comment_quality,
        "04_基金提及增强": enhanced_mentions,
        "05_确认基金运营汇总": fund_summary,
        "06_候选基金词待确认": candidate_summary,
        "07_优先补抓清单": gap_focus,
        "08_失败原因汇总": failed_summary,
        "09_原始博主": source_df,
        "10_原始笔记": note_df,
        "11_原始评论": comment_df,
    }
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        for name, df in sheets.items():
            if df is None or df.empty:
                df = pd.DataFrame({"说明": ["本表暂无数据"]})
            df.to_excel(writer, index=False, sheet_name=name[:31])
        autosize(writer, [name[:31] for name in sheets])

    summary = {
        "input": str(input_path),
        "output": str(out_xlsx),
        "source_rows": len(source_df),
        "blogger_rows": len(blogger_df),
        "note_rows": len(note_df),
        "comment_rows": len(comment_df),
        "old_fund_mentions": len(old_mentions),
        "enhanced_all_mentions": len(enhanced_mentions),
        "confirmed_fund_mentions": int((enhanced_mentions.get("确认程度", pd.Series(dtype=str)).astype(str) == "确认").sum()) if not enhanced_mentions.empty else 0,
        "candidate_fund_mentions": int((enhanced_mentions.get("确认程度", pd.Series(dtype=str)).astype(str) != "确认").sum()) if not enhanced_mentions.empty else 0,
        "fund_alias_count": len(aliases),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
