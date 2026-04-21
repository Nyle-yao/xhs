from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd


def norm_code(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    s = re.sub(r"[^\d]", "", s)
    if not s:
        return ""
    if len(s) < 6:
        s = s.zfill(6)
    elif len(s) > 6:
        s = s[-6:]
    return s


def name_variants(name: str) -> list[str]:
    s = str(name or "").strip()
    if not s:
        return []
    out = [s]
    # 去尾缀版本（A/B/C类份额）
    s2 = re.sub(r"[A-Ca-c]$", "", s).strip()
    if s2 and s2 != s:
        out.append(s2)
    # 去空格
    s3 = re.sub(r"\s+", "", s)
    if s3 and s3 not in out:
        out.append(s3)
    return list(dict.fromkeys(out))


def main() -> None:
    p = argparse.ArgumentParser(description="从基金主表/筛选表构建扩展基金别名库")
    p.add_argument("--main-table", required=True, help="基金主表（csv/xlsx）")
    p.add_argument("--main-sheet", default="基金主表", help="main-table 是 xlsx 时的 sheet")
    p.add_argument("--filter-table", default="", help="基金筛选表（可选，csv/xlsx）")
    p.add_argument("--filter-sheet", default="基金筛选表", help="filter-table 是 xlsx 时的 sheet")
    p.add_argument("--base-aliases", default="./fund_aliases.json", help="原始别名库（可选）")
    p.add_argument("--output", default="./fund_aliases_expanded.json", help="输出路径")
    args = p.parse_args()

    rows: list[dict[str, Any]] = []

    main_path = Path(args.main_table).expanduser().resolve()
    if main_path.suffix.lower() in {".xlsx", ".xls"}:
        df_main = pd.read_excel(main_path, sheet_name=args.main_sheet)
    else:
        df_main = pd.read_csv(main_path)
    rows.extend(df_main.to_dict(orient="records"))

    if args.filter_table:
        fp = Path(args.filter_table).expanduser().resolve()
        if fp.exists():
            if fp.suffix.lower() in {".xlsx", ".xls"}:
                df_filter = pd.read_excel(fp, sheet_name=args.filter_sheet)
            else:
                df_filter = pd.read_csv(fp)
            rows.extend(df_filter.to_dict(orient="records"))

    merged: dict[str, dict[str, Any]] = {}

    # 先读 base aliases
    base_path = Path(args.base_aliases).expanduser().resolve()
    if base_path.exists():
        try:
            base_data = json.loads(base_path.read_text(encoding="utf-8"))
            for r in base_data:
                code = norm_code(r.get("fund_code", ""))
                name = str(r.get("fund_name", "")).strip()
                aliases = [str(x).strip() for x in (r.get("aliases") or []) if str(x).strip()]
                if not code and not name:
                    continue
                key = code or name
                merged[key] = {
                    "fund_code": code,
                    "fund_name": name,
                    "aliases": list(dict.fromkeys(aliases)),
                }
        except Exception:
            pass

    # 再合入表格
    for r in rows:
        code = norm_code(r.get("代码", "") or r.get("基金代码", ""))
        name = str(r.get("产品名称", "") or r.get("基金名称", "")).strip()
        if not code and not name:
            continue
        key = code or name
        if key not in merged:
            merged[key] = {"fund_code": code, "fund_name": name, "aliases": []}
        aliases = merged[key]["aliases"]
        for a in name_variants(name):
            if a not in aliases:
                aliases.append(a)
        if code:
            if code not in aliases:
                aliases.append(code)
            raw_code = str(r.get("代码", "") or r.get("基金代码", "")).strip()
            if raw_code.isdigit() and len(raw_code) == 6 and raw_code not in aliases:
                aliases.append(raw_code)

        # 补 fund_name
        if not merged[key]["fund_name"] and name:
            merged[key]["fund_name"] = name
        if not merged[key]["fund_code"] and code:
            merged[key]["fund_code"] = code

    out = list(merged.values())
    out.sort(key=lambda x: (x.get("fund_code", ""), x.get("fund_name", "")))

    out_path = Path(args.output).expanduser().resolve()
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"count": len(out), "output": str(out_path)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
