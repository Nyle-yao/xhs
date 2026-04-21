#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/yaoruanxingchen/c/xhs_task_dashboard"
INPUT="/Users/yaoruanxingchen/Desktop/小红书博主初筛_剔除空.xlsx"
PY="/Users/yaoruanxingchen/anaconda3/bin/python3"
OUT="$ROOT/outputs"

cd "$ROOT"
LATEST_MERGED=$(ls -1t "$OUT"/xhs_batch_merged_*.xlsx 2>/dev/null | head -n 1 || true)
if [[ -z "${LATEST_MERGED}" ]]; then
  echo "[ERROR] 未找到历史 merged 文件：$OUT/xhs_batch_merged_*.xlsx"
  exit 2
fi

echo "[INFO] 使用基线：$LATEST_MERGED"
$PY run_xhs_resilient_crawl.py \
  --input "$INPUT" \
  --sheet Sheet1 --link-column 博主链接 --id-column 博主ID --nickname-column 博主昵称 \
  --profile-dir ./.xhs_profile --fund-aliases ./fund_aliases_expanded.json \
  --existing-merged "$LATEST_MERGED" --output-dir "$OUT" \
  --headless --include-comments \
  --max-notes-per-blogger 10 --max-comments-per-note 60 --comment-scroll-rounds 10 \
  --retry-times 2 --chunk-size 8 --max-rounds 60 --batch-timeout-sec 480 \
  --chunk-select random --cooldown-sec 180 --stop-if-no-progress-rounds 8 \
  --auto-captcha-recover --captcha-wait-sec 240 \
  --run-post-checks

echo "[DONE] 续跑完成，请查看 outputs/resilient_crawl_report_*.json"
