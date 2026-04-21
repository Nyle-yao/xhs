# 小红书采集看板（MVP）

这是一个独立于基金加减仓看板的采集工具，面向你自己的小红书登录态，提供：

- 评论数据导出（单篇/批量）
- 笔记图片/视频下载（打包）
- 批量导出笔记基础信息
- 一键复制笔记信息（ID/标题/内容/互动）
- 一键复制博主信息（ID/小红书号/简介等）
- 博主信息批量导出
- 关键词搜索导出（笔记/博主）

> 说明：无水印视频/原图能力依赖页面可见资源URL与平台策略，当前实现是 **best-effort** 下载页面可提取的原始媒体链接。

## 1. 安装

```bash
cd /Users/yaoruanxingchen/c/xhs_task_dashboard
python3 -m pip install -r requirements.txt
python3 -m playwright install chromium
```

依赖说明（已内置在 `requirements.txt`）：

- `numpy<2` + `pyarrow==14.0.2`：避免数据链路兼容冲突
- `openai`：通过 OpenAI 兼容接口调用百度千帆视觉大模型 OCR
- `rapidocr-onnxruntime` + `opencv-python(-headless)`：本地图片OCR兜底链路
- `urllib3<2`：规避本机 LibreSSL 警告噪音

## 2. 启动

```bash
cd /Users/yaoruanxingchen/c/xhs_task_dashboard
python3 -m streamlit run app.py
```

## 2.1 启动接口（FastAPI）

```bash
cd /Users/yaoruanxingchen/c/xhs_task_dashboard
python3 -m uvicorn api_server:app --host 0.0.0.0 --port 8790 --reload
```

接口文档：

- Swagger UI: `http://127.0.0.1:8790/docs`
- OpenAPI JSON: `http://127.0.0.1:8790/openapi.json`

## 3. 使用流程

1. 先点「启动并检测登录」，在弹出的浏览器完成登录。
2. 回看板进行：
   - 笔记采集/下载媒体
   - 评论导出
   - 批量笔记导出
   - 博主导出
   - 关键词搜索导出

## 3.1 接口能力（已提供）

- `POST /api/v1/auth/login/check` 登录态检测
- `POST /api/v1/note/detail` 笔记详情
- `POST /api/v1/comment/collect` 评论采集
- `POST /api/v1/comment/export.xlsx` 评论Excel导出
- `POST /api/v1/note/collect/by-links` 笔记批量采集（按笔记链接）
- `POST /api/v1/note/export/by-links.xlsx` 笔记Excel导出（按笔记链接，列名/顺序对齐教程示例；支持字段勾选）
- `POST /api/v1/blogger/collect/by-links` 博主批量采集（可附带笔记/评论）
- `POST /api/v1/blogger/export/by-links.xlsx` 博主Excel导出
- `POST /api/v1/search/notes` 关键词搜索笔记
- `POST /api/v1/search/bloggers` 关键词搜索博主
- `POST /api/v1/note/media/download/by-note.zip` 按笔记下载媒体ZIP
- `POST /api/v1/media/download/by-urls.zip` 按URL列表下载媒体ZIP
- `GET /api/v1/meta/note-export-options` 获取可勾选导出字段与“笔记话题来源”类目

### 3.2 自定义字段与“笔记话题来源”勾选（示例）

`POST /api/v1/note/export/by-links.xlsx`

```json
{
  "note_links": [
    "https://www.xiaohongshu.com/explore/xxxx"
  ],
  "export_fields": ["note_id", "note_url", "note_type", "title", "content", "note_topic", "like_count", "collect_count", "comment_count", "share_count", "publish_time", "update_time", "ip_address", "blogger_id", "blogger_url", "blogger_name", "image_count", "cover_url", "image_urls", "video_urls"],
  "tag_categories": ["all"]
}
```

## 4. 输出目录

- 运行输出：`./outputs`
- 登录态目录：`./.xhs_profile`（可在页面自定义）

## 4.1 二轮增强（运营分析）

在第一轮 `blogger_batch_*.xlsx` 基础上，补评论、增强提及语义、输出运营汇总：

```bash
cd /Users/yaoruanxingchen/c/xhs_task_dashboard
python3 ops_enrich_pipeline.py \
  --input-result ./outputs/blogger_batch_20260417_205451_result.xlsx \
  --crawl-comments \
  --headed \
  --max-notes-for-comments 60 \
  --max-comments-per-note 50
```

输出：`./outputs/ops_enriched_*.xlsx`（含 `comment_export_enhanced` / `fund_mentions_enhanced` / `ops_summary_fund` / `ops_summary_blogger`）

如果评论抓取长时间无返回，优先加 `--headed` 用可见浏览器完成一次登录确认后再跑。

可选增强能力（本轮新增）：

- 笔记/评论图片OCR：默认使用百度千帆视觉大模型，可用 `--ocr-provider rapidocr` 切回本地OCR
- 评论图片OCR（默认开启，可用 `--no-ocr-comment-images` 关闭）
- 外部标签桥接（传入 `--leshu-tag-file`，生成 `fund_tag_bridge` + `external_tag_source`）

## 4.2 失败重试（风控/受限回补）

```bash
cd /Users/yaoruanxingchen/c/xhs_task_dashboard
python3 retry_failed_bloggers.py \
  --input-result ./outputs/blogger_batch_20260417_205451_result.xlsx \
  --max-targets 80 \
  --max-notes 3
```

输出：`./outputs/failed_retry_*.xlsx`（含 `retry_success` / `retry_failed`）

### 4.2.1 从回补成功账号生成“二次深抓输入表”

```bash
python3 build_deep_crawl_input.py \
  --retry-xlsx ./outputs/failed_retry_YYYYMMDD_HHMMSS.xlsx \
  --source-xlsx "/Users/yaoruanxingchen/Desktop/小红书爬虫/03_输入数据/小红书博主初筛_剔除空.xlsx" \
  --source-sheet Sheet1 \
  --output-dir ./outputs
```

输出：`./outputs/deep_crawl_input_from_retry_*.xlsx`

## 4.5 评论本人识别 + OCR（增强）

OCR 默认走百度千帆 OpenAI 兼容接口。运行前设置环境变量，或在代码目录放本地 `.env.local`：

```bash
export BAIDU_QIANFAN_API_KEY="你的百度千帆API Key"
```

在 `ops_enrich_pipeline.py` 中新增：

- `comment_self_only` 分表（只保留“博主本人评论(猜测)=是”）
- `ocr_note_images` 分表（图片OCR结果，含 `ocr_provider` 字段）
- OCR文本会参与基金提及识别（source_field=`ocr_image_text`）
- `image_audit_all`（全部图片质量审计）
- `image_invalid_archive`（无效图片归档，不参与OCR）
- `image_valid_kept`（有效图片保留清单）
- `fund_unmapped_candidates`（未映射候选基金名，便于补词典）
- `signal_tag_summary`（加仓/减仓/定投/合作等内容信号汇总）
- `ops_digest`（运营摘要：规模/质量/识别结果）
- `note_export` 主列只保留有效图片，同时保留 `笔记图片链接(原始)` 便于追溯

示例：

```bash
python3 ops_enrich_pipeline.py \
  --input-result ./outputs/blogger_batch_YYYYMMDD_HHMMSS_result.xlsx \
  --fund-aliases ./fund_aliases_expanded.json \
  --ocr-images \
  --ocr-provider qianfan \
  --qianfan-model ernie-4.5-turbo-vl \
  --ocr-max-notes 80 \
  --ocr-max-images-per-note 2 \
  --output-dir ./outputs
```

## 4.6 多轮结果累计合并（生成当前最全主表）

```bash
python3 merge_xhs_batches.py \
  --inputs \
    ./outputs/blogger_batch_A_result.xlsx \
    ./outputs/blogger_batch_B_result.xlsx \
    ./outputs/blogger_batch_C_result.xlsx \
  --output-dir ./outputs
```

输出：

- `xhs_batch_merged_*.xlsx`（累计去重总表）
- `xhs_batch_merged_*_summary.json`

## 4.7 历史媒体字段刷新（修复旧静态图）

当历史 `note_export` 中图片链接多为 `fe-platform` 静态素材时，可先刷新媒体字段再做增强：

```bash
python3 refresh_note_media.py \
  --input-result ./outputs/xhs_batch_merged_YYYYMMDD_HHMMSS.xlsx \
  --profile-dir ./.xhs_profile \
  --max-notes 0 \
  --output-dir ./outputs
```

输出：`note_media_refreshed_*.xlsx`（在原表基础上更新 `note_export` 的图片/视频/封面链接）

若全量刷新容易被风控卡住，建议用分批脚本（带超时与断点）：

```bash
python3 refresh_note_media_chunked.py \
  --input-result ./outputs/xhs_batch_merged_YYYYMMDD_HHMMSS.xlsx \
  --chunk-size 8 \
  --max-total 80 \
  --batch-timeout-sec 420 \
  --headless \
  --output-dir ./outputs
```

## 4.3 全流程一键运行（推荐）

你要的“完善流程”建议使用统一编排脚本：

```bash
cd /Users/yaoruanxingchen/c/xhs_task_dashboard
python3 run_xhs_pipeline.py \
  --input "/Users/yaoruanxingchen/Desktop/小红书爬虫/03_输入数据/小红书博主初筛_剔除空.xlsx" \
  --sheet Sheet1 \
  --link-column 博主链接 \
  --id-column 博主ID \
  --nickname-column 博主昵称 \
  --fund-aliases ./fund_aliases_expanded.json \
  --run-retry \
  --run-enrich
```

如果要补抓评论并允许登录弹窗：

```bash
python3 run_xhs_pipeline.py \
  --input "/Users/yaoruanxingchen/Desktop/小红书爬虫/03_输入数据/小红书博主初筛_剔除空.xlsx" \
  --run-retry \
  --run-enrich \
  --enrich-crawl-comments \
  --enrich-headed
```

质量闸门说明（本轮更新）：

- `run_xhs_pipeline.py` / `run_xhs_full_force_pipeline.py` / `run_xhs_resilient_crawl.py`
  默认 `strict QA = 开启`（可用 `--no-strict-qa` 关闭）
- 严格模式下，QA出现 WARN/FAIL 会阻断最终“通过”状态
- QA 新增“语义感知提及闸门”：
  - 当样本里本身缺少基金语义（如纯行情观点文）时，`提及=0` 可判定为 `PASS`（避免误杀）
  - 当基金语义样本充足但仍 `提及=0` 时，触发 `FAIL`
  - 阈值可配置：`--qa-min-batch-mentions` / `--qa-min-enriched-mentions` / `--qa-min-notes-for-mention-fail` / `--qa-min-intent-notes-for-mention-fail`

脚本会自动输出：

- `pipeline_run_*.json/.md`：全链路报告
- `qa_pipeline_*.json`：自动质检报告
- 主抓 / 重试 / 增强结果文件

## 4.4 自动质检（可独立执行）

```bash
python3 qa_xhs_pipeline.py \
  --batch-result ./outputs/blogger_batch_YYYYMMDD_HHMMSS_result.xlsx \
  --enriched-result ./outputs/ops_enriched_YYYYMMDD_HHMMSS.xlsx
```

可加 `--strict`，出现 WARN 也阻断。
可加提及闸门参数，例如：

```bash
python3 qa_xhs_pipeline.py \
  --batch-result ./outputs/blogger_batch_YYYYMMDD_HHMMSS_result.xlsx \
  --enriched-result ./outputs/ops_enriched_YYYYMMDD_HHMMSS.xlsx \
  --min-batch-mentions 1 \
  --min-enriched-mentions 1 \
  --min-notes-for-mention-fail 20 \
  --min-intent-notes-for-mention-fail 3 \
  --strict
```

## 5. 规则参考（你指定）

- 采集博主数据：https://smzs.xisence.com/help/xiaohongshu/batch-collect/blogger
- 采集评论数据：https://smzs.xisence.com/help/xiaohongshu/batch-collect/comment
- 采集笔记数据：https://smzs.xisence.com/help/xiaohongshu/batch-collect/note

## 6. 合规与风控建议

- 使用你自己的账号与授权数据。
- 控制频率，分批采集，避免高并发触发风控。
- 请遵守平台服务条款与适用法律法规。
