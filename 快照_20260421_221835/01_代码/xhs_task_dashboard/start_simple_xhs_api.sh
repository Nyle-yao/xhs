#!/usr/bin/env bash
set -euo pipefail
cd /Users/yaoruanxingchen/c/xhs_task_dashboard
export XHS_HEADLESS=false
export XHS_PROFILE_DIR="/Users/yaoruanxingchen/c/xhs_task_dashboard/.xhs_profile"
exec /Users/yaoruanxingchen/anaconda3/bin/python -m uvicorn api_server:app --host 127.0.0.1 --port 8790
