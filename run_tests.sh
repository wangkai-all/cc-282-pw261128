#!/usr/bin/env bash
# 一键运行全部测试：21 个 Python 用例（引擎 18 + HTTP 端到端 3）+ 前端 DOM 冒烟
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Python 单元/端到端测试（隔离模式 python -I） ==="
python3 -I -m unittest discover -s tests -v

echo
echo "=== 前端渲染冒烟（Node，无外部依赖） ==="
node tests/dom_smoke.js

echo
echo "全部测试通过 ✔"
