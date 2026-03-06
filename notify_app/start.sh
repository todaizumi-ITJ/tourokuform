#!/bin/bash
# 通知判定Webアプリ 起動スクリプト
cd "$(dirname "$0")"
echo "========================================="
echo "  新規案件 通知判定システム"
echo "  http://localhost:5000 でアクセス"
echo "  Ctrl+C で停止"
echo "========================================="
python3 app.py
