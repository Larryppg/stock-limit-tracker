#!/bin/bash
# A股连板追踪系统 - MVP快速启动脚本 (Linux/Mac)

echo "===================================="
echo "A股连板高度追踪系统 - MVP模式"
echo "===================================="
echo ""

# 检查Python
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到Python3，请先安装Python 3.9+"
    exit 1
fi

echo "[1/3] 检查依赖..."
if ! python3 -c "import akshare" &> /dev/null; then
    echo "正在安装依赖..."
    pip3 install -r requirements.txt
fi

echo ""
echo "[2/3] 运行MVP流程（最近90天数据）..."
python3 main.py --mode mvp --days 90

echo ""
echo "[3/3] 验证结果..."
python3 verify_results.py

echo ""
echo "===================================="
echo "MVP流程完成！"
echo ""
echo "下一步:"
echo "1. 查看生成的数据: data/stock_limit.db"
echo "2. 查看样本结果: sample_results.csv"
echo "3. 启动Web界面: streamlit run web_interface.py"
echo "===================================="
