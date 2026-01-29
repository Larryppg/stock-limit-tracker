@echo off
REM A股连板追踪系统 - MVP快速启动脚本 (Windows)

echo ====================================
echo A股连板高度追踪系统 - MVP模式
echo ====================================
echo.

REM 检查Python
python --version >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到Python，请先安装Python 3.9+
    pause
    exit /b 1
)

echo [1/3] 检查依赖...
pip show akshare >nul 2>&1
if errorlevel 1 (
    echo 正在安装依赖...
    pip install -r requirements.txt
)

echo.
echo [2/3] 运行MVP流程（最近90天数据）...
python main.py --mode mvp --days 90

echo.
echo [3/3] 验证结果...
python verify_results.py

echo.
echo ====================================
echo MVP流程完成！
echo.
echo 下一步:
echo 1. 查看生成的数据: data/stock_limit.db
echo 2. 查看样本结果: sample_results.csv
echo 3. 启动Web界面: streamlit run web_interface.py
echo ====================================

pause
