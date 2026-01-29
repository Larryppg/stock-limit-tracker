"""
将现有数据库数据导出为增量文件

用于首次部署到 Streamlit Cloud 时，将本地数据库转换为增量文件格式
"""
import pandas as pd
import sqlite3
import os
import config
import increment_manager


def export_database_to_increments(start_date: str = None, end_date: str = None):
    """
    将数据库中的数据导出为增量文件
    
    参数:
        start_date: 开始日期（可选）
        end_date: 结束日期（可选）
    """
    conn = sqlite3.connect(config.DB_PATH)
    
    # 获取所有日期
    dates_query = """
        SELECT DISTINCT date 
        FROM limit_analysis_result 
        ORDER BY date
    """
    dates_df = pd.read_sql_query(dates_query, conn)
    
    if dates_df.empty:
        print("数据库中没有数据")
        conn.close()
        return
    
    dates = dates_df['date'].tolist()
    
    # 应用日期过滤
    if start_date:
        dates = [d for d in dates if d >= start_date]
    if end_date:
        dates = [d for d in dates if d <= end_date]
    
    print(f"准备导出 {len(dates)} 天的数据为增量文件...")
    print(f"日期范围: {dates[0]} ~ {dates[-1]}")
    
    total_market = 0
    total_limits = 0
    
    for i, date in enumerate(dates, 1):
        # 获取市场数据
        market_query = f"""
            SELECT * FROM daily_market_data WHERE date = '{date}'
        """
        market_df = pd.read_sql_query(market_query, conn)
        
        # 获取分析结果
        limits_query = f"""
            SELECT * FROM limit_analysis_result WHERE date = '{date}'
        """
        limits_df = pd.read_sql_query(limits_query, conn)
        
        if market_df.empty and limits_df.empty:
            continue
        
        # 保存增量文件
        increment_manager.save_daily_increment(date, market_df, limits_df)
        
        total_market += len(market_df)
        total_limits += len(limits_df)
        
        if i % 10 == 0:
            print(f"  进度: {i}/{len(dates)}")
    
    conn.close()
    
    # 显示摘要
    summary = increment_manager.get_increments_summary()
    print(f"\n导出完成！")
    print(f"  增量文件数: {summary['total_files']}")
    print(f"  总大小: {summary['total_size_kb']:.1f} KB ({summary['total_size_kb']/1024:.2f} MB)")
    print(f"  市场数据: {total_market} 条")
    print(f"  分析结果: {total_limits} 条")


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='导出数据库为增量文件')
    parser.add_argument('--start-date', type=str, help='开始日期 YYYYMMDD')
    parser.add_argument('--end-date', type=str, help='结束日期 YYYYMMDD')
    parser.add_argument('--recent', type=int, help='只导出最近 N 天')
    
    args = parser.parse_args()
    
    if args.recent:
        from datetime import datetime, timedelta
        args.end_date = datetime.now().strftime('%Y%m%d')
        args.start_date = (datetime.now() - timedelta(days=args.recent)).strftime('%Y%m%d')
    
    export_database_to_increments(args.start_date, args.end_date)
