"""
结果验证模块 - 抽样核对连板计算准确性
"""
import pandas as pd
import sqlite3
from config import DB_PATH
import database


def verify_limit_calculation():
    """验证涨停判定的准确性"""
    print("="*60)
    print("连板计算结果验证")
    print("="*60)
    
    conn = database.get_connection()
    
    # 查询有连板记录的股票
    query = """
    SELECT DISTINCT code 
    FROM limit_analysis_result 
    WHERE chain_height >= 2
    ORDER BY code
    LIMIT 10
    """
    
    stocks_with_chain = pd.read_sql_query(query, conn)
    
    if stocks_with_chain.empty:
        print("\n⚠️  暂无连板数据，请先运行MVP流程")
        conn.close()
        return
    
    print(f"\n找到 {len(stocks_with_chain)} 只有连板记录的股票，开始验证...\n")
    
    for idx, row in stocks_with_chain.iterrows():
        code = row['code']
        verify_single_stock(conn, code)
    
    conn.close()
    
    print("\n" + "="*60)
    print("验证完成")
    print("="*60)


def verify_single_stock(conn, code: str):
    """验证单只股票的连板计算"""
    # 获取该股票的原始数据和计算结果
    query_market = f"""
    SELECT date, code, open, high, low, close, pre_close
    FROM daily_market_data
    WHERE code = '{code}'
    ORDER BY date
    """
    
    query_result = f"""
    SELECT date, code, limit_status, chain_height, is_fried, board_type
    FROM limit_analysis_result
    WHERE code = '{code}'
    ORDER BY date
    """
    
    market_data = pd.read_sql_query(query_market, conn)
    limit_result = pd.read_sql_query(query_result, conn)
    
    # 合并数据
    merged = pd.merge(market_data, limit_result, on=['date', 'code'], how='left')
    
    # 只显示有涨停或炸板的记录
    interesting = merged[
        (merged['limit_status'] == 1) | 
        (merged['is_fried'] == 1) | 
        (merged['chain_height'] > 0)
    ]
    
    if interesting.empty:
        return
    
    print(f"\n股票代码: {code}")
    print("-" * 60)
    
    # 获取股票元信息
    query_meta = f"SELECT name, limit_ratio FROM stock_meta WHERE code = '{code}'"
    meta = pd.read_sql_query(query_meta, conn)
    
    if not meta.empty:
        name = meta.iloc[0]['name']
        limit_ratio = meta.iloc[0]['limit_ratio']
        print(f"股票名称: {name}")
        print(f"涨跌幅限制: {limit_ratio*100:.0f}%")
    
    print("\n涨停/连板记录:")
    print(interesting[['date', 'close', 'pre_close', 'chain_height', 'board_type']].to_string(index=False))
    
    # 手工验证几个关键点
    for _, row in interesting.head(5).iterrows():
        date = row['date']
        close = row['close']
        pre_close = row['pre_close']
        
        if pd.notna(close) and pd.notna(pre_close) and pre_close > 0:
            actual_change = (close - pre_close) / pre_close * 100
            print(f"\n  {date}: 涨幅 {actual_change:.2f}% (收盘{close:.2f}, 前收{pre_close:.2f})")


def check_data_quality():
    """检查数据质量"""
    print("\n" + "="*60)
    print("数据质量检查")
    print("="*60 + "\n")
    
    conn = database.get_connection()
    
    # 检查1: 缺失数据
    print("[1] 检查缺失数据...")
    query_missing = """
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN open IS NULL THEN 1 ELSE 0 END) as missing_open,
        SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) as missing_close,
        SUM(CASE WHEN pre_close IS NULL THEN 1 ELSE 0 END) as missing_pre_close
    FROM daily_market_data
    """
    
    missing = pd.read_sql_query(query_missing, conn)
    print(missing.to_string(index=False))
    
    # 检查2: 异常涨跌幅
    print("\n[2] 检查异常涨跌幅（>50%）...")
    query_abnormal = """
    SELECT date, code, close, pre_close,
           ROUND((close - pre_close) * 100.0 / pre_close, 2) as change_pct
    FROM daily_market_data
    WHERE pre_close > 0 
      AND ABS((close - pre_close) / pre_close) > 0.5
    ORDER BY ABS(change_pct) DESC
    LIMIT 10
    """
    
    abnormal = pd.read_sql_query(query_abnormal, conn)
    if not abnormal.empty:
        print(abnormal.to_string(index=False))
    else:
        print("✓ 未发现异常涨跌幅")
    
    # 检查3: 连板统计
    print("\n[3] 连板高度分布...")
    query_chain_dist = """
    SELECT chain_height, COUNT(*) as count
    FROM limit_analysis_result
    WHERE chain_height > 0
    GROUP BY chain_height
    ORDER BY chain_height
    """
    
    chain_dist = pd.read_sql_query(query_chain_dist, conn)
    if not chain_dist.empty:
        print(chain_dist.to_string(index=False))
    else:
        print("⚠️  未找到连板数据")
    
    # 检查4: 最高连板记录
    print("\n[4] 历史最高连板记录（Top 10）...")
    query_top_chain = """
    SELECT 
        l.code,
        s.name,
        l.date,
        l.chain_height
    FROM limit_analysis_result l
    LEFT JOIN stock_meta s ON l.code = s.code
    ORDER BY l.chain_height DESC
    LIMIT 10
    """
    
    top_chain = pd.read_sql_query(query_top_chain, conn)
    if not top_chain.empty:
        print(top_chain.to_string(index=False))
    
    conn.close()


def export_sample_data(output_file: str = 'sample_results.csv'):
    """导出样本数据供人工核对"""
    print(f"\n导出样本数据到 {output_file}...")
    
    conn = database.get_connection()
    
    query = """
    SELECT 
        l.date,
        l.code,
        s.name,
        m.close,
        m.pre_close,
        ROUND((m.close - m.pre_close) * 100.0 / m.pre_close, 2) as change_pct,
        l.limit_status,
        l.chain_height,
        l.is_fried,
        l.board_type
    FROM limit_analysis_result l
    JOIN daily_market_data m ON l.date = m.date AND l.code = m.code
    LEFT JOIN stock_meta s ON l.code = s.code
    WHERE l.chain_height >= 2 OR l.is_fried = 1
    ORDER BY l.date DESC, l.chain_height DESC
    LIMIT 500
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if not df.empty:
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"✓ 已导出 {len(df)} 条记录到 {output_file}")
    else:
        print("⚠️  无数据可导出")


if __name__ == '__main__':
    # 运行验证
    verify_limit_calculation()
    
    # 数据质量检查
    check_data_quality()
    
    # 导出样本数据
    export_sample_data()
