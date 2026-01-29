"""
连板高度计算核心算法模块
"""
import pandas as pd
import numpy as np
from typing import Tuple
import config


def is_limit_up(close: float, pre_close: float, limit_ratio: float) -> bool:
    """
    判断是否涨停
    
    参数:
        close: 收盘价
        pre_close: 前收盘价
        limit_ratio: 涨跌幅限制比例
    
    返回:
        是否涨停（允许0.1%误差）
    """
    if pd.isna(close) or pd.isna(pre_close) or pre_close <= 0:
        return False
    
    limit_price = pre_close * (1 + limit_ratio)
    # 允许误差范围内算涨停
    return close >= limit_price * (1 - config.LIMIT_TOLERANCE)


def is_yizi_board(open_price: float, high: float, low: float, close: float, 
                  pre_close: float, limit_ratio: float) -> bool:
    """
    判断是否一字板（开盘即涨停且全天封死）
    
    参数:
        open_price: 开盘价
        high: 最高价
        low: 最低价
        close: 收盘价
        pre_close: 前收盘价
        limit_ratio: 涨跌幅限制
    
    返回:
        是否一字板
    """
    if pd.isna(open_price) or pd.isna(high) or pd.isna(low) or pd.isna(close):
        return False
    
    limit_price = pre_close * (1 + limit_ratio)
    
    # 一字板条件：开=高=低=收=涨停价（允许微小误差）
    prices = [open_price, high, low, close]
    tolerance = limit_price * config.LIMIT_TOLERANCE
    
    # 所有价格接近涨停价
    all_near_limit = all(abs(p - limit_price) <= tolerance for p in prices)
    
    return all_near_limit


def is_fried_board(high: float, close: float, pre_close: float, limit_ratio: float) -> bool:
    """
    判断是否炸板（盘中触及涨停但收盘未封住）
    
    参数:
        high: 最高价
        close: 收盘价
        pre_close: 前收盘价
        limit_ratio: 涨跌幅限制
    
    返回:
        是否炸板
    """
    if pd.isna(high) or pd.isna(close) or pd.isna(pre_close):
        return False
    
    limit_price = pre_close * (1 + limit_ratio)
    tolerance = limit_price * config.LIMIT_TOLERANCE
    
    # 最高价触及涨停，但收盘价未封住
    touched_limit = high >= limit_price * (1 - tolerance)
    not_closed_limit = close < limit_price * (1 - tolerance)
    
    return touched_limit and not_closed_limit


def calculate_single_stock_chain(df: pd.DataFrame, code: str, limit_ratio: float) -> pd.DataFrame:
    """
    计算单只股票的连板高度
    
    参数:
        df: 单只股票的日线数据（必须包含：date, open, high, low, close, pre_close）
        code: 股票代码
        limit_ratio: 涨跌幅限制比例
    
    返回:
        包含连板分析结果的DataFrame
    """
    if df.empty:
        return pd.DataFrame()
    
    # 确保按日期排序
    df = df.sort_values('date').copy()
    
    # 初始化结果列
    df['limit_status'] = 0
    df['chain_height'] = 0
    df['is_fried'] = 0
    df['board_type'] = 'normal'
    
    # 逐行计算
    chain_height = 0
    
    for idx in range(len(df)):
        row = df.iloc[idx]
        
        # 跳过数据不完整的行
        if pd.isna(row['close']) or pd.isna(row['pre_close']):
            df.iloc[idx, df.columns.get_loc('chain_height')] = 0
            chain_height = 0
            continue
        
        # 判断是否涨停
        limit_up = is_limit_up(row['close'], row['pre_close'], limit_ratio)
        
        if limit_up:
            # 涨停，连板高度+1
            chain_height += 1
            df.iloc[idx, df.columns.get_loc('limit_status')] = 1
            df.iloc[idx, df.columns.get_loc('chain_height')] = chain_height
            
            # 判断板型
            if is_yizi_board(row['open'], row['high'], row['low'], 
                           row['close'], row['pre_close'], limit_ratio):
                df.iloc[idx, df.columns.get_loc('board_type')] = 'yizi'
            else:
                df.iloc[idx, df.columns.get_loc('board_type')] = 'normal'
        else:
            # 未涨停，检查是否炸板
            fried = is_fried_board(row['high'], row['close'], row['pre_close'], limit_ratio)
            
            if fried:
                df.iloc[idx, df.columns.get_loc('is_fried')] = 1
                df.iloc[idx, df.columns.get_loc('board_type')] = 'fried'
            
            # 连板链断裂
            df.iloc[idx, df.columns.get_loc('chain_height')] = 0
            chain_height = 0
    
    # 选择需要的列
    result = df[['date', 'limit_status', 'chain_height', 'is_fried', 'board_type']].copy()
    result['code'] = code
    
    # 调整列顺序
    result = result[['date', 'code', 'limit_status', 'chain_height', 'is_fried', 'board_type']]
    
    return result


def calculate_batch_chain(market_data: pd.DataFrame, stock_meta: pd.DataFrame) -> pd.DataFrame:
    """
    批量计算市场所有股票的连板高度
    
    参数:
        market_data: 市场日线数据（包含多只股票）
        stock_meta: 股票元信息（包含code, limit_ratio）
    
    返回:
        所有股票的连板分析结果
    """
    all_results = []
    
    # 创建股票代码到涨跌幅限制的映射
    limit_ratio_map = dict(zip(stock_meta['code'], stock_meta['limit_ratio']))
    
    # 按股票代码分组
    grouped = market_data.groupby('code')
    total_stocks = len(grouped)
    
    print(f"开始计算 {total_stocks} 只股票的连板高度...")
    
    for idx, (code, group_df) in enumerate(grouped, 1):
        # 获取该股票的涨跌幅限制
        limit_ratio = limit_ratio_map.get(code, config.LIMIT_RATIO['MAIN'])
        
        # 计算连板高度
        result = calculate_single_stock_chain(group_df, code, limit_ratio)
        
        if not result.empty:
            all_results.append(result)
        
        # 显示进度
        if idx % 100 == 0 or idx == total_stocks:
            print(f"计算进度: {idx}/{total_stocks} ({idx/total_stocks*100:.1f}%)")
    
    if all_results:
        final_result = pd.concat(all_results, ignore_index=True)
        print(f"✓ 连板计算完成，共 {len(final_result)} 条记录")
        return final_result
    else:
        print("✗ 未计算出任何结果")
        return pd.DataFrame()


def get_high_chain_stocks(results_df: pd.DataFrame, date: str, min_height: int = 2) -> pd.DataFrame:
    """
    查询指定日期的高连板股票
    
    参数:
        results_df: 连板分析结果
        date: 日期 YYYYMMDD
        min_height: 最小连板高度
    
    返回:
        符合条件的股票列表
    """
    filtered = results_df[
        (results_df['date'] == date) & 
        (results_df['chain_height'] >= min_height)
    ].copy()
    
    return filtered.sort_values('chain_height', ascending=False)


def get_stock_max_chain(results_df: pd.DataFrame, code: str) -> int:
    """
    查询指定股票的历史最高连板记录
    
    参数:
        results_df: 连板分析结果
        code: 股票代码
    
    返回:
        最高连板高度
    """
    stock_data = results_df[results_df['code'] == code]
    
    if stock_data.empty:
        return 0
    
    return int(stock_data['chain_height'].max())


if __name__ == '__main__':
    # 测试用例：模拟一只股票的连续涨停过程
    test_data = pd.DataFrame({
        'date': ['20240102', '20240103', '20240104', '20240105', '20240108'],
        'code': ['000001'] * 5,
        'open': [10.0, 11.0, 12.1, 13.31, 13.5],
        'high': [11.0, 12.1, 13.31, 13.5, 14.0],
        'low': [10.0, 11.0, 12.1, 13.0, 13.0],
        'close': [11.0, 12.1, 13.31, 13.0, 14.3],
        'pre_close': [10.0, 11.0, 12.1, 13.31, 13.0],
        'volume': [1000000] * 5,
        'amount': [10000000] * 5
    })
    
    print("测试数据:")
    print(test_data[['date', 'close', 'pre_close']])
    
    # 计算连板高度（假设10%涨跌幅限制）
    result = calculate_single_stock_chain(test_data, '000001', 0.10)
    
    print("\n连板分析结果:")
    print(result)
    
    # 预期结果：
    # 20240102: 涨停 1板
    # 20240103: 涨停 2板  
    # 20240104: 涨停 3板
    # 20240105: 未涨停 0板（断链）
    # 20240108: 涨停 1板（新首板）
