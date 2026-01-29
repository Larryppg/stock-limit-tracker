"""
增量数据管理模块

功能：
1. 生成每日增量数据文件（JSON）
2. 从增量文件加载数据
3. 将增量合并到本地数据库
"""
import os
import json
import pandas as pd
from datetime import datetime
from typing import List, Optional, Tuple
import config


# 增量文件目录
INCREMENTS_DIR = os.path.join(os.path.dirname(__file__), 'data', 'increments')


def ensure_increments_dir():
    """确保增量文件目录存在"""
    os.makedirs(INCREMENTS_DIR, exist_ok=True)


def get_increment_path(date: str) -> str:
    """获取指定日期的增量文件路径"""
    return os.path.join(INCREMENTS_DIR, f"{date}.json")


def save_daily_increment(
    date: str,
    market_data: pd.DataFrame,
    limit_results: pd.DataFrame
) -> str:
    """
    保存每日增量数据到 JSON 文件
    
    参数:
        date: 日期 YYYYMMDD
        market_data: 当日市场数据
        limit_results: 当日连板分析结果
    
    返回:
        增量文件路径
    """
    ensure_increments_dir()
    
    # 只保留当日数据
    if not market_data.empty and 'date' in market_data.columns:
        market_data = market_data[market_data['date'].astype(str) == str(date)]
    if not limit_results.empty and 'date' in limit_results.columns:
        limit_results = limit_results[limit_results['date'].astype(str) == str(date)]
    
    increment = {
        'date': date,
        'created_at': datetime.now().isoformat(),
        'market_data': market_data.to_dict(orient='records') if not market_data.empty else [],
        'limit_results': limit_results.to_dict(orient='records') if not limit_results.empty else [],
        'stats': {
            'market_rows': len(market_data),
            'limit_rows': len(limit_results),
            'limit_up_count': int(limit_results['limit_status'].sum()) if not limit_results.empty and 'limit_status' in limit_results.columns else 0
        }
    }
    
    path = get_increment_path(date)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(increment, f, ensure_ascii=False, indent=2)
    
    file_size = os.path.getsize(path) / 1024  # KB
    print(f"✓ 增量文件已保存: {path} ({file_size:.1f} KB)")
    
    return path


def load_increment(date: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    加载指定日期的增量数据
    
    返回:
        (market_data, limit_results)
    """
    path = get_increment_path(date)
    if not os.path.exists(path):
        return pd.DataFrame(), pd.DataFrame()
    
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    market_data = pd.DataFrame(data.get('market_data', []))
    limit_results = pd.DataFrame(data.get('limit_results', []))
    
    return market_data, limit_results


def list_increments() -> List[str]:
    """
    列出所有增量文件的日期
    
    返回:
        日期列表（按日期排序）
    """
    ensure_increments_dir()
    
    dates = []
    for filename in os.listdir(INCREMENTS_DIR):
        if filename.endswith('.json'):
            date = filename[:-5]  # 移除 .json
            if len(date) == 8 and date.isdigit():
                dates.append(date)
    
    return sorted(dates)


def load_all_increments(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    加载所有增量数据
    
    参数:
        start_date: 起始日期（可选）
        end_date: 结束日期（可选）
    
    返回:
        (market_data, limit_results)
    """
    dates = list_increments()
    
    if start_date:
        dates = [d for d in dates if d >= start_date]
    if end_date:
        dates = [d for d in dates if d <= end_date]
    
    if not dates:
        return pd.DataFrame(), pd.DataFrame()
    
    all_market = []
    all_limits = []
    
    for date in dates:
        market, limits = load_increment(date)
        if not market.empty:
            all_market.append(market)
        if not limits.empty:
            all_limits.append(limits)
    
    market_data = pd.concat(all_market, ignore_index=True) if all_market else pd.DataFrame()
    limit_results = pd.concat(all_limits, ignore_index=True) if all_limits else pd.DataFrame()
    
    return market_data, limit_results


def merge_increments_to_db(dates: Optional[List[str]] = None):
    """
    将增量数据合并到本地数据库
    
    参数:
        dates: 要合并的日期列表，None 表示全部
    """
    import database
    
    if dates is None:
        dates = list_increments()
    
    if not dates:
        print("没有找到增量文件")
        return
    
    print(f"准备合并 {len(dates)} 个增量文件到数据库...")
    
    total_market = 0
    total_limits = 0
    
    for date in dates:
        market_data, limit_results = load_increment(date)
        
        if not market_data.empty:
            try:
                database.save_daily_data(market_data)
                total_market += len(market_data)
            except Exception as e:
                print(f"  ⚠️ {date} 市场数据合并警告: {e}")
        
        if not limit_results.empty:
            try:
                database.save_limit_results(limit_results)
                total_limits += len(limit_results)
            except Exception as e:
                print(f"  ⚠️ {date} 分析结果合并警告: {e}")
    
    print(f"✓ 合并完成: {total_market} 条市场数据, {total_limits} 条分析结果")


def get_increments_summary() -> dict:
    """
    获取增量文件摘要信息
    
    返回:
        {
            'total_files': int,
            'date_range': (start, end),
            'total_size_kb': float
        }
    """
    ensure_increments_dir()
    
    dates = list_increments()
    if not dates:
        return {
            'total_files': 0,
            'date_range': (None, None),
            'total_size_kb': 0
        }
    
    total_size = 0
    for date in dates:
        path = get_increment_path(date)
        if os.path.exists(path):
            total_size += os.path.getsize(path)
    
    return {
        'total_files': len(dates),
        'date_range': (dates[0], dates[-1]),
        'total_size_kb': total_size / 1024
    }


def cleanup_old_increments(keep_days: int = 90):
    """
    清理旧的增量文件（已合并到数据库的）
    
    参数:
        keep_days: 保留最近多少天的增量文件
    """
    from datetime import datetime, timedelta
    
    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime('%Y%m%d')
    dates = list_increments()
    
    removed = 0
    for date in dates:
        if date < cutoff:
            path = get_increment_path(date)
            try:
                os.remove(path)
                removed += 1
            except Exception as e:
                print(f"  ⚠️ 删除 {date} 失败: {e}")
    
    if removed:
        print(f"✓ 已清理 {removed} 个旧增量文件")
    else:
        print("没有需要清理的增量文件")


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='增量数据管理工具')
    parser.add_argument('--list', action='store_true', help='列出所有增量文件')
    parser.add_argument('--merge', action='store_true', help='合并增量到数据库')
    parser.add_argument('--cleanup', type=int, metavar='DAYS', help='清理超过 N 天的增量文件')
    parser.add_argument('--summary', action='store_true', help='显示增量文件摘要')
    
    args = parser.parse_args()
    
    if args.list:
        dates = list_increments()
        print(f"共 {len(dates)} 个增量文件:")
        for d in dates:
            print(f"  - {d}")
    
    elif args.merge:
        merge_increments_to_db()
    
    elif args.cleanup:
        cleanup_old_increments(args.cleanup)
    
    elif args.summary:
        summary = get_increments_summary()
        print("增量文件摘要:")
        print(f"  文件数: {summary['total_files']}")
        print(f"  日期范围: {summary['date_range'][0]} ~ {summary['date_range'][1]}")
        print(f"  总大小: {summary['total_size_kb']:.1f} KB")
    
    else:
        parser.print_help()
