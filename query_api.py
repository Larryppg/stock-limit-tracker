"""
查询接口模块 - 提供数据查询功能
"""
import pandas as pd
from typing import List, Optional, Dict, Any
import database
from config import DB_PATH
import time
import json
import os
import threading

# #region agent log
_DEBUG_LOG_PATH = r"c:\Users\Larryppg\Desktop\stock.cursor\.cursor\debug.log"
def _dbg_log(hypothesis_id: str, location: str, message: str, data: dict):
    try:
        payload = {
            "sessionId": "debug-session",
            "runId": "pre-fix",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        with open(_DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass
# #endregion agent log


class LimitQueryAPI:
    """连板数据查询API"""
    
    def __init__(self):
        self.conn = None
    
    def connect(self):
        """建立数据库连接"""
        # #region agent log
        _dbg_log("H5", "query_api.py:LimitQueryAPI.connect", "enter", {
            "pid": os.getpid(),
            "thread_id": threading.get_ident(),
            "db_path": DB_PATH
        })
        # #endregion agent log
        self.conn = database.get_connection()
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            # #region agent log
            _dbg_log("H5", "query_api.py:LimitQueryAPI.close", "close", {
                "pid": os.getpid(),
                "thread_id": threading.get_ident()
            })
            # #endregion agent log
            self.conn.close()
    
    def query_high_chain_stocks(self, date: str, min_height: int = 2) -> pd.DataFrame:
        """
        查询指定日期的高连板股票
        
        参数:
            date: 日期 YYYYMMDD
            min_height: 最小连板高度
        
        返回:
            股票列表（按连板高度降序）
        """
        if not self.conn:
            self.connect()
        
        query = f"""
        SELECT 
            l.code,
            s.name,
            l.chain_height,
            l.board_type,
            l.is_fried,
            m.close,
            m.pre_close,
            ROUND((m.close - m.pre_close) * 100.0 / m.pre_close, 2) as change_pct
        FROM limit_analysis_result l
        LEFT JOIN stock_meta s ON l.code = s.code
        LEFT JOIN daily_market_data m ON l.date = m.date AND l.code = m.code
        WHERE l.date = '{date}' 
          AND l.chain_height >= {min_height}
        ORDER BY l.chain_height DESC, l.code
        """
        
        df = pd.read_sql_query(query, self.conn)
        return df
    
    def query_stock_chain_history(self, code: str, start_date: str = None, 
                                  end_date: str = None) -> pd.DataFrame:
        """
        查询指定股票的连板历史
        
        参数:
            code: 股票代码
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
        
        返回:
            连板历史记录
        """
        if not self.conn:
            self.connect()
        
        query = f"""
        SELECT 
            l.date,
            l.chain_height,
            l.board_type,
            l.is_fried,
            m.open,
            m.high,
            m.low,
            m.close,
            m.volume
        FROM limit_analysis_result l
        LEFT JOIN daily_market_data m ON l.date = m.date AND l.code = m.code
        WHERE l.code = '{code}'
        """
        
        if start_date:
            query += f" AND l.date >= '{start_date}'"
        if end_date:
            query += f" AND l.date <= '{end_date}'"
        
        query += " ORDER BY l.date"
        
        df = pd.read_sql_query(query, self.conn)
        return df
    
    def query_stock_max_chain(self, code: str) -> Dict[str, Any]:
        """
        查询股票的历史最高连板记录
        
        参数:
            code: 股票代码
        
        返回:
            最高连板信息
        """
        if not self.conn:
            self.connect()
        
        query = f"""
        SELECT 
            l.date,
            l.chain_height,
            s.name
        FROM limit_analysis_result l
        LEFT JOIN stock_meta s ON l.code = s.code
        WHERE l.code = '{code}'
        ORDER BY l.chain_height DESC
        LIMIT 1
        """
        
        df = pd.read_sql_query(query, self.conn)
        
        if df.empty:
            return {'code': code, 'max_chain': 0, 'date': None, 'name': None}
        
        row = df.iloc[0]
        return {
            'code': code,
            'name': row['name'],
            'max_chain': int(row['chain_height']),
            'date': row['date']
        }
    
    def query_daily_summary(self, date: str) -> Dict[str, Any]:
        """
        查询指定日期的市场摘要
        
        参数:
            date: 日期 YYYYMMDD
        
        返回:
            市场摘要数据
        """
        if not self.conn:
            self.connect()
        
        # #region agent log
        try:
            count_row = pd.read_sql_query(
                f"SELECT COUNT(*) as cnt FROM daily_market_data WHERE date = '{date}'",
                self.conn
            ).iloc[0]
            _dbg_log("H30", "query_api.py:query_daily_summary", "daily_market_data_count", {
                "date": date,
                "count": int(count_row["cnt"])
            })
        except Exception as e:
            _dbg_log("H30", "query_api.py:query_daily_summary", "daily_market_data_count_exception", {
                "date": date,
                "error": str(e),
                "error_type": type(e).__name__
            })
        # #endregion agent log

        # 涨停数量统计
        query_limit = f"""
        SELECT 
            COUNT(*) as total_limit,
            SUM(CASE WHEN board_type = 'yizi' THEN 1 ELSE 0 END) as yizi_count,
            SUM(CASE WHEN is_fried = 1 THEN 1 ELSE 0 END) as fried_count
        FROM limit_analysis_result
        WHERE date = '{date}' AND limit_status = 1
        """
        
        limit_stats = pd.read_sql_query(query_limit, self.conn).iloc[0]
        
        # 连板高度分布
        query_dist = f"""
        SELECT chain_height, COUNT(*) as count
        FROM limit_analysis_result
        WHERE date = '{date}' AND chain_height > 0
        GROUP BY chain_height
        ORDER BY chain_height
        """
        
        chain_dist = pd.read_sql_query(query_dist, self.conn)
        
        return {
            'date': date,
            'total_limit': int(limit_stats['total_limit']),
            'yizi_count': int(limit_stats['yizi_count']),
            'fried_count': int(limit_stats['fried_count']),
            'chain_distribution': chain_dist.to_dict('records')
        }

    def query_daily_limit_stocks(self, date: str, include_fried: bool = True) -> pd.DataFrame:
        """
        查询指定日期的涨停股票明细

        参数:
            date: 日期 YYYYMMDD
            include_fried: 是否包含炸板股票（默认包含）

        返回:
            涨停股票明细
        """
        if not self.conn:
            self.connect()

        query = f"""
        SELECT 
            l.date,
            l.code,
            s.name,
            l.chain_height,
            l.board_type,
            l.is_fried,
            m.close,
            m.pre_close,
            m.volume,
            m.amount,
            ROUND((m.close - m.pre_close) * 100.0 / m.pre_close, 2) as change_pct
        FROM limit_analysis_result l
        LEFT JOIN stock_meta s ON l.code = s.code
        LEFT JOIN daily_market_data m ON l.date = m.date AND l.code = m.code
        WHERE l.date = '{date}' AND l.limit_status = 1
        """

        if not include_fried:
            query += " AND l.is_fried = 0"

        query += " ORDER BY l.chain_height DESC, l.code"

        df = pd.read_sql_query(query, self.conn)
        return df

    def query_daily_fried_stocks(self, date: str) -> pd.DataFrame:
        """
        查询指定日期的炸板股票明细

        参数:
            date: 日期 YYYYMMDD

        返回:
            炸板股票明细
        """
        if not self.conn:
            self.connect()

        query = f"""
        SELECT 
            l.date,
            l.code,
            s.name,
            l.chain_height,
            l.board_type,
            l.is_fried,
            m.close,
            m.pre_close,
            m.volume,
            m.amount,
            ROUND((m.close - m.pre_close) * 100.0 / m.pre_close, 2) as change_pct
        FROM limit_analysis_result l
        LEFT JOIN stock_meta s ON l.code = s.code
        LEFT JOIN daily_market_data m ON l.date = m.date AND l.code = m.code
        WHERE l.date = '{date}' AND l.is_fried = 1
        ORDER BY l.chain_height DESC, l.code
        """

        df = pd.read_sql_query(query, self.conn)
        return df
    
    def query_recent_limit_stocks(self, days: int = 5, min_height: int = 1) -> pd.DataFrame:
        """
        查询最近N天的涨停股票
        
        参数:
            days: 最近天数
            min_height: 最小连板高度
        
        返回:
            涨停股票列表
        """
        if not self.conn:
            self.connect()
        
        query = f"""
        SELECT 
            l.date,
            l.code,
            s.name,
            l.chain_height,
            l.board_type
        FROM limit_analysis_result l
        LEFT JOIN stock_meta s ON l.code = s.code
        WHERE l.chain_height >= {min_height}
        ORDER BY l.date DESC, l.chain_height DESC
        LIMIT {days * 100}
        """
        
        df = pd.read_sql_query(query, self.conn)
        return df
    
    def search_stocks_by_name(self, keyword: str) -> pd.DataFrame:
        """
        根据股票名称搜索
        
        参数:
            keyword: 搜索关键词
        
        返回:
            匹配的股票列表
        """
        if not self.conn:
            self.connect()
        
        query = f"""
        SELECT code, name, board_type, is_st
        FROM stock_meta
        WHERE name LIKE '%{keyword}%'
        ORDER BY code
        """
        
        df = pd.read_sql_query(query, self.conn)
        return df


def demo_queries():
    """演示查询功能"""
    api = LimitQueryAPI()
    api.connect()
    
    print("="*60)
    print("查询API演示")
    print("="*60)
    
    # 获取最新日期
    conn = database.get_connection()
    latest_date_df = pd.read_sql_query(
        "SELECT MAX(date) as latest_date FROM limit_analysis_result",
        conn
    )
    conn.close()
    
    if latest_date_df.empty or pd.isna(latest_date_df.iloc[0]['latest_date']):
        print("\n⚠️  数据库中暂无数据，请先运行MVP流程")
        api.close()
        return
    
    latest_date = latest_date_df.iloc[0]['latest_date']
    
    # 查询1: 当日高连板股票
    print(f"\n【查询1】{latest_date} 的高连板股票（>=3板）:")
    high_chain = api.query_high_chain_stocks(latest_date, min_height=3)
    
    if not high_chain.empty:
        print(high_chain[['code', 'name', 'chain_height', 'board_type', 'change_pct']].to_string(index=False))
    else:
        print("  当日无3板及以上股票")
    
    # 查询2: 市场摘要
    print(f"\n【查询2】{latest_date} 市场摘要:")
    summary = api.query_daily_summary(latest_date)
    print(f"  涨停总数: {summary['total_limit']}")
    print(f"  一字板: {summary['yizi_count']}")
    print(f"  炸板: {summary['fried_count']}")
    
    if summary['chain_distribution']:
        print("  连板分布:")
        for item in summary['chain_distribution']:
            print(f"    {item['chain_height']}板: {item['count']}只")
    
    # 查询3: 股票最高连板记录
    if not high_chain.empty:
        test_code = high_chain.iloc[0]['code']
        print(f"\n【查询3】股票 {test_code} 的历史最高连板:")
        max_chain = api.query_stock_max_chain(test_code)
        print(f"  股票名称: {max_chain['name']}")
        print(f"  最高连板: {max_chain['max_chain']}板")
        print(f"  日期: {max_chain['date']}")
    
    # 查询4: 最近涨停股票
    print(f"\n【查询4】最近的涨停股票（前20只）:")
    recent = api.query_recent_limit_stocks(days=5, min_height=2)
    
    if not recent.empty:
        print(recent.head(20).to_string(index=False))
    
    api.close()
    
    print("\n" + "="*60)
    print("查询演示完成")
    print("="*60)


if __name__ == '__main__':
    demo_queries()
