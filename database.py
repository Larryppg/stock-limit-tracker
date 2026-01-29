"""
数据库初始化与操作模块
"""
import sqlite3
import os
import time
import json
import threading
import config
import argparse
from typing import List, Dict, Any
import pandas as pd
from config import DB_PATH

# 调试版本标记（用于确认运行时加载的是哪一份文件）
DB_MODULE_VERSION = "2026-01-24-debug-1"

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

# #region agent log
try:
    _dbg_log("H32", "database.py:module", "loaded", {
        "file": __file__,
        "db_path": DB_PATH,
        "mtime": os.path.getmtime(__file__),
        "size": os.path.getsize(__file__),
    })
except Exception:
    pass
# #endregion agent log

# #region agent log
def _log_db_file_state(hypothesis_id: str, location: str):
    try:
        base = DB_PATH
        wal = f"{DB_PATH}-wal"
        shm = f"{DB_PATH}-shm"
        data = {
            "db_exists": os.path.exists(base),
            "wal_exists": os.path.exists(wal),
            "shm_exists": os.path.exists(shm),
        }
        if data["db_exists"]:
            data["db_size"] = os.path.getsize(base)
        if data["wal_exists"]:
            data["wal_size"] = os.path.getsize(wal)
        if data["shm_exists"]:
            data["shm_size"] = os.path.getsize(shm)
        _dbg_log(hypothesis_id, location, "db_file_state", data)
    except Exception:
        pass
# #endregion agent log


def init_database():
    """初始化数据库表结构"""
    # 确保数据目录存在
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()
    # #region agent log
    _dbg_log("H20", "database.py:init_database", "enter", {
        "db_path": DB_PATH
    })
    # #endregion agent log
    
    # 股票元信息表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_meta (
        code TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        market TEXT,
        board_type TEXT,
        limit_ratio REAL,
        is_st INTEGER DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # 日线行情表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS daily_market_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        code TEXT NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        pre_close REAL,
        volume REAL,
        amount REAL,
        UNIQUE(date, code)
    )
    """)
    
    # 创建索引
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_daily_date_code 
    ON daily_market_data(date, code)
    """)
    
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_daily_code_date 
    ON daily_market_data(code, date)
    """)
    
    # 涨停分析结果表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS limit_analysis_result (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        code TEXT NOT NULL,
        limit_status INTEGER DEFAULT 0,  -- 0:未涨停 1:涨停
        chain_height INTEGER DEFAULT 0,   -- 连板高度
        is_fried INTEGER DEFAULT 0,       -- 是否炸板
        board_type TEXT,                  -- 板型：yizi/normal/fried
        UNIQUE(date, code)
    )
    """)
    
    # 创建索引
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_limit_date_height 
    ON limit_analysis_result(date, chain_height)
    """)
    
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_limit_code_date 
    ON limit_analysis_result(code, date)
    """)
    
    # 数据获取进度表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fetch_progress (
        task_id TEXT PRIMARY KEY,
        start_date TEXT NOT NULL,
        end_date TEXT NOT NULL,
        current_date TEXT,
        status TEXT DEFAULT 'pending',  -- pending/running/completed/failed
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # 可选：初始化时清理旧数据
    prune_before = getattr(config, "DATA_PRUNE_BEFORE_DATE", None)
    # #region agent log
    _dbg_log("H21", "database.py:init_database", "prune_config", {
        "prune_before": prune_before
    })
    # #endregion agent log
    if prune_before:
        try:
            # #region agent log
            _dbg_log("H22", "database.py:init_database", "prune_start", {
                "prune_before": prune_before
            })
            # #endregion agent log
            conn.execute("PRAGMA busy_timeout=30000;")
            conn.execute("PRAGMA journal_mode=WAL;")

            def _prune_table(table_name: str, batch_size: int = 50000) -> int:
                total_deleted = 0
                while True:
                    cursor.execute(
                        f"DELETE FROM {table_name} "
                        f"WHERE rowid IN (SELECT rowid FROM {table_name} WHERE date < ? LIMIT ?)",
                        (prune_before, batch_size),
                    )
                    deleted = cursor.rowcount
                    total_deleted += deleted
                    # #region agent log
                    _dbg_log("H22", "database.py:init_database", "prune_batch", {
                        "table": table_name,
                        "deleted": int(deleted),
                        "total_deleted": int(total_deleted)
                    })
                    # #endregion agent log
                    if deleted:
                        print(f"清理中: {table_name} 已删除 {total_deleted} 条")
                    conn.commit()
                    if deleted == 0:
                        break
                return total_deleted

            daily_deleted = _prune_table("daily_market_data")
            limit_deleted = _prune_table("limit_analysis_result")
            # #region agent log
            _dbg_log("H22", "database.py:init_database", "prune_done", {
                "prune_before": prune_before,
                "daily_deleted": int(daily_deleted),
                "limit_deleted": int(limit_deleted)
            })
            # #endregion agent log
        except Exception as e:
            # #region agent log
            _dbg_log("H23", "database.py:init_database", "prune_exception", {
                "prune_before": prune_before,
                "error": str(e),
                "error_type": type(e).__name__
            })
            # #endregion agent log
    
    conn.commit()
    conn.close()
    print(f"✓ 数据库初始化完成: {DB_PATH}")


def delete_database_file() -> bool:
    """删除数据库文件（仅删除文件，不初始化）"""
    # #region agent log
    _dbg_log("H24", "database.py:delete_database_file", "enter", {"db_path": DB_PATH})
    # #endregion agent log
    if not os.path.exists(DB_PATH):
        print(f"⚠️  数据库文件不存在: {DB_PATH}")
        # #region agent log
        _dbg_log("H24", "database.py:delete_database_file", "missing", {"db_path": DB_PATH})
        # #endregion agent log
        return False
    try:
        os.remove(DB_PATH)
        print(f"✓ 已删除数据库文件: {DB_PATH}")
        # #region agent log
        _dbg_log("H24", "database.py:delete_database_file", "deleted", {"db_path": DB_PATH})
        # #endregion agent log
        return True
    except Exception as e:
        print(f"✗ 删除数据库文件失败: {e}")
        # #region agent log
        _dbg_log("H24", "database.py:delete_database_file", "delete_exception", {
            "db_path": DB_PATH,
            "error": str(e),
            "error_type": type(e).__name__
        })
        # #endregion agent log
        return False


def get_connection():
    """获取数据库连接"""
    # #region agent log
    _dbg_log("H1", "database.py:get_connection", "create_connection", {
        "db_path": DB_PATH,
        "pid": os.getpid(),
        "thread_id": threading.get_ident()
    })
    _log_db_file_state("H1", "database.py:get_connection")
    # #endregion agent log
    conn = sqlite3.connect(DB_PATH, timeout=30)
    # #region agent log
    _dbg_log("H1", "database.py:get_connection", "apply_pragmas", {
        "busy_timeout_ms": 30000,
        "journal_mode": "WAL"
    })
    # #endregion agent log
    try:
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA journal_mode=WAL;")
    except Exception as e:
        # #region agent log
        _dbg_log("H2", "database.py:get_connection", "pragma_exception", {
            "error": str(e),
            "error_type": type(e).__name__
        })
        # #endregion agent log
    return conn


# #region agent log
def _probe_write_lock():
    try:
        probe_conn = sqlite3.connect(DB_PATH, timeout=1)
        try:
            probe_conn.execute("BEGIN IMMEDIATE;")
            probe_conn.execute("ROLLBACK;")
            _dbg_log("H4", "database.py:_probe_write_lock", "write_lock_free", {})
        finally:
            probe_conn.close()
    except Exception as e:
        _dbg_log("H4", "database.py:_probe_write_lock", "write_lock_blocked", {
            "error": str(e),
            "error_type": type(e).__name__
        })
# #endregion agent log


def save_stock_meta(stocks_df: pd.DataFrame):
    """保存股票元信息"""
    # #region agent log
    _dbg_log("H3", "database.py:save_stock_meta", "enter", {
        "rows": int(len(stocks_df)),
        "pid": os.getpid(),
        "thread_id": threading.get_ident()
    })
    # #endregion agent log
    conn = get_connection()
    # #region agent log
    _dbg_log("H3", "database.py:save_stock_meta", "before_to_sql", {
        "if_exists": "replace",
        "table": "stock_meta"
    })
    # #endregion agent log
    try:
        # #region agent log
        _probe_write_lock()
        # #endregion agent log
        # 避免 DROP TABLE 触发更重的锁；先清表再追加
        # #region agent log
        _dbg_log("H3", "database.py:save_stock_meta", "before_delete", {
            "table": "stock_meta"
        })
        # #endregion agent log
        conn.execute("DELETE FROM stock_meta;")
        # #region agent log
        _dbg_log("H3", "database.py:save_stock_meta", "after_delete", {
            "table": "stock_meta"
        })
        # #endregion agent log
        stocks_df.to_sql('stock_meta', conn, if_exists='append', index=False)
    except Exception as e:
        # #region agent log
        _dbg_log("H2", "database.py:save_stock_meta", "to_sql_exception", {
            "error": str(e),
            "error_type": type(e).__name__
        })
        # #endregion agent log
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def save_daily_data(data_df: pd.DataFrame):
    """保存日线行情数据"""
    conn = get_connection()
    # #region agent log
    try:
        if "date" in data_df.columns:
            date_series = data_df["date"].astype(str)
            date_min = date_series.min()
            date_max = date_series.max()
            date_counts = date_series.value_counts().head(5).to_dict()
        else:
            date_min = None
            date_max = None
            date_counts = {}
        unique_codes = int(data_df["code"].nunique()) if "code" in data_df.columns else 0
        _dbg_log("H25", "database.py:save_daily_data", "enter", {
            "rows": int(len(data_df)),
            "unique_codes": int(unique_codes),
            "date_min": date_min,
            "date_max": date_max,
            "top_date_counts": date_counts
        })
    except Exception:
        pass
    # #endregion agent log
    # #region agent log
    try:
        _dbg_log("H31", "database.py:save_daily_data", "version_marker", {
            "chunk_size": int(getattr(config, "DAILY_SAVE_CHUNK_SIZE", 50000)),
            "rows": int(len(data_df))
        })
    except Exception:
        pass
    # #endregion agent log
    try:
        total_rows = int(len(data_df))
        # SQLite 变量数限制为 999，每行约 9 列，安全 chunk_size = 999/9 ≈ 100
        # 使用较小的 chunk_size 避免 "too many SQL variables" 错误
        sqlite_safe_chunk = 100  # 保守值，确保不超过 SQLite 限制
        chunk_size = min(int(getattr(config, "DAILY_SAVE_CHUNK_SIZE", 50000)), sqlite_safe_chunk)
        # #region agent log
        _dbg_log("H31", "database.py:save_daily_data", "chunked_write_start", {
            "total_rows": total_rows,
            "chunk_size": int(chunk_size),
            "sqlite_safe_chunk": sqlite_safe_chunk
        })
        # #endregion agent log
        for start in range(0, total_rows, chunk_size):
            end = min(start + chunk_size, total_rows)
            chunk = data_df.iloc[start:end]
            chunk.to_sql('daily_market_data', conn, if_exists='append', index=False, method='multi')
            conn.commit()
            # #region agent log
            if start == 0 or (start + chunk_size) >= total_rows:
                _dbg_log("H31", "database.py:save_daily_data", "chunked_write_progress", {
                    "start": int(start),
                    "end": int(end),
                    "written": int(end)
                })
            # #endregion agent log
        # #region agent log
        _dbg_log("H25", "database.py:save_daily_data", "success", {
            "rows": int(len(data_df))
        })
        # #endregion agent log
        # #region agent log
        try:
            if "date" in data_df.columns:
                date_max = data_df["date"].astype(str).max()
                count_row = pd.read_sql_query(
                    f"SELECT COUNT(*) as cnt FROM daily_market_data WHERE date = '{date_max}'",
                    conn
                ).iloc[0]
                _dbg_log("H31", "database.py:save_daily_data", "post_insert_count", {
                    "date": date_max,
                    "count": int(count_row["cnt"])
                })
        except Exception:
            pass
        # #endregion agent log
    except Exception as e:
        # #region agent log
        _dbg_log("H31", "database.py:save_daily_data", "to_sql_exception", {
            "error": str(e),
            "error_type": type(e).__name__
        })
        # #endregion agent log
        raise
    conn.close()


def save_limit_results(results_df: pd.DataFrame):
    """保存涨停分析结果"""
    conn = get_connection()
    results_df.to_sql('limit_analysis_result', conn, if_exists='append', index=False)
    conn.close()


def get_stock_daily_data(code: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """获取指定股票的日线数据"""
    conn = get_connection()
    query = f"SELECT * FROM daily_market_data WHERE code = '{code}'"
    
    if start_date:
        query += f" AND date >= '{start_date}'"
    if end_date:
        query += f" AND date <= '{end_date}'"
    
    query += " ORDER BY date ASC"
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def update_fetch_progress(task_id: str, status: str, current_date: str = None):
    """更新数据获取进度"""
    conn = get_connection()
    cursor = conn.cursor()
    
    if current_date:
        cursor.execute("""
        UPDATE fetch_progress 
        SET status = ?, current_date = ?, updated_at = CURRENT_TIMESTAMP
        WHERE task_id = ?
        """, (status, current_date, task_id))
    else:
        cursor.execute("""
        UPDATE fetch_progress 
        SET status = ?, updated_at = CURRENT_TIMESTAMP
        WHERE task_id = ?
        """, (status, task_id))
    
    conn.commit()
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='数据库初始化/重置工具')
    parser.add_argument(
        '--delete-db',
        action='store_true',
        help='删除数据库文件，不进行初始化'
    )
    parser.add_argument(
        '--reset-db',
        action='store_true',
        help='删除数据库文件后重新初始化'
    )
    args = parser.parse_args()

    if args.reset_db:
        delete_database_file()
        init_database()
    elif args.delete_db:
        delete_database_file()
    else:
        init_database()
