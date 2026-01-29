"""
数据获取模块 - 基于AkShare
"""
import akshare as ak
import pandas as pd
import time
import sys
from datetime import datetime, timedelta
from typing import List, Optional
import config
import json
import os
from typing import Tuple

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
    import database as _db_mod
    _db_file = getattr(_db_mod, "__file__", "")
    _db_version = getattr(_db_mod, "DB_MODULE_VERSION", "missing")
except Exception:
    _db_file = ""
    _db_version = "error"
_dbg_log("H7", "data_fetcher.py:module", "loaded", {
    "python": sys.version.split(" ")[0],
    "akshare": getattr(ak, "__version__", "unknown"),
    "data_fetcher_file": __file__,
    "cwd": os.getcwd(),
    "database_file": _db_file,
    "db_module_version": _db_version
})
# #endregion agent log

# #region agent log
try:
    hist_candidates = [name for name in dir(ak) if "stock_zh_a_hist" in name]
    _dbg_log("H12", "data_fetcher.py:module", "akshare_hist_candidates", {
        "count": len(hist_candidates),
        "names": hist_candidates[:20]
    })
except Exception:
    pass
# #endregion agent log

# 确保必要配置存在，避免属性缺失导致异常
if not hasattr(config, "VERBOSE_OUTPUT"):
    config.VERBOSE_OUTPUT = True
if not hasattr(config, "PROGRESS_EVERY"):
    config.PROGRESS_EVERY = 1
if not hasattr(config, "HISTORY_MAX_ATTEMPTS"):
    config.HISTORY_MAX_ATTEMPTS = 2

# 应用可选网络配置（不写入日志）
try:
    if hasattr(ak, "set_http_timeout") and config.AK_HTTP_TIMEOUT:
        ak.set_http_timeout(config.AK_HTTP_TIMEOUT)
    if hasattr(ak, "set_proxy") and config.AK_PROXY:
        ak.set_proxy(config.AK_PROXY)
except Exception:
    pass


def get_stock_list() -> pd.DataFrame:
    """获取A股股票列表"""
    try:
        # #region agent log
        _dbg_log("H1", "data_fetcher.py:get_stock_list", "enter", {})
        # #endregion agent log
        def _normalize_stock_list(df: pd.DataFrame) -> pd.DataFrame:
            code_candidates = ['代码', 'code', '证券代码', 'A股代码', '股票代码', 'ts_code']
            name_candidates = ['名称', 'name', '证券简称', '股票简称', '简称', 'A股简称', '证券简称(中文)']
            code_col = next((c for c in code_candidates if c in df.columns), None)
            name_col = next((c for c in name_candidates if c in df.columns), None)
            if not code_col or not name_col:
                raise ValueError("未找到股票代码或名称列")
            stocks = df[[code_col, name_col]].copy()
            stocks.columns = ['code', 'name']
            if code_col == 'ts_code':
                stocks['code'] = stocks['code'].astype(str).str.split('.').str[0]

            # 添加板块类型
            stocks['board_type'] = stocks['code'].apply(config.get_board_type)
            # 判断是否ST
            stocks['is_st'] = stocks['name'].apply(config.is_st_stock).astype(int)
            # 根据板块和ST设置涨跌幅限制
            # 注意：只有主板ST股票是±5%，创业板/科创板/北交所ST股票跟普通股一样
            def get_limit_ratio(row):
                board = row['board_type']
                if row['is_st'] and board == 'MAIN':
                    # 只有主板ST股票是±5%
                    return config.LIMIT_RATIO['ST']
                # 创业板/科创板/北交所的ST股票与普通股涨跌幅限制相同
                return config.LIMIT_RATIO.get(board, config.LIMIT_RATIO['MAIN'])
            stocks['limit_ratio'] = stocks.apply(get_limit_ratio, axis=1)
            stocks['market'] = 'A'
            return stocks

        # 多数据源尝试，避免单一接口偶发断连
        sources = [("spot_em", ak.stock_zh_a_spot_em)]
        if hasattr(ak, "stock_info_a_code_name"):
            sources.append(("info_a", ak.stock_info_a_code_name))
        if hasattr(ak, "stock_info_sh_name_code"):
            sources.append(("info_sh", ak.stock_info_sh_name_code))
        if hasattr(ak, "stock_info_sz_name_code"):
            sources.append(("info_sz", ak.stock_info_sz_name_code))
        # Tushare 兜底（避免 AkShare 网络异常导致列表不全）
        token = getattr(config, "TUSHARE_TOKEN", None)
        # #region agent log
        _dbg_log("H34", "data_fetcher.py:get_stock_list", "tushare_token_state", {
            "token_set": bool(token)
        })
        # #endregion agent log
        if token:
            def _tushare_stock_basic():
                import tushare as ts
                pro = ts.pro_api(token)
                df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')
                return df
            sources.append(("tushare_basic", _tushare_stock_basic))
        # #region agent log
        _dbg_log("H34", "data_fetcher.py:get_stock_list", "sources_built", {
            "sources": [name for name, _ in sources]
        })
        # #endregion agent log

        last_error = None
        collected = []
        success_sources = []
        for source_name, func in sources:
            try:
                # #region agent log
                _dbg_log("H5", "data_fetcher.py:get_stock_list", "source_try", {"source": source_name})
                # #endregion agent log
                raw_df = func()
                if raw_df is None or raw_df.empty:
                    raise ValueError("返回结果为空")
                stocks = _normalize_stock_list(raw_df)
                # #region agent log
                try:
                    prefix_92 = stocks['code'].astype(str).str.startswith('92')
                    bj_count = int((stocks['board_type'] == 'BJ').sum())
                    prefix_92_count = int(prefix_92.sum())
                    prefix_92_bj_count = int(((stocks['board_type'] == 'BJ') & prefix_92).sum())
                    board_type_counts = stocks['board_type'].value_counts().to_dict()
                    _dbg_log("H17", "data_fetcher.py:get_stock_list", "board_type_stats", {
                        "total": int(len(stocks)),
                        "bj_count": bj_count,
                        "prefix_92_count": prefix_92_count,
                        "prefix_92_bj_count": prefix_92_bj_count,
                        "board_type_counts": board_type_counts
                    })
                except Exception:
                    pass
                # #endregion agent log
                print(f"✓ 获取股票列表成功({source_name})，共 {len(stocks)} 只")
                # #region agent log
                _dbg_log("H5", "data_fetcher.py:get_stock_list", "source_success", {"source": source_name, "count": int(len(stocks)), "columns": list(raw_df.columns)})
                # #endregion agent log
                collected.append(stocks)
                success_sources.append(source_name)
                # 如果已接近全量，直接返回
                if len(stocks) >= 4000:
                    # #region agent log
                    _dbg_log("H33", "data_fetcher.py:get_stock_list", "early_return", {
                        "source": source_name,
                        "count": int(len(stocks))
                    })
                    _dbg_log("H1", "data_fetcher.py:get_stock_list", "success", {"count": int(len(stocks))})
                    # #endregion agent log
                    return stocks
            except Exception as e:
                last_error = e
                # #region agent log
                _dbg_log("H5", "data_fetcher.py:get_stock_list", "source_exception", {"source": source_name, "error": str(e), "error_type": type(e).__name__})
                # #endregion agent log
                continue

        if collected:
            merged = pd.concat(collected, ignore_index=True)
            merged = merged.drop_duplicates(subset=["code"], keep="last")
            # 若数量仍偏少，尝试 Tushare 兜底补全
            if len(merged) < 4000 and token:
                # #region agent log
                _dbg_log("H34", "data_fetcher.py:get_stock_list", "tushare_fallback_try", {
                    "current_total": int(len(merged))
                })
                # #endregion agent log
                try:
                    ts_df = _tushare_stock_basic()
                    ts_stocks = _normalize_stock_list(ts_df)
                    merged = pd.concat([merged, ts_stocks], ignore_index=True)
                    merged = merged.drop_duplicates(subset=["code"], keep="last")
                    # #region agent log
                    _dbg_log("H34", "data_fetcher.py:get_stock_list", "tushare_fallback_success", {
                        "tushare_count": int(len(ts_stocks)),
                        "merged_total": int(len(merged))
                    })
                    # #endregion agent log
                except Exception as e:
                    # #region agent log
                    _dbg_log("H34", "data_fetcher.py:get_stock_list", "tushare_fallback_exception", {
                        "error": str(e),
                        "error_type": type(e).__name__
                    })
                    # #endregion agent log
            # #region agent log
            try:
                prefix_92 = merged['code'].astype(str).str.startswith('92')
                bj_count = int((merged['board_type'] == 'BJ').sum())
                prefix_92_count = int(prefix_92.sum())
                prefix_92_bj_count = int(((merged['board_type'] == 'BJ') & prefix_92).sum())
                board_type_counts = merged['board_type'].value_counts().to_dict()
                _dbg_log("H33", "data_fetcher.py:get_stock_list", "combined_success", {
                    "total": int(len(merged)),
                    "sources": success_sources,
                    "bj_count": bj_count,
                    "prefix_92_count": prefix_92_count,
                    "prefix_92_bj_count": prefix_92_bj_count,
                    "board_type_counts": board_type_counts
                })
            except Exception:
                pass
            _dbg_log("H1", "data_fetcher.py:get_stock_list", "success", {"count": int(len(merged))})
            # #endregion agent log
            return merged

        raise last_error if last_error else RuntimeError("股票列表获取失败")
        
    except Exception as e:
        print(f"✗ 获取股票列表失败: {e}")
        # #region agent log
        _dbg_log("H1", "data_fetcher.py:get_stock_list", "exception", {"error": str(e)})
        # #endregion agent log
        return pd.DataFrame()


def get_stock_history(code: str, start_date: str, end_date: str, adjust: str = '') -> pd.DataFrame:
    """
    获取单只股票历史行情
    
    参数:
        code: 股票代码
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD
        adjust: 复权类型 ''不复权 'qfq'前复权 'hfq'后复权
    """
    try:
        # #region agent log
        # 符号变体（部分接口可能要求带交易所前缀）
        symbol_variants = [code]
        if code.startswith(("00", "30")):
            symbol_variants.append(f"sz{code}")
        elif code.startswith("60"):
            symbol_variants.append(f"sh{code}")
        elif code.startswith(("68",)):
            symbol_variants.append(f"sh{code}")
        elif code.startswith(("8", "4")):
            symbol_variants.append(f"bj{code}")

        _dbg_log("H2", "data_fetcher.py:get_stock_history", "enter", {
            "code": code,
            "start_date": start_date,
            "end_date": end_date,
            "adjust": adjust,
            "symbol_variants": symbol_variants,
            "proxy_set": bool(config.AK_PROXY),
            "timeout_set": bool(config.AK_HTTP_TIMEOUT),
        })
        # #endregion agent log
        # AkShare 需要 YYYYMMDD（不带连字符）
        start = start_date
        end = end_date

        max_attempts = config.HISTORY_MAX_ATTEMPTS
        last_error = None
        for attempt in range(1, max_attempts + 1):
            # #region agent log
            _dbg_log("H6", "data_fetcher.py:get_stock_history", "attempt", {"code": code, "attempt": attempt})
            # #endregion agent log
            try:
                providers = [("em", ak.stock_zh_a_hist)]
                if hasattr(ak, "stock_zh_a_hist_tx"):
                    providers.append(("tx", ak.stock_zh_a_hist_tx))
                if hasattr(ak, "stock_zh_a_hist_163"):
                    providers.append(("163", ak.stock_zh_a_hist_163))
                if hasattr(ak, "stock_zh_a_hist_sina"):
                    providers.append(("sina", ak.stock_zh_a_hist_sina))
                # #region agent log
                _dbg_log("H11", "data_fetcher.py:get_stock_history", "providers_available", {
                    "code": code,
                    "attempt": attempt,
                    "providers": [name for name, _ in providers],
                })
                # #endregion agent log

                df = None
                for provider_name, provider_func in providers:
                    # #region agent log
                    _dbg_log("H8", "data_fetcher.py:get_stock_history", "provider_try", {"code": code, "attempt": attempt, "provider": provider_name})
                    # #endregion agent log
                    for symbol in symbol_variants:
                        try:
                            if provider_name == "tx":
                                # #region agent log
                                _dbg_log("H16", "data_fetcher.py:get_stock_history", "tx_signature_try", {
                                    "code": code,
                                    "attempt": attempt,
                                    "symbol": symbol,
                                    "signature": "no_period"
                                })
                                # #endregion agent log
                                try:
                                    df = provider_func(
                                        symbol=symbol,
                                        start_date=start,
                                        end_date=end,
                                        adjust=adjust
                                    )
                                except TypeError:
                                    # fallback without adjust
                                    # #region agent log
                                    _dbg_log("H16", "data_fetcher.py:get_stock_history", "tx_signature_try", {
                                        "code": code,
                                        "attempt": attempt,
                                        "symbol": symbol,
                                        "signature": "no_period_no_adjust"
                                    })
                                    # #endregion agent log
                                    df = provider_func(
                                        symbol=symbol,
                                        start_date=start,
                                        end_date=end
                                    )
                            else:
                                df = provider_func(
                                    symbol=symbol,
                                    period="daily",
                                    start_date=start,
                                    end_date=end,
                                    adjust=adjust
                                )
                            # 成功拿到数据（非空才算成功）
                            if df is not None and not df.empty:
                                # #region agent log
                                _dbg_log("H8", "data_fetcher.py:get_stock_history", "provider_success", {
                                    "code": code,
                                    "attempt": attempt,
                                    "provider": provider_name,
                                    "symbol": symbol,
                                    "rows": int(len(df))
                                })
                                # #endregion agent log
                                break
                        except Exception as e:
                            # #region agent log
                            _dbg_log("H6", "data_fetcher.py:get_stock_history", "attempt_exception", {
                                "code": code,
                                "attempt": attempt,
                                "symbol": symbol,
                                "provider": provider_name,
                                "error": str(e),
                                "error_type": type(e).__name__
                            })
                            # #endregion agent log
                            df = None
                            continue
                    if df is not None and not df.empty:
                        break

                if df is None or df.empty:
                    # #region agent log
                    _dbg_log("H2", "data_fetcher.py:get_stock_history", "empty_df", {
                        "code": code,
                        "start": start,
                        "end": end,
                        "attempt": attempt
                    })
                    # #endregion agent log
                    last_error = ValueError("empty_df")
                    continue

                # 标准化列名
                df = df.rename(columns={
                    '日期': 'date',
                    '开盘': 'open',
                    '最高': 'high',
                    '最低': 'low',
                    '收盘': 'close',
                    '成交量': 'volume',
                    '成交额': 'amount'
                })

                # 选择需要的列
                columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
                df = df[[col for col in columns if col in df.columns]]

                # 添加股票代码
                df['code'] = code

                # 计算前收盘价
                df['pre_close'] = df['close'].shift(1)

                # 去除第一行（没有前收盘价）
                df = df.dropna(subset=['pre_close'])

                # 转换日期格式为YYYYMMDD
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d')

                # #region agent log
                _dbg_log("H2", "data_fetcher.py:get_stock_history", "success", {"code": code, "rows": int(len(df)), "attempt": attempt})
                # #endregion agent log
                return df

            except Exception as e:
                last_error = e
                # #region agent log
                _dbg_log("H6", "data_fetcher.py:get_stock_history", "attempt_exception", {
                    "code": code,
                    "attempt": attempt,
                    "error": str(e),
                    "error_type": type(e).__name__
                })
                # #endregion agent log
                continue

        print(f"✗ 获取 {code} 历史数据失败: {last_error}")
        # #region agent log
        _dbg_log("H2", "data_fetcher.py:get_stock_history", "exception", {"code": code, "error": str(last_error)})
        # #endregion agent log
        # 尝试 Tushare 兜底（可选）
        if getattr(config, "TUSHARE_TOKEN", None):
            try:
                import tushare as ts
                pro = ts.pro_api(config.TUSHARE_TOKEN)
                if code.startswith("6"):
                    ts_code = f"{code}.SH"
                elif code.startswith(("0", "3")):
                    ts_code = f"{code}.SZ"
                elif code.startswith(("8", "4")):
                    ts_code = f"{code}.BJ"
                else:
                    ts_code = f"{code}.SZ"
                df = pro.daily(ts_code=ts_code, start_date=start, end_date=end)
                if df is None or df.empty:
                    # #region agent log
                    _dbg_log("H13", "data_fetcher.py:get_stock_history", "tushare_empty", {"code": code, "ts_code": ts_code})
                    # #endregion agent log
                    return pd.DataFrame()
                # 标准化列名
                df = df.rename(columns={
                    'trade_date': 'date',
                    'open': 'open',
                    'high': 'high',
                    'low': 'low',
                    'close': 'close',
                    'vol': 'volume',
                    'amount': 'amount',
                    'pre_close': 'pre_close',
                })
                df['code'] = code
                df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'pre_close', 'code']]
                # #region agent log
                _dbg_log("H13", "data_fetcher.py:get_stock_history", "tushare_success", {"code": code, "rows": int(len(df))})
                # #endregion agent log
                return df
            except Exception as e:
                # #region agent log
                _dbg_log("H13", "data_fetcher.py:get_stock_history", "tushare_exception", {"code": code, "error": str(e), "error_type": type(e).__name__})
                # #endregion agent log
                return pd.DataFrame()
        else:
            # #region agent log
            _dbg_log("H13", "data_fetcher.py:get_stock_history", "tushare_unavailable", {"code": code, "reason": "token_missing"})
            # #endregion agent log
        return pd.DataFrame()

    except Exception as e:
        print(f"✗ 获取 {code} 历史数据失败: {e}")
        # #region agent log
        _dbg_log("H2", "data_fetcher.py:get_stock_history", "exception", {"code": code, "error": str(e)})
        # #endregion agent log
        return pd.DataFrame()


def fetch_market_data(stock_codes: List[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    批量获取市场数据
    
    参数:
        stock_codes: 股票代码列表
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD
    """
    all_data = []
    total = len(stock_codes)
    
    # #region agent log
    _dbg_log("H10", "data_fetcher.py:fetch_market_data", "config_state", {
        "config_file": getattr(config, "__file__", ""),
        "has_verbose": hasattr(config, "VERBOSE_OUTPUT"),
        "has_progress_every": hasattr(config, "PROGRESS_EVERY")
    })
    # #endregion agent log

    if getattr(config, "VERBOSE_OUTPUT", True):
        print(f"开始获取 {total} 只股票的历史数据...")
    # #region agent log
    _dbg_log("H3", "data_fetcher.py:fetch_market_data", "enter", {"total": total, "start_date": start_date, "end_date": end_date})
    # #endregion agent log
    
    for idx, code in enumerate(stock_codes, 1):
        if getattr(config, "PRINT_EACH_STOCK", True):
            print(f"获取中: {idx}/{total} {code}")
        df = get_stock_history(code, start_date, end_date)
        
        if not df.empty:
            all_data.append(df)
        
        # 显示进度
        verbose = getattr(config, "VERBOSE_OUTPUT", True)
        progress_every = int(getattr(config, "PROGRESS_EVERY", 5))
        if verbose and (idx % progress_every == 0 or idx == total):
            print(f"进度: {idx}/{total} ({idx/total*100:.1f}%)")
        
        # 避免请求过快
        time.sleep(config.FETCH_SLEEP_INTERVAL)
    
    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        print(f"✓ 数据获取完成，共 {len(result)} 条记录")
        # #region agent log
        _dbg_log("H3", "data_fetcher.py:fetch_market_data", "success", {"rows": int(len(result))})
        # #endregion agent log
        return result
    else:
        print("✗ 未获取到任何数据")
        # #region agent log
        _dbg_log("H3", "data_fetcher.py:fetch_market_data", "empty_all_data", {"total": total})
        # #endregion agent log
        return pd.DataFrame()


def _to_ts_code(code: str) -> str:
    if code.startswith("6"):
        return f"{code}.SH"
    if code.startswith(("0", "3")):
        return f"{code}.SZ"
    if code.startswith(("8", "4")):
        return f"{code}.BJ"
    return f"{code}.SZ"


def fetch_market_data_tushare(
    stock_codes: List[str],
    start_date: str,
    end_date: str,
    max_calls: int | None = None,
    rate_limit_enable: bool = True,
    run_label: str = "mvp",
) -> Tuple[pd.DataFrame, bool, int]:
    """使用 Tushare 拉取历史行情（MVP/回填）"""
    # #region agent log
    _dbg_log("H14", "data_fetcher.py:fetch_market_data_tushare", "enter", {
        "total": len(stock_codes),
        "start_date": start_date,
        "end_date": end_date,
        "token_set": bool(getattr(config, "TUSHARE_TOKEN", None)),
        "run_label": run_label
    })
    # #endregion agent log

    token = getattr(config, "TUSHARE_TOKEN", None)
    if not token:
        _dbg_log("H14", "data_fetcher.py:fetch_market_data_tushare", "token_missing", {})
        return pd.DataFrame(), False, 0

    try:
        import tushare as ts
        pro = ts.pro_api(token)
    except Exception as e:
        _dbg_log("H14", "data_fetcher.py:fetch_market_data_tushare", "init_exception", {"error": str(e)})
        return pd.DataFrame(), False, 0

    max_calls = int(max_calls or getattr(config, "TUSHARE_MAX_CALLS_PER_MIN", 50))
    call_count = 0
    rate_limited = False
    all_data = []

    for idx, code in enumerate(stock_codes, 1):
        # --- RATE LIMIT BEGIN (EASY REMOVE) ---
        if rate_limit_enable and call_count >= max_calls:
            # #region agent log
            _dbg_log("H15", "data_fetcher.py:fetch_market_data_tushare", "rate_limit_hit", {
                "call_count": call_count,
                "max_calls": max_calls,
                "run_label": run_label,
                "idx": int(idx),
                "next_code": code
            })
            # #endregion agent log
            rate_limited = True
            break
        # --- RATE LIMIT END ---
        if getattr(config, "PRINT_EACH_STOCK", True):
            print(f"Tushare获取中: {idx}/{len(stock_codes)} {code}")
        ts_code = _to_ts_code(code)
        try:
            df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            call_count += 1
            if df is None or df.empty:
                _dbg_log("H14", "data_fetcher.py:fetch_market_data_tushare", "empty_df", {"code": code, "ts_code": ts_code})
                continue
            df = df.rename(columns={
                'trade_date': 'date',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'vol': 'volume',
                'amount': 'amount',
                'pre_close': 'pre_close',
            })
            df['code'] = code
            df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'pre_close', 'code']]
            all_data.append(df)
        except Exception as e:
            _dbg_log("H14", "data_fetcher.py:fetch_market_data_tushare", "fetch_exception", {
                "code": code,
                "ts_code": ts_code,
                "error": str(e),
                "error_type": type(e).__name__
            })
            continue

    if not all_data:
        _dbg_log("H14", "data_fetcher.py:fetch_market_data_tushare", "empty_all_data", {"call_count": call_count})
        return pd.DataFrame(), rate_limited, call_count

    result = pd.concat(all_data, ignore_index=True)
    # #region agent log
    try:
        if "date" in result.columns:
            date_series = result["date"].astype(str)
            rows_for_end_date = int((date_series == str(end_date)).sum())
            date_min = date_series.min()
            date_max = date_series.max()
        else:
            rows_for_end_date = 0
            date_min = None
            date_max = None
        unique_codes = int(result["code"].nunique()) if "code" in result.columns else 0
    except Exception:
        rows_for_end_date = 0
        date_min = None
        date_max = None
        unique_codes = 0
    _dbg_log("H14", "data_fetcher.py:fetch_market_data_tushare", "success", {
        "rows": int(len(result)),
        "call_count": int(call_count),
        "unique_codes": int(unique_codes),
        "date_min": date_min,
        "date_max": date_max,
        "rows_for_end_date": int(rows_for_end_date)
    })
    # #endregion agent log
    return result, rate_limited, call_count


def fetch_market_data_tushare_by_date(
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """按交易日批量拉取日线数据（降低接口调用次数）"""
    # #region agent log
    _dbg_log("H27", "data_fetcher.py:fetch_market_data_tushare_by_date", "enter", {
        "start_date": start_date,
        "end_date": end_date,
        "token_set": bool(getattr(config, "TUSHARE_TOKEN", None)),
    })
    # #endregion agent log

    token = getattr(config, "TUSHARE_TOKEN", None)
    if not token:
        _dbg_log("H27", "data_fetcher.py:fetch_market_data_tushare_by_date", "token_missing", {})
        return pd.DataFrame()

    try:
        import tushare as ts
        pro = ts.pro_api(token)
    except Exception as e:
        _dbg_log("H27", "data_fetcher.py:fetch_market_data_tushare_by_date", "init_exception", {"error": str(e)})
        return pd.DataFrame()

    try:
        cal = pro.trade_cal(start_date=start_date, end_date=end_date)
        if cal is None or cal.empty:
            _dbg_log("H27", "data_fetcher.py:fetch_market_data_tushare_by_date", "trade_cal_empty", {})
            return pd.DataFrame()
        trade_dates = cal[cal["is_open"] == 1]["cal_date"].astype(str).tolist()
        if not trade_dates:
            _dbg_log("H27", "data_fetcher.py:fetch_market_data_tushare_by_date", "no_trade_dates", {})
            return pd.DataFrame()
        # #region agent log
        _dbg_log("H27", "data_fetcher.py:fetch_market_data_tushare_by_date", "trade_dates", {
            "count": int(len(trade_dates)),
            "first": trade_dates[0],
            "last": trade_dates[-1],
        })
        # #endregion agent log
    except Exception as e:
        _dbg_log("H27", "data_fetcher.py:fetch_market_data_tushare_by_date", "trade_cal_exception", {
            "error": str(e),
            "error_type": type(e).__name__
        })
        return pd.DataFrame()

    all_data = []
    for trade_date in trade_dates:
        try:
            df = pro.daily(trade_date=trade_date)
            if df is None or df.empty:
                continue
            df = df.rename(columns={
                'trade_date': 'date',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'vol': 'volume',
                'amount': 'amount',
                'pre_close': 'pre_close',
            })
            df['code'] = df['ts_code'].astype(str).str.split('.').str[0]
            df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'pre_close', 'code']]
            all_data.append(df)
        except Exception as e:
            _dbg_log("H27", "data_fetcher.py:fetch_market_data_tushare_by_date", "daily_exception", {
                "trade_date": trade_date,
                "error": str(e),
                "error_type": type(e).__name__
            })
            continue

    if not all_data:
        _dbg_log("H27", "data_fetcher.py:fetch_market_data_tushare_by_date", "empty_all_data", {})
        return pd.DataFrame()

    result = pd.concat(all_data, ignore_index=True)
    # #region agent log
    try:
        unique_codes = int(result["code"].nunique()) if "code" in result.columns else 0
        date_min = result["date"].astype(str).min() if "date" in result.columns else None
        date_max = result["date"].astype(str).max() if "date" in result.columns else None
    except Exception:
        unique_codes = 0
        date_min = None
        date_max = None
    _dbg_log("H27", "data_fetcher.py:fetch_market_data_tushare_by_date", "success", {
        "rows": int(len(result)),
        "unique_codes": int(unique_codes),
        "date_min": date_min,
        "date_max": date_max
    })
    # #endregion agent log
    return result


def get_recent_trading_days(days: int = 90) -> tuple:
    """获取最近N个交易日的日期范围"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    
    return start_str, end_str


if __name__ == '__main__':
    # 测试获取股票列表
    stocks = get_stock_list()
    print(stocks.head())
    
    # 测试获取单只股票数据
    if not stocks.empty:
        test_code = stocks.iloc[0]['code']
        start, end = get_recent_trading_days(30)
        data = get_stock_history(test_code, start, end)
        print(f"\n{test_code} 最近30天数据:")
        print(data.head())
