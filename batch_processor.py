"""
批量处理模块 - MVP版本（近3个月数据）
"""
import pandas as pd
import sqlite3
from datetime import datetime
import database
import data_fetcher
import limit_calculator
import config


def run_mvp_pipeline(recent_days: int = None):
    """
    运行MVP流程：获取最近N天数据并计算连板高度
    
    参数:
        recent_days: 最近天数，默认使用config中的配置
    """
    if recent_days is None:
        recent_days = config.DEFAULT_RECENT_DAYS
    
    print("="*60)
    print(f"开始运行MVP流程 - 最近 {recent_days} 天数据")
    print("="*60)
    # #region agent log
    try:
        from data_fetcher import _dbg_log as _dbg_log_local
        import importlib
        import database as _db_mod
        _db_mod = importlib.reload(_db_mod)
        _dbg_log_local("H4", "batch_processor.py:run_mvp_pipeline", "enter", {
            "recent_days": int(recent_days),
            "database_file": getattr(_db_mod, "__file__", ""),
            "db_module_version": getattr(_db_mod, "DB_MODULE_VERSION", "missing"),
            "db_has_version": hasattr(_db_mod, "DB_MODULE_VERSION")
        })
    except Exception:
        pass
    # #endregion agent log
    
    # 步骤1: 初始化数据库
    print("\n[1/5] 初始化数据库...")
    database.init_database()
    
    # 步骤2: 获取股票列表
    print("\n[2/5] 获取股票列表...")
    stocks = data_fetcher.get_stock_list()
    
    if stocks.empty:
        print("✗ 获取股票列表失败，流程终止")
        return
    
    # 保存股票元信息
    database.save_stock_meta(stocks)
    print(f"✓ 股票元信息已保存，共 {len(stocks)} 只")
    
    # 步骤3: 获取市场数据
    print(f"\n[3/5] 获取最近 {recent_days} 天市场数据...")
    start_date, end_date = data_fetcher.get_recent_trading_days(recent_days)
    print(f"日期范围: {start_date} ~ {end_date}")
    # #region agent log
    try:
        _dbg_log_local("H4", "batch_processor.py:run_mvp_pipeline", "date_range", {"start_date": start_date, "end_date": end_date})
    except Exception:
        pass
    # #endregion agent log
    
    # MVP 模式可选限制股票数量（默认不限制）
    mvp_limit = getattr(config, "MVP_LIMIT_STOCKS", 0)
    if mvp_limit and int(mvp_limit) > 0:
        print(f"\n⚠️  MVP模式：只处理前 {int(mvp_limit)} 只股票用于快速验证")
        stock_codes = stocks['code'].head(int(mvp_limit)).tolist()
    else:
        stock_codes = stocks['code'].tolist()
    
    # #region agent log
    try:
        _dbg_log_local("H9", "batch_processor.py:run_mvp_pipeline", "before_fetch_market", {
            "codes_count": len(stock_codes),
            "mvp_limit": int(mvp_limit) if mvp_limit else 0
        })
    except Exception:
        pass
    # #endregion agent log

    try:
        # 优先使用 Tushare（MVP），不再回退 AkShare
        if getattr(config, "TUSHARE_USE_IN_MVP", False):
            max_calls = int(getattr(config, "TUSHARE_MAX_CALLS_PER_MIN", 50))
            # #region agent log
            try:
                _dbg_log_local("H18", "batch_processor.py:run_mvp_pipeline", "mvp_use_tushare_only", {
                    "max_calls": int(max_calls)
                })
            except Exception:
                pass
            # #endregion agent log

            market_data = pd.DataFrame()
            # 是否使用按日期批量拉取模式（需要较高Tushare积分）
            use_by_date_mode = getattr(config, "TUSHARE_USE_BY_DATE_MODE", False)
            if not mvp_limit and use_by_date_mode:
                # === 按日期批量拉取模式（需要较高积分，免费用户数据不全） ===
                print("\n⚙️  使用按日期批量拉取模式...")
                market_data = data_fetcher.fetch_market_data_tushare_by_date(
                    start_date,
                    end_date
                )
                if market_data.empty:
                    # #region agent log
                    try:
                        _dbg_log_local("H29", "batch_processor.py:run_mvp_pipeline", "by_date_empty_fallback", {
                            "start_date": start_date,
                            "end_date": end_date
                        })
                    except Exception:
                        pass
                    # #endregion agent log
                else:
                    # 过滤到当前股票列表（避免意外品种）
                    if "code" in market_data.columns:
                        market_data = market_data[market_data["code"].isin(set(stock_codes))]
                    # #region agent log
                    try:
                        _dbg_log_local("H28", "batch_processor.py:run_mvp_pipeline", "market_data_filtered", {
                            "rows": int(len(market_data)),
                            "unique_codes": int(market_data["code"].nunique()) if "code" in market_data.columns else 0
                        })
                    except Exception:
                        pass
                    # #endregion agent log

                    # #region agent log
                    try:
                        _dbg_log_local("H9", "batch_processor.py:run_mvp_pipeline", "after_fetch_market", {"rows": int(len(market_data))})
                    except Exception:
                        pass
                    # #endregion agent log

                # 保存市场数据（直接写入，避免旧模块缓存）
                print("\n保存市场数据到数据库...")
                try:
                    chunk_size = int(getattr(config, "DAILY_SAVE_CHUNK_SIZE", 50000))
                    # #region agent log
                    try:
                        _dbg_log_local("H35", "batch_processor.py:run_mvp_pipeline", "save_daily_direct_start", {
                            "rows": int(len(market_data)),
                            "chunk_size": int(chunk_size)
                        })
                    except Exception:
                        pass
                    # #endregion agent log
                    conn = sqlite3.connect(config.DB_PATH, timeout=30)
                    conn.execute("PRAGMA busy_timeout=30000;")
                    conn.execute("PRAGMA journal_mode=WAL;")
                    market_data.to_sql(
                        'daily_market_data',
                        conn,
                        if_exists='append',
                        index=False,
                        chunksize=chunk_size,
                        method='multi'
                    )
                    conn.commit()
                    conn.close()
                    # #region agent log
                    try:
                        _dbg_log_local("H35", "batch_processor.py:run_mvp_pipeline", "save_daily_direct_done", {
                            "rows": int(len(market_data))
                        })
                    except Exception:
                        pass
                    # #endregion agent log
                    print(f"✓ 市场数据已保存，共 {len(market_data)} 条记录")
                except Exception as e:
                    print(f"⚠️  保存数据时出现警告: {e}")
                    print("继续执行后续步骤...")

                    # 步骤4: 计算连板高度
                    print("\n[4/5] 计算连板高度...")
                    # 只使用已获取数据对应的股票元信息
                    stocks_subset = stocks[stocks['code'].isin(stock_codes)]
                    limit_results = limit_calculator.calculate_batch_chain(market_data, stocks_subset)
                    batch_mode = False

            # 判断是否使用批量逐股拉取模式
            # 条件：1) 数据为空（按日期模式失败或未启用） 2) 未限制股票数 3) 股票数超过单次调用上限
            use_batch = market_data.empty and (not mvp_limit) and (len(stock_codes) > max_calls)
            if not use_by_date_mode and not mvp_limit:
                # 未使用按日期模式时，强制使用逐股批量拉取
                use_batch = True
                print("\n⚙️  使用逐股批量拉取模式（积分要求低，但调用次数多）...")
            if use_batch:
                batch_size = int(getattr(config, "MVP_BATCH_SIZE", config.FETCH_BATCH_SIZE))
                total_batches = (len(stock_codes) + batch_size - 1) // batch_size
                print(f"\n⚙️  MVP批量模式：{total_batches} 批，每批 {batch_size} 只股票")
                limit_results_list = []

                for batch_idx in range(total_batches):
                    start_idx = batch_idx * batch_size
                    end_idx = min((batch_idx + 1) * batch_size, len(stock_codes))
                    batch_codes = stock_codes[start_idx:end_idx]
                    # #region agent log
                    try:
                        _dbg_log_local("H26", "batch_processor.py:run_mvp_pipeline", "batch_start", {
                            "batch_idx": int(batch_idx + 1),
                            "codes_count": int(len(batch_codes)),
                            "batch_size": int(batch_size),
                        })
                    except Exception:
                        pass
                    # #endregion agent log

                    batch_data, rate_limited, call_count = data_fetcher.fetch_market_data_tushare(
                        batch_codes,
                        start_date,
                        end_date,
                        max_calls=max_calls,
                        rate_limit_enable=True,
                        run_label="mvp_batch"
                    )
                    if rate_limited:
                        print(f"⚠️  MVP 批次 {batch_idx + 1} 触发 Tushare 限速（已调用 {call_count} 次），停止后续批次")
                        break
                    if batch_data.empty:
                        print(f"⚠️  MVP 批次 {batch_idx + 1} 返回空数据，跳过")
                        continue

                    # 保存市场数据
                    print("\n保存市场数据到数据库...")
                    try:
                        database.save_daily_data(batch_data)
                        print(f"✓ 批次 {batch_idx + 1} 市场数据已保存，共 {len(batch_data)} 条记录")
                    except Exception as e:
                        print(f"⚠️  批次 {batch_idx + 1} 保存数据警告: {e}")

                    # 计算连板高度
                    print("\n[4/5] 计算连板高度...")
                    batch_stocks = stocks[stocks['code'].isin(batch_codes)]
                    batch_results = limit_calculator.calculate_batch_chain(batch_data, batch_stocks)
                    if not batch_results.empty:
                        try:
                            database.save_limit_results(batch_results)
                            print(f"✓ 批次 {batch_idx + 1} 连板分析结果已保存，共 {len(batch_results)} 条记录")
                        except Exception as e:
                            print(f"⚠️  批次 {batch_idx + 1} 保存结果警告: {e}")
                        limit_results_list.append(batch_results)

                if not limit_results_list:
                    print("✗ MVP批量模式未获取到任何有效连板结果，流程终止")
                    return
                limit_results = pd.concat(limit_results_list, ignore_index=True)
                batch_mode = True
            else:
                if market_data.empty:
                    market_data, rate_limited, call_count = data_fetcher.fetch_market_data_tushare(
                        stock_codes,
                        start_date,
                        end_date,
                        max_calls=max_calls,
                        rate_limit_enable=True,
                        run_label="mvp"
                    )
                    if rate_limited:
                        print(f"⚠️  MVP 触发 Tushare 限速（已调用 {call_count} 次），可重新运行继续获取")
                    if market_data.empty:
                        print("✗ Tushare 返回空数据，MVP终止（按交易日拉取失败，且逐股拉取为空）")
                        return

                    # #region agent log
                    try:
                        _dbg_log_local("H9", "batch_processor.py:run_mvp_pipeline", "after_fetch_market", {"rows": int(len(market_data))})
                    except Exception:
                        pass
                    # #endregion agent log

                    # 保存市场数据
                    print("\n保存市场数据到数据库...")
                    try:
                        database.save_daily_data(market_data)
                        print(f"✓ 市场数据已保存，共 {len(market_data)} 条记录")
                    except Exception as e:
                        print(f"⚠️  保存数据时出现警告: {e}")
                        print("继续执行后续步骤...")

                    # 步骤4: 计算连板高度
                    print("\n[4/5] 计算连板高度...")
                    # 只使用已获取数据对应的股票元信息
                    stocks_subset = stocks[stocks['code'].isin(stock_codes)]
                    limit_results = limit_calculator.calculate_batch_chain(market_data, stocks_subset)
                    batch_mode = False
        else:
            print("✗ 未启用 Tushare（MVP），且已禁用AkShare回退")
            return
    except Exception as e:
        # #region agent log
        try:
            _dbg_log_local("H9", "batch_processor.py:run_mvp_pipeline", "fetch_market_exception", {"error": str(e), "error_type": type(e).__name__})
        except Exception:
            pass
        # #endregion agent log
        raise
    
    if limit_results.empty:
        print("✗ 连板计算失败，流程终止")
        return
    
    # 保存连板分析结果（非批量模式）
    if not batch_mode:
        print("\n保存连板分析结果到数据库...")
        try:
            database.save_limit_results(limit_results)
            print(f"✓ 连板分析结果已保存，共 {len(limit_results)} 条记录")
        except Exception as e:
            print(f"⚠️  保存结果时出现警告: {e}")
    
    # 步骤5: 生成摘要统计
    print("\n[5/5] 生成数据摘要...")
    generate_summary(limit_results, end_date)
    
    print("\n" + "="*60)
    print("✓ MVP流程完成！")
    print("="*60)


def run_full_backfill(start_date: str = None, end_date: str = None):
    """
    全量历史数据回填
    
    参数:
        start_date: 开始日期 YYYYMMDD，默认使用config配置
        end_date: 结束日期 YYYYMMDD，默认为今天
    """
    if start_date is None:
        start_date = config.HISTORY_START_DATE
    
    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')
    
    print("="*60)
    print(f"开始全量历史回填: {start_date} ~ {end_date}")
    print("="*60)
    
    # 初始化数据库
    print("\n[1/4] 初始化数据库...")
    database.init_database()
    
    # 获取股票列表
    print("\n[2/4] 获取股票列表...")
    stocks = data_fetcher.get_stock_list()
    
    if stocks.empty:
        print("✗ 获取股票列表失败")
        return
    
    database.save_stock_meta(stocks)
    print(f"✓ 股票元信息已保存，共 {len(stocks)} 只")
    
    # 批量获取历史数据
    print(f"\n[3/4] 批量获取历史数据...")
    print(f"⚠️  全量回填可能需要较长时间（预计2-4小时）")
    
    stock_codes = stocks['code'].tolist()
    
    # 分批处理，避免内存溢出
    batch_size = config.FETCH_BATCH_SIZE
    total_batches = (len(stock_codes) + batch_size - 1) // batch_size
    
    # #region agent log
    try:
        from data_fetcher import _dbg_log as _dbg_log_local
        _dbg_log_local("H19", "batch_processor.py:run_full_backfill", "enter", {
            "start_date": start_date,
            "end_date": end_date,
            "batch_size": int(batch_size),
            "total_batches": int(total_batches)
        })
    except Exception:
        pass
    # #endregion agent log

    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(stock_codes))
        batch_codes = stock_codes[start_idx:end_idx]
        
        print(f"\n处理批次 {batch_idx + 1}/{total_batches}...")
        
        # 获取批次数据（仅使用 Tushare）
        if not getattr(config, "TUSHARE_USE_IN_BACKFILL", False):
            print("✗ 未启用 Tushare 回填，且已禁用其他数据源")
            return
        # #region agent log
        try:
            _dbg_log_local("H19", "batch_processor.py:run_full_backfill", "backfill_use_tushare_only", {
                "batch_idx": int(batch_idx + 1),
                "codes_count": int(len(batch_codes)),
                "max_calls": int(getattr(config, "BACKFILL_RATE_LIMIT_CALLS_PER_MIN", 45))
            })
        except Exception:
            pass
        # #endregion agent log
        batch_data, rate_limited, call_count = data_fetcher.fetch_market_data_tushare(
            batch_codes,
            start_date,
            end_date,
            max_calls=getattr(config, "BACKFILL_RATE_LIMIT_CALLS_PER_MIN", 45),
            rate_limit_enable=getattr(config, "BACKFILL_RATE_LIMIT_ENABLE", True),
            run_label="backfill"
        )
        if rate_limited:
            print(f"⚠️  回填触发 Tushare 限速（已调用 {call_count} 次），停止后续批次")
            break
        
        if not batch_data.empty:
            # 保存市场数据
            try:
                database.save_daily_data(batch_data)
            except Exception as e:
                print(f"⚠️  批次 {batch_idx + 1} 保存数据警告: {e}")
            
            # 计算连板
            batch_stocks = stocks[stocks['code'].isin(batch_codes)]
            batch_results = limit_calculator.calculate_batch_chain(batch_data, batch_stocks)
            
            if not batch_results.empty:
                try:
                    database.save_limit_results(batch_results)
                except Exception as e:
                    print(f"⚠️  批次 {batch_idx + 1} 保存结果警告: {e}")
    
    print("\n[4/4] 回填完成！")
    print("="*60)


def run_daily_update(target_date: str = None):
    """
    每日增量更新
    
    参数:
        target_date: 目标日期 YYYYMMDD，默认为今天
    """
    if target_date is None:
        target_date = datetime.now().strftime('%Y%m%d')
    
    print("="*60)
    print(f"开始每日增量更新: {target_date}")
    print("="*60)
    
    # 获取股票列表
    print("\n[1/3] 获取股票列表...")
    stocks = data_fetcher.get_stock_list()
    
    if stocks.empty:
        print("✗ 获取股票列表失败")
        return
    
    # 更新股票元信息
    database.save_stock_meta(stocks)
    
    # 获取当日数据
    print(f"\n[2/3] 获取 {target_date} 数据...")
    stock_codes = stocks['code'].tolist()
    
    daily_data = data_fetcher.fetch_market_data(stock_codes, target_date, target_date)
    
    if daily_data.empty:
        print(f"✗ {target_date} 无交易数据（可能为休市日）")
        return
    
    # 保存当日数据
    try:
        database.save_daily_data(daily_data)
        print(f"✓ {target_date} 市场数据已保存")
    except Exception as e:
        print(f"⚠️  保存数据警告: {e}")
    
    # 计算连板（需要结合历史数据）
    print(f"\n[3/3] 计算 {target_date} 连板高度...")
    
    # 为了计算连板，需要获取每只股票的前一交易日状态
    # 简化版：重新计算最近30天的连板状态
    lookback_days = 30
    start_date, _ = data_fetcher.get_recent_trading_days(lookback_days)
    
    all_results = []
    for code in stock_codes[:50]:  # MVP: 只处理前50只
        historical_data = database.get_stock_daily_data(code, start_date, target_date)
        
        if not historical_data.empty:
            stock_info = stocks[stocks['code'] == code].iloc[0]
            limit_ratio = stock_info['limit_ratio']
            
            result = limit_calculator.calculate_single_stock_chain(
                historical_data, code, limit_ratio
            )
            
            # 只保存目标日期的结果
            result = result[result['date'] == target_date]
            if not result.empty:
                all_results.append(result)
    
    if all_results:
        final_results = pd.concat(all_results, ignore_index=True)
        try:
            database.save_limit_results(final_results)
            print(f"✓ {target_date} 连板分析结果已保存")
        except Exception as e:
            print(f"⚠️  保存结果警告: {e}")
    
    print("\n" + "="*60)
    print("✓ 每日更新完成！")
    print("="*60)


def generate_summary(limit_results: pd.DataFrame, date: str = None):
    """生成数据摘要统计"""
    if date is None:
        # 使用最新日期
        date = limit_results['date'].max()
    
    print(f"\n📊 数据摘要 ({date}):")
    print("-" * 40)
    
    # 当日涨停统计
    daily_data = limit_results[limit_results['date'] == date]
    
    if not daily_data.empty:
        limit_count = daily_data['limit_status'].sum()
        fried_count = daily_data['is_fried'].sum()
        yizi_count = len(daily_data[daily_data['board_type'] == 'yizi'])
        
        print(f"涨停数量: {limit_count}")
        print(f"炸板数量: {fried_count}")
        print(f"一字板数量: {yizi_count}")
        
        # 连板高度分布
        print("\n连板高度分布:")
        for height in range(1, 11):
            count = len(daily_data[daily_data['chain_height'] == height])
            if count > 0:
                print(f"  {height}板: {count}只")
        
        # 高连板股票
        high_chain = daily_data[daily_data['chain_height'] >= 3].sort_values(
            'chain_height', ascending=False
        )
        
        if not high_chain.empty:
            print(f"\n3板及以上股票 (共{len(high_chain)}只):")
            for _, row in high_chain.head(10).iterrows():
                print(f"  {row['code']}: {int(row['chain_height'])}板 ({row['board_type']})")
    
    print("-" * 40)


if __name__ == '__main__':
    # 运行MVP流程
    run_mvp_pipeline(recent_days=90)
