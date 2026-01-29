"""
全量历史回填管理器 - 支持断点续传
"""
import pandas as pd
from datetime import datetime, timedelta
import time
import uuid
import database
import data_fetcher
import limit_calculator
import config


class BackfillManager:
    """历史数据回填管理器"""
    
    def __init__(self, start_date: str, end_date: str, batch_size: int = None):
        """
        初始化回填管理器
        
        参数:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            batch_size: 每批次处理的股票数量
        """
        self.start_date = start_date
        self.end_date = end_date
        self.batch_size = batch_size or config.FETCH_BATCH_SIZE
        self.task_id = f"backfill_{start_date}_{end_date}_{uuid.uuid4().hex[:8]}"
    
    def run(self):
        """执行回填任务"""
        print("="*60)
        print(f"全量历史回填任务")
        print(f"任务ID: {self.task_id}")
        print(f"日期范围: {self.start_date} ~ {self.end_date}")
        print("="*60 + "\n")
        
        # 初始化数据库
        database.init_database()
        
        # 创建任务记录
        self._create_task_record()
        
        # 获取股票列表
        print("[1/3] 获取股票列表...")
        stocks = data_fetcher.get_stock_list()
        
        if stocks.empty:
            print("✗ 获取股票列表失败")
            self._update_task_status('failed')
            return
        
        database.save_stock_meta(stocks)
        print(f"✓ 股票列表已保存，共 {len(stocks)} 只\n")
        
        # 分批回填
        print(f"[2/3] 开始分批回填（每批 {self.batch_size} 只股票）...")
        stock_codes = stocks['code'].tolist()
        total_batches = (len(stock_codes) + self.batch_size - 1) // self.batch_size
        
        self._update_task_status('running')
        
        success_count = 0
        fail_count = 0
        
        for batch_idx in range(total_batches):
            try:
                start_idx = batch_idx * self.batch_size
                end_idx = min((batch_idx + 1) * self.batch_size, len(stock_codes))
                batch_codes = stock_codes[start_idx:end_idx]
                
                print(f"\n批次 {batch_idx + 1}/{total_batches}:")
                print(f"  处理股票 {start_idx + 1} - {end_idx} / {len(stock_codes)}")
                
                # 回填批次数据
                success, rate_limited = self._process_batch(batch_codes, stocks)
                
                if success:
                    success_count += 1
                else:
                    fail_count += 1
                
                # --- RATE LIMIT BEGIN (EASY REMOVE) ---
                if rate_limited:
                    print("⚠️  触发全量回填限速，已停止后续批次")
                    self._update_task_status('rate_limited')
                    break
                # --- RATE LIMIT END ---

                # 更新进度
                progress_pct = (batch_idx + 1) / total_batches * 100
                print(f"  总体进度: {progress_pct:.1f}% ({batch_idx + 1}/{total_batches})")
                
            except KeyboardInterrupt:
                print("\n⚠️  用户中断，保存当前进度...")
                self._update_task_status('interrupted')
                break
            except Exception as e:
                print(f"✗ 批次 {batch_idx + 1} 处理失败: {e}")
                fail_count += 1
        
        # 完成任务
        print(f"\n[3/3] 回填任务完成")
        print(f"成功批次: {success_count}")
        print(f"失败批次: {fail_count}")
        
        self._update_task_status('completed')
        
        print("\n" + "="*60)
        print("✓ 全量回填完成")
        print("="*60)
    
    def _process_batch(self, batch_codes: list, stocks: pd.DataFrame) -> tuple[bool, bool]:
        """处理单个批次"""
        try:
            # 获取批次数据
            print(f"  [1/3] 获取历史数据...")
            rate_limited = False
            if getattr(config, "TUSHARE_USE_IN_BACKFILL", False):
                # --- RATE LIMIT BEGIN (EASY REMOVE) ---
                batch_data, rate_limited, call_count = data_fetcher.fetch_market_data_tushare(
                    batch_codes,
                    self.start_date,
                    self.end_date,
                    max_calls=getattr(config, "BACKFILL_RATE_LIMIT_CALLS_PER_MIN", 45),
                    rate_limit_enable=getattr(config, "BACKFILL_RATE_LIMIT_ENABLE", True),
                    run_label="backfill"
                )
                if rate_limited:
                    print(f"  ⚠️  回填触发限速（已调用 {call_count} 次）")
                # --- RATE LIMIT END ---
            else:
                print("  ⚠️  未启用 Tushare 回填，跳过批次")
                return False, rate_limited
            
            if batch_data.empty:
                print(f"  ⚠️  批次数据为空")
                return False, rate_limited
            
            # 保存市场数据
            print(f"  [2/3] 保存市场数据 ({len(batch_data)} 条)...")
            try:
                database.save_daily_data(batch_data)
            except Exception as e:
                # 可能是重复数据，继续执行
                print(f"  ⚠️  保存数据警告: {e}")
            
            # 计算连板
            print(f"  [3/3] 计算连板高度...")
            batch_stocks = stocks[stocks['code'].isin(batch_codes)]
            batch_results = limit_calculator.calculate_batch_chain(batch_data, batch_stocks)
            
            if not batch_results.empty:
                try:
                    database.save_limit_results(batch_results)
                    print(f"  ✓ 批次完成，计算 {len(batch_results)} 条连板记录")
                except Exception as e:
                    print(f"  ⚠️  保存结果警告: {e}")
            
            return True, rate_limited
            
        except Exception as e:
            print(f"  ✗ 批次处理异常: {e}")
            return False, False
    
    def _create_task_record(self):
        """创建任务记录"""
        conn = database.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
        INSERT INTO fetch_progress (task_id, start_date, end_date, status)
        VALUES (?, ?, ?, 'pending')
        """, (self.task_id, self.start_date, self.end_date))
        
        conn.commit()
        conn.close()
    
    def _update_task_status(self, status: str):
        """更新任务状态"""
        database.update_fetch_progress(self.task_id, status)


def resume_interrupted_task(task_id: str):
    """恢复中断的任务"""
    print(f"尝试恢复任务: {task_id}")
    
    conn = database.get_connection()
    query = f"SELECT * FROM fetch_progress WHERE task_id = '{task_id}'"
    task = pd.read_sql_query(query, conn)
    conn.close()
    
    if task.empty:
        print("✗ 任务不存在")
        return
    
    task_info = task.iloc[0]
    print(f"任务信息: {task_info['start_date']} ~ {task_info['end_date']}")
    print(f"当前状态: {task_info['status']}")
    
    if task_info['status'] == 'completed':
        print("⚠️  任务已完成，无需恢复")
        return
    
    # 重新执行任务
    manager = BackfillManager(task_info['start_date'], task_info['end_date'])
    manager.task_id = task_id
    manager.run()


if __name__ == '__main__':
    # 示例：回填2020年至今的全量数据
    start = config.HISTORY_START_DATE
    end = datetime.now().strftime('%Y%m%d')
    
    manager = BackfillManager(start, end, batch_size=50)
    manager.run()
