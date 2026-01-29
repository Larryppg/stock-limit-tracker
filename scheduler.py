"""
定时任务调度器 - 每日自动更新
"""
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta
import batch_processor
import config
import logging


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class DailyUpdateScheduler:
    """每日更新调度器"""
    
    def __init__(self):
        self.scheduler = BlockingScheduler()
        self.setup_jobs()
    
    def setup_jobs(self):
        """设置定时任务"""
        # 解析更新时间
        update_time = config.DAILY_UPDATE_TIME
        hour, minute = map(int, update_time.split(':'))
        
        # 添加每日更新任务（每个交易日收盘后执行）
        self.scheduler.add_job(
            func=self.daily_update_job,
            trigger=CronTrigger(
                day_of_week='mon-fri',  # 周一到周五
                hour=hour,
                minute=minute
            ),
            id='daily_update',
            name='每日连板数据更新',
            replace_existing=True
        )
        
        logger.info(f"✓ 定时任务已配置: 每个交易日 {update_time} 执行")
    
    def daily_update_job(self):
        """每日更新任务"""
        logger.info("="*60)
        logger.info("开始执行每日更新任务")
        logger.info("="*60)
        
        try:
            # 获取今天日期
            today = datetime.now().strftime('%Y%m%d')
            
            # 执行更新
            batch_processor.run_daily_update(target_date=today)
            
            logger.info("✓ 每日更新任务完成")
            
        except Exception as e:
            logger.error(f"✗ 每日更新任务失败: {e}", exc_info=True)
    
    def start(self):
        """启动调度器"""
        logger.info("\n" + "="*60)
        logger.info("A股连板追踪系统 - 定时调度器启动")
        logger.info("="*60)
        logger.info("\n已注册的任务:")
        
        for job in self.scheduler.get_jobs():
            logger.info(f"  - {job.name}: {job.next_run_time}")
        
        logger.info("\n调度器运行中，按 Ctrl+C 停止...")
        
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("\n调度器已停止")


def run_manual_update(date: str = None):
    """手动触发更新"""
    if date is None:
        date = datetime.now().strftime('%Y%m%d')
    
    logger.info(f"手动触发更新: {date}")
    batch_processor.run_daily_update(target_date=date)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='连板追踪系统定时调度器')
    parser.add_argument(
        '--mode',
        type=str,
        choices=['start', 'manual'],
        default='start',
        help='运行模式: start(启动调度器) / manual(手动执行一次)'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='手动模式的目标日期 YYYYMMDD'
    )
    
    args = parser.parse_args()
    
    if args.mode == 'start':
        # 启动定时调度器
        scheduler = DailyUpdateScheduler()
        scheduler.start()
    else:
        # 手动执行一次
        run_manual_update(date=args.date)
