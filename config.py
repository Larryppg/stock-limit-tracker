"""
配置文件 - A股连板高度追踪系统
"""
import os
from datetime import datetime, timedelta

# 数据库配置
DB_PATH = os.path.join(os.path.dirname(__file__), 'data', 'stock_limit.db')

# 数据源配置
DATA_SOURCE = 'akshare'  # 主力数据源

# 日期范围配置
HISTORY_START_DATE = '20251009' # 历史数据起始日期
DEFAULT_RECENT_DAYS = 90  # MVP阶段：最近3个月

# MVP 限制处理股票数量（<=0 或 None 表示不限制）
MVP_LIMIT_STOCKS = 0

# 数据清理配置（可选）
# 初始化数据库时清理早于该日期的数据；设为 None 则不清理
DATA_PRUNE_BEFORE_DATE = '20251009'

# 涨跌幅限制配置（按板块）
LIMIT_RATIO = {
    'MAIN': 0.10,    # 主板±10%
    'GEM': 0.20,     # 创业板±20%
    'STAR': 0.20,    # 科创板±20%
    'BJ': 0.30,      # 北交所±30%
    'ST': 0.05,      # ST股票±5%
}

# 涨停判定误差容忍度
LIMIT_TOLERANCE = 0.001  # 0.1%误差

# 数据获取配置
FETCH_BATCH_SIZE = 100  # 每批次获取股票数量
FETCH_SLEEP_INTERVAL = 0.5  # 请求间隔（秒），避免限流

# 调度配置
DAILY_UPDATE_TIME = '16:00'  # 每日更新时间（收盘后）

# 输出控制
VERBOSE_OUTPUT = True
PROGRESS_EVERY = 1  # 每隔多少只股票打印进度（数值越小输出越频繁）
PRINT_EACH_STOCK = True  # 是否每只股票都输出开始提示

# AkShare 网络配置（可选）
# 例如: AK_PROXY = "http://127.0.0.1:7890"
AK_PROXY = None
# 例如: AK_HTTP_TIMEOUT = 15
AK_HTTP_TIMEOUT = None

# 历史数据获取重试次数
HISTORY_MAX_ATTEMPTS = 2

# Tushare 备用数据源（可选）
# 需要你自行在 https://tushare.pro/ 注册并获取 Token
TUSHARE_TOKEN = "b7c40ce1fb64e1e46d404414606a4cba5e0b6234c15f84ffe5a0e465"
TUSHARE_MAX_CALLS_PER_MIN = 195
TUSHARE_USE_IN_MVP = True
TUSHARE_USE_IN_BACKFILL = True

# Tushare 数据拉取模式
# True: 按日期批量拉取（pro.daily(trade_date=...)），需要较高积分
# False: 逐股拉取（pro.daily(ts_code=...)），积分要求低但调用次数多
TUSHARE_USE_BY_DATE_MODE = False  # 免费用户建议设为 False

# 全量回填限速（易移除标记）
BACKFILL_RATE_LIMIT_ENABLE = True
BACKFILL_RATE_LIMIT_CALLS_PER_MIN = 195

# 板块识别规则（股票代码前缀）
def get_board_type(code: str) -> str:
    """根据股票代码识别板块类型"""
    if code.startswith('60'):
        return 'MAIN'  # 沪市主板
    elif code.startswith('00'):
        return 'MAIN'  # 深市主板
    elif code.startswith('30'):
        return 'GEM'   # 创业板
    elif code.startswith('68'):
        return 'STAR'  # 科创板
    elif code.startswith('92'):
        return 'BJ'    # 北交所
    else:
        return 'MAIN'  # 默认主板

def is_st_stock(name: str) -> bool:
    """判断是否为ST股票"""
    return 'ST' in name or '*ST' in name or 'S' in name[:2]
