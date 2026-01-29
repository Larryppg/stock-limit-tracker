# A股连板高度追踪系统

## 项目概述
追踪A股市场连续涨停板（连板）高度的量化分析系统。

## 核心功能
- 计算每只股票每天的连续涨停板数（连板高度）
- 识别一字板、炸板、涨停类型
- 支持历史数据查询（2020年至今）
- 每日自动增量更新

## 技术栈
- Python 3.9+
- AkShare（数据源）
- SQLite（数据库）
- Pandas（数据处理）
- APScheduler（任务调度）

## 项目结构
```
stock.cursor/
├── config.py              # 配置文件
├── database.py            # 数据库初始化与操作
├── data_fetcher.py        # 数据获取模块
├── limit_calculator.py    # 连板计算核心算法
├── batch_processor.py     # 批量处理模块
├── scheduler.py           # 定时任务
├── query_api.py           # 查询接口
├── main.py                # 主入口
├── requirements.txt       # 依赖列表
└── data/                  # 数据目录
    └── stock_limit.db     # SQLite数据库
```

## 快速开始

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 初始化数据库
```bash
python database.py
```

### 3. 运行MVP（最近3个月数据）
```bash
python main.py --mode mvp
```

### 4. 全量历史回填
```bash
python main.py --mode backfill
```

### 5. 启动每日更新
```bash
python main.py --mode scheduler
```

## 数据库表结构

### stock_meta - 股票元信息
- code: 股票代码
- name: 股票名称
- board_type: 板块类型（MAIN/GEM/STAR/BJ）
- limit_ratio: 涨跌幅限制
- is_st: 是否ST股票

### daily_market_data - 日线行情
- date: 交易日期
- code: 股票代码
- open/high/low/close: OHLC
- pre_close: 前收盘价
- volume/amount: 成交量/成交额

### limit_analysis_result - 涨停分析结果
- date: 交易日期
- code: 股票代码
- limit_status: 涨停状态（0/1）
- chain_height: 连板高度
- is_fried: 是否炸板
- board_type: 板型（yizi/normal/fried）

## 查询示例

### 查询某日所有3板以上的股票
```sql
SELECT code, chain_height 
FROM limit_analysis_result 
WHERE date = '20240101' AND chain_height >= 3
ORDER BY chain_height DESC;
```

### 统计某股票历史最高连板记录
```sql
SELECT MAX(chain_height) as max_chain
FROM limit_analysis_result
WHERE code = '300059';
```

## 开发计划
- [x] Phase 1: 项目骨架与配置
- [x] Phase 2: 核心算法实现
- [x] Phase 3: MVP验证（3个月数据）
- [x] Phase 4: 全量历史回填
- [x] Phase 5: 每日增量更新
- [x] Phase 6: 查询接口/可视化

## 系统架构

```
┌─────────────────────────────────────────────────────────┐
│                   数据获取层                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │ AkShare  │  │ 股票列表  │  │ 日线数据  │              │
│  └──────────┘  └──────────┘  └──────────┘              │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                   核心算法层                              │
│  ┌──────────────────────────────────────────────────┐  │
│  │  涨停判定  │  连板计算  │  板型识别  │  炸板检测  │  │
│  └──────────────────────────────────────────────────┘  │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                   数据存储层                              │
│  ┌──────────────────────────────────────────────────┐  │
│  │     SQLite数据库 (stock_limit.db)                 │  │
│  │  • stock_meta            • limit_analysis_result  │  │
│  │  • daily_market_data     • fetch_progress         │  │
│  └──────────────────────────────────────────────────┘  │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                   应用层                                  │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  │
│  │ 查询API │  │ Web界面 │  │  调度器  │  │  验证器  │  │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘  │
└─────────────────────────────────────────────────────────┘
```

## 注意事项
- 数据源限流：请求间隔0.5秒
- 复权选择：默认不复权（避免历史涨停价失真）
- ST股票：涨跌幅±5%特殊处理
- 北交所：涨跌幅±30%

## License
MIT
