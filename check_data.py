# -*- coding: utf-8 -*-
"""数据完整性检查脚本"""
import sqlite3
import os

db_path = os.path.join(os.path.dirname(__file__), 'data', 'stock_limit.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("=" * 70)
print("                    数据完整性检查报告")
print("=" * 70)

# 1. 基本统计
print("\n📊 基本统计")
print("-" * 50)

cursor.execute('SELECT COUNT(*) FROM stock_meta')
stock_count = cursor.fetchone()[0]
print(f"股票总数 (stock_meta): {stock_count}")

cursor.execute('SELECT COUNT(*) FROM daily_market_data')
market_count = cursor.fetchone()[0]
print(f"日线数据总数 (daily_market_data): {market_count}")

cursor.execute('SELECT COUNT(*) FROM limit_analysis_result')
limit_count = cursor.fetchone()[0]
print(f"涨停分析结果数 (limit_analysis_result): {limit_count}")

# 2. 日期范围
print("\n📅 日期范围")
print("-" * 50)

cursor.execute('SELECT MIN(date), MAX(date) FROM daily_market_data')
date_range = cursor.fetchone()
print(f"数据起始日期: {date_range[0]}")
print(f"数据结束日期: {date_range[1]}")

cursor.execute('SELECT COUNT(DISTINCT date) FROM daily_market_data')
trade_days = cursor.fetchone()[0]
print(f"交易日总数: {trade_days} 天")

# 3. 每日数据量分布
print("\n📈 每日数据量分布")
print("-" * 50)

cursor.execute('''
    SELECT date, COUNT(*) as cnt 
    FROM daily_market_data 
    GROUP BY date 
    ORDER BY date
''')
daily_stats = cursor.fetchall()

# 计算统计
counts = [r[1] for r in daily_stats]
avg_count = sum(counts) / len(counts) if counts else 0
min_count = min(counts) if counts else 0
max_count = max(counts) if counts else 0

print(f"每日平均数据量: {avg_count:.0f} 条")
print(f"每日最小数据量: {min_count} 条")
print(f"每日最大数据量: {max_count} 条")

# 显示前5天和后5天
print("\n前5个交易日:")
for row in daily_stats[:5]:
    print(f"  {row[0]}: {row[1]} 条")
    
print("\n后5个交易日:")
for row in daily_stats[-5:]:
    print(f"  {row[0]}: {row[1]} 条")

# 4. 数据覆盖率
print("\n📋 数据覆盖率分析")
print("-" * 50)

theoretical_max = stock_count * trade_days
coverage = (market_count / theoretical_max * 100) if theoretical_max > 0 else 0
print(f"理论最大数据量: {stock_count} × {trade_days} = {theoretical_max} 条")
print(f"实际数据量: {market_count} 条")
print(f"数据覆盖率: {coverage:.1f}%")

# 5. 涨停统计
print("\n🔥 涨停统计")
print("-" * 50)

cursor.execute('SELECT COUNT(*) FROM limit_analysis_result WHERE limit_status = 1')
limit_up_count = cursor.fetchone()[0]
print(f"涨停记录数: {limit_up_count}")

cursor.execute('SELECT MAX(chain_height) FROM limit_analysis_result')
max_chain = cursor.fetchone()[0]
print(f"最高连板数: {max_chain}")

# 连板高度分布
cursor.execute('''
    SELECT chain_height, COUNT(*) as cnt 
    FROM limit_analysis_result 
    WHERE chain_height > 0
    GROUP BY chain_height 
    ORDER BY chain_height
''')
chain_stats = cursor.fetchall()
print("\n连板高度分布:")
for row in chain_stats[:10]:
    print(f"  {row[0]}板: {row[1]} 次")

# 6. 数据异常检查
print("\n⚠️  数据异常检查")
print("-" * 50)

# 检查是否有空值
cursor.execute('SELECT COUNT(*) FROM daily_market_data WHERE close IS NULL OR pre_close IS NULL')
null_count = cursor.fetchone()[0]
print(f"空值记录数: {null_count}")

# 检查重复记录
cursor.execute('''
    SELECT date, code, COUNT(*) as cnt 
    FROM daily_market_data 
    GROUP BY date, code 
    HAVING cnt > 1
''')
duplicates = cursor.fetchall()
print(f"重复记录数: {len(duplicates)}")

if null_count == 0 and len(duplicates) == 0:
    print("\n✅ 数据完整性检查通过！")
else:
    print("\n⚠️  存在数据异常，请检查")

conn.close()
print("\n" + "=" * 70)